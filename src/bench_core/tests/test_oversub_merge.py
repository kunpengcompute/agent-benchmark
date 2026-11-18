"""Unit + stub-e2e tests for process-sharded oversub trials (oversub_shards).

Mechanism units (planning, per-shard config, merge algebra, group supervision)
are pure/stdlib-only; the e2e test drives ``oversub.main`` with a stub
bench-core (the established ``--bench-core-bin`` pattern) so no kernel, SDK,
or host is needed."""

from __future__ import annotations

import csv

import json

import logging

import os

import shutil

import subprocess

import sys

import time

from pathlib import Path

import pytest

import psutil

import yaml

from bench_core import oversub_shards

from bench_core.oversub import main

from bench_core.oversub_shards import (
    GroupResult,
    ShardArtifacts,
    ShardGroupRunner,
    ShardStressBracket,
    build_shard_command,
    build_shard_config,
    coerce_shards,
    collect_shard_artifacts,
    merge_lifecycle_series,
    merge_run_summaries,
    merge_trajectory_indexes,
    plan_shards,
    resolve_shard_count,
    shard_sizes,
    start_host_monitor,
    write_merged_artifacts,
    write_shard_configs,
)

_SHARD_STUB = """import json, sys, time
from pathlib import Path
import yaml
cfg = yaml.safe_load(open(sys.argv[sys.argv.index("--config") + 1]))
pfx = cfg["report"]["filename_prefix"]
out = Path(cfg["report"]["output_dir"])
out.mkdir(parents=True, exist_ok=True)
(out / "argv.json").write_text(json.dumps(sys.argv[1:]))
# Stand in for the kernel's stress bracket: publish this shard's marker for a
# window that differs per shard, so a union bracket is distinguishable from
# one shard's window. (The driver-owned collector watches exactly this.)
lock = Path((cfg.get("monitor") or {}).get("stress_file", "/dev/shm/stub.lock"))
lock.parent.mkdir(parents=True, exist_ok=True)
idx = int(out.name.rsplit("-", 1)[-1])
time.sleep(0.3 * idx)
began = time.time()
lock.touch()
time.sleep(0.5 + 0.8 * idx)
lock.unlink()
(out / "marker.json").write_text(json.dumps({"start": began, "end": time.time()}))
run_dir = out / f"{pfx}_{time.strftime('%Y%m%d-%H%M%S')}"
(run_dir / "trajectories").mkdir(parents=True, exist_ok=True)
total = cfg["sandbox"]["total_count"]
m = (cfg.get("replay") or {}).get("multiplier") or 1
# Real wall-clock epochs, like the kernel writes: the driver scopes artifact
# selection to the current attempt by started_epoch (a stale-summary guard).
finished = time.time()
summary = {
  "schema_version": 2, "workflow_type": "replay",
  "replay_mode": cfg["replay"]["mode"], "provider": "stub",
  "started_at": "2026-10-30T10:00:00+08:00", "completed_at": "2026-10-30T10:18:00+08:00",
  "started_epoch": began, "completed_epoch": finished,
  "test_duration": cfg["test"]["duration"], "wall_sec": round(finished - began, 3),
  "total_count": total, "running_concurrency": cfg["replay"]["running_concurrency"],
  "overcommit_ratio": 1.0,
  "throughput": {"total": total * m, "succeeded": total * m, "failed": 0,
                 "total_steps": 10 * m, "steps_per_sec": 0.01, "tasks_per_sec": 0.7},
  "admission": None, "lifecycle_overhead": None,
  "paths": {"report": None, "obs_xlsx": None,
            "lifecycle_series": str(run_dir / (pfx + "_lifecycle_series.jsonl")),
            "trajectory_index": None, "vm_monitor_dir": None},
  "error": None,
}
(run_dir / f"{pfx}_run_summary.json").write_text(json.dumps(summary, indent=2) + "\\n")
run = {"sandbox_index": 0, "round_id": 0, "n_steps": 1, "n_failed": 0, "n_timeout": 0,
       "success_rate": 1.0, "elapsed_sec": 1.0,
       "time_breakdown_sec": {"slice_total": 0.8, "exec": 0.5, "create": 0.05, "kill": 0.05,
                              "stress": 0.9},
       "create_error_type": None, "kill_error_type": None}
idx = {"n_trajectories": 1, "trajectories": [
  {"trajectory_id": "t0", "n_runs": 1, "n_success": 1, "n_failed": 0, "elapsed_sec": 1.0,
   "action_series": "echo hi", "file": "t0/replay_result.json", "runs": [run]}]}
(run_dir / "trajectories" / "index.json").write_text(json.dumps(idx, indent=2) + "\\n")
series = [{"event": "step", "sandbox_index": i, "timestamp": 1000.0 + i} for i in range(2)]
(run_dir / f"{pfx}_lifecycle_series.jsonl").write_text("".join(json.dumps(e) + "\\n" for e in series))
"""

_IDLE_VM_MONITOR_STUB = """import os, time
from pathlib import Path
Path(os.environ["VM_STUB_PID_FILE"]).write_text(str(os.getpid()))
time.sleep(120)  # idle: the stress file never appears
"""

_VM_MONITOR_STUB = """import json, sys, time
from pathlib import Path
argv = sys.argv[1:]
lock = Path(argv[argv.index("--stress-file") + 1])
log_dir = Path(argv[argv.index("--log-dir") + 1])
log_dir.mkdir(parents=True, exist_ok=True)
while not lock.exists():
    time.sleep(0.02)
events = [["start", time.time()]]
while lock.exists():
    time.sleep(0.02)
events.append(["end", time.time()])
(log_dir / "stub.json").write_text(json.dumps(events))
(log_dir / "resource_report.xlsx").write_text("stub")
"""


def _trial_cfg() -> dict:
    return {
        "sandbox": {"total_count": 8},
        "replay": {"running_concurrency": 4, "trajectory_limit": 4, "mode": "exec_ephemeral", "multiplier": 2},
        "test": {"round_size": 8, "duration": 60},
        "report": {"output_dir": "trial", "filename_prefix": "rb"},
        "docker": {"image": "img", "container_prefix": "oc-bench"},
    }


def test_collect_warns_when_summary_lacks_trajectory_index(tmp_path, caplog):
    """A shard that produced a summary but lost its index.json must not vanish
    silently from the merged index -- symmetric with the series-missing check."""
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=1, n=4, setting="auto")
    write_shard_configs(_trial_cfg(), plan)
    run_dir = plan.specs[0].directory / "rb_20260101_000000"
    run_dir.mkdir(parents=True)
    (run_dir / "rb_run_summary.json").write_text(json.dumps({"started_epoch": 1, "succeeded": 4}), encoding="utf-8")

    out = collect_shard_artifacts(plan, prefix="rb")
    assert len(out.summaries) == 1 and out.summaries[0]["succeeded"] == 4
    assert out.catalogs == []
    assert any("without trajectories/index.json" in r.getMessage() for r in caplog.records)


def _shard_summary(**over) -> dict:
    s = {
        "schema_version": 2,
        "workflow_type": "replay",
        "replay_mode": "exec_ephemeral",
        "provider": "aenv",
        "started_at": "2026-10-30T10:00:00+08:00",
        "completed_at": "2026-10-30T10:18:00+08:00",
        "started_epoch": 1000.0,
        "completed_epoch": 2080.0,
        "test_duration": 3600,
        "wall_sec": 1080.0,
        "total_count": 4,
        "running_concurrency": 4,
        "overcommit_ratio": 1.0,
        "throughput": {
            "total": 8,
            "succeeded": 8,
            "failed": 0,
            "total_steps": 40,
            "steps_per_sec": 0.037,
            "tasks_per_sec": 0.0074,
        },
        "admission": None,
        "lifecycle_overhead": None,
        "paths": {"trajectory_index": "/x/trajectories/index.json", "vm_monitor_dir": None},
        "error": None,
    }
    s.update(over)
    return s


def _plan2(tmp_path):
    return plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")


def test_merge_run_summaries_counts_sum_rates_recompute_span_envelope(tmp_path):
    plan = _plan2(tmp_path)
    s0 = _shard_summary()
    # s1 starts late AND finishes last: envelope (1000..2085) = 1085 differs
    # from max-of-shard-walls (1080) -- the merged wall must be the envelope,
    # matching the kernel's wall_sec == completed_epoch - started_epoch.
    s1 = _shard_summary(
        started_epoch=1005.0,
        started_at="2026-10-30T10:00:05+08:00",
        completed_epoch=2085.0,
        completed_at="2026-10-30T10:18:05+08:00",
        wall_sec=1075.0,
    )
    artifacts = ShardArtifacts(summaries=[s0, s1], summary_paths=["/a.json", "/b.json"])
    result = GroupResult(returncodes=[0, 0], interrupted=False, timed_out=False)
    merged = merge_run_summaries([s0, s1], plan=plan, artifacts=artifacts, result=result)

    assert merged["throughput"]["total"] == 16
    assert merged["throughput"]["succeeded"] == 16
    assert merged["throughput"]["total_steps"] == 80
    assert merged["wall_sec"] == 1085.0  # envelope: max(completed) - min(started)
    # rates recomputed over the envelope wall, NOT summed per-shard rates
    assert merged["throughput"]["tasks_per_sec"] == round(16 / 1085.0, 3)
    assert merged["throughput"]["steps_per_sec"] == round(80 / 1085.0, 3)
    assert merged["started_epoch"] == 1000.0 and merged["started_at"] == s0["started_at"]
    assert merged["completed_epoch"] == 2085.0 and merged["completed_at"] == s1["completed_at"]
    assert merged["shard_start_skew_sec"] == 5.0
    assert merged["total_count"] == 8  # k*N echo restored
    assert merged["overcommit_ratio"] == 2.0  # 8 slots / N=4
    assert merged["admission"] is None and merged["lifecycle_overhead"] is None
    assert merged["error"] is None
    assert merged["shard_count"] == 2 and merged["shard_return_codes"] == [0, 0]
    assert merged["shard_run_summaries"] == ["/a.json", "/b.json"]


def test_merge_run_summaries_span_falls_back_to_max_wall_without_epochs(tmp_path):
    plan = _plan2(tmp_path)
    s0 = _shard_summary(started_epoch=None, completed_epoch=None, wall_sec=1080.0)
    s1 = _shard_summary(started_epoch=None, completed_epoch=None, wall_sec=900.0)
    artifacts = ShardArtifacts(summaries=[s0, s1], summary_paths=["/a.json", "/b.json"])
    result = GroupResult(returncodes=[0, 0], interrupted=False, timed_out=False)
    merged = merge_run_summaries([s0, s1], plan=plan, artifacts=artifacts, result=result)
    assert merged["wall_sec"] == 1080.0  # partially-flushed shard: max wall fallback
    assert merged["started_epoch"] is None and merged["completed_epoch"] is None
    assert merged["throughput"]["tasks_per_sec"] == round(16 / 1080.0, 3)


def test_merge_run_summaries_missing_and_failed_shards_surface_in_error(tmp_path):
    plan = _plan2(tmp_path)
    s0 = _shard_summary(error="create pool exhausted")
    artifacts = ShardArtifacts(summaries=[s0, None], summary_paths=["/a.json", None])
    result = GroupResult(returncodes=[0, 1], interrupted=False, timed_out=False)
    merged = merge_run_summaries([s0, None], plan=plan, artifacts=artifacts, result=result)
    assert merged["error"]
    assert "shard 1: no run_summary.json" in merged["error"]
    assert "shard 1: exit code 1" in merged["error"]
    assert "create pool exhausted" in merged["error"]
    assert merged["throughput"]["succeeded"] == 8  # present shard only; never backfilled
    assert result.trial_return_code() == 1


def test_merge_run_summaries_nothing_present_yields_minimal_invalid_shape(tmp_path):
    plan = _plan2(tmp_path)
    artifacts = ShardArtifacts(summaries=[None, None], summary_paths=[None, None])
    result = GroupResult(returncodes=[-9, -9], interrupted=False, timed_out=False)
    merged = merge_run_summaries([None, None], plan=plan, artifacts=artifacts, result=result)
    assert merged["throughput"] == {} and merged["error"]


def test_group_result_interrupt_maps_to_130():
    assert GroupResult([0, 0], interrupted=True, timed_out=False).trial_return_code() == 130
    assert GroupResult([0, 0], interrupted=False, timed_out=False).trial_return_code() == 0


def _catalog(
    sandbox_index: int,
    *,
    runs_shape: bool = True,
    run_over: dict | None = None,
    entry_over: dict | None = None,
) -> dict:
    run = {
        "sandbox_index": sandbox_index,
        "round_id": 0,
        "n_steps": 2,
        "n_failed": 0,
        "n_timeout": 0,
        "success_rate": 1.0,
        "elapsed_sec": 3.0,
        "time_breakdown_sec": {"exec": 1.0},
        "create_error_type": None,
        "kill_error_type": None,
    }
    run.update(run_over or {})
    entry = {"trajectory_id": "t0", "action_series": "echo hi", "file": "t0/replay_result.json"}
    if runs_shape:
        entry.update(
            {
                "n_runs": 1,
                "n_success": 1,
                "n_failed": 0,
                "success_rate": run["success_rate"],
                "elapsed_sec": run["elapsed_sec"],
                "all_failed": False,
                "n_steps_max": run["n_steps"],
                "time_breakdown_sec": dict(run["time_breakdown_sec"]),
                "create_error_type": run["create_error_type"],
                "kill_error_type": run["kill_error_type"],
                "runs": [run],
            }
        )
    else:
        entry.update(run)  # legacy flat shape: the entry IS the run
    entry.update(entry_over or {})
    return {"n_trajectories": 1, "trajectories": [entry]}


def test_merge_trajectory_indexes_groups_by_tid_and_offsets_sandbox_index():
    merged = merge_trajectory_indexes([(0, _catalog(0)), (4, _catalog(0))])
    assert merged["n_trajectories"] == 1
    entry = merged["trajectories"][0]
    assert entry["trajectory_id"] == "t0"
    assert entry["n_runs"] == 2 and entry["n_success"] == 2 and entry["n_failed"] == 0
    assert [r["sandbox_index"] for r in entry["runs"]] == [0, 4]  # run keys stay unique
    assert entry["action_series"] == "echo hi"  # scalar inherited once
    assert entry.get("sandbox_index") is None  # run-scoped keys never leak to entry scalars
    assert entry["file"] is None  # deep replay_result.json stays per-shard; no dead link


def test_merge_trajectory_indexes_recomputes_entry_aggregates_with_kernel_rules():
    # shard 0: t0 succeeds (elapsed 10.0). shard 1: t0 fails at create.
    # The merged entry must describe the FULL run set, not shard 0's subset.
    ok = _catalog(0, run_over={"elapsed_sec": 10.0})
    bad = _catalog(
        0,
        run_over={
            "success_rate": 0.0,
            "n_steps": 0,
            "elapsed_sec": None,
            "time_breakdown_sec": {"create": 30.0},
            "create_error_type": "timeout",
        },
        entry_over={
            "success_rate": 0.0,
            "n_success": 0,
            "n_failed": 1,
            "all_failed": True,
            "elapsed_sec": None,
            "create_error_type": "timeout",
            "time_breakdown_sec": {"create": 30.0},
        },
    )
    merged = merge_trajectory_indexes([(0, ok), (4, bad)])
    e = merged["trajectories"][0]
    assert e["n_runs"] == 2 and e["n_success"] == 1 and e["n_failed"] == 1
    assert e["all_failed"] is False
    assert e["success_rate"] == 0.5  # mean of per-run rates, NOT shard 0's inherited 1.0
    assert e["elapsed_sec"] == 10.0  # success-only p50 (the failing run's None is excluded)
    assert e["create_error_type"] == "timeout"  # first-seen across ALL shards' runs
    assert e["n_steps_max"] == 2
    # breakdown p50 over the success use-set only (kernel rule)
    assert e["time_breakdown_sec"] == {"exec": 1.0}
    assert [r["sandbox_index"] for r in e["runs"]] == [0, 4]  # sorted by (sandbox_index, round_id)


def test_merge_trajectory_indexes_all_failed_falls_back_to_all_runs():
    f1 = _catalog(
        0,
        run_over={"success_rate": 0.0, "elapsed_sec": 4.0},
        entry_over={"success_rate": 0.0, "n_success": 0, "n_failed": 1, "all_failed": True, "elapsed_sec": 4.0},
    )
    f2 = _catalog(
        0,
        run_over={"success_rate": 0.0, "elapsed_sec": 8.0},
        entry_over={"success_rate": 0.0, "n_success": 0, "n_failed": 1, "all_failed": True, "elapsed_sec": 8.0},
    )
    merged = merge_trajectory_indexes([(0, f1), (4, f2)])
    e = merged["trajectories"][0]
    assert e["all_failed"] is True and e["n_success"] == 0 and e["n_failed"] == 2
    assert e["success_rate"] == 0.0
    assert e["elapsed_sec"] == 8.0  # all-runs fallback, kernel upper-median p50 (NOT the 6.0 mean)


def test_merge_trajectory_indexes_elapsed_p50_is_upper_median_like_kernel():
    # kernel calc_percentiles p50 = sorted[n//2]: for 2 runs (10.0, 12.0) it is
    # 12.0, not statistics.median's 11.0 -- k=2 is the common multiplier case.
    a = _catalog(0, run_over={"elapsed_sec": 10.0})
    b = _catalog(0, run_over={"elapsed_sec": 12.0})
    merged = merge_trajectory_indexes([(0, a), (4, b)])
    assert merged["trajectories"][0]["elapsed_sec"] == 12.0


def test_merge_trajectory_indexes_promotes_legacy_flat_entries():
    merged = merge_trajectory_indexes([(0, _catalog(1, runs_shape=False))])
    entry = merged["trajectories"][0]
    assert entry["n_runs"] == 1
    assert entry["runs"][0]["sandbox_index"] == 1
    assert entry["runs"][0]["n_steps"] == 2


def test_write_merged_artifacts_fixed_path_and_index_patching(tmp_path):
    summary = {"throughput": {"total": 1}, "paths": {"trajectory_index": None}}
    index = {"n_trajectories": 1, "trajectories": []}
    path = write_merged_artifacts(tmp_path, "rb", summary, index)
    assert path == tmp_path / "merged" / "rb_run_summary.json"
    assert json.loads(path.read_text(encoding="utf-8"))["paths"]["trajectory_index"].endswith(
        str(Path("merged") / "trajectories" / "index.json")
    )
    assert (tmp_path / "merged" / "trajectories" / "index.json").exists()


def _series(path: Path, n: int) -> Path:
    path.write_text(
        "".join(json.dumps({"event": "step", "sandbox_index": i, "timestamp": 1000.0 + i}) + "\n" for i in range(n)),
        encoding="utf-8",
    )
    return path


def test_merge_lifecycle_series_concats_and_rekeys_all_shards(tmp_path, caplog):
    # Fleet-wide inflight analysis needs every shard's events with re-keyed
    # sandbox_index; shard-local keys would conflate sandboxes across shards
    # (and reading one shard caps the observed peak at N -- the T2 misread).
    plan = _plan2(tmp_path)  # sizes [4, 4] -> offsets [0, 4]
    s0 = {"paths": {"lifecycle_series": str(_series(tmp_path / "a.jsonl", 2))}}
    s1 = {"paths": {"lifecycle_series": str(_series(tmp_path / "b.jsonl", 3))}}
    gone = {"paths": {"lifecycle_series": str(tmp_path / "gone.jsonl")}}  # missing file skipped
    out = merge_lifecycle_series([s0, s1, gone, None, {"paths": {}}], plan, tmp_path / "merged", "rb")
    assert out == tmp_path / "merged" / "rb_lifecycle_series.jsonl"
    events = [json.loads(x) for x in out.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(events) == 5
    assert [e["sandbox_index"] for e in events[:2]] == [0, 1]  # shard 0: offset 0
    assert [e["sandbox_index"] for e in events[2:]] == [4, 5, 6]  # shard 1: offset 4
    # A shard that reported a series it no longer has must surface: the merged
    # series then under-counts against the merged summary, and silence would
    # hide exactly that.
    partial = merge_lifecycle_series([s0, gone], plan, tmp_path / "m3", "rb")
    assert len([x for x in partial.read_text(encoding="utf-8").splitlines() if x.strip()]) == 2
    assert any("reported lifecycle series" in r.getMessage() for r in caplog.records)
    assert merge_lifecycle_series([None, {"paths": {}}], plan, tmp_path / "m2", "rb") is None


def _write_shard_summary(directory: Path, stamp: str, started_epoch: float) -> Path:
    run_dir = directory / f"rb_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "rb_run_summary.json"
    path.write_text(json.dumps(_shard_summary(started_epoch=started_epoch)), encoding="utf-8")
    return path


def test_collect_shard_artifacts_scopes_to_current_attempt(tmp_path):
    # Shard dirs keep one run dir per attempt. A shard that crashes before
    # flushing this attempt must read as missing -- not as the previous
    # attempt's summary, whose counts would silently join the merge.
    plan = _plan2(tmp_path)
    s0, s1 = plan.specs
    _write_shard_summary(s0.directory, "20260101-000000", 100.0)  # previous attempt
    current = _write_shard_summary(s0.directory, "20260102-000000", 2000.0)
    _write_shard_summary(s1.directory, "20260101-000000", 100.0)  # stale only

    scoped = collect_shard_artifacts(plan, "rb", since_epoch=1500.0)
    assert scoped.summary_paths[0] == str(current)
    assert scoped.summary_paths[1] is None and scoped.summaries[1] is None
    assert scoped.missing == [1]

    # Unscoped (pre-restart behavior): newest-by-name wins for both.
    unscoped = collect_shard_artifacts(plan, "rb")
    assert unscoped.summary_paths[0] == str(current)
    assert unscoped.summary_paths[1] is not None


def test_shard_group_runner_collects_all_return_codes(tmp_path):
    cmds = [
        [sys.executable, "-c", "import sys; sys.exit(0)"],
        [sys.executable, "-c", "import sys; sys.exit(3)"],
    ]
    result = ShardGroupRunner(cmds, [tmp_path / "a.log", tmp_path / "b.log"], 60).run()
    assert result.returncodes == [0, 3]
    assert not result.interrupted and not result.timed_out
    assert result.trial_return_code() == 3


def test_shard_group_runner_deadline_terminates_whole_group(tmp_path):
    # One fast shard, one sleeper: the shared deadline must tear down BOTH
    # (an orphaned shard would keep stressing the host).
    cmds = [
        [sys.executable, "-c", "pass"],
        [sys.executable, "-c", "import time; time.sleep(60)"],
    ]
    started = time.monotonic()
    result = ShardGroupRunner(cmds, [tmp_path / "a.log", tmp_path / "b.log"], 2).run()
    assert time.monotonic() - started < 40  # killed, not waited out
    assert result.timed_out
    assert all(rc != 0 for rc in result.returncodes[1:])


def test_shard_group_runner_launch_failure_terminates_started_shards(tmp_path, monkeypatch):
    # A Popen failure mid-launch (fd exhaustion, missing interpreter) must tear
    # down the shards already stressing the host, not orphan them.
    real_popen = subprocess.Popen
    started = []

    def fake_popen(cmd, **kwargs):
        if len(started) == 1:
            raise OSError("simulated launch failure")
        proc = real_popen(cmd, **kwargs)
        started.append(proc)
        return proc

    monkeypatch.setattr(oversub_shards.subprocess, "Popen", fake_popen)
    cmds = [
        [sys.executable, "-c", "import time; time.sleep(60)"],
        [sys.executable, "-c", "pass"],
    ]
    with pytest.raises(OSError, match="simulated launch failure"):
        ShardGroupRunner(cmds, [tmp_path / "a.log", tmp_path / "b.log"], 60).run()
    assert len(started) == 1
    assert started[0].poll() is not None  # terminated, not orphaned
