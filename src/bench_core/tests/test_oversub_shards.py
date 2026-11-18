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


def test_coerce_shards_forms():
    assert coerce_shards("auto") == "auto"
    assert coerce_shards("AUTO") == "auto"
    assert coerce_shards("4") == 4
    assert coerce_shards(4) == 4
    with pytest.raises(ValueError):
        coerce_shards(0)
    with pytest.raises(ValueError):
        coerce_shards("lots")
    with pytest.raises(ValueError):
        coerce_shards(True)


def test_resolve_shard_count_auto_follows_ratio_for_shardable_modes():
    assert resolve_shard_count("auto", mode="exec_ephemeral", ratio=10, n=384) == 10
    assert resolve_shard_count("auto", mode="exec_only", ratio=3, n=4) == 3


def test_resolve_shard_count_full_scale_lifecycle_never_shards():
    # lifecycle runs its full k*N fleet (pause/resume) in one process: never
    # sharded, at any P or ratio.
    assert resolve_shard_count("auto", mode="lifecycle", ratio=8, n=384) == 1
    assert resolve_shard_count(4, mode="lifecycle", ratio=8, n=384) == 1  # explicit downgraded


def test_resolve_shard_count_explicit_and_capped():
    assert resolve_shard_count(2, mode="exec_ephemeral", ratio=4, n=8) == 2
    assert resolve_shard_count(1, mode="exec_ephemeral", ratio=4, n=8) == 1
    # capped at the fleet size: every shard needs >= 1 slot
    assert resolve_shard_count(100, mode="exec_ephemeral", ratio=2, n=4) == 8


def test_shard_sizes_balanced_remainder_first():
    assert shard_sizes(8, 2) == [4, 4]
    assert shard_sizes(10, 3) == [4, 3, 3]
    assert shard_sizes(7, 7) == [1] * 7


def test_plan_shards_lifecycle_never_plans(tmp_path, caplog):
    # lifecycle's full-scale pause/resume fleet stays in one process: no plan,
    # the legacy single-process path, at any P. The autodetected default is silent,
    # but an explicit P > 1 is a documented downgrade and is surfaced with a
    # WARNING (we validate, not silently rewrite, the operator's request).
    assert plan_shards(trial_dir=tmp_path, mode="lifecycle", ratio=4, n=8, setting="auto") is None
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)
    caplog.clear()
    plan_shards(trial_dir=tmp_path, mode="lifecycle", ratio=4, n=8, setting=2)
    plan_shards(trial_dir=tmp_path, mode="lifecycle", ratio=4, n=8, setting=4)
    assert (
        sum(
            1
            for r in caplog.records
            if r.levelno >= logging.WARNING
            and "cannot be sharded" in r.getMessage()
            and "running single-process" in r.getMessage()
        )
        == 2
    )


def test_plan_shards_single_shard_for_ratio_one_and_offsets_cumulative(tmp_path):
    # P == 1 still builds a plan for shardable modes: ratio-1 (auto -> P=1) and
    # an explicit --shards 1 both run one shard-00 through the sharded
    # machinery, so ratio-1 trials share the shard-00//merged//driver-owned
    # vm_monitor layout with P >= 2 trials (one structure across every ratio).
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=1, n=8, setting="auto")
    assert plan is not None and plan.count == 1
    assert [s.size for s in plan.specs] == [8]
    assert [s.slot_offset for s in plan.specs] == [0]
    assert plan.specs[0].directory == tmp_path / "shard-00"
    assert plan.specs[0].log_path == tmp_path / "shard-00" / "driver.log"

    single = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting=1)
    assert single is not None and single.count == 1
    assert [s.size for s in single.specs] == [8]

    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=5, setting="auto")
    assert plan.count == 2
    assert [s.size for s in plan.specs] == [5, 5]
    assert [s.slot_offset for s in plan.specs] == [0, 5]
    assert plan.specs[0].directory == tmp_path / "shard-00"
    assert plan.specs[1].log_path == tmp_path / "shard-01" / "driver.log"


def test_plan_shards_group_aligned_for_p_le_ratio(tmp_path):
    # ratio=3, P=2, N=4 -> groups [2,1] -> sizes [8,4]: every shard size is a
    # multiple of N, so slot k -> pool[k % N] gives exact per-trajectory parity
    # with a single-process trial (same run counts per trajectory, not just totals).
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=3, n=4, setting=2)
    assert [s.size for s in plan.specs] == [8, 4]
    assert [s.slot_offset for s in plan.specs] == [0, 8]


def test_plan_shards_slot_level_with_warning_for_p_gt_ratio(tmp_path, caplog):
    # P > ratio: shards fall below the pool -> slot-level split, totals
    # preserved but per-trajectory counts redistribute; must warn.
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting=4)
    assert [s.size for s in plan.specs] == [2, 2, 2, 2]
    assert [s.slot_offset for s in plan.specs] == [0, 2, 4, 6]
    assert any("redistribute" in r.getMessage() for r in caplog.records)


def _trial_cfg() -> dict:
    return {
        "sandbox": {"total_count": 8},
        "replay": {"running_concurrency": 4, "trajectory_limit": 4, "mode": "exec_ephemeral", "multiplier": 2},
        "test": {"round_size": 8, "duration": 60},
        "report": {"output_dir": "trial", "filename_prefix": "rb"},
        "docker": {"image": "img", "container_prefix": "oc-bench"},
    }


def test_build_shard_config_derives_self_contained_shard(tmp_path):
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")
    cfg = build_shard_config(_trial_cfg(), plan.specs[1], count=plan.count)
    assert cfg["sandbox"]["total_count"] == 4  # shard share, not k*N
    assert cfg["test"]["round_size"] == 4
    assert cfg["report"]["output_dir"] == str(tmp_path / "shard-01")
    # global-by-design knobs stay untouched
    assert cfg["replay"]["trajectory_limit"] == 4
    assert cfg["replay"]["running_concurrency"] == 4
    assert cfg["replay"]["multiplier"] == 2
    # docker names get a shard suffix (keeps startswith(base) for cleanup reach)
    assert cfg["docker"]["container_prefix"] == "oc-bench-s1"
    assert cfg["docker"]["container_prefix"].startswith("oc-bench")


def test_build_shard_config_hoists_shard0_monitor_dir_to_trial_root(tmp_path):
    """Host metrics are fleet-wide: one fixed path, whether or not we sharded."""
    cfg_in = {**_trial_cfg(), "monitor": {"interval": 2, "log_dir": None}}
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")

    shard0 = build_shard_config(cfg_in, plan.specs[0], count=plan.count)
    assert shard0["monitor"]["log_dir"] == str(tmp_path / "vm_monitor")
    assert shard0["monitor"]["interval"] == 2  # other monitor keys untouched

    # Non-owning shards keep the default (they run --no-vm-monitor anyway).
    assert build_shard_config(cfg_in, plan.specs[1], count=plan.count)["monitor"]["log_dir"] is None

    # An explicit log_dir is the operator's call -- never overridden.
    cfg_explicit = {**_trial_cfg(), "monitor": {"log_dir": "/mnt/fast/vm"}}
    assert build_shard_config(cfg_explicit, plan.specs[0], count=plan.count)["monitor"]["log_dir"] == "/mnt/fast/vm"


def test_build_shard_config_gives_every_shard_a_marker_and_no_collector(tmp_path):
    """The driver owns the host collector: shards only publish their window."""
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")
    shard0 = build_shard_config(_trial_cfg(), plan.specs[0], count=plan.count)["monitor"]
    shard1 = build_shard_config(_trial_cfg(), plan.specs[1], count=plan.count)["monitor"]

    assert shard0["stress_file"] == str(tmp_path / "shard-00" / "stress.lock")
    assert shard1["stress_file"] == str(tmp_path / "shard-01" / "stress.lock")
    assert shard0["external_collector"] is True and shard1["external_collector"] is True
    # One fleet gauge per trial; shards > 0 never poll the provider API (P
    # simultaneous samplers would multiply the control-plane list-sandbox load).
    assert shard0["api_state"] == "true"
    assert shard1["api_state"] == "false"


def test_build_shard_config_keeps_one_fleet_gauge_and_clamps_concurrency(tmp_path):
    """Shard 0 samples the fleet gauge unless explicitly disabled; other shards
    never poll the provider API. On the P > ratio slot split a shard's
    running_concurrency clamps to its own total_count so the kernel's config
    validate does not reject the shard at startup."""
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting=4)  # P = 4 > ratio
    assert [s.size for s in plan.specs] == [2, 2, 2, 2]
    base = _trial_cfg()
    base["replay"]["running_concurrency"] = 8

    m0 = build_shard_config(base, plan.specs[0], count=plan.count)["monitor"]
    assert m0["api_state"] == "true"
    # An explicit "auto" (the schema default) must not silently drop the gauge.
    auto = {**_trial_cfg(), "monitor": {"api_state": "auto"}}
    assert build_shard_config(auto, plan.specs[0], count=plan.count)["monitor"]["api_state"] == "true"
    # An explicit "false" is an operator opt-out.
    off = {**_trial_cfg(), "monitor": {"api_state": "false"}}
    assert build_shard_config(off, plan.specs[0], count=plan.count)["monitor"]["api_state"] == "false"

    for spec in plan.specs[1:]:
        assert build_shard_config(base, spec, count=plan.count)["monitor"]["api_state"] == "false"
        cfg = build_shard_config(base, spec, count=plan.count)
        assert cfg["replay"]["running_concurrency"] == spec.size


def test_build_shard_config_leaves_monitor_alone_when_driver_does_not_own_it(tmp_path):
    """--no-vm-monitor on the driver: no markers, no external-collector handoff."""
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")
    monitor = build_shard_config(_trial_cfg(), plan.specs[0], count=plan.count, owns_collector=False)["monitor"]
    assert "external_collector" not in monitor
    assert "api_state" not in monitor
    assert monitor["log_dir"] == str(tmp_path / "vm_monitor")  # still the fixed path

    off = {**_trial_cfg(), "monitor": {"enabled": "false"}}
    assert "external_collector" not in build_shard_config(off, plan.specs[0], count=plan.count)["monitor"]


def test_build_shard_config_scales_launch_interval_to_preserve_global_rate(tmp_path):
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=3, n=4, setting="auto")
    cfg_in = _trial_cfg()
    cfg_in["replay"]["launch_interval_sec"] = 0.5
    cfg = build_shard_config(cfg_in, plan.specs[0], count=plan.count)
    assert cfg["replay"]["launch_interval_sec"] == pytest.approx(1.5)  # P * interval


def test_build_shard_config_scales_batch_intervals_to_preserve_global_rate(tmp_path):
    # P independently-pacing shards would otherwise create/dispatch at P x the
    # global rate: create_batch/task_batch intervals scale by P too.
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")
    cfg_in = _trial_cfg()
    cfg_in["create_batch"] = {"interval": 1}
    cfg_in["task_batch"] = {"interval": 2}
    cfg = build_shard_config(cfg_in, plan.specs[0], count=plan.count)
    assert cfg["create_batch"]["interval"] == 2
    assert cfg["task_batch"]["interval"] == 4


def test_write_shard_configs_materializes_one_yaml_per_shard(tmp_path):
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")
    paths = write_shard_configs(_trial_cfg(), plan)
    assert paths == [tmp_path / "shard-00" / "shard.yaml", tmp_path / "shard-01" / "shard.yaml"]
    for p in paths:
        loaded = yaml.safe_load(p.read_text(encoding="utf-8"))
        assert loaded["sandbox"]["total_count"] == 4


def test_write_shard_configs_removes_a_stale_stress_marker(tmp_path):
    """A prior attempt killed hard leaves shard-XX/stress.lock behind (a fresh
    kernel clears it only at monitor start, after create + warmup). The driver's
    bracket polls marker EXISTENCE, so the next attempt must start clean or it
    opens the host window on its first poll, over create/warmup."""
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")
    for spec in plan.specs:
        spec.directory.mkdir(parents=True)
        spec.stress_path.write_text("")
    write_shard_configs(_trial_cfg(), plan)
    assert all(not spec.stress_path.exists() for spec in plan.specs)
