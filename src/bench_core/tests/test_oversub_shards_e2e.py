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


def _base_yaml(tmp_path) -> Path:
    base = tmp_path / "base.yaml"
    base.write_text(
        yaml.safe_dump(
            {
                "workflow_type": "replay",
                "sandbox": {"total_count": 4},
                "replay": {"running_concurrency": 4, "mode": "exec_ephemeral"},
                "test": {"duration": 60, "round_size": 4, "round_count": 1, "benchmark_mode": "fixed"},
                "report": {"output_dir": "results/replay", "filename_prefix": "rb", "format": "both"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return base


def _common_argv(tmp_path, base: Path, out_root: Path, stub: Path) -> list[str]:
    return [
        "--config",
        str(base),
        "--provider",
        "aenv",
        "--output-root",
        str(out_root),
        "--bench-core-bin",
        sys.executable,
        str(stub),
        "--cooldown-sec",
        "0",
        "--cleanup-between-trials",
        "off",
        # Shard layout/merge cases are about the non-multiplier model, while the
        # driver's built-in M default is 2; the M cases pass --multiplier
        # explicitly and win as the later arg.
        "--multiplier",
        "0",
    ]


def test_e2e_sharded_trial_merges_into_contract_artifacts(tmp_path):
    """ratio 2 x M 2 -> P=2 shards x 4 slots; merged summary passes M*k*N validity."""
    stub = tmp_path / "stub.py"
    stub.write_text(_SHARD_STUB, encoding="utf-8")
    base = _base_yaml(tmp_path)
    out_root = tmp_path / "sweep"
    rc = main(
        _common_argv(tmp_path, base, out_root, stub)
        + [
            "--ratios",
            "2",
            "--modes",
            "exec_ephemeral",
            "--multiplier",
            "2",
            "--shards",
            "auto",
            "--test-duration",
            "60",
        ]
    )
    assert rc == 0
    trial_dir = out_root / "exec_ephemeral" / "ratio-02" / "repeat-00"

    # Per-shard layout + vm_monitor ownership (shard 0 monitors, shard 1 doesn't).
    argv0 = json.loads((trial_dir / "shard-00" / "argv.json").read_text(encoding="utf-8"))
    argv1 = json.loads((trial_dir / "shard-01" / "argv.json").read_text(encoding="utf-8"))
    assert "--no-vm-monitor" not in argv0
    assert "--no-vm-monitor" in argv1

    # Merged summary: the schema-2 contract, with shard provenance attached.
    merged_path = trial_dir / "merged" / "rb_run_summary.json"
    merged = json.loads(merged_path.read_text(encoding="utf-8"))
    assert merged["shard_count"] == 2
    assert merged["throughput"]["succeeded"] == 16  # M*k*N = 2*2*4
    assert merged["throughput"]["total"] == 16
    assert merged["total_count"] == 8  # k*N echo

    # Fleet-wide merged series: both shards' events, sandbox_index re-keyed by
    # slot offset (shard 1's local 0/1 -> 4/5); inflight analysis must not be
    # capped at one shard's N slots nor conflate cross-shard sandboxes.
    series = trial_dir / "merged" / "rb_lifecycle_series.jsonl"
    events = [json.loads(x) for x in series.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(events) == 4  # 2 shards x 2 events
    assert sorted(e["sandbox_index"] for e in events) == [0, 1, 4, 5]
    assert merged["paths"]["lifecycle_series"] == str(series)

    # Driver outputs see ONE trial row, valid, keyed to the merged artifacts.
    with open(out_root / "trial-summary.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["valid"] == "True" and rows[0]["succeeded"] == "16" and rows[0]["target_count"] == "8"
    assert rows[0]["run_summary_path"].endswith(str(merged_path))

    # trajectory-detail: per-run rows with re-keyed sandbox_index (0 and 4).
    with open(out_root / "trajectory-detail.csv", newline="", encoding="utf-8") as f:
        trj = list(csv.DictReader(f))
    assert len(trj) == 2
    assert sorted(int(r["sandbox_index"]) for r in trj) == [0, 4]
    assert all(r["trajectory_id"] == "t0" and r["exec_sec"] == "0.5" for r in trj)


def test_e2e_ratio_one_trial_shares_the_sharded_layout(tmp_path):
    """ratio 1 (auto -> P=1) runs through the sharded machinery: one shard.

    The whole point of P=1 sharding: an agent comparing oversub ratios reads
    ONE structure at every ratio (trial.yaml + shard-00/ + merged/) instead of
    a special flat layout at ratio 1 -- and the host collector is driver-owned
    at every ratio, so a short ratio-1 window can never strand it idle.
    """
    stub = tmp_path / "stub.py"
    stub.write_text(_SHARD_STUB, encoding="utf-8")
    base = _base_yaml(tmp_path)
    out_root = tmp_path / "sweep"
    rc = main(
        _common_argv(tmp_path, base, out_root, stub)
        + ["--ratios", "1", "--modes", "exec_ephemeral", "--shards", "auto", "--test-duration", "60"]
    )
    assert rc == 0
    trial_dir = out_root / "exec_ephemeral" / "ratio-01" / "repeat-00"

    assert not (trial_dir / "replay_bench").exists()  # no legacy flat run dir
    assert (trial_dir / "shard-00" / "shard.yaml").exists()
    assert (trial_dir / "shard-00" / "driver.log").exists()
    assert not (trial_dir / "shard-01").exists()

    merged = json.loads((trial_dir / "merged" / "rb_run_summary.json").read_text(encoding="utf-8"))
    assert merged["shard_count"] == 1
    assert merged["throughput"]["succeeded"] == 4  # k*N = 1*4
    assert merged["total_count"] == 4  # k*N echo
    assert sorted(trial_dir.glob("merged/*"))  # merged artifacts exist

    with open(out_root / "trial-summary.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1 and rows[0]["valid"] == "True"
    assert rows[0]["run_summary_path"].endswith(str(trial_dir / "merged" / "rb_run_summary.json"))


def test_e2e_ratio_one_driver_owns_the_host_collector(tmp_path, monkeypatch):
    """P=1 still hands the host collector to the driver, over the one shard's window.

    This is the case the original report was about (ratio-1 host report missing):
    a kernel-owned collector could miss a short window and idle forever. With the
    driver owning it, the output lands at <trial_dir>/vm_monitor and is bracketed
    by the shard's marker -- the same contract as P >= 2.
    """
    stub = tmp_path / "stub.py"
    stub.write_text(_SHARD_STUB, encoding="utf-8")
    vm_stub = tmp_path / "vm-monitor-stub.py"
    vm_stub.write_text(_VM_MONITOR_STUB, encoding="utf-8")

    real_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda n: str(vm_stub) if n == "vm-monitor" else real_which(n))
    real_popen = subprocess.Popen

    def _popen(cmd, **kw):  # the stub is a .py script, not an executable
        return real_popen([sys.executable, *cmd] if str(cmd[0]).endswith(".py") else cmd, **kw)

    monkeypatch.setattr(subprocess, "Popen", _popen)

    base = _base_yaml(tmp_path)
    out_root = tmp_path / "sweep"
    rc = main(
        _common_argv(tmp_path, base, out_root, stub)
        + ["--ratios", "1", "--modes", "exec_ephemeral", "--shards", "auto", "--test-duration", "60"]
    )
    assert rc == 0

    trial_dir = out_root / "exec_ephemeral" / "ratio-01" / "repeat-00"
    events = json.loads((trial_dir / "vm_monitor" / "stub.json").read_text(encoding="utf-8"))
    assert [e[0] for e in events] == ["start", "end"]  # one bracket over the single shard

    mark = json.loads((trial_dir / "shard-00" / "marker.json").read_text(encoding="utf-8"))
    began, ended = events[0][1], events[1][1]
    assert began <= mark["start"] + 1.0  # opened on the shard's window
    assert ended >= mark["end"] - 0.1  # closed only after it ended
    assert not (trial_dir / "shard-00" / "vm_monitor").exists()  # never under the shard


def test_e2e_explicit_shards_override_and_cap(tmp_path):
    """--shards 4 on ratio 2 (8 slots) -> 4 shards x 2 slots."""
    stub = tmp_path / "stub.py"
    stub.write_text(_SHARD_STUB, encoding="utf-8")
    base = _base_yaml(tmp_path)
    out_root = tmp_path / "sweep"
    rc = main(
        _common_argv(tmp_path, base, out_root, stub)
        + ["--ratios", "2", "--modes", "exec_ephemeral", "--shards", "4", "--test-duration", "60"]
    )
    assert rc == 0
    trial_dir = out_root / "exec_ephemeral" / "ratio-02" / "repeat-00"
    shard_cfgs = sorted(trial_dir.glob("shard-*/shard.yaml"))
    assert len(shard_cfgs) == 4
    for p in shard_cfgs:
        assert yaml.safe_load(p.read_text(encoding="utf-8"))["sandbox"]["total_count"] == 2
    with open(out_root / "trial-summary.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # non-multiplier validity: merged total = 8 attempts >= 0.9 * N(=4)
    assert rows[0]["valid"] == "True" and rows[0]["total"] == "8"


def test_e2e_crashed_shard_invalidates_trial_without_backfill(tmp_path):
    """A shard that exits nonzero with no summary -> trial invalid, error recorded."""
    stub = tmp_path / "stub.py"
    # Shard 1 crashes before writing anything (rc from env), shard 0 succeeds.
    stub.write_text(
        "import os, sys\n"
        "if os.environ.get('SHARD_CRASH') and 'shard-01' in sys.argv[sys.argv.index('--config') + 1]:\n"
        "    sys.exit(2)\n" + _SHARD_STUB,
        encoding="utf-8",
    )
    base = _base_yaml(tmp_path)
    out_root = tmp_path / "sweep"
    old = os.environ.get("SHARD_CRASH")
    os.environ["SHARD_CRASH"] = "1"
    try:
        rc = main(
            _common_argv(tmp_path, base, out_root, stub)
            + ["--ratios", "2", "--modes", "exec_ephemeral", "--shards", "auto", "--test-duration", "60"]
        )
    finally:
        if old is None:
            del os.environ["SHARD_CRASH"]
        else:
            os.environ["SHARD_CRASH"] = old
    assert rc == 0  # the sweep continues; the TRIAL is what fails
    with open(out_root / "trial-summary.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["valid"] == "False"
    assert rows[0]["return_code"] == "2"
    assert "shard 1" in rows[0]["error"]
    # the surviving shard's 4 attempts are merged as-is -- never padded to 8
    assert rows[0]["succeeded"] == "4"


def test_e2e_dry_run_writes_shard_yamls(tmp_path):
    stub = tmp_path / "stub.py"
    stub.write_text(_SHARD_STUB, encoding="utf-8")
    base = _base_yaml(tmp_path)
    out_root = tmp_path / "sweep"
    rc = main(
        _common_argv(tmp_path, base, out_root, stub)
        + ["--ratios", "3", "--modes", "exec_ephemeral", "--shards", "auto", "--dry-run", "--test-duration", "60"]
    )
    assert rc == 0
    trial_dir = out_root / "exec_ephemeral" / "ratio-03" / "repeat-00"
    assert (trial_dir / "trial.yaml").exists()  # logical provenance kept
    yamls = sorted(trial_dir.glob("shard-*/shard.yaml"))
    assert len(yamls) == 3
    for p in yamls:
        assert yaml.safe_load(p.read_text(encoding="utf-8"))["sandbox"]["total_count"] == 4  # 3*4/3


def test_e2e_idle_collector_is_abandoned_when_no_shard_reaches_stress(tmp_path, monkeypatch):
    """Every shard dies before dispatch -> no window, no report, no 60 s wait."""
    dying = tmp_path / "dying.py"
    dying.write_text("import sys\nsys.exit(2)\n", encoding="utf-8")
    vm_stub = tmp_path / "vm-monitor-stub.py"
    vm_stub.write_text(_IDLE_VM_MONITOR_STUB, encoding="utf-8")
    pid_file = tmp_path / "vm.pid"
    monkeypatch.setenv("VM_STUB_PID_FILE", str(pid_file))

    real_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda n: str(vm_stub) if n == "vm-monitor" else real_which(n))
    real_popen = subprocess.Popen

    def _popen(cmd, **kw):
        return real_popen([sys.executable, *cmd] if str(cmd[0]).endswith(".py") else cmd, **kw)

    monkeypatch.setattr(subprocess, "Popen", _popen)

    base = _base_yaml(tmp_path)
    cfg = yaml.safe_load(base.read_text(encoding="utf-8"))
    cfg["monitor"] = {"enabled": True, "vmm": "qemu", "report_timeout": 60}
    base.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    out_root = tmp_path / "sweep"
    started = time.monotonic()
    rc = main(
        _common_argv(tmp_path, base, out_root, dying)
        + ["--ratios", "2", "--modes", "exec_ephemeral", "--shards", "2", "--test-duration", "60"]
    )
    elapsed = time.monotonic() - started
    assert rc == 0
    # Abandoned, not waited out: the collector's report_timeout is 60 s and an
    # idle collector would also never exit on its own.
    assert elapsed < 30, f"sweep blocked {elapsed:.1f}s on an idle collector"

    pid = int(pid_file.read_text())
    deadline = time.monotonic() + 5
    alive = True
    while time.monotonic() < deadline:
        alive = psutil.pid_exists(pid)
        if not alive:
            break
        time.sleep(0.05)
    assert not alive, "idle collector left running after the trial"

    with open(out_root / "trial-summary.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["valid"] == "False"


def test_e2e_driver_owns_one_host_collector_over_the_union_window(tmp_path, monkeypatch):
    """One collector for the whole trial, bracketed by the UNION of shard windows.

    The collector must still be sampling when the LAST shard is stressing (shard
    1 here outlives shard 0), which a shard-0-owned collector would not do, and
    its output must land at the trial root, never under ``shard-00/``.
    """
    stub = tmp_path / "stub.py"
    stub.write_text(_SHARD_STUB, encoding="utf-8")
    vm_stub = tmp_path / "vm-monitor-stub.py"
    vm_stub.write_text(_VM_MONITOR_STUB, encoding="utf-8")

    real_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda n: str(vm_stub) if n == "vm-monitor" else real_which(n))
    real_popen = subprocess.Popen

    def _popen(cmd, **kw):  # the stub is a .py script, not an executable
        return real_popen([sys.executable, *cmd] if str(cmd[0]).endswith(".py") else cmd, **kw)

    monkeypatch.setattr(subprocess, "Popen", _popen)

    base = _base_yaml(tmp_path)
    out_root = tmp_path / "sweep"
    rc = main(
        _common_argv(tmp_path, base, out_root, stub)
        + ["--ratios", "2", "--modes", "exec_ephemeral", "--shards", "2", "--test-duration", "60"]
    )
    assert rc == 0

    trial_dir = out_root / "exec_ephemeral" / "ratio-02" / "repeat-00"
    events = json.loads((trial_dir / "vm_monitor" / "stub.json").read_text(encoding="utf-8"))
    assert [e[0] for e in events] == ["start", "end"]  # one bracket, not one per shard
    began, ended = events[0][1], events[1][1]

    marks = [json.loads((trial_dir / f"shard-0{i}" / "marker.json").read_text(encoding="utf-8")) for i in range(2)]
    assert marks[1]["end"] > marks[0]["end"] + 0.2  # shard 1 outlives shard 0
    assert began <= min(m["start"] for m in marks) + 1.0  # opened on the first shard's window
    assert ended >= max(m["end"] for m in marks) - 0.1  # closed only after the last one
    assert not (trial_dir / "shard-00" / "vm_monitor").exists()


def test_e2e_reuse_skips_completed_sharded_trial(tmp_path):
    stub = tmp_path / "stub.py"
    stub.write_text(_SHARD_STUB, encoding="utf-8")
    base = _base_yaml(tmp_path)
    out_root = tmp_path / "sweep"
    argv = _common_argv(tmp_path, base, out_root, stub) + [
        "--ratios",
        "2",
        "--modes",
        "exec_ephemeral",
        "--multiplier",
        "2",
        "--shards",
        "auto",
        "--test-duration",
        "60",
    ]
    assert main(argv) == 0
    trial_dir = out_root / "exec_ephemeral" / "ratio-02" / "repeat-00"
    marker = trial_dir / "shard-00" / "argv.json"
    first_run = marker.read_text(encoding="utf-8")
    marker.write_text("SENTINEL", encoding="utf-8")
    assert main(argv + ["--reuse"]) == 0
    # reuse hit the merged summary: the stub never re-ran (sentinel intact)
    assert marker.read_text(encoding="utf-8") == "SENTINEL"
    assert first_run != "SENTINEL"
    with open(out_root / "trial-summary.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows[-1]["reused"] == "True" and rows[-1]["valid"] == "True"
