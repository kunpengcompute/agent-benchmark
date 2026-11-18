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


def test_stress_bracket_latches_open_until_stop(tmp_path):
    """A gapped union -- one shard finishing while another is still creating --
    must never end the sampling: the collector is single-shot and would export
    the create/warmup window before the tail shards' stress starts."""
    m1 = tmp_path / "shard-00" / "stress.lock"
    m2 = tmp_path / "shard-01" / "stress.lock"
    ctl = _RecordingController()
    bracket = ShardStressBracket([m1, m2], ctl, poll_sec=0.02)
    bracket.start()
    try:
        m1.parent.mkdir(parents=True)
        m1.touch()
        _wait_for(lambda: ctl.calls == ["begin"], 2.0)
        m1.unlink()  # first shard's window ends ...
        m2.parent.mkdir(parents=True)
        m2.touch()  # ... while the second shard only now reaches stress
        _wait_for(lambda: ctl.calls == ["begin"] and m2.exists(), 2.0)
        assert ctl.calls == ["begin"], "the gap must not close the sampling window"
    finally:
        bracket.stop()
    assert ctl.calls == ["begin", "end"]


def test_stress_bracket_without_any_shard_marker_stays_closed(tmp_path):
    """No shard reached stress: the window never opens and stop() does not end
    one -- the driver then abandons the collector instead of exporting a window
    that cannot exist."""
    ctl = _RecordingController()
    bracket = ShardStressBracket([tmp_path / "shard-00" / "stress.lock"], ctl, poll_sec=0.02)
    bracket.start()
    time.sleep(0.1)
    bracket.stop()
    assert not bracket.opened
    assert ctl.calls == []


def test_start_host_monitor_bounds_sampling_by_the_trial_deadline(tmp_path, monkeypatch):
    """vm_monitor's -t back-stop must cover the WHOLE union window (inter-shard
    start skew + the trial) -- the group deadline is the sound bound, because
    the window cannot outlive it. test_duration alone understates exec_only
    trials where every shard runs a full create_all first."""
    captured: dict[str, int | None] = {}

    class _StubController:
        def __init__(self, cfg, hint) -> None:  # noqa: ANN001 (test stub)
            captured["test_duration"] = cfg.test_duration
            self.proc = object()  # not None -> start() returns the collector

        def start(self) -> None:
            pass

    monkeypatch.setattr(oversub_shards, "MonitorController", _StubController)
    start_host_monitor(_trial_cfg(), tmp_path, provider="fake", test_duration=60, timeout_sec=240)
    assert captured["test_duration"] == 240
    start_host_monitor(_trial_cfg(), tmp_path, provider="fake", test_duration=180, timeout_sec=None)
    assert captured["test_duration"] == 180


def test_build_shard_command_vm_monitor_owned_by_shard_zero(tmp_path):
    plan = plan_shards(trial_dir=tmp_path, mode="exec_ephemeral", ratio=2, n=4, setting="auto")
    cmd0 = build_shard_command(["bench-core"], plan.specs[0], provider="aenv", no_vm_monitor=False)
    cmd1 = build_shard_command(["bench-core"], plan.specs[1], provider="aenv", no_vm_monitor=False)
    assert "--no-vm-monitor" not in cmd0
    assert "--no-vm-monitor" in cmd1
    assert cmd0[:5] == ["bench-core", "--config", str(plan.specs[0].config_path), "--provider", "aenv"]
    # global opt-out applies to every shard
    cmd0n = build_shard_command(["bench-core"], plan.specs[0], provider="aenv", no_vm_monitor=True)
    assert "--no-vm-monitor" in cmd0n


class _RecordingController:
    """Stands in for MonitorController: records the bracket it was given."""

    def __init__(self):
        self.calls: list[str] = []

    def begin_stress(self):
        self.calls.append("begin")

    def end_stress(self):
        self.calls.append("end")


def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"condition not met within {timeout}s")


def test_stress_bracket_spans_the_union_of_shard_markers(tmp_path):
    """First shard start to driver stop -- an early finisher never cuts it."""
    markers = [tmp_path / "a.lock", tmp_path / "b.lock"]
    ctl = _RecordingController()
    bracket = ShardStressBracket(markers, ctl, poll_sec=0.01)
    bracket.start()
    try:
        markers[1].touch()
        _wait_for(lambda: ctl.calls == ["begin"])
        markers[0].touch()
        markers[1].unlink()
        markers[0].unlink()  # union momentarily empty mid-trial ...
        time.sleep(0.05)
        assert ctl.calls == ["begin"]  # ... but the window stays latched open
    finally:
        bracket.stop()
    assert ctl.calls == ["begin", "end"]


def test_stress_bracket_closes_on_stop_when_a_shard_died_mid_stress(tmp_path):
    marker = tmp_path / "shard.lock"
    ctl = _RecordingController()
    bracket = ShardStressBracket([marker], ctl, poll_sec=0.01)
    bracket.start()
    marker.touch()  # a SIGKILLed shard never removes its own marker
    _wait_for(lambda: ctl.calls == ["begin"])
    bracket.stop()
    assert ctl.calls == ["begin", "end"]
    assert bracket.opened is True


def test_stress_bracket_reports_never_opened_when_no_shard_starts(tmp_path):
    """Every shard crashed before stress: the driver must abandon, not wait."""
    ctl = _RecordingController()
    bracket = ShardStressBracket([tmp_path / "a.lock", tmp_path / "b.lock"], ctl, poll_sec=0.01)
    bracket.start()
    time.sleep(0.05)
    bracket.stop()
    assert ctl.calls == []
    assert bracket.opened is False


def test_start_host_monitor_uses_the_trial_root_and_a_driver_lock(tmp_path, monkeypatch):
    seen = {}

    class _FakeController:
        proc = object()  # "spawned": start_host_monitor returns the controller

        def __init__(self, cfg, provider):
            seen["cfg"], seen["provider"] = cfg, provider

        def start(self):
            pass

    monkeypatch.setattr(oversub_shards, "MonitorController", _FakeController)
    monkeypatch.setattr(oversub_shards, "provider_vmm_type", lambda _name: "cubesandbox")

    ctl = start_host_monitor(_trial_cfg(), tmp_path, provider="cubesandbox", test_duration=60)

    assert isinstance(ctl, _FakeController)
    mc = seen["cfg"].monitor
    assert mc.log_dir == str(tmp_path / "vm_monitor")
    assert mc.stress_file == str(tmp_path / "vm_monitor" / "stress.lock")
    assert mc.external_collector is False
    assert (seen["cfg"].output_dir, seen["cfg"].test_duration) == (str(tmp_path), 60)
    assert seen["provider"].name == "cubesandbox-driver"  # log lines must not claim the provider has no list API
    assert seen["provider"].vmm_type == "cubesandbox"
    assert seen["provider"].list_sandbox_states() is None  # no handle: shard 0 gauges
