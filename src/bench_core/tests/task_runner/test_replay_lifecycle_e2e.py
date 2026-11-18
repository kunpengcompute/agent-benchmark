"""Replay runner lifecycle tests: e2e lifecycle, exec_only regression,
_init_lifecycle idempotency, and warmup-stays-exec-only."""

from __future__ import annotations

import json

import threading

import time

from pathlib import Path

import pytest

from bench_core.bench import run_benchmark

from bench_core.config import KernelConfig

from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

from bench_core.schemas import BenchSandbox

from bench_core.task_runner.replay import (
    ReplayBaseRunner,
    ReplayConfig,
    ReplayRoundRunner,
    ReplayTaskRunner,
    ReplayWarmupRunner,
)

from bench_core.workflow_registry import RunContext, WorkflowConfigError

from env_provider import SandboxInstance

from env_provider.tests.lifecycle_fake import FakeLifecycleProvider

REPLAY_FIXTURES = Path(__file__).parent.parent / "fixtures" / "replay"

from bench_core.admission import Admission, RunningSlotScheduler

from bench_core.task_runner.replay import ReplayStep


def _lifecycle_config(tmp_path, **kw):
    shared_keys = {
        "workflow_type",
        "total_count",
        "benchmark_mode",
        "test_duration",
        "round_size",
        "output_dir",
        "filename_prefix",
        "warmup_only",
        "detect_existing",
        "create_only",
        "create_batch_size",
        "task_batch_size",
        "task_batch_interval",
        "benchmark_percent",
        "stats_interval",
        "provider_label",
    }
    shared = dict(
        workflow_type="replay",
        total_count=1,
        benchmark_mode="fixed",
        test_duration=1,
        output_dir=str(tmp_path),
        filename_prefix="lc",
    )
    replay = dict(
        replay_trajectory_dir=str(REPLAY_FIXTURES),
        replay_mode="lifecycle",
        replay_delay_scale=0.0,
    )
    for k, v in kw.items():
        if k in shared_keys:
            shared[k] = v
        else:
            replay[k] = v
    shared["workflow_config"] = ReplayConfig(**replay)
    return KernelConfig(**shared)


class TestExecOnlyBuildsNoAdmission:
    def test_exec_only_runner_admission_is_none(self, tmp_path):
        config = _lifecycle_config(tmp_path, replay_mode="exec_only", filename_prefix="eo")
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        runner = ReplayRoundRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider)
        )
        assert runner.admission is None


class TestOvercommitE2E:
    """P2.6 Task 6: end-to-end overcommit and exec_only regression."""

    def test_m_lt_n_emits_paused_windows_and_slot_wait(self, tmp_path):
        """3 sandboxes, 1 running slot -> steady-state paused majority +
        slot contention. Uses a slow-pause monkeypatch (0.1s) so the single
        slot is held long enough for the other sandboxes to queue -- this
        makes the contention assertion deterministic on fast machines.
        """
        config = _lifecycle_config(
            tmp_path,
            total_count=3,
            benchmark_mode="fixed",
            test_duration=2,
            filename_prefix="oc",
            replay_running_concurrency=1,
        )
        provider = FakeLifecycleProvider(count=3)

        # Slow pause so the single running slot stays occupied long enough
        # for the other two sandboxes to queue deterministically.
        orig_pause = FakeLifecycleProvider.pause

        def slow_pause(self_provider, inst):
            self_provider.pause_calls += 1
            time.sleep(0.1)

        FakeLifecycleProvider.pause = slow_pause
        try:
            result = run_benchmark(config, provider)
        finally:
            FakeLifecycleProvider.pause = orig_pause

        series_path = Path(config.output_dir) / f"{config.filename_prefix}_lifecycle_series.jsonl"
        assert series_path.exists()
        events = [json.loads(l) for l in series_path.read_text().splitlines()]
        steps = [e for e in events if e["event"] == "step"]
        assert len(steps) >= 3, f"expected >=3 step slices, got {len(steps)}"
        assert any(
            s["slot_contention_wait_sec"] > 0 for s in steps
        ), "no slot_contention_wait_sec > 0 observed across slices"
        assert "Running slots:" in result["report"]
        assert "maximum=1" in result["report"]
        assert "Slot contention:" in result["report"]

    def test_exec_only_regression_unchanged(self, tmp_path):
        """exec_only must IGNORE admission knobs: no lifecycle calls, no
        lifecycle/admission report lines. It now writes a step-only series
        (the exec_only-observability fix) but still emits no pause/resume /
        initial_pause / snapshot_size events even with admission knobs set.
        Byte-for-byte P2.5 regression modulo the series file now existing.
        """
        config = _lifecycle_config(
            tmp_path,
            replay_mode="exec_only",
            filename_prefix="eo",
            replay_running_concurrency=1,
            replay_control_plane_qps=50.0,
        )
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        report = result["report"]

        assert "[Lifecycle Overhead]" not in report
        assert "Admission:" not in report
        assert "Slot contention:" not in report
        assert provider.pause_calls == 0
        assert provider.resume_calls == 0
        series_path = Path(config.output_dir) / f"{config.filename_prefix}_lifecycle_series.jsonl"
        assert series_path.exists()  # exec_only now writes a step-only series
        events = [json.loads(l) for l in series_path.read_text().splitlines()]
        assert any(e["event"] == "step" for e in events)
        # admission knobs ignored: no lifecycle events despite the knobs
        assert not any(
            e["event"] in ("initial_pause", "snapshot_size", "trajectory_create", "trajectory_kill") for e in events
        )


class TestConfigP26Knobs:
    """P2.6: config knobs for admission controllers."""

    def test_from_raw_reads_admission_knobs(self):
        raw = {
            "workflow_type": "replay",
            "replay": {
                "running_concurrency": 4,
                "control_plane_qps": 20.0,
                "control_plane_inflight_cap": 64,
                "ready_probe": False,
            },
            "sandbox": {"total_count": 10},
        }
        config = KernelConfig.from_raw(raw)
        assert config.workflow_config.replay_running_concurrency == 4
        assert config.workflow_config.replay_control_plane_qps == 20.0
        assert config.workflow_config.replay_control_plane_inflight_cap == 64
        assert config.workflow_config.replay_ready_probe is False

    def test_validation_rejects_running_concurrency_over_total(self):
        cfg = KernelConfig(total_count=5, workflow_config=ReplayConfig(replay_running_concurrency=6))
        with pytest.raises(WorkflowConfigError, match="replay_running_concurrency.*must be <="):
            cfg.workflow_config.validate(cfg)

    def test_validation_rejects_zero_qps(self):
        cfg = KernelConfig(workflow_config=ReplayConfig(replay_control_plane_qps=0.0))
        with pytest.raises(WorkflowConfigError, match="replay_control_plane_qps must be > 0"):
            cfg.workflow_config.validate(cfg)

    def test_exec_only_forces_ready_probe_false(self):
        config = KernelConfig(
            total_count=2,
            workflow_type="replay",
            workflow_config=ReplayConfig(replay_mode="exec_only", replay_ready_probe=True),
        )
        assert config.workflow_config.replay_ready_probe is False


class TestL7DecompositionFields:
    """L7 (Task 4): running_slot_held_sec + interaction_total_sec decomposition."""

    def test_replay_metrics_collect_slot_held_and_interaction_lists(self):
        """L7: ReplayMetrics collects running_slot_held_sec + interaction_total_sec."""
        from bench_core.schemas import ReplayMetrics

        m = ReplayMetrics()
        m.add(
            latency=0.5,
            success=True,
            slice_total_sec=1.0,
            resume_sec=0.2,
            pause_sec=0.3,
            running_slot_held_sec=0.9,
            interaction_total_sec=1.4,
        )
        assert m.running_slot_held_secs == [0.9]
        assert m.interaction_total_secs == [1.4]

    def test_replay_metrics_lists_stay_aligned_on_failure_exclusion(self):
        """A zero slice_total_sec (synthesized failure) is excluded from ALL lists."""
        from bench_core.schemas import ReplayMetrics

        m = ReplayMetrics()
        m.add(
            latency=0.5,
            success=False,
            slice_total_sec=0.0,
            resume_sec=0.0,
            pause_sec=0.0,
            running_slot_held_sec=0.0,
            interaction_total_sec=0.0,
        )
        m.add(
            latency=0.5,
            success=True,
            slice_total_sec=1.0,
            resume_sec=0.1,
            pause_sec=0.2,
            running_slot_held_sec=0.8,
            interaction_total_sec=1.1,
        )
        # All 10 lists length-aligned to 1 (the failure excluded).
        for lst in (
            m.resume_secs,
            m.pause_secs,
            m.slice_total_secs,
            m.resume_api_secs,
            m.resume_ready_wait_secs,
            m.slot_contention_wait_secs,
            m.pause_api_secs,
            m.resume_rate_pacing_wait_secs,
            m.running_slot_held_secs,
            m.interaction_total_secs,
        ):
            assert len(lst) == 1


class TestRunBenchmarkP26Wiring:
    """P2.6: run_benchmark constructs Admission + threads it through managers."""

    def test_admission_constructed_in_lifecycle(self, tmp_path):
        """With running_concurrency=1 and qps=50.0, admission is built (qps triggers
        pass-through slots when running_concurrency == total_count)."""
        config = _lifecycle_config(
            tmp_path,
            replay_running_concurrency=1,
            replay_control_plane_qps=50.0,
        )
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        assert "Admission:" in result["report"]
        assert "Slot contention:" in result["report"]

    def test_no_admission_when_knobs_unset(self, tmp_path):
        """Vanilla lifecycle config (no admission knobs) -> no admission in report."""
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        assert "Admission:" not in result["report"]
        assert "Slot contention:" not in result["report"]

    def test_admission_running_fraction_in_report(self, tmp_path):
        """With total_count=3 and running_concurrency=1, report shows maximum=1 in Running slots."""
        config = _lifecycle_config(tmp_path, total_count=3, replay_running_concurrency=1)
        provider = FakeLifecycleProvider(count=3)
        result = run_benchmark(config, provider)
        assert "Running slots:" in result["report"]
        assert "maximum=1" in result["report"]

    def test_full_scale_lifecycle_rc_equals_total_builds_pass_through_admission(self, tmp_path):
        """Full-scale lifecycle (running_concurrency == total_count == k*N) must
        still emit an admission block: the scheduler is a pass-through (no thread
        ever blocks) but its slot accounting feeds effective_parallelism and the
        run_summary admission header -- without it those read 0.00/blank for the
        whole k*N fleet."""
        config = _lifecycle_config(tmp_path, total_count=3, replay_running_concurrency=3)
        provider = FakeLifecycleProvider(count=3)
        result = run_benchmark(config, provider)
        assert "Admission:" in result["report"]
        assert "Running slots:" in result["report"]
        assert "maximum=3" in result["report"]
        snap = result["admission_snapshot"]
        assert snap is not None
        assert snap["running_slots"]["maximum"] == 3

    def test_admission_snapshot_merges_full_sub_snapshots(self, tmp_path):
        """Phase 3: admission_snapshot embeds the full running_slots + qps_limiter
        sub-snapshots (not just the flattened peak/wait keys) so the report and
        xlsx renderer can read the complete controller state."""
        config = _lifecycle_config(
            tmp_path,
            replay_running_concurrency=1,
            replay_control_plane_qps=50.0,
        )
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        snap = result["admission_snapshot"]
        assert snap is not None
        # Full running-slots sub-snapshot
        assert "running_slots" in snap
        rs = snap["running_slots"]
        for k in ("maximum", "active", "peak_active", "granted", "average_queue_wait_sec", "waiting"):
            assert k in rs, f"running_slots missing {k}"
        # Full qps-limiter sub-snapshot
        assert "qps_limiter" in snap
        ql = snap["qps_limiter"]
        for k in (
            "qps",
            "inflight_cap",
            "in_flight",
            "dispatched",
            "average_wait_sec",
            "max_wait_sec",
            "dispatched_by_operation",
            "waiting",
            "waiting_by_operation",
        ):
            assert k in ql, f"qps_limiter missing {k}"
        # The flattened backward-compat keys are still present (existing report reads them).
        assert "peak_active" in snap and "avg_queue_wait_sec" in snap
        assert "qps_dispatched" in snap


def test_exec_is_qps_gated_in_lifecycle_mode():
    """G1: provider.exec is wrapped in qps.slot('command') in lifecycle mode."""
    from bench_core.admission import QpsRateLimiter
    from bench_core.config import KernelConfig
    from bench_core.schemas import BenchSandbox
    from env_provider import CommandResult

    calls = []

    class _RecordingProvider(FakeLifecycleProvider):
        def exec(self, inst, command, *, timeout=None, cwd=None, env=None):
            calls.append(command)
            return CommandResult(exit_code=0, stdout="ok", stderr="")

    cfg = KernelConfig(
        workflow_type="replay",
        workflow_config=ReplayConfig(replay_mode="lifecycle", replay_ready_probe=False),
    )
    provider = _RecordingProvider(count=1)
    provider.create_all()
    state = BenchSandbox.from_instance(provider._instances[0], "replay")
    stop = threading.Event()
    qps = QpsRateLimiter(qps=100.0, inflight_cap=4)
    adm = Admission(slots=RunningSlotScheduler(maximum=1), qps=qps)
    runner = ReplayBaseRunner(RunContext(state=state, config=cfg, stop_event=stop, provider=provider, admission=adm))
    step = ReplayStep(index=0, action_type="shell", action="true", delay_time_sec=0.0)
    runner._run_slice(step, trajectory_id="t1")
    snap = qps.snapshot()
    # command bucket must have dispatched at least once (the exec was gated).
    assert snap["dispatched_by_operation"].get("command", 0) >= 1
    assert calls  # exec actually ran


def test_lifecycle_call_splits_queue_wait_from_api_sec():
    """Wait-decoupling: _lifecycle_call_with_retry returns
    (rate_pacing_wait, inflight_wait, api_sec) separately, un-folding the
    earlier G3 conflation that jammed the QPS wait into api_sec.

    With rate_wait=True (default) the rate pacing is paid ONCE pre-loop;
    inflight_wait is the fuse block (0 when uncontended); api_sec is the pure
    call. Forces a future dispatch deadline so the rate wait is deterministic
    (~the deadline gap) regardless of box speed.
    """
    import time

    from bench_core.admission import QpsRateLimiter
    from bench_core.config import KernelConfig
    from bench_core.schemas import BenchSandbox

    cfg = KernelConfig(
        workflow_type="replay",
        workflow_config=ReplayConfig(replay_mode="lifecycle", replay_ready_probe=False),
    )
    provider = FakeLifecycleProvider(count=1)
    provider.create_all()
    state = BenchSandbox.from_instance(provider._instances[0], "replay")
    stop = threading.Event()
    qps = QpsRateLimiter(qps=100.0, inflight_cap=4)
    adm = Admission(slots=RunningSlotScheduler(maximum=1), qps=qps)
    runner = ReplayBaseRunner(RunContext(state=state, config=cfg, stop_event=stop, provider=provider, admission=adm))

    # Force a future dispatch deadline ~0.05s out so the rate wait is observable.
    qps._next_dispatch_at = time.monotonic() + 0.05
    rate_wait, inflight_wait, api = runner._lifecycle_call_with_retry("resume", lambda: provider.resume(state))

    # rate_wait captures the QPS time-wait (>= 0.04 tolerant lower bound);
    # inflight_wait is ~0 (cap=4, uncontended -- only sub-ms acquire overhead);
    # api_sec captures the pure resume call (FakeLifecycleProvider sleeps
    # 0.02 -> >= 0.01).
    assert rate_wait >= 0.04
    assert inflight_wait < 0.001
    assert api >= 0.01
    # Without an admission controller the waits are 0 and only the API duration
    # is measured (returns a 3-tuple, all-zero waits).
    runner_noqps = ReplayBaseRunner(
        RunContext(state=state, config=cfg, stop_event=stop, provider=provider, admission=None)
    )
    r2, inf2, a2 = runner_noqps._lifecycle_call_with_retry("pause", lambda: provider.pause(state))
    assert r2 == 0.0 and inf2 == 0.0
    assert a2 >= 0.01


def test_execute_measures_command_rate_pacing_wait_explicitly():
    """_execute returns (result, command_rate_pacing_wait_sec, body_elapsed_sec):
    the "command" QPS-bucket 1/qps dispatch wait is measured as its OWN quantity
    (forced future deadline -> deterministic ~0.05s) and the timer wraps ONLY
    the provider.exec body, so body_elapsed (exec_sec) EXCLUDES the pacing --
    comparable across qps on/off -- and the legacy command_start alias resolves
    to the quantity its name promises. Without an admission controller the wait
    is exactly 0.0 and body == the full call wall."""
    from bench_core.admission import QpsRateLimiter
    from bench_core.config import KernelConfig

    cfg = KernelConfig(
        workflow_type="replay",
        workflow_config=ReplayConfig(replay_mode="lifecycle", replay_ready_probe=False),
    )
    provider = FakeLifecycleProvider(count=1)
    provider.create_all()
    state = BenchSandbox.from_instance(provider._instances[0], "replay")
    stop = threading.Event()
    qps = QpsRateLimiter(qps=100.0, inflight_cap=4)
    adm = Admission(slots=RunningSlotScheduler(maximum=1), qps=qps)
    runner = ReplayBaseRunner(RunContext(state=state, config=cfg, stop_event=stop, provider=provider, admission=adm))

    # Ungated: the wait is exactly 0 and the body is the full call wall.
    step = ReplayStep(index=0, action_type="shell", action="true", delay_time_sec=0.0)
    t0 = time.perf_counter()
    result, wait, body = runner._execute(step)
    wall = time.perf_counter() - t0
    assert result.exit_code == 0
    assert wait == 0.0  # no future deadline pending at qps=100 on a quiet limiter
    assert body <= wall + 0.05  # no pacing: body ~= full wall

    # Gated with a forced future deadline ~0.05s out: the wait is observable
    # and deterministic, the command bucket dispatched, and the pacing is
    # EXCLUDED from the body (body ~= wall - pacing; the old wrap-around
    # timing would have reported body ~= wall, pacing included).
    qps._next_dispatch_at = time.monotonic() + 0.05
    t0 = time.perf_counter()
    result, wait, body = runner._execute(step)
    wall = time.perf_counter() - t0
    assert result.exit_code == 0
    assert wait >= 0.04
    assert qps.snapshot()["dispatched_by_operation"].get("command", 0) >= 1
    # exec_sec is the PURE body: body + wait covers the whole wall, and body
    # itself is NOT inflated by the pacing (wall - wait is all body + slack).
    assert body + wait >= wall - 0.02
    assert body <= wall - wait + 0.02

    # No admission controller: wait always exactly 0.0.
    runner_noqps = ReplayBaseRunner(
        RunContext(state=state, config=cfg, stop_event=stop, provider=provider, admission=None)
    )
    result, wait, body = runner_noqps._execute(step)
    assert result.exit_code == 0
    assert wait == 0.0
    assert body >= 0.0


def test_lifecycle_call_shutdown_during_retry_bypasses_except():
    """ShutdownInterrupted (BaseException) raised mid-retry must bypass the
    ``except Exception`` retry handler and propagate to the caller. This is the
    load-bearing BaseException-vs-Exception path: a shutdown is not retryable,
    so it must not be swallowed into a retry loop or recorded as slice_failed.

    Post-decoupling, rate pacing is ONCE pre-loop (not per-attempt), so a
    mid-loop shutdown surfaces from the call body. Attempt 1 raises a transient
    503 (caught -> retry_queued); attempt 2 raises ShutdownInterrupted directly
    (simulating an admission-layer shutdown during a nested call), which must
    propagate unchanged -- no retry_exhausted event.
    """
    from bench_core.admission import (
        QpsRateLimiter,
        ShutdownInterrupted,
    )
    from bench_core.config import KernelConfig
    from bench_core.schemas import BenchSandbox

    cfg = KernelConfig(
        workflow_type="replay",
        workflow_config=ReplayConfig(
            replay_mode="lifecycle",
            replay_lifecycle_retries=2,
            replay_ready_probe=False,
        ),
    )

    class _TransientThenShutdownProvider(FakeLifecycleProvider):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self._n = 0

        def resume(self, inst):
            self._n += 1
            if self._n == 1:
                raise RuntimeError("503 gateway timeout")  # transient -> retried
            raise ShutdownInterrupted("simulated admission shutdown on attempt 2")

    provider = _TransientThenShutdownProvider(count=1)
    provider.create_all()
    state = BenchSandbox.from_instance(provider._instances[0], "replay")
    stop = threading.Event()
    # qps=1000 -> 1ms interval (pre-loop rate pacing is a no-op delay on the
    # first call: dispatch deadline is in the past). inflight_cap=4 so the fuse
    # never blocks; the ShutdownInterrupted comes from the body, not admission.
    qps = QpsRateLimiter(qps=1000.0, inflight_cap=4, stop_event=stop)
    adm = Admission(slots=RunningSlotScheduler(maximum=1, stop_event=stop), qps=qps)
    runner = ReplayBaseRunner(RunContext(state=state, config=cfg, stop_event=stop, provider=provider, admission=adm))

    with pytest.raises(ShutdownInterrupted):
        runner._lifecycle_call_with_retry("resume", lambda: provider.resume(state))
    # Pre-loop rate pacing dispatched once; both attempts acquired the inflight
    # fuse before their bodies ran. No retry_exhausted (shutdown != retryable).
    snap = qps.snapshot()
    assert snap["dispatched"] >= 1
    assert snap["inflight_dispatched"] >= 2


def test_exec_ephemeral_mode_guard_rejects_non_ephemeral_provider():
    """Spine: replay_mode=exec_ephemeral on a non-EphemeralCapable provider fails fast."""
    from bench_core.bench import run_benchmark
    from bench_core.config import KernelConfig

    class _Plain:
        name = "plain"

    cfg = KernelConfig(
        workflow_type="replay",
        total_count=1,
        workflow_config=ReplayConfig(replay_mode="exec_ephemeral"),
    )
    with pytest.raises(ValueError, match="EphemeralCapable"):
        run_benchmark(cfg, _Plain())  # type: ignore[arg-type]


def test_exec_ephemeral_mode_skips_create_all_and_builds_shells():
    """Spine: exec_ephemeral mode does NOT call create_all; builds N ready shells.

    Uses create_only=True so the spine exits after the create block (before the
    runner), isolating the create-block wiring from the Task 8 runner logic.
    """
    from bench_core.bench import run_benchmark
    from bench_core.config import KernelConfig
    from env_provider.fake import FakeProvider

    cfg = KernelConfig(
        workflow_type="replay",
        total_count=3,
        create_only=True,
        workflow_config=ReplayConfig(replay_mode="exec_ephemeral"),
    )
    provider = FakeProvider(count=0)
    called = {"create_all": False}
    orig = provider.create_all

    def _spy():
        called["create_all"] = True
        return orig()

    provider.create_all = _spy  # type: ignore[assignment]
    run_benchmark(cfg, provider)
    assert called["create_all"] is False


def test_create_only_ephemeral_is_explicit_noop(tmp_path):
    """Spine: --create-only in an ephemeral mode exits with a no-op report --
    no zero-filled creation timing report and no sandbox_ids_file full of
    "traj-shell-N" placeholders (the shells are not real sandboxes)."""
    from bench_core.bench import run_benchmark
    from bench_core.config import KernelConfig
    from env_provider.fake import FakeProvider

    cfg = KernelConfig(
        workflow_type="replay",
        total_count=2,
        create_only=True,
        output_dir=str(tmp_path),
        workflow_config=ReplayConfig(replay_mode="exec_ephemeral"),
    )
    provider = FakeProvider(count=0)
    result = run_benchmark(cfg, provider)
    assert "no-op" in result["report"]
    assert result["filepath"] is None
    assert provider.save_ids_calls == 0


def test_run_trajectory_creates_runs_and_kills(tmp_path):
    """E2E: _run_trajectory does create -> steps -> kill; create/kill timing
    lands in the lifecycle series (trajectory_create / trajectory_kill events).

    Uses FakeLifecycleProvider (not bare FakeProvider) because _run_slice may
    reach provider.resume/pause in some paths, and FakeProvider has neither.
    Admission-free by construction: exec_ephemeral never gets a lease, so
    running_slot_held_secs stays empty (unlike lifecycle, which overlays it on
    the last step).
    """
    from pathlib import Path

    from bench_core.payload.replay_payload import Trajectory

    class _Provider(FakeLifecycleProvider):
        def __init__(self):
            super().__init__(count=0)
            self.killed = []

        def kill_one(self, inst):
            self.killed.append(inst.index)
            super().kill_one(inst)

    cfg = KernelConfig(
        workflow_type="replay",
        total_count=1,
        workflow_config=ReplayConfig(
            replay_mode="exec_ephemeral",
            replay_running_concurrency=1,
            replay_control_plane_qps=100.0,
            replay_lifecycle_retries=2,
            replay_ready_probe=False,
        ),
    )
    provider = _Provider()
    stop = threading.Event()
    inst = SandboxInstance(id="shell", index=1)
    state = BenchSandbox.from_instance(inst, workflow_type="replay")
    state.ready = True
    state.is_alive = True
    series = LifecycleSeriesWriter(tmp_path / "series.jsonl")
    runner = ReplayTaskRunner(RunContext(state=state, config=cfg, stop_event=stop, provider=provider, series=series))

    traj = Trajectory(
        path=Path("tr-1"),
        instance_id="tr-1",
        environment="env",
        steps=(ReplayStep(index=0, action_type="shell", action="echo hi", delay_time_sec=0.0),),
    )
    runner._run_trajectory(traj)
    series.close()

    # create happened (metadata forwarded)
    assert 1 in provider._meta_log
    # kill happened
    assert 1 in provider.killed
    # the single step replayed into metrics
    m = state.replay_metrics
    assert len(m.slice_total_secs) == 1
    # admission-free path: no running-slot lease was ever taken (per-step
    # slot-held stays at the 0.0 default; lifecycle overlays the lease there)
    assert m.running_slot_held_secs == [0.0]
    # create/kill timings live in the series events (the exec_ephemeral path)
    events = {e["event"]: e for e in (json.loads(l) for l in (tmp_path / "series.jsonl").read_text().splitlines())}
    assert events["trajectory_create"]["create_sec"] > 0
    assert events["trajectory_create"]["trajectory_id"] == "tr-1"
    assert "kill_sec" in events["trajectory_kill"]


def test_report_renders_slot_held_line_in_lifecycle_mode(tmp_path):
    """L7: lifecycle mode renders [Lifecycle Overhead] with Slot held + Interaction lines."""
    from bench_core.observability.stats_collector import ReportFormatter

    cfg = KernelConfig(
        workflow_type="replay",
        workflow_config=ReplayConfig(replay_mode="lifecycle", replay_running_concurrency=1),
    )
    state = BenchSandbox.from_instance(SandboxInstance(id="s1", index=1), workflow_type="replay")
    m = state.replay_metrics
    # Seed two aligned samples so all 12 lists stay length-consistent.
    for _ in range(2):
        m.add(
            latency=0.5,
            success=True,
            action_type="shell",
            resume_sec=0.1,
            pause_sec=0.1,
            slice_total_sec=1.0,
            resume_api_sec=0.05,
            resume_ready_wait_sec=0.0,
            resume_rate_pacing_wait_sec=0.0,
            slot_contention_wait_sec=0.0,
            pause_api_sec=0.05,
        )
    # Overlay L7 trajectory-level durations on the last recorded step.
    m._running_slot_held_secs[-1] = 0.7
    m._interaction_total_secs[-1] = 1.2
    fmt = ReportFormatter(
        cfg,
        {1: state},
        admission_snapshot={"running": 1, "total": 1, "qps": "off", "peak_active": 1, "avg_queue_wait_sec": 0.0},
    )
    report = "\n".join(fmt.format_stats_section())
    assert "[Lifecycle Overhead]" in report
    assert "Slot held:" in report
    assert "Interaction:" in report


class TestAdmissionConstructionIntegration:
    """bench.py admission construction is decoupled: the limiter is built when
    EITHER knob is set (qps or inflight_cap), each function no-ops when its own
    knob is None, and the two are NOT a forced pair. Drives the full
    ``run_benchmark`` spine with FakeLifecycleProvider (lifecycle mode) and
    asserts the returned ``admission_snapshot`` reports each knob per config.
    """

    def test_knobs_off_admission_is_none(self, tmp_path):
        # 1:1 (no oversub) + both knobs unset -> admission not constructed.
        config = _lifecycle_config(tmp_path, filename_prefix="adm-off")
        result = run_benchmark(config, FakeLifecycleProvider(count=1))
        assert result["admission_snapshot"] is None

    def test_qps_only_snapshot_reports_qps_inflight_off(self, tmp_path):
        config = _lifecycle_config(tmp_path, replay_control_plane_qps=100.0, filename_prefix="adm-qps")
        result = run_benchmark(config, FakeLifecycleProvider(count=1))
        snap = result["admission_snapshot"]
        assert snap is not None
        assert snap["qps"] == 100.0
        assert snap["inflight_cap"] == "off"  # inflight fuse bypassed (cap unset)

    def test_inflight_only_snapshot_reports_inflight_qps_off(self, tmp_path):
        config = _lifecycle_config(tmp_path, replay_control_plane_inflight_cap=2, filename_prefix="adm-if")
        result = run_benchmark(config, FakeLifecycleProvider(count=1))
        snap = result["admission_snapshot"]
        assert snap is not None
        assert snap["qps"] == "off"  # rate pacing bypassed (qps unset)
        assert snap["inflight_cap"] == 2

    def test_both_knobs_snapshot_reports_both(self, tmp_path):
        config = _lifecycle_config(
            tmp_path,
            replay_control_plane_qps=100.0,
            replay_control_plane_inflight_cap=2,
            filename_prefix="adm-both",
        )
        result = run_benchmark(config, FakeLifecycleProvider(count=1))
        snap = result["admission_snapshot"]
        assert snap is not None
        assert snap["qps"] == 100.0
        assert snap["inflight_cap"] == 2
