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


class TestLifecycleE2E:
    def test_lifecycle_e2e_records_resume_pause_and_initial_pause(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)

        assert "Replay Task Statistics" in result["report"]
        assert "Initial Pause" in result["report"]
        # lifecycle hooks fired: resume + pause per step, plus one initial pause.
        assert provider.pause_calls >= 1
        assert provider.resume_calls >= 1


class TestExecOnlyRegression:
    def test_exec_only_does_not_call_lifecycle_hooks(self, tmp_path):
        config = _lifecycle_config(tmp_path, replay_mode="exec_only", filename_prefix="eo")
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)

        assert "Replay Task Statistics" in result["report"]
        # exec_only: no pause/resume at all (not even the initial pause).
        assert provider.pause_calls == 0
        assert provider.resume_calls == 0


class TestInitLifecycleIdempotency:
    def test_init_lifecycle_pauses_once_across_calls(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        runner = ReplayRoundRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider)
        )

        runner._init_lifecycle()
        first_pause = provider.pause_calls
        first_initial = state.replay_metrics.initial_pause_sec
        assert first_pause == 1
        assert first_initial > 0
        assert state.lifecycle_paused is True

        # Second call no-ops (guard on lifecycle_paused).
        runner._init_lifecycle()
        assert provider.pause_calls == 1
        # initial_pause_sec not overwritten (still the first value).
        assert state.replay_metrics.initial_pause_sec == first_initial


class TestWarmupStaysExecOnly:
    def test_warmup_runner_does_not_pause_or_resume(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        state.ready = True
        runner = ReplayWarmupRunner(RunContext(state=state, config=config, stop_event=stop, provider=provider))
        runner.run()

        # Warmup probes exec directly; lifecycle hooks must NOT fire.
        assert provider.pause_calls == 0
        assert provider.resume_calls == 0


def _series_path(tmp_path):
    return tmp_path / "series.jsonl"


class TestRunSliceSeriesRecord:
    def test_run_slice_writes_step_record_with_six_timestamps(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=2)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        state.ready = True
        path = _series_path(tmp_path)
        series = LifecycleSeriesWriter(path)
        runner = ReplayRoundRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider, series=series)
        )
        runner._init_lifecycle()

        # drive one slice via the real _run_slice using a fixture step
        from bench_core.payload.replay_payload import ReplayStep

        step = ReplayStep(index=3, action_type="shell", action="true", delay_time_sec=0.0)
        sr = runner._run_slice(step, trajectory_id="traj-abc")
        series.close()

        lines = path.read_text().splitlines()
        # one initial_pause + one step record
        events = [json.loads(l) for l in lines]
        step_records = [r for r in events if r["event"] == "step"]
        assert len(step_records) == 1
        rec = step_records[0]
        assert rec["sandbox_index"] == 2
        assert rec["trajectory_id"] == "traj-abc"
        assert rec["round_id"] == 0
        assert rec["step_index"] == 3
        for f in ("resume_start", "resume_end", "exec_start", "exec_end", "pause_start", "pause_end"):
            assert f in rec and isinstance(rec[f], float)
        # monotonic boundary invariant
        assert rec["resume_end"] >= rec["resume_start"]
        assert rec["exec_start"] >= rec["resume_end"]
        assert rec["exec_end"] >= rec["exec_start"]
        assert rec["pause_start"] >= rec["exec_end"]
        assert rec["pause_end"] >= rec["pause_start"]
        assert rec["slice_failed"] is False

    def test_init_lifecycle_writes_initial_pause_record(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=1)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        path = _series_path(tmp_path)
        series = LifecycleSeriesWriter(path)
        runner = ReplayRoundRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider, series=series)
        )
        runner._init_lifecycle()
        series.close()

        lines = path.read_text().splitlines()
        recs = [json.loads(l) for l in lines]
        init = [r for r in recs if r["event"] == "initial_pause"]
        assert len(init) == 1
        assert init[0]["sandbox_index"] == 1
        assert init[0]["pause_end"] >= init[0]["pause_start"]
        assert init[0]["initial_pause_sec"] > 0

    def test_synthesized_failure_path_is_in_caller(self, tmp_path):
        # A slice that throws -> _run_slice raises; the caller (_replay_trajectory)
        # synthesizes the zero StepResult. _run_slice itself does NOT catch.
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)

        class _BoomRunner(ReplayBaseRunner):
            def _resume(self):
                raise RuntimeError("resume blew up")

        stop = threading.Event()
        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        path = _series_path(tmp_path)
        series = LifecycleSeriesWriter(path)
        runner = _BoomRunner(RunContext(state=state, config=config, stop_event=stop, provider=provider, series=series))
        from bench_core.payload.replay_payload import ReplayStep

        step = ReplayStep(index=0, action_type="shell", action="true", delay_time_sec=0.0)
        with pytest.raises(RuntimeError):
            runner._run_slice(step, trajectory_id="t")
        series.close()

    def test_synthesized_failure_emits_slice_failed_record(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)

        class _BoomRunner(ReplayRoundRunner):
            def _resume(self):
                raise RuntimeError("resume blew up")

        stop = threading.Event()
        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        state.ready = True
        path = _series_path(tmp_path)
        series = LifecycleSeriesWriter(path)
        runner = _BoomRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider, series=series)
        )
        runner.run()
        series.close()

        recs = [json.loads(l) for l in path.read_text().splitlines()]
        failed = [r for r in recs if r.get("event") == "step" and r.get("slice_failed")]
        assert len(failed) >= 1
        assert failed[0]["exit_code"] == 1
        assert failed[0]["slice_failed"] is True
        # spec §B: durations and timestamps are honestly zeroed on the
        # synthesized failure path (partial pre-throw timings are out of scope).
        for f in ("resume_start", "resume_end", "exec_start", "exec_end", "pause_start", "pause_end"):
            assert failed[0][f] == 0.0
        for f in ("resume_sec", "exec_sec", "pause_sec", "slice_total_sec"):
            assert failed[0][f] == 0.0


class TestRecordStepPassesDurations:
    def test_record_step_feeds_resume_pause_slice_into_metrics(self, tmp_path):
        from bench_core.task_runner.replay import StepResult

        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        runner = ReplayRoundRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider)
        )
        sr = StepResult(
            step_index=0,
            action_type="shell",
            exit_code=0,
            exec_elapsed_sec=1.0,
            slice_total_sec=1.5,
            resume_sec=0.2,
            pause_sec=0.3,
            requested_delay_sec=0.0,
        )
        runner._record_step(sr, timed_out=False, actual_delay=0.0, trajectory_complete=False, trajectory_id="t")
        assert state.replay_metrics.resume_secs == [0.2]
        assert state.replay_metrics.pause_secs == [0.3]
        assert state.replay_metrics.slice_total_secs == [1.5]


class TestExecOnlySkipsSeries:
    def test_exec_only_runner_series_arg_is_none(self, tmp_path):
        # exec_only: a runner built without series must not touch any file.
        config = _lifecycle_config(tmp_path, replay_mode="exec_only", filename_prefix="eo")
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        runner = ReplayRoundRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider)
        )
        assert runner.series is None


class TestLifecycleOverheadReport:
    def test_lifecycle_report_has_overhead_section(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        report = result["report"]
        assert "[Lifecycle Overhead]" in report
        assert "Resume:" in report
        assert "Pause:" in report
        assert "Overhead per-sample" in report
        assert "Overhead aggregate" in report

    def test_exec_only_report_has_no_overhead_section(self, tmp_path):
        config = _lifecycle_config(tmp_path, replay_mode="exec_only", filename_prefix="eo")
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        assert "[Lifecycle Overhead]" not in result["report"]

    def test_overhead_near_zero_guard_drops_tiny_slices(self, tmp_path):
        # Manually seed ReplayMetrics with one tiny slice (< MIN_SLICE_SEC)
        # and one normal slice; the tiny one must not explode the per-sample
        # overhead (excluded by the guard).
        from bench_core.observability.stats_collector import StatsCollector

        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        config = _lifecycle_config(tmp_path)
        sc = StatsCollector(config, {0: state}, "fake")
        m = state.replay_metrics
        # tiny slice (below MIN_SLICE_SEC=0.001) -- excluded from overhead
        m.add(
            latency=0.0001,
            success=True,
            action_type="shell",
            resume_sec=0.00001,
            pause_sec=0.00001,
            slice_total_sec=0.0001,
        )
        # normal slice
        m.add(
            latency=1.0,
            success=True,
            action_type="shell",
            resume_sec=0.1,
            pause_sec=0.1,
            slice_total_sec=1.2,
        )

        lines = sc.format_stats_section()
        joined = "\n".join(lines)
        assert "[Lifecycle Overhead]" in joined
        # the tiny slice's overhead is (0.00001+0.00001)/0.0001 = 20.0% -- it
        # must be EXCLUDED by the MIN_SLICE_SEC guard (slice_total_sec=0.0001
        # < 0.001), so 20.0% must NOT appear. The normal sample's overhead is
        # 0.2/1.2 = 16.7%, which must appear.
        assert "20.0%" not in joined
        assert "16.7%" in joined

    def test_decomposition_block_renders_in_lifecycle(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        report = result["report"]
        assert "Resume decomp:" in report
        assert "Pause decomp:" in report
        assert "inflight_wait" in report  # both decomp lines carry the inflight fuse
        assert "rate_pacing" in report  # Pause decomp carries pause rate-pacing (in-lease)
        assert "qps_wait" not in report  # removed: it mis-attributed resume rate-pacing into resume_sec
        assert "ready_wait" in report

    def test_decomposition_columns_stable_when_probe_off(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        # Task 5 adds replay_ready_probe as a real config field; for Task 4
        # we set it directly so the test runs without Task 5's field.
        config.workflow_config.replay_ready_probe = False  # type: ignore[union-attr]
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        report = result["report"]
        # Column present, value 0.000s (probe was off)
        assert "ready_wait" in report

    def test_admission_block_absent_when_no_controller(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        report = result["report"]
        assert "Slot contention:" not in report
        assert "Admission:" not in report

    def test_admission_block_present_when_admission_snapshot_set(self, tmp_path):
        from bench_core.observability.stats_collector import StatsCollector

        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        config = _lifecycle_config(tmp_path)
        m = state.replay_metrics
        # Seed one normal slice so the decomposition block renders
        m.add(
            latency=1.0,
            success=True,
            action_type="shell",
            resume_sec=0.1,
            pause_sec=0.1,
            slice_total_sec=1.2,
            slot_contention_wait_sec=0.05,
        )
        sc = StatsCollector(config, {0: state}, "fake")
        sc.admission_snapshot = {
            "running": 1,
            "total": 3,
            "qps": "off",
            "peak_active": 1,
            "avg_queue_wait_sec": 0.01,
            "running_slots": {
                "maximum": 1,
                "active": 0,
                "peak_active": 1,
                "granted": 3,
                "average_queue_wait_sec": 0.01,
                "waiting": 0,
            },
        }
        lines = sc.format_stats_section()
        joined = "\n".join(lines)
        assert "Slot contention:" in joined
        assert "Admission:" in joined
        assert "Running slots:" in joined
        assert "QPS limiter:" not in joined

    def test_qps_pacing_delay_line_shows_per_phase_split(self, tmp_path):
        from bench_core.observability.stats_collector import StatsCollector

        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        config = _lifecycle_config(tmp_path)
        m = state.replay_metrics
        # Seed one slice with non-zero resume/pause rate-pacing so the QPS pacing
        # delay line renders with the per-phase split (resume is pre-lease, moved
        # off the Resume decomp line onto this summary line; pause is in-lease).
        m.add(
            latency=1.0,
            success=True,
            action_type="shell",
            resume_sec=0.1,
            pause_sec=0.1,
            slice_total_sec=1.2,
            slot_contention_wait_sec=0.05,
            resume_rate_pacing_wait_sec=0.02,
            pause_rate_pacing_wait_sec=0.03,
        )
        sc = StatsCollector(config, {0: state}, "fake")
        sc.admission_snapshot = {
            "running": 1,
            "total": 3,
            "qps": 10.0,  # != "off" -> QPS pacing delay line renders
            "peak_active": 1,
            "avg_queue_wait_sec": 0.01,
            "running_slots": {
                "maximum": 1,
                "active": 0,
                "peak_active": 1,
                "granted": 3,
                "average_queue_wait_sec": 0.01,
                "waiting": 0,
            },
        }
        lines = sc.format_stats_section()
        joined = "\n".join(lines)
        # Enriched per-phase split on the summary line.
        assert "QPS pacing delay:" in joined
        assert "resume" in joined
        assert "pause" in joined
        assert "sum" in joined
        # The stale qps_wait decomp label is gone from the whole section.
        assert "qps_wait" not in joined


class TestSeriesFileE2E:
    def test_lifecycle_run_emits_series_jsonl(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        series_path = Path(config.output_dir) / f"{config.filename_prefix}_lifecycle_series.jsonl"
        assert series_path.exists()
        lines = series_path.read_text().splitlines()
        assert len(lines) >= 1
        events = [json.loads(l) for l in lines]
        # at least one initial_pause and one step
        assert any(e["event"] == "initial_pause" for e in events)
        assert any(e["event"] == "step" for e in events)
        # every step record carries the six timestamps
        for e in events:
            if e["event"] == "step":
                for f in ("resume_start", "resume_end", "exec_start", "exec_end", "pause_start", "pause_end"):
                    assert f in e

    def test_exec_only_run_emits_step_only_series(self, tmp_path):
        # exec_only now writes a series too: the step event carries real exec
        # timings (resume/pause are no-ops, but exec_sec / exec_end / the six
        # timestamps are genuine time.time() epochs), feeding the obs workbook's
        # Step detail / Trajectory summary / Concurrency states / Gantt sheets.
        # Only lifecycle emits pause/resume/initial_pause/snapshot_size
        # -- exec_only emits step events only.
        config = _lifecycle_config(tmp_path, replay_mode="exec_only", filename_prefix="eo")
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        series_path = Path(config.output_dir) / f"{config.filename_prefix}_lifecycle_series.jsonl"
        assert series_path.exists()
        events = [json.loads(l) for l in series_path.read_text().splitlines()]
        step_records = [e for e in events if e["event"] == "step"]
        assert len(step_records) >= 1
        # exec_only emits step events only -- no lifecycle signals.
        assert not any(e["event"] == "initial_pause" for e in events)
        assert not any(e["event"] == "snapshot_size" for e in events)
        assert not any(e["event"] in ("trajectory_create", "trajectory_kill") for e in events)
        # every step record carries the six timestamps (real epochs even though
        # resume/pause are no-ops -- resume_start_ts / exec_end_ts = time.time()).
        for e in step_records:
            for f in ("resume_start", "resume_end", "exec_start", "exec_end", "pause_start", "pause_end"):
                assert f in e

    def test_round_robin_lifecycle_emits_series(self, tmp_path):
        config = _lifecycle_config(tmp_path, benchmark_mode="round_robin", total_count=2, filename_prefix="rr")
        provider = FakeLifecycleProvider(count=2)
        result = run_benchmark(config, provider)
        series_path = Path(config.output_dir) / f"{config.filename_prefix}_lifecycle_series.jsonl"
        assert series_path.exists()
        events = [json.loads(l) for l in series_path.read_text().splitlines()]
        step_records = [e for e in events if e["event"] == "step"]
        assert len(step_records) >= 1
        # round_id present and int on every step record (round-robin mode)
        for r in step_records:
            assert isinstance(r["round_id"], int)

    def test_fixed_mode_round_id_is_pass_ordinal(self, tmp_path):
        # Fixed mode: round_id is the slot's 0-based PASS ordinal (stamped by
        # the _begin_pass hook), int like round-robin. It used to be null in
        # fixed mode, which collapsed repeated passes of one trajectory on one
        # slot into a single merged run row downstream (double n_steps, summed
        # create/kill) -- the per-pass ordinal is what keeps runs distinct.
        # The default _lifecycle_config is fixed mode.
        config = _lifecycle_config(tmp_path, filename_prefix="fixed")
        provider = FakeLifecycleProvider(count=1)
        result = run_benchmark(config, provider)
        series_path = Path(config.output_dir) / f"{config.filename_prefix}_lifecycle_series.jsonl"
        assert series_path.exists()
        events = [json.loads(l) for l in series_path.read_text().splitlines()]
        step_records = [e for e in events if e["event"] == "step"]
        assert len(step_records) >= 1
        for r in step_records:
            assert isinstance(r["round_id"], int) and r["round_id"] >= 0


class TestRunSliceP26Decomposition:
    """P2.6: _run_slice acquires a slot, gates resume/pause via QPS, runs a
    post-resume ``true`` ready-probe, and stamps six segment durations."""

    _SEGMENT_KEYS = (
        "slot_contention_wait_sec",
        "resume_rate_pacing_wait_sec",
        "resume_api_sec",
        "resume_ready_wait_sec",
        "pause_rate_pacing_wait_sec",
        "pause_api_sec",
        # Wait-decoupling split (the four independent components + per-phase inflight).
        "natural_delay_sec",
        "capacity_wait_sec",
        "resume_inflight_wait_sec",
        "pause_inflight_wait_sec",
        "rate_pacing_wait_sec",
        "inflight_wait_sec",
    )

    def test_step_record_has_six_segment_fields(self, tmp_path):
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=2)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        state.ready = True
        path = _series_path(tmp_path)
        series = LifecycleSeriesWriter(path)
        runner = ReplayRoundRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider, series=series)
        )
        runner._init_lifecycle()

        step = ReplayStep(index=3, action_type="shell", action="true", delay_time_sec=0.0)
        sr = runner._run_slice(step, trajectory_id="traj-abc")
        series.close()

        events = [json.loads(l) for l in path.read_text().splitlines()]
        step_records = [r for r in events if r["event"] == "step"]
        assert len(step_records) == 1
        rec = step_records[0]

        # All six segment keys present and floats
        for key in self._SEGMENT_KEYS:
            assert key in rec, f"missing segment key {key}"
            assert isinstance(rec[key], float), f"{key} is not a float"

        # Invariants (wait-decoupling decomposition must sum exactly to totals).
        # Post-decoupling: resume_sec EXCLUDES resume rate-pacing (pre-lease);
        # pause_sec INCLUDES pause rate-pacing (in-lease) + pause inflight.
        assert (
            abs(
                rec["resume_sec"]
                - (rec["resume_inflight_wait_sec"] + rec["resume_api_sec"] + rec["resume_ready_wait_sec"])
            )
            < 1e-6
        )
        assert (
            abs(
                rec["pause_sec"]
                - (rec["pause_rate_pacing_wait_sec"] + rec["pause_inflight_wait_sec"] + rec["pause_api_sec"])
            )
            < 1e-6
        )
        assert abs(rec["slice_total_sec"] - (rec["resume_sec"] + rec["exec_sec"] + rec["pause_sec"])) < 1e-6
        # slot_contention is the natural_delay + capacity_wait composite.
        assert abs(rec["slot_contention_wait_sec"] - (rec["natural_delay_sec"] + rec["capacity_wait_sec"])) < 1e-6
        # StepResult mirrors the record
        assert abs(sr.resume_sec - (sr.resume_inflight_wait_sec + sr.resume_api_sec + sr.resume_ready_wait_sec)) < 1e-6
        assert (
            abs(sr.pause_sec - (sr.pause_rate_pacing_wait_sec + sr.pause_inflight_wait_sec + sr.pause_api_sec)) < 1e-6
        )
        assert abs(sr.slice_total_sec - (sr.resume_sec + sr.exec_elapsed_sec + sr.pause_sec)) < 1e-6
        # resume_api captured the FakeLifecycleProvider sleep (measurably non-zero)
        assert sr.resume_api_sec > 0.0
        assert sr.pause_api_sec > 0.0

    def test_think_delay_attributed_to_natural_delay_not_doubled(self, tmp_path):
        # Regression: a step's recorded think-delay (delay_time_sec * scale) is
        # real inter-step pacing slept by the run loop's _sleep_delay BEFORE
        # _run_slice. It must be attributed to natural_delay_sec (the "think-time"
        # column), and interaction_total must add it exactly ONCE -- via
        # natural_delay -- never twice.
        #
        # Bug: natural_delay was sourced only from lease.natural_delay_sec, which
        # is ~0 because _sleep_delay pre-consumes the think-delay so the
        # scheduler's ready_at pre-park is a no-op. The think-delay then appeared
        # ONLY in interaction_total via a separate + step.delay_time_sec term,
        # so the decomposition column read 0 while interaction_total carried the
        # full think-delay (observed 1:1 lifecycle: interaction_total=568,
        # natural_delay=0 -- ~528s unattributed).
        config = _lifecycle_config(tmp_path, replay_delay_scale=1.0)  # the prod default
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=2)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        state.ready = True
        path = _series_path(tmp_path)
        series = LifecycleSeriesWriter(path)
        runner = ReplayRoundRunner(
            RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider, series=series)
        )
        runner._init_lifecycle()

        think = 2.0
        step = ReplayStep(index=3, action_type="shell", action="true", delay_time_sec=think)
        runner._run_slice(step, trajectory_id="traj-abc")
        series.close()

        rec = [r for r in (json.loads(line) for line in path.read_text().splitlines()) if r["event"] == "step"][0]

        # The think-delay is attributed to natural_delay (not lost to 0).
        assert abs(rec["natural_delay_sec"] - think) < 1e-6, rec["natural_delay_sec"]
        # The slot_contention composite carries it too.
        assert abs(rec["slot_contention_wait_sec"] - (rec["natural_delay_sec"] + rec["capacity_wait_sec"])) < 1e-6
        # INV7: interaction_total = slice + natural_delay + capacity_wait +
        # resume_rate_pacing -- the think-delay lives in natural_delay, NOT in a
        # second + step.delay_time_sec term (that would double-count once
        # natural_delay is correctly populated).
        expected_interaction = (
            rec["slice_total_sec"]
            + rec["natural_delay_sec"]
            + rec["capacity_wait_sec"]
            + rec["resume_rate_pacing_wait_sec"]
        )
        assert abs(rec["interaction_total_sec"] - expected_interaction) < 1e-6, (
            rec["interaction_total_sec"],
            expected_interaction,
        )

    def test_resume_rate_pacing_excluded_pause_pacing_included_in_slot_held(self, tmp_path):
        """Asymmetric wait-decoupling归属 regression (point 4 of the directive).

        Resume rate-pacing runs PRE-lease (sandbox paused, no lease/memory
        demand) so it is EXCLUDED from ``running_slot_held``. Pause rate-pacing
        runs IN-lease (sandbox running, memory held until pause confirms) so it
        IS included. A future dispatch deadline is injected so BOTH rate-pacing
        waits are non-zero, making the归属 observable rather than a vacuous 0==0.
        """
        from bench_core.admission import Admission, QpsRateLimiter, RunningSlotScheduler

        config = _lifecycle_config(tmp_path, replay_ready_probe=False)
        provider = FakeLifecycleProvider(count=1)
        stop = threading.Event()
        inst = SandboxInstance(id="x", index=2)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        state.ready = True
        # qps=20 -> interval 0.05s; inflight_cap=4 (uncontended -> inflight_wait ~0,
        # so rate-pacing is the only non-zero wait under test).
        qps = QpsRateLimiter(qps=20.0, inflight_cap=4)
        adm = Admission(slots=RunningSlotScheduler(maximum=1), qps=qps)
        path = _series_path(tmp_path)
        series = LifecycleSeriesWriter(path)
        runner = ReplayRoundRunner(
            RunContext(
                state=state, config=config, stop_event=stop, round_id=0, provider=provider, series=series, admission=adm
            )
        )
        runner._init_lifecycle()

        # Inject a FUTURE dispatch deadline so the first resume time_wait sleeps
        # (the limiter seeds _next_dispatch_at = construction time -> 0 delay on the
        # first call). The deadline slides forward by interval (0.05s) per call, so
        # the subsequent pause time_wait also lands in the future -> non-zero.
        qps._next_dispatch_at = time.monotonic() + 0.04
        step = ReplayStep(index=3, action_type="shell", action="true", delay_time_sec=0.0)
        sr = runner._run_slice(step, trajectory_id="traj-abc")
        series.close()

        # Both rate-pacing waits are non-zero (precondition: the归属 is observable).
        assert sr.resume_rate_pacing_wait_sec > 0.0, "resume rate-pacing should be non-zero"
        assert sr.pause_rate_pacing_wait_sec > 0.0, "pause rate-pacing should be non-zero"

        # EXCLUSION: running_slot_held spans [acquire, release]; acquire runs
        # AFTER the pre-lease resume time_wait, so resume rate-pacing sits OUTSIDE
        # the lease bracket. The gap between (resume_pacing + resume_sec + exec +
        # pause_sec) and running_slot_held IS the excluded pre-lease wait.
        bracket = sr.resume_rate_pacing_wait_sec + sr.resume_sec + sr.exec_elapsed_sec + sr.pause_sec
        excluded = bracket - sr.running_slot_held_sec
        assert excluded > 0.001, (
            f"resume rate-pacing must be EXCLUDED from running_slot_held; "
            f"excluded={excluded:.5f} should ~= resume_rate_pacing_wait={sr.resume_rate_pacing_wait_sec:.5f}"
        )
        # The excluded portion IS the resume rate-pacing (within timing jitter).
        assert abs(excluded - sr.resume_rate_pacing_wait_sec) < 0.02, (
            f"excluded={excluded:.5f} should match resume_rate_pacing_wait="
            f"{sr.resume_rate_pacing_wait_sec:.5f} (the pre-lease wait)"
        )

        # INCLUSION: pause rate-pacing is in pause_sec (in-lease) -> in slot_held.
        assert sr.pause_sec >= sr.pause_rate_pacing_wait_sec
        # running_slot_held == resume_sec + exec + pause_sec exactly (resume_sec
        # excludes resume rate-pacing; pause_sec includes pause rate-pacing).
        assert abs(sr.running_slot_held_sec - (sr.resume_sec + sr.exec_elapsed_sec + sr.pause_sec)) < 0.02

    def test_ready_probe_runs_in_lifecycle(self, tmp_path):
        """In lifecycle mode, ``true`` is exec'd as a ready probe after resume."""
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)

        # Class-level wrap to count ``true`` execs without recursion.
        probe_calls = {"n": 0}
        orig_exec = FakeLifecycleProvider.exec

        def counting_exec(self_provider, inst, command, **kw):
            if command == "true":
                probe_calls["n"] += 1
            return orig_exec(self_provider, inst, command, **kw)

        FakeLifecycleProvider.exec = counting_exec
        try:
            stop = threading.Event()
            inst = SandboxInstance(id="x", index=0)
            state = BenchSandbox.from_instance(inst, workflow_type="replay")
            state.ready = True
            runner = ReplayRoundRunner(
                RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider)
            )
            runner._init_lifecycle()
            step = ReplayStep(index=0, action_type="shell", action="echo hi", delay_time_sec=0.0)
            sr = runner._run_slice(step)
        finally:
            FakeLifecycleProvider.exec = orig_exec

        # At least one probe fired (action itself was ``echo hi``, not ``true``).
        assert probe_calls["n"] >= 1
        assert sr.resume_ready_wait_sec >= 0.0

    def test_ready_probe_skipped_in_exec_only(self, tmp_path):
        """exec_only mode does NOT run the ready probe (no lifecycle calls)."""
        config = _lifecycle_config(tmp_path, replay_mode="exec_only", filename_prefix="eo")
        provider = FakeLifecycleProvider(count=1)

        probe_calls = {"n": 0}
        orig_exec = FakeLifecycleProvider.exec

        def counting_exec(self_provider, inst, command, **kw):
            if command == "true":
                probe_calls["n"] += 1
            return orig_exec(self_provider, inst, command, **kw)

        FakeLifecycleProvider.exec = counting_exec
        try:
            stop = threading.Event()
            inst = SandboxInstance(id="x", index=0)
            state = BenchSandbox.from_instance(inst, workflow_type="replay")
            state.ready = True
            runner = ReplayRoundRunner(
                RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider)
            )
            step = ReplayStep(index=0, action_type="shell", action="echo hi", delay_time_sec=0.0)
            runner._run_slice(step)
        finally:
            FakeLifecycleProvider.exec = orig_exec

        # exec_only: no probe (the action ``echo hi`` is NOT ``true``).
        assert probe_calls["n"] == 0

    def test_probe_exhaustion_emits_failed_record(self, tmp_path):
        """When ``true`` always fails, the probe raises and the caller emits
        a slice_failed record with all six segment fields zeroed."""
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)

        # Class-level wrap: force every ``true`` exec to return exit_code=1.
        orig_exec = FakeLifecycleProvider.exec

        def failing_true_exec(self_provider, inst, command, **kw):
            if command == "true":
                return CommandResult(exit_code=1, stdout="", stderr="not ready")
            return orig_exec(self_provider, inst, command, **kw)

        FakeLifecycleProvider.exec = failing_true_exec
        try:
            stop = threading.Event()
            inst = SandboxInstance(id="x", index=0)
            state = BenchSandbox.from_instance(inst, workflow_type="replay")
            state.ready = True
            path = _series_path(tmp_path)
            series = LifecycleSeriesWriter(path)
            runner = ReplayRoundRunner(
                RunContext(state=state, config=config, stop_event=stop, round_id=0, provider=provider, series=series)
            )
            runner.run()
            series.close()
        finally:
            FakeLifecycleProvider.exec = orig_exec

        recs = [json.loads(l) for l in path.read_text().splitlines()]
        failed = [r for r in recs if r.get("event") == "step" and r.get("slice_failed")]
        assert len(failed) >= 1
        rec = failed[0]
        for key in self._SEGMENT_KEYS:
            assert key in rec, f"failed record missing segment key {key}"
            assert rec[key] == 0.0

    def test_slot_contention_nonzero_when_m_lt_n(self, tmp_path):
        """Two sandboxes, one running slot -> at least one slice waits."""
        config = _lifecycle_config(tmp_path, total_count=2, benchmark_mode="round_robin", replay_delay_scale=0.0)
        provider = FakeLifecycleProvider(count=2)

        slots = RunningSlotScheduler(maximum=1)
        admission = Admission(slots=slots, qps=None)

        # Make pause slow so the single slot stays held across both runners.
        orig_pause = FakeLifecycleProvider.pause

        def slow_pause(self_provider, inst):
            import time as _t

            _t.sleep(0.2)
            self_provider.pause_calls += 1

        FakeLifecycleProvider.pause = slow_pause
        try:
            runners = []
            for i in range(2):
                stop = threading.Event()
                inst = SandboxInstance(id=f"x{i}", index=i)
                state = BenchSandbox.from_instance(inst, workflow_type="replay")
                state.ready = True
                r = ReplayRoundRunner(
                    RunContext(
                        state=state, config=config, stop_event=stop, round_id=0, provider=provider, admission=admission
                    )
                )
                runners.append(r)
            for r in runners:
                r.start()
            for r in runners:
                r.join(timeout=30)
                assert not r.is_alive(), "runner did not finish"
        finally:
            FakeLifecycleProvider.pause = orig_pause

        snap = slots.snapshot()
        assert snap["granted"] >= 2
        # At least one runner waited on the slot.
        contention = [s.replay_metrics.slot_contention_wait_secs for s in (runners[0].state, runners[1].state)]
        assert any(v and v[0] > 0 for v in contention), f"no contention observed: {contention}"

    def test_lease_released_on_slice_exception(self, tmp_path):
        """A mid-slice exception must NOT leak the running slot."""
        config = _lifecycle_config(tmp_path)
        provider = FakeLifecycleProvider(count=1)
        slots = RunningSlotScheduler(maximum=1)
        admission = Admission(slots=slots, qps=None)

        class _BoomRunner(ReplayBaseRunner):
            def _resume(self):
                raise RuntimeError("resume blew up")

        stop = threading.Event()
        inst = SandboxInstance(id="x", index=0)
        state = BenchSandbox.from_instance(inst, workflow_type="replay")
        runner = _BoomRunner(
            RunContext(state=state, config=config, stop_event=stop, provider=provider, admission=admission)
        )
        step = ReplayStep(index=0, action_type="shell", action="true", delay_time_sec=0.0)
        with pytest.raises(RuntimeError):
            runner._run_slice(step, trajectory_id="t")

        snap = slots.snapshot()
        assert snap["active"] == 0, f"leaked slot: active={snap['active']}"
        # A lease was granted (the acquire happened before the raise).
        assert snap["granted"] == 1
