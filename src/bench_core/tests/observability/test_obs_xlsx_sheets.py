"""Phase 3: XlsxReportRenderer produces the expected sheets/columns."""

from __future__ import annotations

import pytest

openpyxl = pytest.importorskip("openpyxl", reason="openpyxl is a core dep")

from openpyxl import load_workbook  # noqa: E402

from bench_core.config import KernelConfig  # noqa: E402

from bench_core.task_runner.replay import ReplayConfig  # noqa: E402

from bench_core.observability.obs_xlsx import XlsxReportRenderer  # noqa: E402

from bench_core.observability.replay_obs import ReplayObservability  # noqa: E402

from bench_core.schemas import BenchSandbox, ReplayMetrics  # noqa: E402

from env_provider import SandboxInstance  # noqa: E402


def _seeded_observability(
    *, replay_mode="lifecycle", with_retry=True, natural_delay_sec=0.0
) -> tuple[ReplayObservability, KernelConfig]:
    state = BenchSandbox.from_instance(SandboxInstance(id="x", index=0), "replay")
    m = ReplayMetrics()
    for i in range(3):
        m.add(
            latency=0.1 + i * 0.05,
            success=True,
            action_type="shell",
            resume_sec=0.05,
            pause_sec=0.05,
            slice_total_sec=1.0,
            running_slot_held_sec=0.8,
            interaction_total_sec=1.0,
            natural_delay_sec=natural_delay_sec,
        )
    if with_retry:
        m.record_retry_event("retry_queued", operation="resume", time_lost_sec=0.05)
        m.append_retries_per_slice(1)
    state.replay_metrics = m
    cfg = KernelConfig(
        workflow_type="replay",
        total_count=2,
        test_duration=300,
        workflow_config=ReplayConfig(replay_mode=replay_mode, replay_running_concurrency=1),
    )
    obs = ReplayObservability(
        cfg,
        {0: state},
        admission_snapshot={
            "running": 1,
            "total": 2,
            "qps": 100.0,
            "running_slots": {
                "maximum": 1,
                "active": 0,
                "peak_active": 1,
                "granted": 3,
                "average_queue_wait_sec": 0.01,
                "waiting": 0,
            },
            "qps_limiter": {
                "qps": 100.0,
                "inflight_cap": 4,
                "in_flight": 0,
                "dispatched": 9,
                "average_wait_sec": 0.001,
                "max_wait_sec": 0.01,
                "dispatched_by_operation": {
                    "resume": 3,
                    "pause": 3,
                    "cleanup": 0,
                    "create": 3,
                    "command": 0,
                },
                "waiting": 0,
                "waiting_by_operation": {
                    "resume": 0,
                    "pause": 0,
                    "cleanup": 0,
                    "create": 0,
                    "command": 0,
                },
            },
        },
        wall_sec=10.0,
    )
    return obs, cfg


def test_snapshot_sizes_sheet(tmp_path):
    from unittest.mock import MagicMock

    from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

    sp = tmp_path / "s.jsonl"
    w = LifecycleSeriesWriter(sp)
    w.write(
        {
            "event": "snapshot_size",
            "sandbox_index": 0,
            "sandbox_id": "a",
            "pause_seq": 1,
            "logical_bytes": 2 * 1024 * 1024,
            "disk_bytes": 1024 * 1024,
            "inherited_bytes": 0,
            "cumulative_bytes": 1024 * 1024,
            "generations": 1,
            "files": 1,
        }
    )
    w.close()

    r = XlsxReportRenderer(MagicMock(), series_path=sp)
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    r._sheet_snapshot_sizes(wb)
    wb.save(tmp_path / "o.xlsx")
    ws = openpyxl.load_workbook(tmp_path / "o.xlsx")["Snapshot sizes"]
    assert ws.max_row >= 2
    assert len(ws._charts) >= 1


def test_step_detail_sheet_breaks_down_per_trajectory(tmp_path):
    """Step detail: one row per step event, broken down by trajectory_id,
    sorted by (trajectory, sandbox, step), with a frozen header + autofilter."""
    from unittest.mock import MagicMock

    from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

    sp = tmp_path / "s.jsonl"
    w = LifecycleSeriesWriter(sp)
    # Two trajectories, two sandboxes. Written out of order to exercise the sort.
    w.write(
        {
            "event": "step",
            "sandbox_index": 1,
            "trajectory_id": "traj-b",
            "round_id": 0,
            "step_index": 0,
            "action_type": "shell",
            "action": "ls -la /workspace",
            "slice_failed": False,
            # New invariants: resume_sec = inflight + api + ready (rate-pacing
            # resume_rate_pacing_wait is PRE-lease, NOT in resume_sec); pause_sec =
            # pause_rate_pacing_wait + pause_inflight + pause_api (rate-pacing IN-lease).
            "resume_sec": 0.08,
            "resume_rate_pacing_wait_sec": 0.02,  # rate-pacing, pre-lease
            "resume_inflight_wait_sec": 0.0,
            "resume_api_sec": 0.05,
            "resume_ready_wait_sec": 0.03,
            "exec_sec": 0.5,
            "pause_sec": 0.20,
            "pause_rate_pacing_wait_sec": 0.04,  # rate-pacing, in-lease
            "pause_inflight_wait_sec": 0.0,
            "pause_api_sec": 0.16,
            "slice_total_sec": 0.78,  # resume_sec + exec + pause_sec
            "interaction_total_sec": 0.80,  # + resume rate-pacing (pre-lease)
            "slot_contention_wait_sec": 0.0,
            "natural_delay_sec": 0.0,
            "capacity_wait_sec": 0.0,
            "rate_pacing_wait_sec": 0.06,  # resume_rate_pacing_wait + pause_rate_pacing_wait
            "inflight_wait_sec": 0.0,
            "running_slot_held_sec": 0.78,
            "exit_code": 0,
            "timed_out": False,
        }
    )
    w.write(
        {
            "event": "step",
            "sandbox_index": 0,
            "trajectory_id": "traj-a",
            "round_id": 0,
            "step_index": 1,
            "action_type": "edit",
            "action": "str_replace_editor view file.py",
            "slice_failed": False,
            "resume_sec": 0.06,
            "resume_rate_pacing_wait_sec": 0.0,
            "resume_inflight_wait_sec": 0.0,
            "resume_api_sec": 0.04,
            "resume_ready_wait_sec": 0.02,
            "exec_sec": 0.4,
            "pause_sec": 0.14,
            "pause_rate_pacing_wait_sec": 0.03,
            "pause_inflight_wait_sec": 0.0,
            "pause_api_sec": 0.11,
            "slice_total_sec": 0.60,
            "interaction_total_sec": 0.60,
            "slot_contention_wait_sec": 0.0,
            "natural_delay_sec": 0.0,
            "capacity_wait_sec": 0.0,
            "rate_pacing_wait_sec": 0.03,
            "inflight_wait_sec": 0.0,
            "running_slot_held_sec": 0.60,
            "exit_code": 0,
            "timed_out": False,
        }
    )
    # a failed step (slice_failed=True) is still emitted as a row, with all
    # wait components honestly zeroed
    w.write(
        {
            "event": "step",
            "sandbox_index": 0,
            "trajectory_id": "traj-a",
            "round_id": 0,
            "step_index": 0,
            "action_type": "shell",
            "action": "cat /etc/hosts",
            "slice_failed": True,
            "resume_sec": 0.0,
            "resume_rate_pacing_wait_sec": 0.0,
            "resume_inflight_wait_sec": 0.0,
            "resume_api_sec": 0.0,
            "resume_ready_wait_sec": 0.0,
            "exec_sec": 0.0,
            "pause_sec": 0.0,
            "pause_rate_pacing_wait_sec": 0.0,
            "pause_inflight_wait_sec": 0.0,
            "pause_api_sec": 0.0,
            "slice_total_sec": 0.0,
            "interaction_total_sec": 0.0,
            "slot_contention_wait_sec": 0.0,
            "natural_delay_sec": 0.0,
            "capacity_wait_sec": 0.0,
            "rate_pacing_wait_sec": 0.0,
            "inflight_wait_sec": 0.0,
            "running_slot_held_sec": 0.0,
            "exit_code": 1,
            "timed_out": True,
        }
    )
    # a non-step event is ignored
    w.write({"event": "snapshot_size", "sandbox_index": 0})
    w.close()

    r = XlsxReportRenderer(MagicMock(), series_path=sp)
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    r._sheet_step_detail(wb)
    out = tmp_path / "o.xlsx"
    wb.save(out)
    ws = openpyxl.load_workbook(out)["Step detail"]

    headers = [c.value for c in ws[1]]
    assert "trajectory_id" in headers
    assert "exec_sec" in headers
    assert "slice_failed" in headers
    # the recorded command text surfaces per step; reading down a trajectory's
    # rows = its executed command sequence.
    assert "action" in headers
    act_col = headers.index("action") + 1
    # sorted order: traj-a step0 (failed), traj-a step1, traj-b step0
    assert ws.cell(2, act_col).value == "cat /etc/hosts"
    assert ws.cell(3, act_col).value == "str_replace_editor view file.py"
    assert ws.cell(4, act_col).value == "ls -la /workspace"
    # sub-decomposition columns (already emitted by the runner into the series)
    # are surfaced so the sum invariants are inspectable in the sheet itself.
    assert "resume_rate_pacing_wait_sec" in headers
    assert "resume_api_sec" in headers
    assert "resume_ready_wait_sec" in headers
    assert "pause_rate_pacing_wait_sec" in headers
    assert "pause_api_sec" in headers
    assert "running_slot_held_sec" in headers
    # wait-decoupling columns (the four independent components + per-phase inflight)
    assert "natural_delay_sec" in headers
    assert "capacity_wait_sec" in headers
    assert "rate_pacing_wait_sec" in headers
    assert "inflight_wait_sec" in headers
    assert "resume_inflight_wait_sec" in headers
    assert "pause_inflight_wait_sec" in headers
    # header + 3 data rows (the snapshot_size event is dropped)
    assert ws.max_row == 4
    # sorted: traj-a step0, traj-a step1, traj-b step0
    traj_col = headers.index("trajectory_id") + 1
    step_col = headers.index("step_index") + 1
    assert ws.cell(2, traj_col).value == "traj-a"
    assert ws.cell(2, step_col).value == 0
    assert ws.cell(3, traj_col).value == "traj-a"
    assert ws.cell(3, step_col).value == 1
    assert ws.cell(4, traj_col).value == "traj-b"
    # the failed step row carries slice_failed=True and timed_out=True, and all
    # wait components honestly zeroed.
    failed_col = headers.index("slice_failed") + 1
    timed_col = headers.index("timed_out") + 1
    assert ws.cell(2, failed_col).value is True
    assert ws.cell(2, timed_col).value is True
    assert ws.cell(2, headers.index("rate_pacing_wait_sec") + 1).value in (0.0, None)
    # Post-decoupling sum invariants on the success row (traj-a step1):
    #   resume_sec == resume_inflight + resume_api + resume_ready_wait
    #     (resume rate-pacing is PRE-lease, NOT in resume_sec -- the decoupling)
    #   pause_sec == pause_rate_pacing_wait + pause_inflight + pause_api
    #     (pause rate-pacing is IN-lease)
    rsm = ws.cell(3, headers.index("resume_sec") + 1).value
    ri = ws.cell(3, headers.index("resume_inflight_wait_sec") + 1).value
    ra = ws.cell(3, headers.index("resume_api_sec") + 1).value
    rr = ws.cell(3, headers.index("resume_ready_wait_sec") + 1).value
    assert rsm is not None and round((ri or 0) + (ra or 0) + (rr or 0), 3) == rsm
    psm = ws.cell(3, headers.index("pause_sec") + 1).value
    pq = ws.cell(3, headers.index("pause_rate_pacing_wait_sec") + 1).value
    pi = ws.cell(3, headers.index("pause_inflight_wait_sec") + 1).value
    pa = ws.cell(3, headers.index("pause_api_sec") + 1).value
    assert psm is not None and round((pq or 0) + (pi or 0) + (pa or 0), 3) == psm
    # the resume rate-pacing that is excluded from resume_sec still shows up in
    # interaction_total (pre-lease wait is part of the full interaction budget).
    # traj-b row (row 4): interaction_total = slice_total + resume_rate_pacing_wait.
    it = ws.cell(4, headers.index("interaction_total_sec") + 1).value
    st = ws.cell(4, headers.index("slice_total_sec") + 1).value
    rqr = ws.cell(4, headers.index("resume_rate_pacing_wait_sec") + 1).value
    assert it is not None and st is not None and round((st or 0) + (rqr or 0), 3) == it
    # frozen header + autofilter on the data range
    assert ws.freeze_panes == "A2"
    assert ws.auto_filter.ref is not None


def test_step_detail_sheet_empty_without_series(tmp_path):
    """No series file -> Step detail sheet exists with a header only (no crash)."""
    from unittest.mock import MagicMock

    from openpyxl import Workbook

    r = XlsxReportRenderer(MagicMock(), series_path=None)
    wb = Workbook()
    wb.remove(wb.active)
    r._sheet_step_detail(wb)
    wb.save(tmp_path / "o.xlsx")
    ws = openpyxl.load_workbook(tmp_path / "o.xlsx")["Step detail"]
    assert ws.max_row == 1  # header only
    assert ws.cell(1, 1).value == "trajectory_id"


def test_trajectory_summary_attributes_cost_per_instance(tmp_path):
    """Trajectory summary: per-instance MEDIANS across runs (not sums) so each
    trajectory's wall-clock decomposes into pause/resume/exec/wait totals --
    "where did this trajectory's time go". With one run per trajectory (pool>=N)
    the median of one == the run's sums, so the cost-attribution decomposition
    still holds; with pool < N the median is across the N/pool runs (NOT an
    inflated sum). Percentiles live in the Step detail / Lifecycle overhead sheets."""
    from unittest.mock import MagicMock

    from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

    sp = tmp_path / "s.jsonl"
    w = LifecycleSeriesWriter(sp)
    # Two trajectories, 3 steps each on ONE sandbox (pool>=N: 1 run per tid),
    # distinct resume/pause/wait so the per-run sums are attributable. With 1 run
    # the median == the run's sums. slice_total = resume + exec + pause exactly.
    for tid, resume, pause, slot_wait in (("traj-a", 0.10, 0.20, 0.01), ("traj-b", 0.50, 0.60, 0.05)):
        for i in range(3):
            w.write(
                {
                    "event": "step",
                    "sandbox_index": 0,
                    "trajectory_id": tid,
                    "step_index": i,
                    "exec_sec": 0.4,
                    "resume_sec": resume,
                    "pause_sec": pause,
                    "slot_contention_wait_sec": slot_wait,
                    "resume_rate_pacing_wait_sec": 0.02,
                    "pause_rate_pacing_wait_sec": 0.03,
                    "running_slot_held_sec": 0.9,
                    "slice_total_sec": round(resume + 0.4 + pause, 3),
                    "interaction_total_sec": round(resume + 0.4 + pause + 0.05, 3),
                }
            )
    w.close()

    obs = MagicMock()
    obs.config.workflow_config.replay_mode = "lifecycle"
    r = XlsxReportRenderer(obs, series_path=sp)
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    r._sheet_trajectory_summary(wb)
    wb.save(tmp_path / "o.xlsx")
    ws = openpyxl.load_workbook(tmp_path / "o.xlsx")["Trajectory summary"]

    headers = [c.value for c in ws[1]]
    assert "trajectory_id" in headers
    assert "n_runs" in headers  # 1 run per tid (pool>=N)
    assert "n_steps_median" in headers
    # median-based cost-attribution columns (medians across runs; with n_runs==1
    # the median == the single run's sums, so the decomposition still holds)
    assert "slice_total_median_s" in headers
    assert "exec_median_s" in headers
    assert "resume_median_s" in headers
    assert "pause_median_s" in headers
    assert "slot_contention_wait_median_s" in headers
    assert "resume_rate_pacing_wait_median_s" in headers
    assert "pause_rate_pacing_wait_median_s" in headers
    assert "running_slot_held_median_s" in headers
    assert "avg_slice_median_s" in headers
    # no percentile columns remain (they live in Step detail / Lifecycle overhead)
    assert not any(h.endswith("_p50_s") for h in headers)
    # one row per trajectory (a, b), header + 2 rows
    assert ws.max_row == 3
    traj_col = headers.index("trajectory_id") + 1
    assert ws.cell(2, traj_col).value == "traj-a"
    assert ws.cell(3, traj_col).value == "traj-b"
    # traj-a (1 run, 3 steps): resume 3x0.10=0.30, pause 3x0.20=0.60, exec 3x0.4=1.2,
    # slice_total 3x0.70=2.10, slot_wait 3x0.01=0.03. Median of 1 == the sum.
    rs = headers.index("resume_median_s") + 1
    ps = headers.index("pause_median_s") + 1
    es = headers.index("exec_median_s") + 1
    ss = headers.index("slice_total_median_s") + 1
    sw = headers.index("slot_contention_wait_median_s") + 1
    assert ws.cell(2, rs).value == 0.3
    assert ws.cell(2, ps).value == 0.6
    assert ws.cell(2, es).value == 1.2
    assert ws.cell(2, ss).value == 2.1
    assert ws.cell(2, sw).value == 0.03
    # traj-b resume (1.50) > traj-a resume (0.30) -- per-instance separation
    assert ws.cell(3, rs).value == 1.5
    # sum invariant within the run: slice_total_median == resume+exec+pause medians
    assert round(ws.cell(2, rs).value + ws.cell(2, es).value + ws.cell(2, ps).value, 3) == ws.cell(2, ss).value
    # avg_slice = slice_total / n_steps (traj-a: 2.10 / 3 = 0.70); n_steps_median = 3
    n_col = headers.index("n_steps_median") + 1
    avg_col = headers.index("avg_slice_median_s") + 1
    assert ws.cell(2, n_col).value == 3
    assert ws.cell(2, avg_col).value == 0.7
    # n_runs column reports the per-trajectory run count (1 here = pool>=N)
    nr = headers.index("n_runs") + 1
    assert ws.cell(2, nr).value == 1


def test_trajectory_summary_has_stacked_cost_bar_chart(tmp_path):
    """The per-trajectory cost-attribution sums get a stacked bar chart so the
    "where did each trajectory's wall-clock go" table reads at a glance: exec /
    resume / pause stacked per trajectory (bar height == slice_total_sum)."""
    from unittest.mock import MagicMock

    from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

    sp = tmp_path / "s.jsonl"
    w = LifecycleSeriesWriter(sp)
    for tid, resume, pause in (("traj-a", 0.10, 0.20), ("traj-b", 0.50, 0.60)):
        for i in range(3):
            w.write(
                {
                    "event": "step",
                    "sandbox_index": i,
                    "trajectory_id": tid,
                    "step_index": i,
                    "exec_sec": 0.4,
                    "resume_sec": resume,
                    "pause_sec": pause,
                    "slice_total_sec": round(resume + 0.4 + pause, 3),
                    "interaction_total_sec": round(resume + 0.4 + pause + 0.05, 3),
                }
            )
    w.close()

    obs = MagicMock()
    obs.config.workflow_config.replay_mode = "lifecycle"
    r = XlsxReportRenderer(obs, series_path=sp)
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    r._sheet_trajectory_summary(wb)
    out = tmp_path / "o.xlsx"
    wb.save(out)
    ws = openpyxl.load_workbook(out)["Trajectory summary"]
    charts = ws._charts
    assert charts, "expected a stacked bar chart on Trajectory summary"
    ch = charts[0]
    # three stacked cost-decomposition series: exec / resume / pause
    assert len(ch.series) == 3, f"expected 3 cost series; got {len(ch.series)}"


def test_trajectory_summary_chart_legend_uses_header_names(tmp_path):
    """The stacked cost bar chart's legend must take titles from the HEADER row
    (exec_median_s / resume_median_s / pause_median_s), not from the first data
    row (which would surface the first trajectory's numeric medians as legend labels).

    Regression guard: header_row was derived as ws.max_row+1 on a fresh empty
    sheet, but openpyxl reports max_row==1 for an empty sheet, so the +1 aimed
    the chart's title Reference at row 2 (first data row) instead of row 1.
    """
    import re
    from unittest.mock import MagicMock

    from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

    sp = tmp_path / "s.jsonl"
    w = LifecycleSeriesWriter(sp)
    # One run per tid (sandbox_index=0); median of 1 == the run's sums.
    for tid, resume, pause in (("traj-a", 0.10, 0.20), ("traj-b", 0.50, 0.60)):
        for i in range(3):
            w.write(
                {
                    "event": "step",
                    "sandbox_index": 0,
                    "trajectory_id": tid,
                    "step_index": i,
                    "exec_sec": 0.4,
                    "resume_sec": resume,
                    "pause_sec": pause,
                    "slice_total_sec": round(resume + 0.4 + pause, 3),
                    "interaction_total_sec": round(resume + 0.4 + pause + 0.05, 3),
                }
            )
    w.close()

    obs = MagicMock()
    obs.config.workflow_config.replay_mode = "lifecycle"
    r = XlsxReportRenderer(obs, series_path=sp)
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    r._sheet_trajectory_summary(wb)
    out = tmp_path / "o.xlsx"
    wb.save(out)
    ws = load_workbook(out)["Trajectory summary"]
    ch = ws._charts[0]

    # Each series title is a StrRef into a header cell (titles_from_data=True).
    # Resolve the referenced cell and read its value -- must be a column name,
    # not a numeric median from the first trajectory's data row.
    labels = []
    for s in ch.series:
        f = s.tx.strRef.f if (s.tx is not None and s.tx.strRef is not None) else None
        m = re.search(r"!([A-Z]+)(\d+)$", f or "")
        assert m, f"unexpected series title reference: {f!r}"
        labels.append(ws[f"{m.group(1)}{m.group(2)}"].value)
    assert labels == [
        "exec_median_s",
        "resume_median_s",
        "pause_median_s",
    ], f"chart legend should be header column names, got {labels}"


def _traj_series_file(sp, events):
    """Write a list of step-event dicts to a lifecycle series JSONL file."""
    from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

    w = LifecycleSeriesWriter(sp)
    for ev in events:
        w.write(ev)
    w.close()


def _step_ev(
    tid,
    *,
    step_index=0,
    sandbox_index=0,
    exec_sec=0.4,
    resume_sec=0.1,
    pause_sec=0.2,
    slice_failed=False,
    timed_out=False,
    exit_code=0,
    slot_contention_wait_sec=0.0,
):
    s = round(resume_sec + exec_sec + pause_sec, 3)
    return {
        "event": "step",
        "sandbox_index": sandbox_index,
        "trajectory_id": tid,
        "step_index": step_index,
        "action_type": "shell",
        "resume_sec": resume_sec,
        "exec_sec": exec_sec,
        "pause_sec": pause_sec,
        "slice_total_sec": s,
        "interaction_total_sec": round(s + 0.05, 3),
        "slot_contention_wait_sec": slot_contention_wait_sec,
        "resume_rate_pacing_wait_sec": 0.0,
        "pause_rate_pacing_wait_sec": 0.0,
        "running_slot_held_sec": 0.0,
        "slice_failed": slice_failed,
        "timed_out": timed_out,
        "exit_code": exit_code,
    }


def test_trajectory_summary_has_failure_and_success_columns(tmp_path):
    """n_failed / n_timeout / success_rate columns let the user spot which
    trajectory fails or times out most -- the reference's Per-task summary has
    ok/failed/timeout; bench-core now matches + adds the rate."""
    from unittest.mock import MagicMock

    sp = tmp_path / "s.jsonl"
    _traj_series_file(
        sp,
        [
            # traj-a: 2 success + 1 failed slice
            _step_ev("traj-a", step_index=0),
            _step_ev("traj-a", step_index=1),
            _step_ev(
                "traj-a", step_index=2, slice_failed=True, exit_code=1, exec_sec=0.0, resume_sec=0.0, pause_sec=0.0
            ),
            # traj-b: 1 success + 1 timeout (exec completed but hit the timeout)
            _step_ev("traj-b", step_index=0),
            _step_ev("traj-b", step_index=1, timed_out=True, exit_code=124),
        ],
    )

    obs = MagicMock()
    obs.config.workflow_config.replay_mode = "lifecycle"
    r = XlsxReportRenderer(obs, series_path=sp)
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    r._sheet_trajectory_summary(wb)
    out = tmp_path / "o.xlsx"
    wb.save(out)
    ws = load_workbook(out)["Trajectory summary"]

    headers = [c.value for c in ws[1]]
    assert "n_failed" in headers  # runs that did not fully succeed (count OF runs)
    assert "n_timeout_median" in headers  # median per-run timeout count
    assert "success_rate" in headers

    traj_col = headers.index("trajectory_id") + 1
    nf = headers.index("n_failed") + 1
    nt = headers.index("n_timeout_median") + 1
    sr = headers.index("success_rate") + 1
    # traj-a (1 run, 2 success + 1 failed slice): the run's success_rate is 2/3
    # (<1.0) so it counts as a failed run -> n_failed=1; n_timeout_median=0.
    assert ws.cell(2, traj_col).value == "traj-a"
    assert ws.cell(2, nf).value == 1
    assert ws.cell(2, nt).value == 0
    assert round(ws.cell(2, sr).value, 6) == round(2 / 3, 6)
    # traj-b (1 run, 1 success + 1 timeout): timeout != slice_failed, so the run
    # fully succeeded -> n_failed=0; n_timeout_median=1; success_rate=1.0.
    assert ws.cell(3, traj_col).value == "traj-b"
    assert ws.cell(3, nf).value == 0
    assert ws.cell(3, nt).value == 1
    assert ws.cell(3, sr).value == 1.0


def test_trajectory_summary_has_data_bars_and_failure_color_scale(tmp_path):
    """Conditional formatting: data bars on slice_total_median_s + slot_contention_wait_median_s
    (longer bar = slower / more queueing) and a red color scale on n_failed --
    the at-a-glance outlier highlighting the reference's per-trial table lacks."""
    from unittest.mock import MagicMock

    sp = tmp_path / "s.jsonl"
    _traj_series_file(
        sp,
        [
            _step_ev("traj-a", step_index=0, slot_contention_wait_sec=0.01),
            _step_ev("traj-a", step_index=1, slot_contention_wait_sec=0.01),
            _step_ev("traj-b", step_index=0, exec_sec=2.0, slot_contention_wait_sec=0.5),
            _step_ev("traj-b", step_index=1, exec_sec=2.0, slot_contention_wait_sec=0.5),
        ],
    )

    obs = MagicMock()
    obs.config.workflow_config.replay_mode = "lifecycle"
    r = XlsxReportRenderer(obs, series_path=sp)
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    r._sheet_trajectory_summary(wb)
    out = tmp_path / "o.xlsx"
    wb.save(out)
    ws = load_workbook(out)["Trajectory summary"]

    headers = [c.value for c in ws[1]]
    from openpyxl.utils import get_column_letter

    slice_col = get_column_letter(headers.index("slice_total_median_s") + 1)
    slot_col = get_column_letter(headers.index("slot_contention_wait_median_s") + 1)
    nf_col = get_column_letter(headers.index("n_failed") + 1)

    rule_cols = []  # (type, covered-column-letter)
    for cf in ws.conditional_formatting:
        for rule in cf.rules:
            for rng in str(cf.sqref).split():
                col = "".join(ch for ch in rng.split(":")[0] if ch.isalpha())
                rule_cols.append((rule.type, col))
    types = {t for t, _ in rule_cols}
    cols_by_type = {t: {c for _, c in rule_cols if _[0] == t} for t in types}
    # simpler: gather per-type column sets
    databar_cols = {c for t, c in rule_cols if t == "dataBar"}
    colorscale_cols = {c for t, c in rule_cols if t == "colorScale"}
    assert slice_col in databar_cols, f"expected dataBar on {slice_col}; got {databar_cols}"
    assert slot_col in databar_cols, f"expected dataBar on {slot_col}; got {databar_cols}"
    assert nf_col in colorscale_cols, f"expected colorScale on {nf_col}; got {colorscale_cols}"


def test_trajectory_summary_chart_anchor_clear_of_table(tmp_path):
    """Adding the failure cluster widened the table to 15 columns; the chart
    anchor must be COMPUTED from the header count (not the old hardcoded "N2")
    so the floating chart never sits on top of a data column. Regression guard."""
    import re
    from unittest.mock import MagicMock

    from openpyxl.utils import column_index_from_string

    sp = tmp_path / "s.jsonl"
    _traj_series_file(sp, [_step_ev("traj-a"), _step_ev("traj-b")])

    obs = MagicMock()
    obs.config.workflow_config.replay_mode = "lifecycle"
    r = XlsxReportRenderer(obs, series_path=sp)
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    r._sheet_trajectory_summary(wb)
    ws = wb["Trajectory summary"]
    charts = ws._charts
    assert charts, "expected the cost chart"
    n_headers = ws.max_column  # 15
    # Read the anchor robustly (string in-memory; OneCellAnchor after reload).
    anchor = charts[0].anchor
    if isinstance(anchor, str):
        m = re.match(r"([A-Z]+)", anchor)
        anchor_col = column_index_from_string(m.group(1))
    else:
        marker = getattr(anchor, "from_", None) or getattr(anchor, "_from", None)
        anchor_col = marker.col + 1  # openpyxl markers are 0-based
    assert anchor_col > n_headers, (
        f"chart anchor col {anchor_col} must sit past the {n_headers}-col table; " f"anchor={anchor!r}"
    )


def test_trajectory_summary_cf_absent_without_series(tmp_path):
    """No series file -> no conditional-formatting rules (a DataBarRule on an
    empty/inverted range would corrupt the sheet). max_row stays 1."""
    obs, _ = _seeded_observability(replay_mode="lifecycle")
    path = tmp_path / "obs.xlsx"
    XlsxReportRenderer(obs).render(str(path))
    ws = load_workbook(str(path))["Trajectory summary"]
    assert ws.max_row == 1
    rules = [r for cf in ws.conditional_formatting for r in cf.rules]
    assert rules == [], f"no CF rules expected without a series; got {rules}"


def test_lifecycle_overhead_property_aggregates_ratio():
    """ReplayObservability.lifecycle_overhead exposes the (resume+pause)/slice
    ratio the txt report + run_summary compute inline, as a model-owned
    semantic metric (per the obs_xlsx layering rule: semantic metrics in the
    model, renderer only flattens raw series). Near-zero slices excluded."""
    obs, _ = _seeded_observability()  # lifecycle; resume=0.05 pause=0.05 slice=1.0 x3
    oh = obs.lifecycle_overhead
    assert oh is not None
    assert oh["n"] == 3
    # (0.05 + 0.05) / 1.0 = 10% on every step -> aggregate/mean/p95 all 10%
    assert round(oh["aggregate_pct"], 3) == 10.0
    assert round(oh["mean_pct"], 3) == 10.0
    assert round(oh["p50_pct"], 3) == 10.0
    assert round(oh["p95_pct"], 3) == 10.0


def test_lifecycle_overhead_property_none_without_slices():
    """No sandboxes -> no slices -> None; the Overview section is skipped
    (exec_only with no recorded steps, or a fully-failed run)."""
    cfg = KernelConfig(
        workflow_type="replay",
        total_count=1,
        test_duration=10,
        workflow_config=ReplayConfig(replay_mode="exec_only", replay_running_concurrency=1),
    )
    obs = ReplayObservability(cfg, {})
    assert obs.lifecycle_overhead is None


def test_overview_renders_per_step_timing_section(tmp_path):
    """Overview gains a Per-step timing subtable (exec / think / pause / resume)
    so the 4 phases of each step read on the summary sheet -- the reference
    make_obs_xlsx.py puts this in its Overview; bench-core now matches. exec is
    derived as slice_total - resume - pause; think is natural_delay (LLM)."""
    obs, _ = _seeded_observability(natural_delay_sec=0.2)  # resume=0.05 pause=0.05 slice=1.0 think=0.2
    path = tmp_path / "obs.xlsx"
    XlsxReportRenderer(obs).render(str(path))
    ws = load_workbook(str(path))["Overview"]
    all_rows = [[c.value for c in r] for r in ws.iter_rows()]
    # The Admission per-op subtable also carries "pause"/"resume" rows (operation
    # names), so read the Per-step timing subtable positionally from its banner,
    # not by col-A key (which would collide).
    start = next(i for i, r in enumerate(all_rows) if r and r[0] == "Per-step timing")
    assert all_rows[start + 1][:9] == [
        "segment",
        "n",
        "min_s",
        "max_s",
        "avg_s",
        "p50_s",
        "p90_s",
        "p95_s",
        "p99_s",
    ]
    seg_rows = {r[0]: r for r in all_rows[start + 2 : start + 6]}
    for seg in ("exec", "think", "pause", "resume"):
        assert seg in seg_rows, f"{seg} row missing from Per-step timing"
        assert seg_rows[seg][1] == 3, f"{seg} n != 3"
        # p50 <= p90 <= p95 -- guards the inserted p90 cell against header/row misalignment
        assert seg_rows[seg][5] <= seg_rows[seg][6] <= seg_rows[seg][7], f"{seg} percentile order broken"
    assert round(seg_rows["exec"][4], 3) == 0.9  # 1.0 - 0.05 - 0.05
    assert round(seg_rows["think"][4], 3) == 0.2  # natural_delay (LLM)
    assert round(seg_rows["pause"][4], 3) == 0.05
    assert round(seg_rows["resume"][4], 3) == 0.05


def test_overview_renders_lifecycle_overhead_kv(tmp_path):
    """Overview gains lifecycle_overhead_% kv rows (aggregate / mean / p95) read
    from the model property -- the ratio the txt report has but xlsx lacked."""
    obs, _ = _seeded_observability()  # lifecycle -> 10%
    path = tmp_path / "obs.xlsx"
    XlsxReportRenderer(obs).render(str(path))
    ws = load_workbook(str(path))["Overview"]
    kv = {
        r[0].value: r[1].value
        for r in ws.iter_rows(min_col=1, max_col=2)
        if r[0].value is not None and r[1].value is not None
    }
    assert "lifecycle_overhead_aggregate_pct" in kv
    assert round(kv["lifecycle_overhead_aggregate_pct"], 3) == 10.0
    assert "lifecycle_overhead_mean_pct" in kv
    assert "lifecycle_overhead_p95_pct" in kv
    assert round(kv["lifecycle_overhead_p95_pct"], 3) == 10.0


def test_overview_skips_lifecycle_overhead_in_exec_only(tmp_path):
    """exec_only has no pause/resume lifecycle; the overhead ratio is gated off
    (parity with the txt report's [Lifecycle Overhead] block, lifecycle mode
    only). Per-step timing still renders (exec dominates there)."""
    obs, _ = _seeded_observability(replay_mode="exec_only")
    path = tmp_path / "obs.xlsx"
    XlsxReportRenderer(obs).render(str(path))
    ws = load_workbook(str(path))["Overview"]
    keys = [r[0].value for r in ws.iter_rows(min_col=1, max_col=1) if r[0].value is not None]
    assert not any(k.startswith("lifecycle_overhead") for k in keys), keys


def test_paused_gaps_gate_follows_replay_mode():
    """Cross-step idle is painted "paused" only in the modes that really
    snapshot. The series cannot carry that distinction (exec_ephemeral's no-op
    pause still burns ~1us, so its timestamp pairs are non-zero-width), so the
    renderer must read it off the run's replay mode -- the same signal the
    kernel already resolved before dispatch."""
    for mode, expected in (
        ("lifecycle", True),
        ("exec_only", False),
        ("exec_ephemeral", False),
    ):
        obs, _ = _seeded_observability(replay_mode=mode)
        assert XlsxReportRenderer(obs)._paused_gaps() is expected, mode
    # No workflow_config (non-replay / hand-built config): gate closed rather
    # than raising -- a reporting path must never fail a finished run.
    assert XlsxReportRenderer(ReplayObservability(KernelConfig(workflow_type="replay"), {}))._paused_gaps() is False
