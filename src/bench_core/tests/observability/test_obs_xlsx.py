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


class TestXlsxReportRenderer:
    def test_produces_all_expected_sheets(self, tmp_path):
        # Admission & QPS / Throughput & overcommit / Retry impact were folded
        # into Overview; the workbook now carries 8 base sheets and no standalone
        # scalar tabs. (Host-merged sheets are conditional, asserted separately.)
        obs, _ = _seeded_observability()
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs).render(str(path))
        names = load_workbook(str(path)).sheetnames
        for sheet in (
            "Overview",
            "Per-step timings",
            "Lifecycle overhead",
            "Trajectory summary",
            "Step detail",
            "Concurrency states",
            "Gantt",
            "Snapshot sizes",
        ):
            assert sheet in names, f"missing sheet {sheet}; got {names}"
        for removed in ("Admission & QPS", "Throughput & overcommit", "Retry impact"):
            assert removed not in names, f"{removed} was folded into Overview; got {names}"

    def test_overview_consolidates_admission_throughput_retry(self, tmp_path):
        # The former Admission & QPS / Throughput & overcommit / Retry impact
        # sheets' content now lives in Overview as grouped sections, so the
        # whole run reads on one sheet instead of four tiny scalar tabs.
        obs, _ = _seeded_observability()
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs).render(str(path))
        wb = load_workbook(str(path))
        ws = wb["Overview"]
        joined = " ".join(str(c.value) for row in ws.iter_rows() for c in row if c.value is not None)
        # Throughput & overcommit scalars
        assert "steps_per_sec" in joined
        assert "effective_parallelism" in joined
        # Admission & QPS scalars + the per-operation dispatched/waiting table
        assert "qps" in joined
        assert "operation" in joined and "dispatched" in joined
        # Retry impact scalars
        assert "retry_count" in joined
        assert "time_lost_to_retry_sec" in joined

    def test_overview_metric_name_column_is_colored(self, tmp_path):
        # The data-name column (col A) carries a solid fill so metric labels
        # stand out from their values; section banners delimit the groups.
        obs, _ = _seeded_observability()
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs).render(str(path))
        wb = load_workbook(str(path))
        ws = wb["Overview"]
        name_cell = None
        banner_cell = None
        for row in ws.iter_rows():
            if row[0].value == "workflow_type":
                name_cell = row[0]
            if row[0].value == "Run":
                banner_cell = row[0]
        assert name_cell is not None, "workflow_type metric row not found in Overview"
        assert name_cell.fill.fill_type == "solid", "metric-name column must be color-filled"
        assert name_cell.font.bold, "metric-name column must be bold"
        assert banner_cell is not None, "Run section banner not found"
        assert banner_cell.fill.fill_type == "solid", "section banner must be color-filled"

    def test_overview_admission_has_per_op_table(self, tmp_path):
        # The Admission per-operation dispatched/waiting table renders as a
        # sub-table inside Overview (operation/dispatched/waiting header). Compare
        # the first 3 cells: the Per-step timing subtable widens the sheet to 8
        # cols, so a 3-col header row is padded with trailing Nones.
        obs, _ = _seeded_observability()
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs).render(str(path))
        ws = load_workbook(str(path))["Overview"]
        header_rows = [[c.value for c in r] for r in ws.iter_rows()]
        assert any(row[:3] == ["operation", "dispatched", "waiting"] for row in header_rows)

    def test_overview_throughput_carries_values(self, tmp_path):
        obs, _ = _seeded_observability()
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs).render(str(path))
        ws = load_workbook(str(path))["Overview"]
        kv = {
            r[0].value: r[1].value
            for r in ws.iter_rows(min_col=1, max_col=2)
            if r[0].value is not None and r[1].value is not None
        }
        assert kv.get("steps_per_sec") == obs.steps_per_sec

    def test_overview_splits_failed_into_timed_out_and_non_timeout(self, tmp_path):
        # Run outcomes split into two non-overlapping buckets: timed out vs
        # non-zero exit code. success + timed_out + failed(non-timeout) must
        # equal total_steps (the base failed_count accumulates both).
        state = BenchSandbox.from_instance(SandboxInstance(id="x", index=0), "replay")
        m = ReplayMetrics()
        for _ in range(4):
            m.add(0.1, success=True)
        for _ in range(2):
            m.add(0.1, success=False, timeout=True)
        for _ in range(3):
            m.add(0.1, success=False)
        state.replay_metrics = m
        cfg = KernelConfig(
            workflow_type="replay",
            total_count=1,
            test_duration=10,
            workflow_config=ReplayConfig(replay_mode="exec_ephemeral", replay_running_concurrency=1),
        )
        obs = ReplayObservability(cfg, {0: state}, wall_sec=10.0)
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs).render(str(path))
        ws = load_workbook(str(path))["Overview"]
        kv = {
            r[0].value: r[1].value
            for r in ws.iter_rows(min_col=1, max_col=2)
            if r[0].value is not None and r[1].value is not None
        }
        assert kv["total_steps"] == 9
        assert kv["success"] == 4
        assert kv["timed_out"] == 2
        assert kv["failed (non-timeout)"] == 3
        assert kv["success"] + kv["timed_out"] + kv["failed (non-timeout)"] == kv["total_steps"]
        assert kv.get("concurrency") == obs.concurrency

    def test_overview_retry_carries_values(self, tmp_path):
        obs, _ = _seeded_observability(with_retry=True)
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs).render(str(path))
        ws = load_workbook(str(path))["Overview"]
        kv = {
            r[0].value: r[1].value
            for r in ws.iter_rows(min_col=1, max_col=2)
            if r[0].value is not None and r[1].value is not None
        }
        assert kv.get("retry_count") == obs.retry_count
        assert kv.get("time_lost_to_retry_sec") == obs.time_lost_to_retry_sec
        # per-operation retry_queued:<op> rows are present
        assert any(k.startswith("retry_queued:") for k in kv), kv

    def test_trajectory_sheet_omitted_content_when_not_trajectory(self, tmp_path):
        # lifecycle mode: Trajectory summary sheet exists but is empty/flagged
        obs, _ = _seeded_observability(replay_mode="lifecycle")
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs).render(str(path))
        wb = load_workbook(str(path))
        assert "Trajectory summary" in wb.sheetnames
        ws = wb["Trajectory summary"]
        # no series-backed trajectory runs -> header only
        assert ws.max_row == 1

    def test_host_sheets_ingested_in_single_render(self, tmp_path):
        from openpyxl import Workbook as _Wb

        host = tmp_path / "resource_report.xlsx"
        src = _Wb()
        ws = src.active
        ws.title = "VM_Stats"
        ws.cell(row=1, column=1, value="vm")
        ws.cell(row=2, column=1, value="fc-1")
        n2 = src.create_sheet("NUMA_Overview")
        n2.cell(row=1, column=1, value="node0")
        src.save(host)

        obs, _ = _seeded_observability()
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs, host_xlsx=host).render(str(path))
        wb = load_workbook(str(path))
        assert "VM_Stats" in wb.sheetnames
        assert wb["VM_Stats"]["A2"].value == "fc-1"
        assert "NUMA_Overview" in wb.sheetnames

    def test_host_merge_skipped_without_host_xlsx(self, tmp_path):
        obs, _ = _seeded_observability()
        path = tmp_path / "obs.xlsx"
        XlsxReportRenderer(obs, host_xlsx=None).render(str(path))
        wb = load_workbook(str(path))
        assert "VM_Stats" not in wb.sheetnames


def test_renderer_accepts_series_path_and_draws_per_step_linechart(tmp_path):
    from unittest.mock import MagicMock

    obs = MagicMock()
    obs.config.workflow_type = "replay"
    obs.config.workflow_config.replay_mode = "lifecycle"
    obs.config.total_count = 1
    obs.config.workflow_config.replay_running_concurrency = 1
    obs.config.test_duration = 10
    obs.wall_sec = 10.0
    obs.total_steps = 1
    obs.overcommit_ratio = 1.0
    obs.concurrency = 1
    obs.steps_per_sec = 0.1
    obs.effective_parallelism = 1.0
    obs.exec_wall_utilization = 1.0
    obs.retry_count = 0
    obs.retry_count_by_op = {}
    obs.time_lost_to_retry_sec = 0.0
    obs.retries_per_slice_p95 = 0.0
    obs.admission_snapshot = None
    obs.lifecycle_overhead = None  # Overview reads the model property; skip here
    # one sandbox with one step's durations
    m = MagicMock()
    m.latencies = [0.5]
    m.action_type_latencies = {}
    m.resume_secs = [0.1]
    m.pause_secs = [0.2]
    m.slice_total_secs = [0.8]
    m.running_slot_held_secs = [0.8]
    m.interaction_total_secs = [0.8]
    m.natural_delay_secs = []  # Overview per-step timing reads this
    m.success_count = 1
    m.failed_count = 0
    m.timeout_count = 0
    state = MagicMock()
    state.replay_metrics = m
    obs.states = {0: state}

    out = tmp_path / "obs.xlsx"
    XlsxReportRenderer(obs, series_path=None).render(out)
    wb = openpyxl.load_workbook(out)
    assert "Per-step timings" in wb.sheetnames
    # a LineChart exists on the sheet
    assert len(wb["Per-step timings"]._charts) >= 1


def test_per_step_linechart_references_all_data_rows(tmp_path):
    from unittest.mock import MagicMock

    from openpyxl import load_workbook

    obs = MagicMock()
    obs.config.workflow_type = "replay"
    obs.config.workflow_config.replay_mode = "lifecycle"
    obs.config.total_count = 1
    obs.config.workflow_config.replay_running_concurrency = 1
    obs.config.test_duration = 10
    obs.wall_sec = 10.0
    obs.total_steps = 3
    obs.overcommit_ratio = 1.0
    obs.concurrency = 1
    obs.steps_per_sec = 0.3
    obs.effective_parallelism = 1.0
    obs.exec_wall_utilization = 1.0
    obs.retry_count = 0
    obs.retry_count_by_op = {}
    obs.time_lost_to_retry_sec = 0.0
    obs.retries_per_slice_p95 = 0.0
    obs.admission_snapshot = None
    obs.lifecycle_overhead = None  # Overview reads the model property; skip here
    m = MagicMock()
    m.latencies = [0.1, 0.2, 0.3]  # THREE steps
    m.action_type_latencies = {}
    m.resume_secs = [0.1, 0.2, 0.3]
    m.pause_secs = [0.1, 0.2, 0.3]
    m.slice_total_secs = [0.5, 0.6, 0.7]
    m.running_slot_held_secs = [0.4, 0.5, 0.6]
    m.interaction_total_secs = [0.5, 0.6, 0.7]
    m.natural_delay_secs = []  # Overview per-step timing reads this
    m.success_count = 3
    m.failed_count = 0
    m.timeout_count = 0
    state = MagicMock()
    state.replay_metrics = m
    obs.states = {0: state}

    out = tmp_path / "obs.xlsx"
    XlsxReportRenderer(obs, series_path=None).render(out)
    wb = load_workbook(out)
    ws = wb["Per-step timings"]
    # the per-step detail block has 3 data rows; the latency series must
    # reference exactly 3 value cells (not 2 -- the off-by-one bug drops one).
    charts = ws._charts
    assert charts, "expected a LineChart on Per-step timings"
    # find the chart whose series references the latency_ms column (B)
    found = False
    for ch in charts:
        for s in ch.series:
            ref = getattr(s.val, "numRef", None)
            f = ref.f if ref is not None else None
            if f and "$B$" in f:
                # count rows in the range, e.g. '...!$B$6:$B$8' -> 3 rows
                import re

                m_ = re.search(r"\$B\$(\d+):\$B\$(\d+)", f)
                assert m_, f"unexpected val ref format: {f}"
                lo, hi = int(m_.group(1)), int(m_.group(2))
                assert hi - lo + 1 == 3, f"latency series references {hi-lo+1} rows, expected 3 (off-by-one? ref={f})"
                found = True
    assert found, "no LineChart series referenced the latency_ms (B) column"


def _mock_obs_with_n_steps(n: int, replay_mode: str = "lifecycle"):
    """A MagicMock observability with one sandbox carrying n recorded steps.

    Used by the large-series downsample tests (n well above MAX_PERSTEP_POINTS)
    so the per-step detail line charts would otherwise reference all n rows.
    """
    from unittest.mock import MagicMock

    obs = MagicMock()
    obs.config.workflow_type = "replay"
    obs.config.workflow_config.replay_mode = replay_mode
    obs.config.total_count = 1
    obs.config.workflow_config.replay_running_concurrency = 1
    obs.config.test_duration = 10
    obs.wall_sec = 10.0
    obs.total_steps = n
    obs.overcommit_ratio = 1.0
    obs.concurrency = 1
    obs.steps_per_sec = float(n) / 10.0
    obs.effective_parallelism = 1.0
    obs.exec_wall_utilization = 1.0
    obs.retry_count = 0
    obs.retry_count_by_op = {}
    obs.time_lost_to_retry_sec = 0.0
    obs.retries_per_slice_p95 = 0.0
    obs.admission_snapshot = None
    m = MagicMock()
    m.latencies = [0.1 + (i % 10) * 0.01 for i in range(n)]
    m.action_type_latencies = {}
    m.resume_secs = [0.1] * n
    m.pause_secs = [0.1] * n
    m.slice_total_secs = [0.5] * n
    m.running_slot_held_secs = [0.4] * n
    m.interaction_total_secs = [0.5] * n
    m.natural_delay_secs = []  # Overview per-step timing reads this (think/LLM)
    m.success_count = n
    m.failed_count = 0
    m.timeout_count = 0
    state = MagicMock()
    state.replay_metrics = m
    obs.states = {0: state}
    # Overview reads obs.lifecycle_overhead (model property); stub to None so the
    # overhead kv section is skipped (these tests target other sheets).
    obs.lifecycle_overhead = None
    return obs


def test_downsample_indices_bounds_and_preserves_endpoints():
    """Per-step line charts are downsampled to <= MAX_PERSTEP_POINTS so openpyxl
    does not inline a numCache for 150k referenced cells at save time (the
    1:1/384-sandbox run's Per-step + Lifecycle overhead charts each spanned
    ~150k rows). First + last steps are always kept so the chart x-range still
    spans the full run."""
    from bench_core.observability.obs_xlsx import MAX_PERSTEP_POINTS, _downsample_indices

    idxs = _downsample_indices(5000, 2000)
    assert len(idxs) <= 2000
    assert idxs[0] == 0  # first step kept
    assert idxs[-1] == 4999  # last step kept
    assert idxs == sorted(idxs)  # monotonic (chart x-axis order)
    # under cap: identity (every index kept, no downsampling)
    assert _downsample_indices(100, 2000) == list(range(100))
    assert 0 < MAX_PERSTEP_POINTS < 5000  # shipped cap is a real, finite bound


def test_per_step_linechart_downsamples_large_series(tmp_path):
    from bench_core.observability.obs_xlsx import MAX_PERSTEP_POINTS

    n = MAX_PERSTEP_POINTS + 500  # well over the cap
    obs = _mock_obs_with_n_steps(n)
    out = tmp_path / "obs.xlsx"
    XlsxReportRenderer(obs, series_path=None).render(out)
    ws = openpyxl.load_workbook(out)["Per-step timings"]
    charts = ws._charts
    assert charts, "expected a LineChart on Per-step timings"
    # every latency_ms (B) series references <= MAX_PERSTEP_POINTS rows, not n
    import re

    for ch in charts:
        for s in ch.series:
            ref = getattr(s.val, "numRef", None)
            f = ref.f if ref is not None else None
            if f and "$B$" in f:
                m_ = re.search(r"\$B\$(\d+):\$B\$(\d+)", f)
                assert m_, f"unexpected val ref format: {f}"
                span = int(m_.group(2)) - int(m_.group(1)) + 1
                assert span <= MAX_PERSTEP_POINTS, f"per-step chart references {span} rows, cap {MAX_PERSTEP_POINTS}"


def test_lifecycle_overhead_linechart_downsamples_large_series(tmp_path):
    from bench_core.observability.obs_xlsx import MAX_PERSTEP_POINTS

    n = MAX_PERSTEP_POINTS + 500
    obs = _mock_obs_with_n_steps(n)
    out = tmp_path / "obs.xlsx"
    XlsxReportRenderer(obs, series_path=None).render(out)
    ws = openpyxl.load_workbook(out)["Lifecycle overhead"]
    charts = ws._charts
    assert charts, "expected a LineChart on Lifecycle overhead"
    # the resume_ms series (col B) references <= MAX_PERSTEP_POINTS rows, not n
    import re

    found = False
    for ch in charts:
        for s in ch.series:
            ref = getattr(s.val, "numRef", None)
            f = ref.f if ref is not None else None
            if f and "$B$" in f:
                m_ = re.search(r"\$B\$(\d+):\$B\$(\d+)", f)
                assert m_, f"unexpected val ref format: {f}"
                span = int(m_.group(2)) - int(m_.group(1)) + 1
                assert span <= MAX_PERSTEP_POINTS, f"lifecycle chart references {span} rows, cap {MAX_PERSTEP_POINTS}"
                found = True
    assert found, "no LineChart series referenced the resume_ms (B) column"


def test_concurrency_states_sheet_from_series(tmp_path):
    from unittest.mock import MagicMock

    from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

    # write two step events spanning ~2 seconds
    sp = tmp_path / "s.jsonl"
    w = LifecycleSeriesWriter(sp)
    w.write(
        {
            "event": "step",
            "sandbox_index": 0,
            "step_index": 0,
            "resume_start": 0.0,
            "resume_end": 0.5,
            "exec_start": 0.5,
            "exec_end": 1.5,
            "pause_start": 1.5,
            "pause_end": 2.0,
        }
    )
    w.close()

    obs = MagicMock()  # minimal; the concurrency sheet reads only series_path
    r = XlsxReportRenderer(obs, series_path=sp)
    out = tmp_path / "o.xlsx"
    # stub the other sheets by calling just the new method
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    r._sheet_concurrency_states(wb)
    wb.save(out)
    wb2 = openpyxl.load_workbook(out)
    ws = wb2["Concurrency states"]
    # header + >=1 data row, one chart
    assert ws.max_row >= 2
    assert len(ws._charts) >= 1


def test_gantt_sheet_embeds_png(tmp_path):
    pytest.importorskip("matplotlib")
    from unittest.mock import MagicMock

    from bench_core.observability.lifecycle_series import LifecycleSeriesWriter

    sp = tmp_path / "s.jsonl"
    w = LifecycleSeriesWriter(sp)
    w.write(
        {
            "event": "step",
            "sandbox_index": 0,
            "step_index": 0,
            "resume_start": 0.0,
            "resume_end": 0.5,
            "exec_start": 0.5,
            "exec_end": 1.5,
            "pause_start": 1.5,
            "pause_end": 2.0,
        }
    )
    w.close()

    r = XlsxReportRenderer(MagicMock(), series_path=sp)
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    r._sheet_gantt(wb, out_png=tmp_path / "gantt.png")
    wb.save(tmp_path / "o.xlsx")
    assert (tmp_path / "gantt.png").exists()


def test_cap_gantt_segments_downsamples_when_over_cap():
    """Total Gantt segments above the cap are stride-downsampled per sandbox.

    A 1:1/384-sandbox lifecycle run yields ~600k phase segments; the renderer
    drew one ``ax.barh`` Patch per segment and hung for 24h. The cap bounds the
    count (``broken_barh`` in ``_sheet_gantt`` bounds the per-artist cost); the
    full-resolution timeline still lives in the Step detail sheet, so the
    overview Gantt losing resolution at extreme scale is acceptable.
    """
    from bench_core.observability.obs_xlsx import MAX_GANTT_SEGMENTS, _cap_gantt_segments

    # 3 sandboxes x 100 segments = 300 total; cap at a small value to exercise it.
    rows = [
        (f"sbx{i}", [(j, j + 1, ph) for j, ph in enumerate(["exec", "paused", "resuming", "pausing"] * 25)])
        for i in range(3)
    ]
    capped = _cap_gantt_segments(rows, max_segments=50)
    total = sum(len(segs) for _, segs in capped)
    assert total <= 50, f"segment cap not enforced: {total} > 50"
    # [::stride] preserves index 0 (each sandbox keeps its earliest segment)
    for (name, segs), (_, orig) in zip(capped, rows):
        assert segs[0] == orig[0]
    # labels + order preserved
    assert [n for n, _ in capped] == ["sbx0", "sbx1", "sbx2"]
    # the shipped cap is a real, finite bound
    assert 0 < MAX_GANTT_SEGMENTS < 600_000


def test_cap_gantt_segments_noop_under_cap():
    from bench_core.observability.obs_xlsx import _cap_gantt_segments

    rows = [("sbx0", [(0, 1, "exec"), (1, 2, "paused")])]
    assert _cap_gantt_segments(rows, max_segments=10_000) == rows
