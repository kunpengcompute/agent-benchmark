"""Shard artifact collection, merge, and merged-report rendering.

Split out of :mod:`bench_core.oversub_shards` so the planning/launch half stays
small; re-exported there for the historical import path.
"""
from __future__ import annotations

import csv
import json
import logging
import statistics
from dataclasses import dataclass, field
from pathlib import Path

from bench_core.oversub_shards import GroupResult, ShardPlan, ShardSpec

logger = logging.getLogger(__name__)

# Run-scoped fields on an index.json trajectory entry: they describe ONE run
# and must never be inherited as merged-entry scalars (they live on runs[]).
_RUN_SCOPED_KEYS = (
    "sandbox_index",
    "round_id",
    "n_steps",
    "n_failed",
    "n_timeout",
    "success_rate",
    "success",
    "elapsed_sec",
    "time_breakdown_sec",
    "create_error_type",
    "kill_error_type",
)

# Entry-level AGGREGATES recomputed from the merged runs[] with the kernel's
# own rules (trajectory_summary.aggregate_trajectories + trajectory_export.
# _index_row). Never inherited from one shard's entry: that value describes
# only the shard's runs subset (inheriting shard 0's success_rate/error types/
# breakdown p50s would silently drop every other shard's data). ``file`` is
# re-set to None: the deep per-trajectory replay_result.json artifacts stay in
# each shard's run dir (shard_dirs in the merged summary is the provenance).
_AGGREGATE_KEYS = (
    "n_runs",
    "n_success",
    "n_failed",
    "success_rate",
    "elapsed_sec",
    "all_failed",
    "n_steps_max",
    "time_breakdown_sec",
    "create_error_type",
    "kill_error_type",
    "file",
)

# Additive count keys inside run_summary["throughput"].
_THROUGHPUT_COUNT_KEYS = ("total", "succeeded", "failed", "total_steps")


# ---------------------------------------------------------------------------
# Artifact collection + merge
# ---------------------------------------------------------------------------


@dataclass
class ShardArtifacts:
    """What each shard left behind (None = the shard produced no summary)."""

    summaries: list[dict | None] = field(default_factory=list)
    summary_paths: list[str | None] = field(default_factory=list)
    catalogs: list[tuple[int, dict]] = field(default_factory=list)  # (slot_offset, index.json)

    @property
    def missing(self) -> list[int]:
        return [i for i, s in enumerate(self.summaries) if s is None]


def collect_shard_artifacts(plan: ShardPlan, prefix: str, *, since_epoch: float | None = None) -> ShardArtifacts:
    """Glob each shard's run_summary.json + trajectories/index.json.

    A crashed shard simply has no summary -- it surfaces in ``missing`` and in
    the merged summary's ``error``; it is never backfilled with fabricated
    zeros (a crash must not masquerade as a successful measurement), and
    ``compute_valid`` fails any summary carrying an ``error``.

    ``since_epoch`` (the wall clock just before this attempt launched) keeps a
    restarted sweep from merging the PREVIOUS attempt's summary: a shard dir
    accumulates one timestamped run dir per attempt, and a shard that crashes
    before flushing must read as missing rather than silently contributing an
    older run's counts. Summaries with no ``started_epoch`` fail the filter.
    """
    out = ShardArtifacts()
    for spec in plan.specs:
        hits = sorted(spec.directory.glob(f"{prefix}_*/{prefix}_run_summary.json"))
        chosen, summary = _current_attempt_summary(hits, since_epoch)
        if chosen is None:
            out.summaries.append(None)
            out.summary_paths.append(None)
            continue
        out.summaries.append(summary)
        out.summary_paths.append(str(chosen))
        idx_path = chosen.parent / "trajectories" / "index.json"
        if idx_path.exists():
            try:
                catalog = json.loads(idx_path.read_text(encoding="utf-8"))
                out.catalogs.append((spec.slot_offset, catalog))
            except (OSError, ValueError) as exc:
                logger.warning("shard %d: unreadable index.json (%s); trajectory rows will lack it", spec.index, exc)
        else:
            logger.warning(
                "shard %d: summary without trajectories/index.json; its runs vanish from "
                "the merged index and trajectory-detail rows",
                spec.index,
            )
    return out


def _current_attempt_summary(hits: list[Path], since_epoch: float | None) -> tuple[Path | None, dict | None]:
    """Newest summary written by THIS attempt, or ``(None, None)``.

    ``hits`` is sorted ascending by the timestamped run-dir name, so the newest
    candidate is tried first. Without ``since_epoch`` the newest hit wins (the
    pre-restart behavior); with it, a hit whose ``started_epoch`` predates the
    launch is a previous attempt's artifact and is skipped.
    """
    for path in reversed(hits):
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            logger.warning("unreadable shard summary %s (%s); trying the next candidate", path, exc)
            continue
        if since_epoch is None or (summary.get("started_epoch") or 0) >= since_epoch:
            return path, summary
    return None, None


def _merged_throughput(summaries: list[dict], wall_sec: float | None) -> dict:
    """Counts sum across concurrent shards; rates recompute over the trial wall.

    Summing per-shard rates would overstate the fleet rate whenever shard
    walls differ (setup skew, a straggler); ``count / max_wall`` is the honest
    trial-level rate and matches the kernel's own ``succeeded / wall_sec``.
    """
    tp = {k: sum((s.get("throughput") or {}).get(k) or 0 for s in summaries) for k in _THROUGHPUT_COUNT_KEYS}
    tp["tasks_per_sec"] = round(tp["succeeded"] / wall_sec, 3) if wall_sec else None
    tp["steps_per_sec"] = round(tp["total_steps"] / wall_sec, 3) if wall_sec else None
    return tp


def _merged_span(summaries: list[dict]) -> dict:
    """min start / max end across shards (the trial's wall-clock envelope).

    ``wall_sec`` is the ENVELOPE (max completed − min started), not max of the
    shard walls: the kernel's own ``wall_sec`` is measured over exactly its
    ``[started_epoch, completed_epoch]`` span, so the envelope is what a single
    process running k×N slots would have reported -- and every rate is divided
    by it. Max-of-walls would overstate fleet throughput whenever shards start
    or finish at different times. Falls back to max wall when epochs are
    incomplete (a partially-flushed shard summary).
    """
    started = [(s.get("started_epoch"), s.get("started_at")) for s in summaries if s.get("started_epoch")]
    completed = [(s.get("completed_epoch"), s.get("completed_at")) for s in summaries if s.get("completed_epoch")]
    walls = [s.get("wall_sec") for s in summaries if s.get("wall_sec") is not None]
    start_epoch = min(started)[0] if started else None
    end_epoch = max(completed)[0] if completed else None
    if start_epoch is not None and end_epoch is not None:
        wall = round(end_epoch - start_epoch, 3)
    else:
        wall = round(max(walls), 3) if walls else None
    return {
        "started_epoch": start_epoch,
        "started_at": min(started)[1] if started else None,
        "completed_epoch": end_epoch,
        "completed_at": max(completed)[1] if completed else None,
        "wall_sec": wall,
        "shard_start_skew_sec": (
            round(max(e for e, _ in started) - start_epoch, 3) if len(started) > 1 and start_epoch else None
        ),
    }


def _merged_admission(summaries: list[dict]) -> dict | None:
    """None when no shard had admission (the shardable modes); else merge.

    Defensive path only -- SHARDABLE_MODES run admission-free, so today this
    always returns None. Kept correct (peak = max, counts = sum, wait =
    grant-weighted) so a future permit-server design inherits sound algebra.
    """
    blocks = [s.get("admission") for s in summaries if s.get("admission")]
    if not blocks:
        return None
    granted = sum(b.get("granted") or 0 for b in blocks)
    wait = sum((b.get("avg_queue_wait_sec") or 0) * (b.get("granted") or 0) for b in blocks)
    peaks = [b.get("peak_active") for b in blocks if b.get("peak_active") is not None]
    return {
        "maximum": sum(b.get("maximum") or 0 for b in blocks),
        "peak_active": max(peaks) if peaks else None,
        "granted": granted,
        "avg_queue_wait_sec": round(wait / granted, 6) if granted else 0.0,
        "control_qps": blocks[0].get("control_qps"),
        "control_dispatched": sum(b.get("control_dispatched") or 0 for b in blocks),
    }


def _merged_lifecycle_overhead(summaries: list[dict]) -> dict | None:
    """Sums + recomputed pct; None when no shard reported it (exec modes)."""
    blocks = [s.get("lifecycle_overhead") for s in summaries if s.get("lifecycle_overhead")]
    if not blocks:
        return None
    pause = sum(b.get("pause_sec_sum") or 0 for b in blocks)
    resume = sum(b.get("resume_sec_sum") or 0 for b in blocks)
    # slice_total is not carried in the block; reconstruct it per shard from
    # pct. Unreachable for SHARDABLE_MODES (exec modes report no overhead) --
    # kept honest for a future permit-server design, not precise to the digit.
    slice_sum = sum(
        ((b.get("pause_sec_sum") or 0) + (b.get("resume_sec_sum") or 0)) * 100 / b["pct_of_slice_total"]
        for b in blocks
        if b.get("pct_of_slice_total")
    )
    return {
        "pause_sec_sum": round(pause, 6),
        "resume_sec_sum": round(resume, 6),
        "pct_of_slice_total": round((pause + resume) / slice_sum * 100, 3) if slice_sum else 0.0,
    }


def _merged_error(summaries: list[dict], artifacts: ShardArtifacts, result: GroupResult) -> str | None:
    """First real problem, aggregated: shard errors + missing summaries + bad rcs."""
    parts = [s.get("error") for s in summaries if s.get("error")]
    for i in artifacts.missing:
        parts.append(f"shard {i}: no run_summary.json (crashed or killed before flush)")
    for i, rc in enumerate(result.returncodes):
        if rc != 0:
            parts.append(f"shard {i}: exit code {rc}")
    if result.timed_out:
        parts.append("trial deadline breached; shards terminated")
    return "; ".join(str(p) for p in parts) if parts else None


def merge_run_summaries(
    summaries: list[dict],
    *,
    plan: ShardPlan,
    artifacts: ShardArtifacts,
    result: GroupResult,
) -> dict:
    """Compose P shard summaries into one schema-faithful trial summary.

    Schema-2 keys keep their exact meaning at trial level (counts summed,
    wall = envelope, rates recomputed over it, span = envelope); shard
    provenance rides in ADDITIONAL keys (``shard_*``) so a consumer that only
    knows schema 2 reads the merged file as if one kernel run of k*N slots had
    produced it.
    """
    present = [s for s in summaries if s is not None]
    if not present:
        return {
            "throughput": {},
            "test_duration": None,
            "error": _merged_error([], artifacts, result) or "no shard run_summary.json",
        }
    first = present[0]
    span = _merged_span(present)
    tp = _merged_throughput(present, span["wall_sec"])
    total_count = sum(s.get("total_count") or 0 for s in present) or None
    running_concurrency = next((s.get("running_concurrency") for s in present if s.get("running_concurrency")), None)
    vm_dirs = [
        (s.get("paths") or {}).get("vm_monitor_dir") for s in present if (s.get("paths") or {}).get("vm_monitor_dir")
    ]
    return {
        "schema_version": first.get("schema_version", 2),
        "workflow_type": first.get("workflow_type"),
        "replay_mode": first.get("replay_mode"),
        "provider": first.get("provider"),
        "started_at": span["started_at"],
        "completed_at": span["completed_at"],
        "started_epoch": span["started_epoch"],
        "completed_epoch": span["completed_epoch"],
        "test_duration": first.get("test_duration"),
        "wall_sec": span["wall_sec"],
        "total_count": total_count,
        "running_concurrency": running_concurrency,
        "overcommit_ratio": (
            round(total_count / running_concurrency, 3)
            if total_count and running_concurrency
            else first.get("overcommit_ratio")
        ),
        "throughput": tp,
        "admission": _merged_admission(present),
        "lifecycle_overhead": _merged_lifecycle_overhead(present),
        "paths": {
            # The per-shard txt reports stay under shard_dirs (no consumer
            # reads a merged report path). The lifecycle series is
            # concatenated fleet-wide (merge_lifecycle_series) and the obs
            # workbook rendered fleet-wide (render_merged_obs); the driver
            # patches both paths in after this returns.
            "report": None,
            "obs_xlsx": None,
            "lifecycle_series": None,
            # Patched by write_merged_artifacts when a merged index is written.
            "trajectory_index": None,
            "vm_monitor_dir": vm_dirs[0] if vm_dirs else None,
        },
        "error": _merged_error(present, artifacts, result),
        # --- shard provenance (additive; not part of the schema-2 contract) ---
        "shard_count": plan.count,
        "shard_start_skew_sec": span["shard_start_skew_sec"],
        "shard_return_codes": result.returncodes,
        "shard_run_summaries": artifacts.summary_paths,
        "shard_dirs": [str(spec.directory) for spec in plan.specs],
    }


def _promote_legacy_entry(entry: dict) -> list[dict]:
    """Old index.json shape (no ``runs[]``): the entry IS its single run."""
    return [{k: entry.get(k) for k in _RUN_SCOPED_KEYS}]


def _none_last(value) -> tuple[int, float]:
    """None-safe ascending sort key (mirrors the kernel's _sort_key intent)."""
    return (1, 0.0) if value is None else (0, float(value))


def _p50(values: list[float]) -> float:
    """The kernel's p50 (utils.calc_percentiles): upper median, NOT the mean of
    the middle two for even n -- statistics.median would diverge at k=2."""
    ordered = sorted(values)
    return ordered[min(len(ordered) // 2, len(ordered) - 1)]


def _aggregate_entry(merged: dict) -> None:
    """Recompute one merged entry's aggregates from its runs[], kernel rules.

    Mirrors ``aggregate_trajectories`` + ``_index_row`` exactly: counts are
    sums; ``success_rate`` is the mean of per-run rates; headline
    ``elapsed_sec`` is the success-run median, falling back to the all-run
    median + ``all_failed`` when nothing succeeded; ``n_steps_max`` is the max
    across runs; entry-level ``time_breakdown_sec`` is the per-segment median
    over the same use-set (successful runs, else all); error types are
    first-seen across the runs. Runs are sorted by ``(sandbox_index,
    round_id)`` like the kernel's index rows.
    """
    runs = sorted(merged["runs"], key=lambda r: (_none_last(r.get("sandbox_index")), _none_last(r.get("round_id"))))
    merged["runs"] = runs
    success = [r for r in runs if r.get("success_rate") is not None and r["success_rate"] >= 1.0]
    merged["n_runs"] = len(runs)
    merged["n_success"] = len(success)
    merged["n_failed"] = len(runs) - len(success)
    merged["all_failed"] = bool(runs) and not success
    rates = [r["success_rate"] for r in runs if r.get("success_rate") is not None]
    merged["success_rate"] = round(sum(rates) / len(rates), 6) if rates else None
    use_set = success or runs
    elapsed = [r["elapsed_sec"] for r in use_set if r.get("elapsed_sec") is not None]
    merged["elapsed_sec"] = round(_p50(elapsed), 6) if elapsed else None
    merged["n_steps_max"] = max((r.get("n_steps") or 0 for r in runs), default=0)
    seg_keys = sorted({k for r in use_set for k in (r.get("time_breakdown_sec") or {})})
    merged["time_breakdown_sec"] = {
        k: round(_p50([(r.get("time_breakdown_sec") or {}).get(k, 0.0) for r in use_set]), 6) for k in seg_keys
    }
    merged["create_error_type"] = next((r["create_error_type"] for r in runs if r.get("create_error_type")), None)
    merged["kill_error_type"] = next((r["kill_error_type"] for r in runs if r.get("kill_error_type")), None)
    merged["file"] = None


def merge_trajectory_indexes(catalogs: list[tuple[int, dict]]) -> dict:
    """Merge per-shard catalogs into one trial-level ``index.json`` shape.

    Runs are grouped by ``trajectory_id`` (every shard replays the same pool in
    the group-aligned P <= ratio case, so the same trajectory appears in
    several shards; on the P > ratio slot split a shard smaller than the pool
    covers only a part of it) and each run's
    ``sandbox_index`` is re-keyed by the shard's slot offset -- the run
    identity ``(trajectory_id, sandbox_index, round_id)`` stays unique across
    the merged file, exactly as in a single-process k*N trial. Entry-level
    aggregates are recomputed from the merged runs with the kernel's own rules
    (:func:`_aggregate_entry`); only true per-trajectory scalars
    (``action_series`` and friends) are inherited first-non-null.
    """
    by_tid: dict[str, dict] = {}
    for offset, catalog in catalogs:
        for entry in catalog.get("trajectories", []):
            tid = str(entry.get("trajectory_id"))
            merged = by_tid.setdefault(tid, {"trajectory_id": entry.get("trajectory_id"), "runs": []})
            for key, value in entry.items():
                if key in ("runs", "trajectory_id") or key in _RUN_SCOPED_KEYS or key in _AGGREGATE_KEYS:
                    continue
                if merged.get(key) is None and value is not None:
                    merged[key] = value  # first non-null scalar wins (action_series, ...)
            runs = entry.get("runs") or _promote_legacy_entry(entry)
            for run in runs:
                run = dict(run)
                if run.get("sandbox_index") is not None:
                    run["sandbox_index"] = run["sandbox_index"] + offset
                merged["runs"].append(run)
    for merged in by_tid.values():
        _aggregate_entry(merged)
    return {
        "n_trajectories": len(by_tid),
        "trajectories": sorted(by_tid.values(), key=lambda e: str(e["trajectory_id"])),
        "merged_from_shards": len(catalogs),
    }


def merge_lifecycle_series(summaries: list[dict], plan: ShardPlan, merged_dir: Path, prefix: str) -> Path | None:
    """Concatenate the shards' lifecycle series into one trial-level JSONL.

    Without this a sharded trial has no fleet-wide series, and tools measuring
    inflight concurrency from it would see ONE shard (peak capped at N) --
    incomparable with the single-process series of a k*N trial. Each event's
    ``sandbox_index`` is re-keyed by the shard's slot offset (same rule as the
    merged index) so per-sandbox reconstruction never conflates two shards'
    sandboxes; consumers then work off event ``timestamp`` (the kernel's own
    writer ordering is burst-coalesced, not sorted), so a plain concat is
    contract-faithful. Returns ``None`` when no shard wrote a series.
    """
    out = merged_dir / f"{prefix}_lifecycle_series.jsonl"
    merged_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out, "w", encoding="utf-8") as dst:
        for spec, summary in zip(plan.specs, summaries):
            if summary is None:
                continue
            src = (summary.get("paths") or {}).get("lifecycle_series")
            if not src:
                continue
            if not Path(src).exists():
                # A shard that reported a series and lost it must not vanish
                # silently: the merged workbook would under-count its steps
                # while the merged summary still counts them.
                logger.warning(
                    "shard %d reported lifecycle series %s but it is missing; merged series omits its events",
                    spec.index,
                    src,
                )
                continue
            try:
                with open(src, encoding="utf-8") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            event = json.loads(line)
                        except ValueError:
                            logger.warning("dropping malformed series line from %s", src)
                            continue
                        if event.get("sandbox_index") is not None:
                            event["sandbox_index"] += spec.slot_offset
                        dst.write(json.dumps(event) + "\n")
                        written += 1
            except OSError as exc:
                logger.warning("shard series %s unreadable (%s); merged series is partial", src, exc)
    return out if written else None


def render_merged_obs(
    trial_cfg: dict,
    trial_dir: Path,
    prefix: str,
    *,
    wall_sec: float | None,
    series_path: Path,
    host_xlsx: Path | None = None,
) -> Path | None:
    """Render ONE fleet-wide obs workbook for a sharded trial.

    Each shard already wrote its own ``_obs.xlsx`` (its own N slots), which a
    k*N reader cannot consume as a trial result. This rebuilds the kernel's
    model over the merged series instead of concatenating those workbooks:
    the three model-backed sheets (Overview / Per-step timings / Lifecycle
    overhead) come from ``ReplayMetrics`` accumulators replayed out of the
    series by the kernel's own mutators, and the five series-backed sheets
    read the same merged JSONL the kernel renderer reads -- so the workbook is
    produced by one code path, not a shard-aware fork of it.

    ``wall_sec`` is the trial envelope (not max-of-shard-walls), matching the
    merged ``run_summary`` rates. Admission is absent by construction:
    shardable modes run admission-free. Runs after every shard has exited, so
    the render cost never lands inside a stress window.

    Returns ``None`` when the trial does not want a workbook
    (``report.format: txt``, the kernel's own gate) and on any failure (with a
    warning) -- the per-shard workbooks + merged JSON artifacts remain, and a
    reporting step must never fail a trial that already ran.
    """
    try:
        from bench_core.config import KernelConfig
        from bench_core.observability.lifecycle_reconstruct import replay_states_from_events
        from bench_core.observability.lifecycle_series import iter_events
        from bench_core.observability.obs_xlsx import XlsxReportRenderer
        from bench_core.observability.replay_obs import ReplayObservability

        config = KernelConfig.from_raw(trial_cfg)
        if config.report_format not in ("xlsx", "both"):
            return None
        # Stream the series: the rebuild never holds the event dicts, only the
        # accumulators (~9x smaller). The five series-backed sheets parse the
        # file again inside the renderer, exactly as the kernel's own path does.
        obs = ReplayObservability(
            config,
            replay_states_from_events(iter_events(series_path)),
            wall_sec=wall_sec,
        )
        path = trial_dir / "merged" / f"{prefix}_obs.xlsx"
        path.parent.mkdir(parents=True, exist_ok=True)
        XlsxReportRenderer(obs, series_path=Path(series_path), host_xlsx=host_xlsx).render(path)
        return path
    except Exception as exc:  # noqa: BLE001 - reporting must not fail the trial
        logger.warning("merged obs workbook skipped (%s: %s); per-shard workbooks remain", type(exc).__name__, exc)
        return None


def write_merged_artifacts(
    trial_dir: Path,
    prefix: str,
    summary: dict,
    index: dict | None,
) -> Path:
    """Write ``merged/{prefix}_run_summary.json`` (+ index); return summary path.

    The merged pair sits at a driver-owned fixed path (not a kernel timestamped
    run dir) so ``--reuse`` and ``run_summary_path`` consumers have one stable
    location; ``_trajectory_rows`` finds the index via the same parent-relative
    ``trajectories/index.json`` convention as a kernel run dir.
    """
    merged_dir = trial_dir / "merged"
    merged_dir.mkdir(parents=True, exist_ok=True)
    index_path = None
    if index is not None:
        (merged_dir / "trajectories").mkdir(parents=True, exist_ok=True)
        index_path = merged_dir / "trajectories" / "index.json"
        index_path.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")
        summary.setdefault("paths", {})["trajectory_index"] = str(index_path)
    summary_path = merged_dir / f"{prefix}_run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary_path
