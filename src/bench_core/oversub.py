"""Oversubscription benchmark sweep driver.

Runs the bench-core replay kernel across a matrix of memory/CPU
oversubscription ratios: ``running_concurrency`` (N) stays fixed (from the
base config), ``total_count = N * ratio (k)`` scales per trial. One
``bench-core`` invocation per trial; the driver reads each trial's
machine-readable ``run_summary.json`` (the only contract with the kernel) and
aggregates per-trial + per-ratio degradation curves.

Inspired by ``replay-aenv-main/scripts/run_pause_resume_oversubscription_benchmark.py``
but leaner (~400 lines) because the kernel owns the per-trial lifecycle,
admission, observability, trajectory export, and vm_monitor orchestration.

Admission-free modes (``exec_ephemeral`` / ``exec_only``) run PROCESS-SHARDED
by default (``--shards auto`` -> P = ratio): a single CPython process saturates
one GIL (~1 core, ~50-55 steps/s at N=384) and high-ratio trials turn
client-bound, so each trial splits into P kernel subprocesses of ~N slots --
the mechanism lives in :mod:`bench_core.oversub_shards`; this driver composes
it and merges the shard artifacts back into the single-trial
``run_summary.json`` / ``trajectories/index.json`` contract, so every
downstream consumer (validity, CSVs, compare, reuse) is shard-unaware.

The driver directly imports no kernel data-path internals (lifecycle /
admission / stats / task dispatch) -- only the CLI, the YAML schema, the
``run_summary.json`` schema, and the shared ``setup_logging`` CLI helper; the
host-monitor contract is reached through :mod:`bench_core.oversub_shards`.
Pure helpers are module-level so tests import them directly.
"""
from __future__ import annotations

import argparse
import copy
import csv
import json
import logging
import statistics
import subprocess
import time
from pathlib import Path

import yaml

from bench_core.oversub_shards import (
    ShardGroupRunner,
    ShardPlan,
    ShardStressBracket,
    build_shard_command,
    coerce_shards,
    collect_shard_artifacts,
    merge_lifecycle_series,
    merge_run_summaries,
    merge_trajectory_indexes,
    plan_shards,
    render_merged_obs,
    start_host_monitor,
    write_merged_artifacts,
    write_shard_configs,
)
from bench_core.utils import format_duration, setup_logging

logger = logging.getLogger(__name__)


def parse_ratios(text: str) -> list[int]:
    """Parse ``"1,2,3"`` -> ``[1, 2, 3]``; reject non-positive / empty."""
    vals: list[int] = []
    for tok in text.replace(" ", "").split(","):
        if not tok:
            continue
        v = int(tok)
        if v < 1:
            raise ValueError(f"ratio must be >= 1, got {v}")
        vals.append(v)
    if not vals:
        raise ValueError("no ratios given")
    return vals


def _coerce_ratios(v) -> list[int]:
    """YAML list ``[1, 2, 3]`` or comma string ``"1,2,3"`` -> ``[1, 2, 3]``.

    A sweep-config may carry either form (lists are idiomatic in YAML, strings
    match the ``--ratios`` CLI flag); normalize to a validated list of ints.
    """
    if isinstance(v, str):
        return parse_ratios(v)
    if isinstance(v, list | tuple):
        vals = [int(x) for x in v]
        if not vals or any(x < 1 for x in vals):
            raise ValueError(f"ratios must be positive, got {vals}")
        return vals
    raise ValueError(f"ratios must be a list or string, got {type(v).__name__}")


def _coerce_modes(v) -> list[str]:
    """YAML list or comma string -> stripped, non-empty mode names."""
    if isinstance(v, str):
        return [m.strip() for m in v.split(",") if m.strip()]
    if isinstance(v, list | tuple):
        return [str(m).strip() for m in v if str(m).strip()]
    raise ValueError(f"modes must be a list or string, got {type(v).__name__}")


def _coerce_cmd(v) -> list[str]:
    """Normalize a ``bench_core_bin`` value to a list of argv elements.

    A YAML scalar (``bench_core_bin: bench-core``) is ONE argv element -- the
    binary name -- not a shell line, so it is wrapped rather than split. Without
    this, ``[*args.bench_core_bin, ...]`` would spread a scalar string into
    chars (``['b', 'e', ...]``) and the driver would try to exec ``b``. Use a
    list (``[python, -m, stub]``) for a multi-element command.
    """
    if isinstance(v, str):
        return [v]
    if isinstance(v, list | tuple):
        return [str(x) for x in v]
    raise ValueError(f"bench_core_bin must be a string or list, got {type(v).__name__}")


def default_running_concurrency(base: dict) -> int:
    """N = ``replay.running_concurrency``, falling back to ``sandbox.total_count``."""
    replay = base.get("replay") or {}
    n = replay.get("running_concurrency")
    if n is None:
        n = (base.get("sandbox") or {}).get("total_count") or 0
    if n < 1:
        raise ValueError(f"running_concurrency must be >= 1, got {n}")
    return n


# Default outer wall-clock per trial (seconds). A defense-in-depth safety net: a
# hung kernel subprocess (e.g. a pathological post-kill render) cannot block the
# sweep indefinitely. 21600s = 6h -- generous enough that a legitimate large
# lifecycle trial (1152 trajectories, replay + render) completes well within it,
# while still catching a multi-hour hang instead of waiting forever. Override per
# sweep (--trial-timeout-sec) or disable with 0. The render-path caps (Gantt
# segment cap, per-step/overhead downsample) are the primary fix; this is the
# backstop so a future uncapped path cannot wedge the sweep again.
DEFAULT_TRIAL_TIMEOUT_SEC = 21600

# True defaults for the sweep knobs (used when neither a CLI flag nor a
# sweep-config value is given). Mirrors build_arg_parser; main() resolves
# CLI flag > sweep-config > these.
_TRUE_DEFAULTS: dict[str, object] = {
    "provider": "aenv",
    "running_concurrency": None,
    "ratios": "1,2,3",
    "modes": "exec_ephemeral,lifecycle",
    "repeats": 1,
    "test_duration": None,
    "failure_tolerance": 0.0,
    "cooldown_sec": 30,
    "cleanup_between_trials": "on",
    "trial_timeout_sec": DEFAULT_TRIAL_TIMEOUT_SEC,
    "output_root": None,
    "reuse": False,
    "stop_on_failure": False,
    "dry_run": False,
    "no_vm_monitor": False,
    "multiplier": 2,
    "shards": "auto",
    "bench_core_bin": ["bench-core"],
    "auto_compare": True,
}


def load_sweep_config(path: Path | str) -> dict:
    """Load a sweep-config YAML (mirrors the CLI knobs + ``base_config``).

    Returns the raw mapping; ``ratios``/``modes`` stay in their YAML form and
    are coerced later by main(). Unknown keys are rejected so a typo
    (``repeat:`` vs ``repeats:``) fails loudly instead of being silently ignored.
    """
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"sweep config must be a mapping, got {type(raw).__name__}")
    known = set(_TRUE_DEFAULTS) | {"base_config"}
    unknown = set(raw) - known
    if unknown:
        raise ValueError(f"unknown sweep-config key(s): {sorted(unknown)}")
    return raw


def build_trial_config(
    base: dict,
    *,
    mode: str,
    ratio: int,
    n: int,
    test_duration: int,
    trial_dir: str,
    prefix: str,
    multiplier: int | None = None,
) -> dict:
    """Deep-copy the base YAML and merge the per-trial oversub overrides.

    Overrides: ``sandbox.total_count = ratio*N``; ``replay.running_concurrency = N``
    (stays fixed); ``replay.trajectory_limit = N`` (fixes the issue set to the
    first N trajectories PER TEMPLATE across all ratios so only concurrency
    varies); ``replay.mode``; ``test.round_size = ratio*N`` (so all k*N
    sandboxes run concurrently in one group -- without this, k>=2 silently
    runs multiple sequential groups of N, corrupting the oversubscription
    dynamics); ``test.duration`` (the time ceiling); ``report.output_dir``;
    ``report.filename_prefix``. Everything else passes through unchanged --
    notably ``test.round_count`` (the work-amount knob: how many passes of the
    fleet). A trial ends at whichever comes first: ``round_count`` rounds OR
    ``test.duration`` seconds (the kernel's own ``round_count``/``duration``
    whichever-first semantics; see config/common/replay.yaml). Set
    ``round_count: 1`` (base default) for a bounded one-pass trial, ``0`` for a
    sustained-until-duration window.
    """
    cfg = copy.deepcopy(base)
    target = ratio * n
    cfg.setdefault("sandbox", {})["total_count"] = target
    cfg.setdefault("replay", {})["running_concurrency"] = n
    # Fix the issue set across ratios so only concurrency varies. Sets
    # trajectory_limit=N (the single-template strategy: first N per template).
    # For multi-template (T>1) the kernel's ``load_pool`` further clamps this to
    # N//T (the ratio-1 per-template fleet share under the balanced fleet) so
    # count_t <= fleet_t(ratio 1) and the running issue set stays fixed across
    # ratios -- without that clamp, capping to N lets only N/T issues per
    # template run at ratio 1 and the set grows with ratio until k>=T, drifting
    # the baseline. The driver cannot compute T itself (the trajectory pool /
    # manifest live on the test host, not necessarily the driver's box), so the
    # clamp is kernel-side. Per-template (not global first-N) keeps every
    # template covered in a multi-template manifest.
    cfg["replay"]["trajectory_limit"] = n
    cfg["replay"]["mode"] = mode
    cfg.setdefault("test", {})
    cfg["test"]["round_size"] = target
    cfg["test"]["duration"] = test_duration
    cfg.setdefault("report", {})
    cfg["report"]["output_dir"] = trial_dir
    cfg["report"]["filename_prefix"] = prefix
    # When multiplier (M) is set, switch this trial to fixed+exec_ephemeral+M:
    # each of the ratio*N sandboxes runs exactly M trajectories then exits
    # (stop-when-done). total_count/round_size/trajectory_limit are retained
    # for config-object compatibility but unused under fixed+M -- the per-slot
    # bound is replay.multiplier; round_count/round_size are round_robin knobs.
    if multiplier is not None:
        cfg["replay"]["multiplier"] = multiplier
        cfg["test"]["benchmark_mode"] = "fixed"
    return cfg


def parse_run_summary(path: Path | str) -> dict:
    """Read a trial's ``run_summary.json`` (the kernel's raw-facts contract)."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def compute_valid(
    summary: dict,
    return_code: int,
    *,
    n: int,
    failure_tolerance: float,
    multiplier: int | None = None,
    ratio: int | None = None,
) -> bool:
    """Run-to-completion validity (one pass of the k*N fleet).

    A trial runs ONE round (``round_count = 1``) of ``round_size = k*N``
    trajectories and is valid when it actually ran (close to) the full running
    fleet without over-admitting:

      * ``return_code == 0`` (the kernel subprocess succeeded),
      * the summary recorded no ``error`` (a merged sharded summary has no rc;
        a dead shard surfaces here instead),
      * ``throughput.total >= 0.9 * expected``. ``expected = N`` by default
        (mode-agnostic: a non-M trial runs ONE round, so ``total`` -- the
        trajectory-attempt count since run_summary schema 2 -- ~= k*N >= 0.9*N
        on a healthy run). When ``multiplier`` (M) is set
        (fixed+exec_ephemeral+M), each of the k*N sandboxes runs M trajectories,
        so ``expected = M * k * N`` and the floor reads ``throughput.succeeded``
        (the trajectory-completion count = ``sum(trajectory_completions)`` =
        M*k*N on a healthy run) -- NOT ``throughput.total`` (attempts shrink
        when a slot gives up on persistent create-failure, so only completions
        measure the work actually done). This catches a
        silent under-run where slots crashed / exited early before finishing
        their M (mirrors reference B's ``total == target_count`` check).
        Per-trajectory completion is inspectable in trajectory-detail.csv --
        ``valid`` is the gross-failure / over-admission gate, not a "every
        trajectory succeeded" bar,
      * ``admission.peak_active <= N`` (never ran more than N concurrent --
        dropped for exec_only, which has no admission controller),
      * ``failure_rate <= failure_tolerance`` (configurable wrap-noise allowance;
        1:1 modes only -- skipped when ``multiplier`` is set: the M succeeded-floor
        above is the sole gate there, and ``failure_tolerance`` is tuned for
        single-round wrap noise, not M-pass fleets).
    """
    if return_code != 0:
        return False
    # A summary that recorded a problem is not a valid measurement, even when
    # the process rc reads 0. This is the sharded-trial path's only failure
    # signal on --reuse: the merged summary has no single rc (a dead shard
    # surfaces in ``error`` / ``shard_return_codes``), so without this gate a
    # shard-crashed trial whose surviving shards still cleared the count floor
    # would be reused and reported as valid.
    if summary.get("error"):
        return False
    tp = summary.get("throughput") or {}
    # M-aware floor: with multiplier each sandbox runs M trajectories, so the
    # trajectory-completion count is throughput.succeeded (= M*ratio*N on a
    # healthy run). throughput.total counts ATTEMPTS (schema 2), which under-
    # counts when slots give up early -- read succeeded so the floor measures
    # completed work against M*ratio*N. (Schema-1 summaries had total = slot
    # count and a dimensionally broken negative failed; the gates below read
    # the same fields, so rescoring an old file behaves as it did then.)
    if multiplier:
        actual = tp.get("succeeded", 0)
        expected = multiplier * (ratio or 1) * n
    else:
        actual = tp.get("total", 0)
        expected = n
    if actual < 0.9 * expected:
        return False
    adm = summary.get("admission")
    if adm and adm.get("peak_active") is not None and adm["peak_active"] > n:
        return False
    if not multiplier:
        # failure_rate is the 1:1 (single-round) gate; for M>1 the succeeded
        # floor above is the sole gate (step failures already reduce
        # completions).
        failed = tp.get("failed", 0)
        failure_rate = failed / actual if actual else 1.0
        if failure_rate > failure_tolerance:
            return False
    return True


# trial-summary.csv column order (mirrors the reference TRIAL_COLUMNS + the
# sustained-mode failure_rate / test_duration columns).
#
# Two throughput calibers, both kept so a reader can pick the right one:
#   tasks_per_sec      -- succeeded / wall_sec: end-to-end, INCLUDES each
#                         round's create and kill cost.
#   stress_tasks_per_sec -- derived from the per-run stress windows, create/kill
#                         excluded; the caliber to score when sandbox churn is
#                         a server-side variable being diagnosed, not workload.
#
# Interrupted trials: a trial halted mid-run by Ctrl-C / driver SIGTERM (the
# kernel's SIGTERM-cooperative `finally` flushes a PARTIAL run_summary.json +
# trajectories/index.json) surfaces here as ``return_code == 130`` and
# ``valid == False`` -- its ``total`` / ``wall_sec`` reflect only what ran
# before the interrupt, which is itself the degradation signal (a ratio that
# stalls at 200/1152 trajectories is exactly the oversub breakdown). An
# interrupted trial is the END of the sweep (the driver halts); a timed-out
# trial (non-zero rc, != 130) is NOT -- the sweep continues to the next ratio.
# ``_interrupted`` is an internal row sentinel (not a column; DictWriter's
# extrasaction="ignore" drops it) that `main` reads to decide halt-vs-continue.
TRIAL_COLUMNS = [
    "mode",
    "ratio",
    "oversubscription_ratio",
    "repeat",
    "running_concurrency",
    "target_count",
    "total",
    "succeeded",
    "failed",
    "failure_rate",
    "peak_active",
    "granted",
    "avg_queue_wait_sec",
    "control_dispatched",
    "test_duration",
    "wall_sec",
    "tasks_per_sec",
    "mean_stress_sec",
    "stress_tasks_per_sec",
    "steps_per_sec",
    "lifecycle_overhead_pct",
    "return_code",
    "valid",
    "reused",
    "started_at",
    "completed_at",
    "error",
    "trial_dir",
    "run_summary_path",
]

RATIO_COLUMNS = [
    "mode",
    "ratio",
    "attempted",
    "successful",
    "all_repeats_successful",
    "median_wall_sec",
    "median_tasks_per_sec",
    "median_stress_sec",
    "median_stress_tasks_per_sec",
    "median_peak_active",
    "median_queue_wait_sec",
    "avg_cmd_exec_sec",
    "median_cmd_exec_sec",
    "cmd_exec_degradation_vs_1_1_pct",
    "time_degradation_vs_1_1_pct",
    "throughput_gain_vs_1_1_pct",
    "stress_throughput_gain_vs_1_1_pct",
]

# Per-trajectory timing breakdown columns. Sourced from the kernel-emitted
# ``time_breakdown_sec`` sub-dict in trajectories/index.json; together they
# decompose ``elapsed_sec`` into exec / lifecycle-API (resume+pause) /
# inter-step think delay / create+kill cost / queueing waits, so a reader can
# attribute where each trajectory's wall time went without opening replay_result.json.
# ``stress_sec`` is the stress window (elapsed - create - kill, clamped at 0):
# ``elapsed_sec = create_sec + kill_sec + stress_sec`` holds per run row.
# index.json short key -> CSV column name (kept aligned with trajectory_export._index_row).
_TRAJECTORY_BREAKDOWN_COLUMNS = [
    "slice_total_sec",
    "exec_sec",
    "resume_sec",
    "pause_sec",
    "requested_delay_sec",
    "create_sec",
    "kill_sec",
    "stress_sec",
    "interaction_total_sec",
    "slot_contention_wait_sec",
    # Wait-decoupling split (the four independent components + per-phase inflight).
    "natural_delay_sec",
    "capacity_wait_sec",
    "rate_pacing_wait_sec",
    "inflight_wait_sec",
    "resume_rate_pacing_wait_sec",
    "pause_rate_pacing_wait_sec",
    "resume_inflight_wait_sec",
    "pause_inflight_wait_sec",
    "running_slot_held_sec",
]
_BREAKDOWN_KEY_TO_COL = {
    "slice_total": "slice_total_sec",
    "exec": "exec_sec",
    "resume": "resume_sec",
    "pause": "pause_sec",
    "requested_delay": "requested_delay_sec",
    "create": "create_sec",
    "kill": "kill_sec",
    "stress": "stress_sec",
    "interaction_total": "interaction_total_sec",
    "slot_contention_wait": "slot_contention_wait_sec",
    "natural_delay": "natural_delay_sec",
    "capacity_wait": "capacity_wait_sec",
    "rate_pacing_wait": "rate_pacing_wait_sec",
    "inflight_wait": "inflight_wait_sec",
    "resume_rate_pacing_wait": "resume_rate_pacing_wait_sec",
    "pause_rate_pacing_wait": "pause_rate_pacing_wait_sec",
    "resume_inflight_wait": "resume_inflight_wait_sec",
    "pause_inflight_wait": "pause_inflight_wait_sec",
    "running_slot_held": "running_slot_held_sec",
}

TRAJECTORY_COLUMNS = [
    "mode",
    "ratio",
    "repeat",
    "trajectory_id",
    "sandbox_index",
    "round_id",
    "n_steps",
    "n_failed",
    "n_timeout",
    "success_rate",
    "elapsed_sec",
    *_TRAJECTORY_BREAKDOWN_COLUMNS,
    "create_error_type",
    "kill_error_type",
    # action_series (the trajectory's newline-joined command sequence) lives in
    # replay_result.json / index.json per trajectory and in benchmark-report.json;
    # omitted from the CSV so a multi-line cell doesn't bloat every Excel row.
]


def trial_row(
    *,
    mode: str,
    ratio: int,
    repeat: int,
    running_concurrency: int,
    target_count: int,
    summary: dict,
    return_code: int,
    valid: bool,
    reused: bool,
    trial_dir: str,
    run_summary_path: str,
) -> dict:
    """Build a trial-summary.csv row from a parsed run_summary + driver metadata."""
    tp = summary.get("throughput") or {}
    total = tp.get("total", 0)
    failed = tp.get("failed", 0)
    adm = summary.get("admission") or {}
    lc = summary.get("lifecycle_overhead") or {}
    return {
        "mode": mode,
        "ratio": ratio,
        "oversubscription_ratio": ratio,
        "repeat": repeat,
        "running_concurrency": running_concurrency,
        "target_count": target_count,
        "total": total,
        "succeeded": tp.get("succeeded", 0),
        "failed": failed,
        "failure_rate": round(failed / total, 4) if total else 0.0,
        "peak_active": adm.get("peak_active"),
        "granted": adm.get("granted"),
        "avg_queue_wait_sec": adm.get("avg_queue_wait_sec"),
        "control_dispatched": adm.get("control_dispatched"),
        "test_duration": summary.get("test_duration") or 0,
        "wall_sec": summary.get("wall_sec"),
        "tasks_per_sec": tp.get("tasks_per_sec"),
        # Filled from trajectory-detail rows in write_outputs; None here so the
        # columns exist even for trials whose index.json is missing.
        "mean_stress_sec": None,
        "stress_tasks_per_sec": None,
        "steps_per_sec": tp.get("steps_per_sec"),
        "lifecycle_overhead_pct": lc.get("pct_of_slice_total"),
        "return_code": return_code,
        "valid": valid,
        "reused": reused,
        "started_at": summary.get("started_at"),
        "completed_at": summary.get("completed_at"),
        "error": summary.get("error"),
        "trial_dir": trial_dir,
        "run_summary_path": run_summary_path,
    }


def _median(vals: list[float]) -> float:
    return statistics.median(vals) if vals else 0.0


def _cmd_exec_stats(traj_rows: list[dict]) -> dict[tuple[str, int], tuple[float, float]]:
    """Per ``(mode, ratio)``: ``(avg_cmd_exec_sec, median_cmd_exec_sec)``.

    Per-cmd exec time = a run's ``exec_sec / n_steps``, over runs that executed
    at least one step and had **no failed steps** (a failed run carries
    truncated / zero exec that would drag the mean down -- the same exclusion
    rule as the kernel's per-action latency stats). ``avg`` is the true
    per-cmd mean ``sum(exec) / sum(steps)``; ``median`` is the median of
    per-run means (robust to run-length outliers). Keys absent when a ratio
    has no qualifying run (callers default to 0.0, matching _median).
    """
    by_key: dict[tuple[str, int], list[tuple[float, int]]] = {}
    for r in traj_rows:
        n = r.get("n_steps") or 0
        ex = r.get("exec_sec")
        if not n or ex is None or (r.get("n_failed") or 0) > 0:
            continue
        by_key.setdefault((r["mode"], r["ratio"]), []).append((float(ex), int(n)))
    out: dict[tuple[str, int], tuple[float, float]] = {}
    for key, pairs in by_key.items():
        total_exec = sum(e for e, _ in pairs)
        total_steps = sum(n for _, n in pairs)
        per_run = [e / n for e, n in pairs]
        out[key] = (round(total_exec / total_steps, 6), round(statistics.median(per_run), 6))
    return out


def _stress_rate_stats(traj_rows: list[dict], trials: list[dict]) -> dict[tuple[str, int, int], dict]:
    """Per-trial throughput with create/kill excluded from the clock.

    ``stress_sec`` is already the elapsed time minus create and kill
    (``elapsed_sec = create_sec + kill_sec + stress_sec``), so a rate over the
    summed stress windows answers "how fast did the workload itself run" while
    create/kill stay visible as their own columns. ``tasks_per_sec`` keeps the
    end-to-end (wall-clock) view: it is still the right number when sandbox
    churn is part of the workload being measured.

    A run contributes to both numerator and denominator only when its stress
    window was measured (``stress_sec > 0``); a failed create or an older
    index.json without the breakdown is absent rather than counted as zero,
    which would inflate the rate. The numerator counts runs whose every step
    succeeded (index.json's per-run ``success_rate >= 1.0``, the notion behind
    its ``success`` flag) -- stricter than ``tasks_per_sec``'s completion count,
    which still counts a trajectory that ran all its steps with a failed step
    when ``stop_on_error`` is off.
    """
    workers = {(t["mode"], t["ratio"], t["repeat"]): t.get("target_count") for t in trials}
    buckets: dict[tuple[str, int, int], list[dict]] = {}
    for r in traj_rows:
        sec = r.get("stress_sec")
        if sec is None or float(sec) <= 0.0:
            continue
        buckets.setdefault((r["mode"], r["ratio"], r["repeat"]), []).append(r)

    out: dict[tuple[str, int, int], dict] = {}
    for key, rows in buckets.items():
        stress_sum = sum(float(r["stress_sec"]) for r in rows)
        succeeded = sum(1 for r in rows if float(r.get("success_rate") or 0.0) >= 1.0)
        n_workers = workers.get(key)
        out[key] = {
            "mean_stress_sec": round(stress_sum / len(rows), 6),
            # workers * successful runs / total-stress-seconds == what the fleet
            # would deliver if create/kill cost nothing; dims out to tasks/s.
            "stress_tasks_per_sec": round(n_workers * succeeded / stress_sum, 3) if n_workers and stress_sum else None,
        }
    return out


def aggregate_ratio_summary(trials: list[dict], traj_rows: list[dict] | None = None) -> list[dict]:
    """One row per ``(mode, ratio)``: medians across repeats + degradation vs k=1.

    Degradation is computed **within a mode** vs that mode's ``k=1`` baseline,
    so lifecycle and exec_only each get their own curve (memory-overcommit
    overhead vs CPU-oversubscription degradation). When a mode has no k=1
    trial, the degradation columns default to ``0.0`` (no baseline to compare).

    ``traj_rows`` (the trajectory-detail rows) enables the per-cmd exec
    columns; without them those columns stay ``0.0`` (older callers).
    """
    cmd_stats = _cmd_exec_stats(traj_rows or [])
    by_key: dict[tuple[str, int], list[dict]] = {}
    for t in trials:
        by_key.setdefault((t["mode"], t["ratio"]), []).append(t)

    rows: list[dict] = []
    for (mode, ratio), group in sorted(by_key.items()):
        walls = [g["wall_sec"] for g in group if g.get("wall_sec") is not None]
        tps = [g["tasks_per_sec"] for g in group if g.get("tasks_per_sec") is not None]
        stress = [g["mean_stress_sec"] for g in group if g.get("mean_stress_sec") is not None]
        stress_tps = [g["stress_tasks_per_sec"] for g in group if g.get("stress_tasks_per_sec") is not None]
        peaks = [g["peak_active"] for g in group if g.get("peak_active") is not None]
        waits = [g["avg_queue_wait_sec"] for g in group if g.get("avg_queue_wait_sec") is not None]
        attempted = len(group)
        successful = sum(1 for g in group if g["valid"])
        avg_cmd, med_cmd = cmd_stats.get((mode, ratio), (0.0, 0.0))
        rows.append(
            {
                "mode": mode,
                "ratio": ratio,
                "attempted": attempted,
                "successful": successful,
                "all_repeats_successful": successful == attempted,
                "median_wall_sec": _median(walls),
                "median_tasks_per_sec": _median(tps),
                "median_stress_sec": _median(stress) if stress else None,
                "median_stress_tasks_per_sec": _median(stress_tps) if stress_tps else None,
                "median_peak_active": _median(peaks),
                "median_queue_wait_sec": _median(waits),
                "avg_cmd_exec_sec": avg_cmd,
                "median_cmd_exec_sec": med_cmd,
                "cmd_exec_degradation_vs_1_1_pct": 0.0,
                "time_degradation_vs_1_1_pct": 0.0,
                "throughput_gain_vs_1_1_pct": 0.0,
                "stress_throughput_gain_vs_1_1_pct": None,
            }
        )

    by_mode: dict[str, list[dict]] = {}
    for r in rows:
        by_mode.setdefault(r["mode"], []).append(r)
    for mrows in by_mode.values():
        base = next((r for r in mrows if r["ratio"] == 1), None)
        if not base:
            continue
        for r in mrows:
            bw = base["median_wall_sec"] or 0
            r["time_degradation_vs_1_1_pct"] = round(((r["median_wall_sec"] - bw) / bw * 100) if bw else 0.0, 3)
            bt = base["median_tasks_per_sec"] or 0
            r["throughput_gain_vs_1_1_pct"] = round(((r["median_tasks_per_sec"] - bt) / bt * 100) if bt else 0.0, 3)
            bst = base["median_stress_tasks_per_sec"]
            cur = r["median_stress_tasks_per_sec"]
            # Blank (not 0.0) when either side is unmeasured: a missing stress
            # window is "n/a", and reporting it as a rate would fabricate a
            # -100% gain curve for an older index.json without breakdowns.
            r["stress_throughput_gain_vs_1_1_pct"] = (
                round((cur - bst) / bst * 100, 3) if bst and cur is not None else None
            )
            bc = base["avg_cmd_exec_sec"] or 0
            r["cmd_exec_degradation_vs_1_1_pct"] = round(((r["avg_cmd_exec_sec"] - bc) / bc * 100) if bc else 0.0, 3)
    return rows


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _trajectory_rows(trials: list[dict]) -> list[dict]:
    """One row per (trajectory, sandbox, round) per trial.

    Expands each ``trajectories/index.json`` row's ``runs[]`` array into one
    CSV row per run so a reader gets per-run latency at any pool/N ratio (when
    pool < N, the round-robin wraps and each trajectory is run by ~N/pool
    sandboxes -- those are distinct runs, not a sum). The per-run
    ``time_breakdown_sec`` (that run's own sums) is surfaced as flat columns so
    a reader can attribute each run's ``elapsed_sec``; a ``groupby
    trajectory_id`` over the per-run rows yields the per-trajectory median.

    Backward compat: an older index.json without ``runs[]`` yields one row per
    trajectory from the top-level fields (a 1:1 pool>=N run), not a crash.
    """
    out: list[dict] = []
    for t in trials:
        idx_path = t.get("run_summary_path")
        if not idx_path:
            continue
        # run_summary lives in <trial_stamp_dir>/; index.json in <trial_stamp_dir>/trajectories/
        idx = Path(idx_path).parent / "trajectories" / "index.json"
        if not idx.exists():
            continue
        catalog = json.loads(idx.read_text(encoding="utf-8"))
        for tr in catalog.get("trajectories", []):
            runs = tr.get("runs")
            sources = runs if runs else [tr]  # new shape: runs[]; old shape: [tr]
            for src in sources:
                row = {
                    "mode": t["mode"],
                    "ratio": t["ratio"],
                    "repeat": t["repeat"],
                    "trajectory_id": tr.get("trajectory_id"),
                    "sandbox_index": src.get("sandbox_index"),
                    "round_id": src.get("round_id"),
                    "n_steps": src.get("n_steps"),
                    "n_failed": src.get("n_failed"),
                    "n_timeout": src.get("n_timeout"),
                    "success_rate": src.get("success_rate"),
                    "elapsed_sec": src.get("elapsed_sec"),
                    "create_error_type": src.get("create_error_type"),
                    "kill_error_type": src.get("kill_error_type"),
                    # Trajectory-level (same for every run of this trajectory);
                    # blank for an older index.json without it (backward compat).
                    "action_series": tr.get("action_series"),
                }
                bd = src.get("time_breakdown_sec") or {}
                for short_key, col in _BREAKDOWN_KEY_TO_COL.items():
                    row[col] = bd.get(short_key)
                out.append(row)
    return out


_RUN_EXPORTS = {
    "_POSTKILL_GRACE_SEC",
    "_TERMINATE_GRACE_SEC",
    "_auto_compare",
    "_cleanup_output",
    "_find_reusable_summary",
    "_run_cleanup",
    "_run_sharded_trial",
    "_run_subprocess",
    "_run_trial",
    "build_arg_parser",
    "main",
    "write_outputs",
}


def __getattr__(name: str):
    """Lazily re-export the runner symbols split into :mod:`bench_core.oversub_run`."""
    if name in _RUN_EXPORTS:
        from bench_core import oversub_run

        return getattr(oversub_run, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
