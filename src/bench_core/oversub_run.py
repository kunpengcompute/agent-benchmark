"""Run side of the oversubscription sweep: CSV output, CLI, and trial execution.

Split out of :mod:`bench_core.oversub` so the config/aggregation half stays a
small, dependency-light module; re-exported there for the historical import
path. Installed as the ``oversub-bench`` console script.
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics
import subprocess
import time
from pathlib import Path

import yaml

from bench_core.oversub import (
    DEFAULT_TRIAL_TIMEOUT_SEC,
    RATIO_COLUMNS,
    TRAJECTORY_COLUMNS,
    TRIAL_COLUMNS,
    _cmd_exec_stats,
    _coerce_cmd,
    _coerce_modes,
    _coerce_ratios,
    _median,
    _stress_rate_stats,
    _TRUE_DEFAULTS,
    _trajectory_rows,
    _write_csv,
    aggregate_ratio_summary,
    build_trial_config,
    compute_valid,
    default_running_concurrency,
    load_sweep_config,
    parse_ratios,
    parse_run_summary,
    trial_row,
)
from bench_core.oversub_shards import (
    ShardGroupRunner,
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


def write_outputs(trials: list[dict], *, output_root: Path, configuration: dict | None = None) -> None:
    """Write trial/ratio/trajectory CSVs + benchmark-report.json (restart-safe).

    Call after every trial so a killed driver leaves a complete partial set.
    """
    output_root.mkdir(parents=True, exist_ok=True)
    traj_rows = _trajectory_rows(trials)
    # Enrich the trial rows with the create/kill-free rate before the CSV and
    # the ratio summary read them. Recomputed from index.json on every call, so
    # a trial row written by an earlier call is idempotently refreshed.
    stress_stats = _stress_rate_stats(traj_rows, trials)
    for t in trials:
        t.update(stress_stats.get((t["mode"], t["ratio"], t["repeat"]), {}))
    _write_csv(output_root / "trial-summary.csv", TRIAL_COLUMNS, trials)
    ratio_rows = aggregate_ratio_summary(trials, traj_rows)
    _write_csv(output_root / "ratio-summary.csv", RATIO_COLUMNS, ratio_rows)
    _write_csv(output_root / "trajectory-detail.csv", TRAJECTORY_COLUMNS, traj_rows)
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "configuration": configuration or {},
        "trials": trials,
        "ratio_summary": ratio_rows,
        "trajectory_details": traj_rows,
    }
    (output_root / "benchmark-report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def _auto_compare(output_root: Path) -> None:
    """Post-sweep: run compare_sweeps on this single sweep dir.

    Produces ``<output_root>/comparison/`` (per-stat p50/p90/p99/avg degradation
    vs k=1 + the same-trajectory cross-ratio table + workbook) without a manual
    ``oversub-compare`` invocation -- a single-series manifest where the
    cross-series deltas degenerate to zero and the within-series cross-ratio
    comparison is the point. NEVER fails the sweep: any error degrades to a
    warning (the raw CSVs remain the source of truth; ``oversub-compare`` can
    always be re-run by hand).
    """
    from bench_core.compare_sweeps import run_comparison

    manifest = {
        "baseline_series": "sweep",
        "series": [{"label": "sweep", "arch": "", "dir": str(output_root), "caps": {}}],
    }
    try:
        rc = run_comparison(manifest, output_root / "comparison")
    except Exception as exc:
        logger.warning("auto-compare skipped (%s); sweep CSVs are unaffected", exc)
        return
    if rc == 0:
        logger.info("auto-compare written to %s", output_root / "comparison")
    else:
        logger.warning("auto-compare returned %s; sweep CSVs are unaffected", rc)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="oversub-bench",
        description="Sweep the bench-core replay kernel across oversubscription ratios.",
    )
    p.add_argument(
        "--sweep-config", default=None, help="sweep-config YAML (all knobs + base_config); CLI flags override it"
    )
    p.add_argument("--config", default=None, help="base replay.yaml (required unless sweep-config sets base_config)")
    p.add_argument("--provider", default=None, help="provider name (default aenv)")
    p.add_argument(
        "--running-concurrency",
        type=int,
        default=None,
        help="N (ratio basis); lifecycle scales running_concurrency to k*N per trial",
    )
    p.add_argument("--ratios", default=None, help="comma-separated k values (default 1,2,3)")
    p.add_argument("--modes", default=None, help="comma-separated replay modes (default exec_ephemeral,lifecycle)")
    p.add_argument("--repeats", type=int, default=None, help="repeats per (mode,ratio) (default 1)")
    p.add_argument(
        "--test-duration", type=int, default=None, help="sustained window per trial (default = base test.duration)"
    )
    p.add_argument(
        "--multiplier",
        type=int,
        default=None,
        help="per-sandbox trajectory count M for exec_ephemeral: each sandbox runs M "
        "trajectories then stops (stop-when-done); switches the trial to fixed. "
        "Default 2 (M=2 per sandbox); --multiplier 0 disables M and keeps the base "
        "benchmark_mode (round-based only when that base is round_robin).",
    )
    p.add_argument("--failure-tolerance", type=float, default=None, help="max failure_rate for valid (default 0.0)")
    p.add_argument(
        "--shards",
        default=None,
        help="kernel processes per trial for admission-free modes (exec_ephemeral/exec_only): "
        "'auto' = one shard per ratio (each ~N slots, its own GIL) or an explicit P >= 1 "
        "(P=1 = one shard, keeps the same shard-00/ + merged/ + driver-owned vm_monitor "
        "layout as any P, so ratio-1 trials share one structure with higher ratios). "
        "lifecycle always runs single-process (full-scale, not sharded). Default auto.",
    )
    p.add_argument("--cooldown-sec", type=int, default=None, help="settle time between trials (default 30)")
    p.add_argument(
        "--cleanup-between-trials",
        choices=["on", "off"],
        default=None,
        help="pre-trial teardown of leftovers (default on)",
    )
    p.add_argument(
        "--trial-timeout-sec",
        type=int,
        default=None,
        help=f"outer wall-clock per trial; terminate->{_TERMINATE_GRACE_SEC}s->kill "
        f"(0 = off; default {DEFAULT_TRIAL_TIMEOUT_SEC}s = {DEFAULT_TRIAL_TIMEOUT_SEC // 3600}h)",
    )
    p.add_argument("--output-root", default=None, help="default results/oversub/oversub-N{N}-{ts}/")
    p.add_argument(
        "--reuse", action=argparse.BooleanOptionalAction, default=None, help="skip completed-valid trials (default off)"
    )
    p.add_argument(
        "--stop-on-failure",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="halt on first invalid trial (default off)",
    )
    p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="print trial.yaml + commands, write empty outputs (default off)",
    )
    p.add_argument(
        "--no-vm-monitor",
        action="store_true",
        default=None,
        help="pass --no-vm-monitor through to bench-core (default off)",
    )
    p.add_argument(
        "--bench-core-bin", nargs="+", default=None, help="command for the kernel subprocess (default bench-core)"
    )
    p.add_argument(
        "--auto-compare",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="run oversub-compare on the finished sweep (single-series cross-ratio "
        "comparison into <output_root>/comparison/) (default on)",
    )
    return p


# Grace given to bench-core after SIGTERM (terminate) before the hard kill.
# Must exceed the kernel's partial-flush wall time (series.close +
# export_trajectories + write_run_summary -- seconds for a 1152-trajectory
# fleet; larger fleets or vm_monitor-on may need more). Future: expose as a CLI
# flag so a heavy aenv+vm_monitor trial can bump it without code change.
_TERMINATE_GRACE_SEC = 30
# Reaper wait after SIGKILL (the process is already dead; this just collects).
_POSTKILL_GRACE_SEC = 5


def _run_subprocess(cmd: list[str], log_path: Path, timeout_sec: int) -> int:
    """Run cmd, stream stdout+stderr to log_path; portable hard-kill on timeout.

    ``terminate`` -> ``_TERMINATE_GRACE_SEC`` grace -> ``kill``. The grace must
    exceed the kernel's partial-flush wall time (``series.close`` +
    ``export_trajectories`` + ``write_run_summary`` -- seconds for a
    1152-trajectory fleet; larger fleets or vm_monitor-on may need more) so a
    SIGTERM-triggered flush completes before the hard ``kill``. The bench
    targets Linux hosts, but the driver may run on a Windows dev box.
    """
    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        try:
            proc.wait(timeout=timeout_sec if timeout_sec else None)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=_TERMINATE_GRACE_SEC)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=_POSTKILL_GRACE_SEC)
        except KeyboardInterrupt:
            # Tear down the trial subprocess so Ctrl-C does not orphan a run
            # managing hundreds of sandboxes + a vm_monitor host collector. The
            # generous grace lets the kernel's SIGTERM-cooperative handler flush
            # partial artifacts (run_summary.json / trajectories/index.json)
            # before the hard kill -- _run_trial then captures them as a row.
            proc.terminate()
            try:
                proc.wait(timeout=_TERMINATE_GRACE_SEC)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=_POSTKILL_GRACE_SEC)
            raise
        return proc.returncode if proc.returncode is not None else -1


def _run_cleanup(args: argparse.Namespace, *, label: str, halt_on_interrupt: bool = True) -> None:
    """Tear down leftover sandboxes via ``bench-core --cleanup``.

    Idempotent (lists + kills whatever is still alive). Two callers:
      * pre-trial, so a prior run's leftovers don't corrupt the ratio;
      * on a Ctrl+C sweep halt, so an interrupted trial (kernel SIGTERM'd
        mid-stress, which leaves sandboxes for THIS reaper -- the kernel's
        SIGTERM path deliberately skips cleanup so the driver owns teardown)
        does not orphan them when the sweep halts with no next trial to run
        the pre-trial cleanup.

    Respects ``--cleanup-between-trials`` and ``--dry-run`` (the single auto-
    cleanup knob; a user who opted out manages sandboxes themselves). The
    dedicated cleanup timeout caps at 300s so a hung ``bench-core --cleanup``
    cannot block indefinitely; shrinks to ``trial_timeout_sec`` if smaller.

    ``halt_on_interrupt``: a SECOND Ctrl+C during cleanup. Pre-trial we
    propagate it (it halts the sweep, matching the prior behavior where a
    Ctrl+C during pre-trial cleanup unwound straight to main's handler). On
    the post-interrupt exit path we are already inside an interrupt unwind,
    so we swallow it (log + return) instead of replacing the propagating
    interrupt and traceback-ing the driver. Never raises otherwise: a cleanup
    failure or timeout is logged + downgraded to a warning.
    """
    if args.cleanup_between_trials != "on" or args.dry_run:
        return
    cleanup_timeout = min(args.trial_timeout_sec or 300, 300)
    cmd = [*args.bench_core_bin, "--config", args.config, "--provider", args.provider, "--cleanup"]
    logger.info("%s: tearing down leftovers (timeout %ss)...", label, cleanup_timeout)
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=cleanup_timeout)
    except subprocess.TimeoutExpired as exc:
        logger.warning(
            "%s timed out after %ss; leftover sandboxes may remain\n%s",
            label,
            cleanup_timeout,
            _cleanup_output(exc.stdout, exc.stderr),
        )
        return
    except KeyboardInterrupt:
        logger.warning("%s interrupted again; leftover sandboxes may remain", label)
        if halt_on_interrupt:
            raise
        return
    if result.returncode != 0:
        logger.warning(
            "%s returned %s; leftover sandboxes may remain\n%s",
            label,
            result.returncode,
            _cleanup_output(result.stdout, result.stderr),
        )


def _cleanup_output(stdout, stderr, *, limit: int = 2000) -> str:
    """Decode + truncate captured ``bench-core --cleanup`` output for diagnostics.

    ``capture_output=True`` yields bytes (or None); decode defensively and tail
    to ``limit`` chars so a chatty or hung cleanup cannot flood the driver log.
    """
    parts = []
    for name, blob in (("stdout", stdout), ("stderr", stderr)):
        if not blob:
            continue
        text = blob.decode("utf-8", "replace") if isinstance(blob, bytes) else str(blob)
        if len(text) > limit:
            text = "..." + text[-limit:]
        parts.append(f"[{name}]\n{text}")
    return "\n".join(parts)


def _find_reusable_summary(trial_dir: Path, prefix: str) -> Path | None:
    """Locate a finished trial's summary for ``--reuse``.

    Sharded trials carry the driver-merged summary at a fixed path; legacy
    single-process trials keep the kernel's timestamped run dir. Merged first
    (it is the trial-level contract); either shape feeds the same
    ``compute_valid`` because the merge is schema-faithful.
    """
    merged = trial_dir / "merged" / f"{prefix}_run_summary.json"
    if merged.exists():
        return merged
    hits = sorted(trial_dir.glob(f"{prefix}_*/{prefix}_run_summary.json"))
    return hits[-1] if hits else None


def _run_trial(
    args: argparse.Namespace,
    base_yaml: dict,
    mode: str,
    ratio: int,
    repeat: int,
    n: int,
    output_root: Path,
    *,
    index: int,
    total: int,
) -> dict | None:
    """Run one (mode,ratio,repeat) trial; return a trial_row dict or None (dry-run)."""
    prefix = (base_yaml.get("report") or {}).get("filename_prefix", "replay_bench")
    trial_dir = output_root / mode / f"ratio-{ratio:02d}" / f"repeat-{repeat:02d}"
    trial_dir.mkdir(parents=True, exist_ok=True)
    test_duration = args.test_duration or (base_yaml.get("test") or {}).get("duration", 600)
    # multiplier (M) applies only to exec_ephemeral (each sandbox runs M
    # trajectories, stop-when-done); a no-op for lifecycle. The raw value is
    # passed through (not ``or None``) so 0 stays distinct from unset: M=0 is an
    # explicit "disable M" -- build_trial_config then clears any base
    # replay.multiplier so a stray base value cannot fire the kernel's
    # fixed+exec_ephemeral+M triple (stop_when_done). compute_valid reads 0 as
    # falsy (``if multiplier``), identical to unset -- no M-aware floor.
    m = args.multiplier if mode == "exec_ephemeral" else None
    # Shardable modes (exec_ephemeral/exec_only) get a plan at every P, including
    # P == 1, so a ratio-1 trial keeps the sharded layout (shard-00/ + merged/ +
    # driver-owned vm_monitor) byte-identical to P >= 2 trials; lifecycle (its
    # full-scale pause/resume fleet is one shared window) stays None = single
    # process. See oversub_shards.
    plan = plan_shards(trial_dir=trial_dir, mode=mode, ratio=ratio, n=n, setting=args.shards)

    summary_glob = f"{prefix}_*/{prefix}_run_summary.json"

    logger.info(
        "[%s/%s] %s ratio=%d repeat=%d: target=%d, duration=%ss, shards=%d; log=%s",
        index,
        total,
        mode,
        ratio,
        repeat,
        ratio * n,
        test_duration,
        plan.count if plan else 1,
        trial_dir / "driver.log" if plan is None else str(trial_dir / "shard-*") + "/driver.log",
    )

    # --reuse: skip completed-valid trials (restart safety).
    if args.reuse:
        prior = _find_reusable_summary(trial_dir, prefix)
        if prior:
            try:
                s = parse_run_summary(prior)
                if compute_valid(s, 0, n=n, failure_tolerance=args.failure_tolerance, multiplier=m, ratio=ratio):
                    logger.info(
                        "[%s/%s] %s ratio=%d repeat=%d: reused completed-valid trial -- skipped",
                        index,
                        total,
                        mode,
                        ratio,
                        repeat,
                    )
                    return trial_row(
                        mode=mode,
                        ratio=ratio,
                        repeat=repeat,
                        running_concurrency=n,
                        target_count=ratio * n,
                        summary=s,
                        return_code=0,
                        valid=True,
                        reused=True,
                        trial_dir=str(trial_dir),
                        run_summary_path=str(prior),
                    )
            except Exception:
                pass  # corrupt summary -> re-run

    # Cooldown between trials (skip the very first trial; skip in dry-run).
    if args.cooldown_sec and not args.dry_run and index != 1:
        logger.info("cooldown %ds...", args.cooldown_sec)
        time.sleep(args.cooldown_sec)

    # Pre-trial cleanup: tear down leftovers so they don't corrupt the ratio.
    # A Ctrl+C during this window propagates (halt_on_interrupt=True) and unwinds
    # to main's interrupt handler, which then runs its own post-interrupt cleanup.
    _run_cleanup(args, label="pre-trial cleanup", halt_on_interrupt=True)

    cfg = build_trial_config(
        base_yaml,
        mode=mode,
        ratio=ratio,
        n=n,
        test_duration=test_duration,
        trial_dir=str(trial_dir),
        prefix=prefix,
        multiplier=m,
    )
    trial_yaml = trial_dir / "trial.yaml"
    trial_yaml.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    # Sharded path: trial.yaml stays as the logical (unsharded) provenance; the
    # per-shard configs are derived from it under trial_dir/shard-*/.
    if plan is not None:
        return _run_sharded_trial(
            args,
            cfg,
            plan,
            mode=mode,
            ratio=ratio,
            repeat=repeat,
            n=n,
            trial_dir=trial_dir,
            prefix=prefix,
            multiplier=m,
            test_duration=test_duration,
            index=index,
            total=total,
        )

    cmd = [*args.bench_core_bin, "--config", str(trial_yaml), "--provider", args.provider]
    if args.no_vm_monitor:
        cmd.append("--no-vm-monitor")

    if args.dry_run:
        logger.info("[dry-run] %s", " ".join(str(c) for c in cmd))
        return None  # aggregated below; no trial row.

    interrupted = False
    try:
        rc = _run_subprocess(cmd, trial_dir / "driver.log", args.trial_timeout_sec)
    except KeyboardInterrupt:
        # The kernel's SIGTERM-cooperative handler flushed partial artifacts
        # (run_summary.json / trajectories/index.json) during the generous
        # terminate-grace; fall through to capture them as an invalid row
        # instead of orphaning the trial from the CSVs.
        rc = 130
        interrupted = True

    hits = sorted(trial_dir.glob(summary_glob))
    if not hits:
        row = trial_row(
            mode=mode,
            ratio=ratio,
            repeat=repeat,
            running_concurrency=n,
            target_count=ratio * n,
            summary={"throughput": {}, "test_duration": test_duration, "error": "no run_summary.json"},
            return_code=rc,
            valid=False,
            reused=False,
            trial_dir=str(trial_dir),
            run_summary_path="",
        )
    else:
        summary = parse_run_summary(hits[-1])
        valid = compute_valid(summary, rc, n=n, failure_tolerance=args.failure_tolerance, multiplier=m, ratio=ratio)
        row = trial_row(
            mode=mode,
            ratio=ratio,
            repeat=repeat,
            running_concurrency=n,
            target_count=ratio * n,
            summary=summary,
            return_code=rc,
            valid=valid,
            reused=False,
            trial_dir=str(trial_dir),
            run_summary_path=str(hits[-1]),
        )
    wall = row.get("wall_sec")
    logger.info(
        "[%s/%s] %s ratio=%d repeat=%d done in %s: valid=%s, total=%s, succeeded=%s, failed=%s, peak_active=%s",
        index,
        total,
        mode,
        ratio,
        repeat,
        format_duration(wall) if wall else "n/a",
        row["valid"],
        row["total"],
        row["succeeded"],
        row["failed"],
        row.get("peak_active"),
    )
    # Sentinel for main(): a user-initiated interrupt (Ctrl-C/SIGTERM) halted
    # this trial mid-run -- distinguish it from a trial-timeout (non-zero rc,
    # but the sweep continues). Not a CSV column (extrasaction="ignore" drops
    # it from trial-summary.csv); see _run_subprocess / main for the contract.
    if interrupted:
        row["_interrupted"] = True
    return row


def _run_sharded_trial(
    args: argparse.Namespace,
    trial_cfg: dict,
    plan: ShardPlan,
    *,
    mode: str,
    ratio: int,
    repeat: int,
    n: int,
    trial_dir: Path,
    prefix: str,
    multiplier: int | None,
    test_duration: int,
    index: int,
    total: int,
) -> dict | None:
    """Run one trial as P kernel subprocesses; merge back to a single trial row.

    Thin composition: planning / per-shard config derivation / group supervision
    / the driver-owned host collector / artifact merging all live in
    :mod:`bench_core.oversub_shards`; validity and row shaping reuse the same
    driver functions as the single-process path, so a sharded trial is
    indistinguishable downstream (CSVs, compare, reuse).
    A shard that produced no summary is never backfilled -- it surfaces in the
    merged ``error`` and fails the validity floor (a crash must not masquerade
    as a successful measurement).
    """
    write_shard_configs(trial_cfg, plan, owns_collector=not args.no_vm_monitor)
    commands = [
        build_shard_command(args.bench_core_bin, spec, provider=args.provider, no_vm_monitor=args.no_vm_monitor)
        for spec in plan.specs
    ]
    if args.dry_run:
        for cmd in commands:
            logger.info("[dry-run] %s", " ".join(str(c) for c in cmd))
        return None

    # One host collector for the whole trial, owned here rather than by a
    # shard: the shards only publish stress markers, so a crashed or slow shard
    # can no longer take the host report down with it, and the output path is
    # fixed at <trial_dir>/vm_monitor/ regardless of the shard layout.
    monitor = (
        None
        if args.no_vm_monitor
        else start_host_monitor(
            trial_cfg,
            trial_dir,
            provider=args.provider,
            test_duration=test_duration,
            timeout_sec=args.trial_timeout_sec,
        )
    )
    bracket = None if monitor is None else ShardStressBracket([spec.stress_path for spec in plan.specs], monitor)

    result = None
    # Artifact selection is scoped to this attempt: shard dirs accumulate one
    # timestamped run dir per attempt, so a shard that crashes before flushing
    # must not have a previous attempt's summary merged in its place.
    launch_epoch = time.time()
    try:
        if bracket is not None:
            bracket.start()
        result = ShardGroupRunner(commands, [spec.log_path for spec in plan.specs], args.trial_timeout_sec).run()
    finally:
        # Order matters: close the bracket (the collector's sampling window),
        # then let it export. On an interrupt mirror the kernel: a short wait for
        # the fast artifact instead of a 300 s export block after Ctrl+C.
        if bracket is not None:
            bracket.stop()
        if monitor is not None:
            if bracket is not None and not bracket.opened:
                # No shard ever reached its stress phase: there is no window to
                # export. stop() would block the full report_timeout for a report
                # that cannot exist and leave an idle collector behind.
                monitor.abandon()
            elif result is not None and result.interrupted:
                monitor.wait_for_host_resources(timeout=30)
            else:
                monitor.stop()
    artifacts = collect_shard_artifacts(plan, prefix, since_epoch=launch_epoch)
    rc = result.trial_return_code()

    if all(s is None for s in artifacts.summaries):
        summary = {
            "throughput": {},
            "test_duration": test_duration,
            "error": f"no shard produced run_summary.json (rcs={result.returncodes})",
        }
        run_summary_path = ""
    else:
        merged_index = merge_trajectory_indexes(artifacts.catalogs) if artifacts.catalogs else None
        summary = merge_run_summaries(artifacts.summaries, plan=plan, artifacts=artifacts, result=result)
        # Fleet-wide lifecycle series: concat the per-shard JSONLs so inflight-
        # concurrency analysis sees all k*N slots (not one shard's N). Must run
        # before write_merged_artifacts, which serializes `summary`.
        series = merge_lifecycle_series(artifacts.summaries, plan, trial_dir / "merged", prefix)
        if series is not None:
            summary.setdefault("paths", {})["lifecycle_series"] = str(series)
            # Fleet-wide obs workbook (the shards' own workbooks cover N slots
            # each). Rendered before write_merged_artifacts, which serializes
            # the summary carrying its path.
            merged_obs = render_merged_obs(
                trial_cfg,
                trial_dir,
                prefix,
                wall_sec=summary.get("wall_sec"),
                series_path=series,
                host_xlsx=monitor.merge_source() if monitor is not None else None,
            )
            if merged_obs is not None:
                summary["paths"]["obs_xlsx"] = str(merged_obs)
        run_summary_path = str(write_merged_artifacts(trial_dir, prefix, summary, merged_index))

    valid = compute_valid(
        summary, rc, n=n, failure_tolerance=args.failure_tolerance, multiplier=multiplier, ratio=ratio
    )
    row = trial_row(
        mode=mode,
        ratio=ratio,
        repeat=repeat,
        running_concurrency=n,
        target_count=ratio * n,
        summary=summary,
        return_code=rc,
        valid=valid,
        reused=False,
        trial_dir=str(trial_dir),
        run_summary_path=run_summary_path,
    )
    logger.info(
        "[%s/%s] %s ratio=%d repeat=%d done in %s (shards=%d, rcs=%s): valid=%s, total=%s, succeeded=%s, failed=%s",
        index,
        total,
        mode,
        ratio,
        repeat,
        format_duration(row.get("wall_sec")) if row.get("wall_sec") else "n/a",
        plan.count,
        result.returncodes,
        row["valid"],
        row["total"],
        row["succeeded"],
        row["failed"],
    )
    if result.interrupted:
        # Same sentinel as the single-process path: unwinds to main's interrupt
        # handler (partial outputs preserved + post-interrupt cleanup + rc 130).
        row["_interrupted"] = True
    return row


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    args = build_arg_parser().parse_args(argv)
    sweep = load_sweep_config(args.sweep_config) if args.sweep_config else {}

    def resolve(key: str):
        # CLI flag > sweep-config > built-in default. Sentinel ``None`` default on
        # every arg means "not passed", so an explicit --ratios 4 beats the YAML.
        cli = getattr(args, key, None)
        if cli is not None:
            return cli
        return sweep.get(key, _TRUE_DEFAULTS[key])

    # Base replay.yaml: --config wins, else sweep base_config, else fail.
    base_config_path = args.config or sweep.get("base_config")
    if not base_config_path:
        logger.error("must provide --config or a sweep-config with base_config")
        return 2
    with open(base_config_path, encoding="utf-8") as f:
        base_yaml = yaml.safe_load(f)

    n = resolve("running_concurrency")
    n = int(n) if n is not None else None
    n = n or default_running_concurrency(base_yaml)
    ratios = _coerce_ratios(resolve("ratios"))
    modes = _coerce_modes(resolve("modes"))
    # Breaking-change surface: the oversub default sweep modes changed from
    # exec_only to exec_ephemeral (per-round sandbox recreate). Warn loudly when
    # that default is *auto-applied* (no --modes, no sweep modes:) so operators
    # who relied on the long-lived exec_only baseline notice and can pin it back.
    _modes_cli = getattr(args, "modes", None)
    if _modes_cli is None and "modes" not in sweep and "exec_ephemeral" in modes:
        logger.warning(
            "oversub default modes now include exec_ephemeral (per-round sandbox "
            "recreate); behavior changed vs the old exec_only default. Pin "
            "--modes exec_only (or set modes: in the sweep config) to keep the "
            "long-lived baseline."
        )
    repeats = int(resolve("repeats"))
    td = resolve("test_duration")
    test_duration = int(td) if td is not None else None
    test_duration = test_duration or (base_yaml.get("test") or {}).get("duration", 600)

    # Write resolved values back so _run_trial / _run_subprocess read them unchanged.
    args.config = base_config_path
    args.provider = resolve("provider")
    args.ratios = ratios
    args.modes = modes
    args.repeats = repeats
    args.test_duration = test_duration
    args.multiplier = resolve("multiplier")
    args.shards = coerce_shards(resolve("shards"))
    args.failure_tolerance = float(resolve("failure_tolerance"))
    args.cooldown_sec = int(resolve("cooldown_sec"))
    args.cleanup_between_trials = resolve("cleanup_between_trials")
    args.trial_timeout_sec = int(resolve("trial_timeout_sec"))
    args.reuse = resolve("reuse")
    args.stop_on_failure = resolve("stop_on_failure")
    args.dry_run = resolve("dry_run")
    args.no_vm_monitor = resolve("no_vm_monitor")
    args.bench_core_bin = _coerce_cmd(resolve("bench_core_bin"))
    args.auto_compare = resolve("auto_compare")

    output_root_arg = resolve("output_root")
    if output_root_arg:
        output_root = Path(output_root_arg)
    else:
        output_root = Path(f"results/oversub/oversub-N{n}-{time.strftime('%Y%m%d-%H%M%S')}")
    output_root.mkdir(parents=True, exist_ok=True)

    configuration = {
        "base_config": str(base_config_path),
        "sweep_config": str(args.sweep_config) if args.sweep_config else None,
        "provider": args.provider,
        "running_concurrency": n,
        "ratios": ratios,
        "modes": modes,
        "repeats": repeats,
        "test_duration": test_duration,
        "multiplier": args.multiplier,
        "shards": args.shards,
        "failure_tolerance": args.failure_tolerance,
        "cooldown_sec": args.cooldown_sec,
        "trial_timeout_sec": args.trial_timeout_sec,
    }

    # Natural trial order: for mode, for ratio, for repeat.
    matrix = [(mode, ratio, repeat) for mode in modes for ratio in ratios for repeat in range(repeats)]

    trials: list[dict] = []
    total_trials = len(matrix)
    logger.info(
        "oversub sweep: provider=%s N=%d ratios=%s modes=%s repeats=%d -> %d trial(s); output=%s",
        args.provider,
        n,
        ratios,
        modes,
        repeats,
        total_trials,
        output_root,
    )
    logger.info(
        "shards=%s: admission-free modes (exec_ephemeral/exec_only) run one kernel "
        "process per shard ('auto' = P=ratio, ~N slots each); lifecycle "
        "always single-process (full-scale, not sharded)",
        args.shards,
    )
    try:
        for index, (mode, ratio, repeat) in enumerate(matrix, 1):
            row = _run_trial(args, base_yaml, mode, ratio, repeat, n, output_root, index=index, total=total_trials)
            if row is not None:
                trials.append(row)
            write_outputs(trials, output_root=output_root, configuration=configuration)
            if args.stop_on_failure and row is not None and not row["valid"]:
                logger.warning("[stop-on-failure] invalid trial %s/ratio-%s/repeat-%s; halting", mode, ratio, repeat)
                break
            if row is not None and row.get("_interrupted"):
                logger.warning("[interrupted] partial results preserved; halting sweep")
                # The interrupted trial's kernel was SIGTERM'd mid-stress, so it
                # skipped cleanup (the driver owns teardown); with the sweep
                # halting there is no next trial to run the pre-trial cleanup,
                # so tear down the leftovers here.
                _run_cleanup(args, label="post-interrupt cleanup", halt_on_interrupt=False)
                return 130
    except KeyboardInterrupt:
        write_outputs(trials, output_root=output_root, configuration=configuration)
        logger.warning("[interrupted] partial results were preserved")
        # Same leak path: a Ctrl+C between trials leaves the just-SIGTERM'd
        # trial's sandboxes with no next pre-trial cleanup to reap them.
        _run_cleanup(args, label="post-interrupt cleanup", halt_on_interrupt=False)
        return 130

    write_outputs(trials, output_root=output_root, configuration=configuration)
    valid_count = sum(1 for t in trials if t.get("valid"))
    logger.info(
        "oversub sweep done: %d/%d trial(s) valid; report=%s",
        valid_count,
        len(trials),
        output_root / "benchmark-report.json",
    )
    if args.auto_compare and trials and not args.dry_run:
        _auto_compare(output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
