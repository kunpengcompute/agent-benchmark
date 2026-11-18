"""Concrete replay runners: warmup, fixed-mode continuous, and round-robin.

Split out of :mod:`bench_core.task_runner.replay` (re-exported there).
"""
from __future__ import annotations

import logging
import time

from bench_core.admission import ShutdownInterrupted
from bench_core.payload.replay_payload import load_pool, template_at, within_template_rank
from bench_core.schemas import REPLAY_STEP_ORDER, ReplayMetrics
from bench_core.workflow_registry import RunContext, TaskRunner, WorkflowSpec, register_workflow

from .replay import ReplayBaseRunner
from .replay_config import ReplayConfig
from .replay_formatter import ReplayReportFormatter
from .replay_types import _affinity_pool

logger = logging.getLogger(__name__)


class ReplayWarmupRunner(TaskRunner):
    """Warmup phase runner for replay -- load + validate the shared pool, probe exec.

    Lightweight: loads the trajectory pool (cached, shared across sandboxes),
    runs one trivial command to confirm the sandbox is command-responsive,
    and marks the sandbox warmed up. No resident process (none needed).
    """

    def __init__(self, ctx: RunContext) -> None:
        super().__init__(ctx)

    def do_run(self) -> None:
        if not self.state.ready:
            logger.warning(f"[Sandbox{self.state.index}] Cannot start replay warmup: not ready")
            self.state.warmup_done = True
            return

        pool = load_pool(self.config)
        if not pool:
            logger.warning(f"[Sandbox{self.state.index}] Replay pool empty; nothing to replay")
        else:
            logger.info(f"[Sandbox{self.state.index}] Replay pool: {len(pool)} trajectories")

        try:
            self.provider.exec(self.state, "true", timeout=10)
        except Exception as e:
            logger.warning(f"[Sandbox{self.state.index}] Replay warmup exec probe failed: {e}")

        self.state.warmup_done = True
        logger.info(f"[Sandbox{self.state.index}] Replay warmup completed")


class ReplayTaskRunner(ReplayBaseRunner):
    """Fixed-mode replay runner -- continuously cycles the pool until stop_event.

    Each iteration: pick the next trajectory (cursor advances per trajectory,
    wrapping at pool end), replay its steps as slices, record metrics. On
    ``stop_on_error``, a failing step aborts the current trajectory and
    advances to the next -- it does NOT stop the sandbox or the benchmark.
    """

    def _begin_pass(self) -> None:
        # Fixed mode is ONE long-lived instance cycling the pool, so the pass
        # ordinal (0-based, per slot) IS this pass's run identity: two passes
        # of the same trajectory on the same slot get round_id 0 / 1 and stay
        # separate run rows instead of merging into one double-step record.
        self.round_id = self._pass_seq
        self._pass_seq += 1

    def do_run(self) -> None:
        if not self.state.ready:
            logger.warning(f"[Sandbox{self.state.index}] Cannot start replay: not ready")
            return

        pool = load_pool(self.config)
        if not pool:
            logger.info(f"[Sandbox{self.state.index}] Replay pool empty; exiting")
            return

        logger.info(f"[Sandbox{self.state.index}] Replay task runner started ({len(pool)} trajectories)")

        # Last trajectory this sandbox touched. Each branch assigns `traj` as the
        # first statement of its loop body (before the try), so this holds the
        # trajectory running when the loop last iterated (or when ShutdownInterrupted
        # broke out) -- surfaced in the "ended" log for live debug. None if the loop
        # never entered (stop_event preset at dispatch / sandbox not alive).
        traj: Trajectory | None = None

        if self.cfg.replay_mode == "exec_ephemeral":
            # exec_ephemeral: ephemeral (create_one/kill_one per trajectory via
            # _run_trajectory) with NO create_all, so state.template is None --
            # derive this worker's template from its shell index (template_at)
            # and affinity-filter to its sub-pool, then cycle the sub-pool by
            # within-template rank to total_count trajectories. The cursor never
            # leaves my_pool, so the issue SET stays fixed (count_t per
            # template) across ratios. round_robin (not fixed) is the oversub
            # linearity path; fixed keeps the same invariant so direct runs
            # don't drift the pool either.
            my_tpl = template_at(pool, self.state.index)
            my_pool = [t for t in pool if t.template == my_tpl]
            if not my_pool:
                logger.warning(
                    f"[Sandbox{self.state.index}] template {my_tpl!r} "
                    f"has no trajectory in the pool; skipping (orphan)"
                )
                self.state.replay_metrics.record_orphan_skip()
                return
            # M (replay.multiplier) sets a per-sandbox trajectory count: each of
            # the N running slots runs exactly M trajectories, then this loop
            # exits (the bench spine's stop-when-done waits on all slots). Falls
            # back to total_count when M is unset (backward compat).
            target = self.cfg.replay_multiplier or self.config.total_count
            run = 0
            cursor = within_template_rank(pool, self.state.index) % len(my_pool)
            while not self.stop_event.is_set() and self._slot_continuable() and run < target:
                traj = my_pool[cursor]
                try:
                    self._run_trajectory(traj)
                except ShutdownInterrupted:
                    break
                cursor = (cursor + 1) % len(my_pool)
                run += 1
        else:
            # Non-ephemeral (exec_only / lifecycle): affinity-filter the pool so
            # this sandbox only runs trajectories matching its template. Legacy
            # (no manifest): both sides are None -> whole pool returned.
            my_pool = _affinity_pool(pool, self.state.template)
            if not my_pool:
                logger.warning(
                    f"[Sandbox{self.state.index}] template {self.state.template!r} "
                    f"has no trajectory in the pool; skipping (orphan)"
                )
                self.state.replay_metrics.record_orphan_skip()
                return
            self._init_lifecycle()
            # Start cursor at the sandbox's within-template rank (k // T), not
            # its global index, so the +1 cycling has stride 1 within the group
            # (even coverage; avoids the gcd/T trajectory-reachability bug).
            cursor = within_template_rank(pool, self.state.index) % len(my_pool)
            while not self.stop_event.is_set() and self.state.is_alive:
                traj = my_pool[cursor]
                try:
                    self._replay_trajectory(traj)
                except ShutdownInterrupted:
                    break
                cursor = (cursor + 1) % len(my_pool)

        last_traj = traj.instance_id if traj is not None else "none"
        logger.info(f"[Sandbox{self.state.index}] Replay task runner ended (last trajectory: {last_traj})")

    def _replay_trajectory(self, traj: Trajectory) -> None:
        """Fixed-mode non-ephemeral (exec_only / lifecycle) per-step loop."""
        self._begin_pass()
        self.state.replay_metrics._mark_attempt()
        prev_slice_end = time.perf_counter()
        aborted = False
        for step in traj.steps:
            if self.stop_event.is_set() or not self.state.is_alive:
                return
            self._sleep_delay(step)
            if self.stop_event.is_set():
                return
            slice_start = time.perf_counter()
            actual_delay = slice_start - prev_slice_end
            timed_out = False
            try:
                sr = self._run_slice(step, trajectory_id=traj.instance_id)
            except Exception as e:
                msg = str(e).lower()
                timed_out = "timed out" in msg or "context deadline exceeded" in msg
                sr = self._failed_step_result(step)
                if self.series is not None:
                    self.series.write(self._failed_series_record(step, traj.instance_id, timed_out))
                logger.error(f"[Sandbox{self.state.index}] Replay step {step.index} exception: {msg[:120]}")
            prev_slice_end = time.perf_counter()
            self._record_step(
                sr,
                timed_out=timed_out,
                actual_delay=actual_delay,
                trajectory_complete=False,
                trajectory_id=traj.instance_id,
            )

            if sr.exit_code != 0 or timed_out:
                self.consecutive_errors += 1
                if self.consecutive_errors >= 3:
                    self.state.is_alive = False
                    logger.warning(f"[Sandbox{self.state.index}] Marked offline (3 consecutive replay failures)")
                    return
                if self.cfg.replay_stop_on_error:
                    aborted = True
                    break
            else:
                self.consecutive_errors = 0

        if not aborted and not self.stop_event.is_set():
            # Ran every step to the end -> mark one completion.
            self.state.replay_metrics._mark_completion()

        if aborted:
            logger.info(f"[Sandbox{self.state.index}] Trajectory {traj.instance_id} aborted (stop_on_error); advancing")


class ReplayRoundRunner(ReplayBaseRunner):
    """Round-robin replay runner -- replays one trajectory per round.

    Trajectory selection is deterministic: index
    ``(within_template_rank(pool, state.index) + round_id) % len(my_pool)``
    -- the sandbox's within-template rank (k // T), not its global index, so
    coverage is stride-1 within the group. Round N, sandbox S always maps to
    the same trajectory, and successive rounds rotate each sandbox through
    its template's sub-pool. No cursor state is held across rounds (round_id
    encodes the position).
    """

    def do_run(self) -> None:
        if not self.state.ready or not self.state.is_alive:
            logger.info(f"[Sandbox{self.state.index}] Not ready/alive for replay round")
            return

        pool = load_pool(self.config)
        if not pool:
            logger.info(f"[Sandbox{self.state.index}] Replay pool empty; skipping round {self.round_id}")
            return

        if self.cfg.replay_mode == "exec_ephemeral":
            # exec_ephemeral: ephemeral (one create_one/kill_one per round via
            # _run_trajectory) with NO create_all, so state.template is None --
            # derive this worker's template from its shell index (template_at)
            # and affinity-filter to its sub-pool, then pick by within-template
            # rank + round rotation. Round N's _run_trajectory ends in kill_one;
            # round N+1's starts in create_one -> kill+recreate between rounds
            # (the customer's real ephemeral workload). round_id rotates
            # statelessly across rounds (no cursor held between rounds); the
            # fixed runner cycles a stateful cursor -- the two differ by design,
            # not a bug.
            my_tpl = template_at(pool, self.state.index)
            my_pool = [t for t in pool if t.template == my_tpl]
            if not my_pool:
                logger.warning(
                    f"[Sandbox{self.state.index}] template {my_tpl!r} "
                    f"has no trajectory in the pool; skipping (orphan)"
                )
                self.state.replay_metrics.record_orphan_skip()
                return
            rank = within_template_rank(pool, self.state.index)
            idx = (rank + self.round_id) % len(my_pool)
            traj = my_pool[idx]
            logger.info(f"[Sandbox{self.state.index}] Replay round {self.round_id}: trajectory {traj.instance_id}")
            try:
                self._run_trajectory(traj)
            except ShutdownInterrupted:
                pass  # benchmark shutdown mid-slice; round ends cleanly
        else:
            # Non-ephemeral (exec_only / lifecycle): affinity-filter the pool.
            my_pool = _affinity_pool(pool, self.state.template)
            if not my_pool:
                logger.warning(
                    f"[Sandbox{self.state.index}] template {self.state.template!r} "
                    f"has no trajectory in the pool; skipping (orphan)"
                )
                self.state.replay_metrics.record_orphan_skip()
                return
            # Pick by the sandbox's rank WITHIN its template group (k // T), not
            # its global index -- stride-1 coverage kills the gcd/T bug where
            # gcd(T, count_t) > 1 left some trajectories unreachable per round.
            rank = within_template_rank(pool, self.state.index)
            idx = (rank + self.round_id) % len(my_pool)
            traj = my_pool[idx]
            logger.info(f"[Sandbox{self.state.index}] Replay round {self.round_id}: trajectory {traj.instance_id}")
            try:
                self._init_lifecycle()
                self._replay_round_loop(traj)
            except ShutdownInterrupted:
                pass  # benchmark shutdown mid-slice; round ends cleanly
        logger.info(f"[Sandbox{self.state.index}] Replay round {self.round_id} done")

    def _replay_round_loop(self, traj: Trajectory) -> None:
        """Per-step loop for non-ephemeral round-robin (exec_only / lifecycle)."""
        self.state.replay_metrics._mark_attempt()
        prev_slice_end = time.perf_counter()
        aborted = False
        for step in traj.steps:
            if self.stop_event.is_set():
                return
            self._sleep_delay(step)
            if self.stop_event.is_set():
                return
            slice_start = time.perf_counter()
            actual_delay = slice_start - prev_slice_end
            timed_out = False
            try:
                sr = self._run_slice(step, trajectory_id=traj.instance_id)
            except Exception as e:
                msg = str(e).lower()
                timed_out = "timed out" in msg or "context deadline exceeded" in msg
                sr = self._failed_step_result(step)
                if self.series is not None:
                    self.series.write(self._failed_series_record(step, traj.instance_id, timed_out))
                logger.error(f"[Sandbox{self.state.index}] Replay step {step.index} exception: {msg[:120]}")
            prev_slice_end = time.perf_counter()
            self._record_step(
                sr,
                timed_out=timed_out,
                actual_delay=actual_delay,
                trajectory_complete=False,
                trajectory_id=traj.instance_id,
            )
            if (sr.exit_code != 0 or timed_out) and self.cfg.replay_stop_on_error:
                aborted = True
                break

        if not aborted and not self.stop_event.is_set():
            self.state.replay_metrics._mark_completion()


register_workflow(
    WorkflowSpec(
        name="replay",
        warmup_runner=ReplayWarmupRunner,
        task_runner=ReplayTaskRunner,
        round_runner=ReplayRoundRunner,
        metrics_cls=ReplayMetrics,
        step_order=tuple(REPLAY_STEP_ORDER),
        config_section="replay",
        config_cls=ReplayConfig,
        report_formatters=ReplayReportFormatter(),
    )
)
