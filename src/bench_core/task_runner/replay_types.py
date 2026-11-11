"""Shared value types for the replay runners.

Split out of :mod:`bench_core.task_runner.replay` so the base runner module
stays under the file-size budget; re-exported there for the historical import
path.
"""
from __future__ import annotations

from dataclasses import dataclass

from bench_core.payload.replay_payload import Trajectory

# Ready-probe kernel constants (not YAML knobs -- provider-transparent, like
# ``_ready.py``'s ``READY_MAX_WAIT``). The probe runs ``true`` via the exec
# contract until the sandbox command plane is ready after resume.
READY_PROBE_MAX_ATTEMPTS = 5
READY_PROBE_TIMEOUT = 10  # seconds per attempt

# Sentinel exit code providers return for a command that timed out with partial
# output but no integer exit code (e.g. the E2B SDK's request-timeout exception).
# Mirrors the timeout(1) convention. The runner treats this as a timed-out step
# (failed, recorded, continues when replay_stop_on_error is false) instead of a
# normal nonzero exit, so the timeout metric bucket stays meaningful.
TIMEOUT_EXIT_CODE = 124

# Cap on the per-step ``action`` text stored in the lifecycle series. A SWE-bench
# ``str_replace_editor`` action can carry whole-file content (tens of KB); the
# series is written per-step on a background drain, so an untruncated action
# bloats the jsonl and the obs workbook. The full text lives in the trajectory
# ``*.replay.json`` source -- this compact form is enough to identify the step
# without joining back. Mirrors the ``stderr: null`` compact-by-design choice.
ACTION_SERIES_LIMIT = 512


def _compact_action(action: str | None, *, limit: int = ACTION_SERIES_LIMIT) -> str:
    """Single-line + truncate a recorded action for the series ``step`` event."""
    s = (action or "").replace("\r", "")
    s = s.replace("\n", "\\n")
    return s if len(s) <= limit else s[:limit] + "…<truncated>"


def _affinity_pool(pool: tuple[Trajectory, ...], template: str | None) -> list[Trajectory]:
    """Trajectories whose template matches this sandbox's template.

    Legacy (no manifest): every trajectory template is None and every sandbox
    template is None, so this returns the whole pool unchanged.
    """
    return [t for t in pool if t.template == template]


class SandboxInfrastructureError(RuntimeError):
    """Sandbox transport/readiness failure that must stop the current slice."""


@dataclass(slots=True)
class StepResult:
    """Internal per-step record produced by :meth:`ReplayBaseRunner._run_slice`.

    ``exec_elapsed_sec`` isolates the exec wall time so P2's lifecycle overhead
    (``slice_total_sec - exec_elapsed_sec`` = ``resume_sec + pause_sec``) is
    computable without re-instrumenting the metrics. ``_run_slice`` times the
    resume / pause hooks and sums all three phases into ``slice_total_sec``; in
    P1 the hooks are no-ops so ``resume_sec`` / ``pause_sec`` are ~0 and
    ``slice_total_sec`` ~= ``exec_elapsed_sec``. P2 overrides the hooks with
    real lifecycle calls and the timings flow through -- no schema change.
    """

    step_index: int
    action_type: str
    exit_code: int
    exec_elapsed_sec: float
    slice_total_sec: float
    resume_sec: float
    pause_sec: float
    requested_delay_sec: float
    # P2.6 segment decomposition. The wait-decoupling change splits the QPS
    # limiter's contribution into rate pacing (``*_queue_wait_sec`` -- the 1/qps
    # time-wait; a RATE control) and the inflight fuse (``*_inflight_wait_sec``;
    # a CONCURRENCY control), and exposes the slot-scheduler's two components
    # (natural_delay / capacity_wait) separately. ``slot_contention_wait_sec``
    # stays as the composite (natural_delay + capacity_wait) for downstream
    # parse-compat. Invariants (post-decoupling):
    #   resume_sec = resume_inflight_wait + resume_api + resume_ready_wait
    #     (resume rate-pacing is PRE-lease, NOT in resume_sec)
    #   pause_sec = pause_rate_pacing_wait + pause_inflight_wait + pause_api
    #     (pause rate-pacing is IN-lease)
    #   running_slot_held = resume_sec + exec + pause_sec
    #     (excludes resume rate-pacing -- the decoupling)
    resume_api_sec: float = 0.0
    resume_ready_wait_sec: float = 0.0
    slot_contention_wait_sec: float = 0.0
    resume_rate_pacing_wait_sec: float = 0.0
    pause_rate_pacing_wait_sec: float = 0.0
    pause_api_sec: float = 0.0
    # L7 decomposition (Phase 1): slot hold + full interaction budget.
    running_slot_held_sec: float = 0.0
    interaction_total_sec: float = 0.0
    # Wait-decoupling split: the four independent components.
    natural_delay_sec: float = 0.0
    capacity_wait_sec: float = 0.0
    resume_inflight_wait_sec: float = 0.0
    pause_inflight_wait_sec: float = 0.0
