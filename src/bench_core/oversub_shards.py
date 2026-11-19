"""Process-sharded trial execution for the oversub driver (mechanism layer).

One CPython process saturates a single GIL: a thread-per-sandbox kernel pinned
at ~1 core caps step dispatch (~50-55 steps/s measured at N=384) regardless of
host vCPU count, so high-ratio trials become client-bound instead of host-bound
(inter-step gaps overshoot the recorded think time; server-side load stops
growing). The fix mirrors the reference agent's ``process-per-ratio`` topology:
split each trial's ``k*N`` slots into P shard subprocesses of ~N slots each --
every process carries the load profile already proven at 1:1, and the GIL
ceiling moves to ~P x single-process throughput.

Layering contract:

- This module is MECHANISM: shard planning, per-shard config derivation,
  subprocess-group supervision, and artifact merging. It imports nothing from
  :mod:`bench_core.oversub` (the driver composes it, never the reverse); from
  the kernel it pulls only the observability (host-monitor) contract and the
  VMM-type helper (:func:`bench_core.bench.provider_vmm_type`) -- never
  lifecycle, admission, stats, or task dispatch.
- The shard boundary sits BELOW the kernel artifact contract: merging produces
  a schema-faithful ``run_summary.json`` + ``trajectories/index.json`` pair, so
  every downstream consumer (``compute_valid``, ``trial_row``,
  ``_trajectory_rows``, ratio summaries, ``compare_sweeps``, ``--reuse``)
  works on a sharded trial unchanged.
- Only admission-free modes shard (``SHARDABLE_MODES``). ``lifecycle`` runs
  single-process by design: the k*N pause/resume fleet is driven as one shared
  window (full-scale lifecycle has no per-shard running-slot cap to split --
  rc == total_count is a pass-through scheduler), and the lifecycle series + obs
  Concurrency-states reconstruction is a single-process kernel artifact.
  Sharding lifecycle needs a kernel-side split of the pause/resume window --
  deliberately out of scope here.

Known simplifications (measured, not assumed away):

- No cross-process start barrier: shards self-start after their own setup.
  The skew is recorded (``shard_start_skew_sec`` in the merged summary) so it
  can be quantified; add a READY/START gate only if the data shows it matters.
- vm_monitor is owned by the DRIVER, not by a shard: each shard publishes its
  own stress marker (``monitor.stress_file``) and the driver runs one
  ``vm-monitor`` over the union of those markers (see ``ShardStressBracket``).
  The host report therefore lives at a fixed ``<trial_dir>/vm_monitor/`` and
  survives any one shard crashing; shard 0 keeps only the provider-backed fleet
  gauge (``monitor.api_state: true``), which the driver cannot poll.
- ``launch_interval_sec`` is scaled by P per shard: each shard paces its own
  creates at ``P * interval``, preserving the GLOBAL average create rate (the
  control-plane protection the knob exists for) without cross-process IPC.
  Round-boundary bursts can still align across shards (up to P concurrent
  creates); a parent-side pacer would remove that if it ever hurts.
- Multi-template manifests with ``N % T != 0``: a group-aligned shard offset
  (a multiple of N) shifts the kernel's within-template rank, so the merged
  (sandbox_index -> trajectory) ASSOCIATION can differ from single-process even
  though per-trajectory run counts are preserved. Single-template (the oversub
  default) is exactly faithful.
"""
from __future__ import annotations

import copy
import json
import logging
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import yaml

from bench_core.bench import provider_vmm_type
from bench_core.observability.monitor import MonitorConfig, MonitorController

logger = logging.getLogger(__name__)

# Modes whose kernel runs have admission=None (bench.py builds the Admission
# controller only for lifecycle), i.e. no cross-fleet coordination
# invariant -- each slot is independent, so slot sets compose across processes.
SHARDABLE_MODES = frozenset({"exec_ephemeral", "exec_only"})

# Grace after terminate() before kill(); mirrors oversub._TERMINATE_GRACE_SEC
# (kept local so this module has no driver dependency). Must exceed the
# kernel's SIGTERM partial-flush wall time.
_TERMINATE_GRACE_SEC = 30
_POSTKILL_GRACE_SEC = 5


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def coerce_shards(value: object) -> int | str:
    """Normalize a ``shards`` setting: ``"auto"`` or an int >= 1.

    Accepts the CLI string form and the YAML forms (int or string). Raises
    ``ValueError`` on anything else so a typo fails loudly at startup.
    """
    if isinstance(value, str):
        text = value.strip().lower()
        if text == "auto":
            return "auto"
        value = int(text)  # ValueError on garbage
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"shards must be 'auto' or an int, got {value!r}")
    if value < 1:
        raise ValueError(f"shards must be >= 1, got {value}")
    return value


def resolve_shard_count(setting: int | str, *, mode: str, ratio: int, n: int) -> int:
    """P for one trial: ``auto`` -> ratio (reference topology), int -> as asked.

    Non-shardable modes always collapse to 1 (only ``lifecycle``: its full-scale
    k*N pause/resume fleet is deliberately driven by one process); an explicit
    P > 1 on such a mode is downgraded with a warning, not an error, so one
    sweep config can span modes. P is capped at the fleet size (every shard
    needs >= 1 slot).
    """
    if mode not in SHARDABLE_MODES:
        if setting != "auto" and setting > 1:
            logger.warning(
                "shards=%s ignored for mode %s: lifecycle always runs single-process "
                "(its k*N pause/resume fleet is not sharded); running "
                "single-process. Shardable modes: %s",
                setting,
                mode,
                sorted(SHARDABLE_MODES),
            )
        return 1
    p = ratio if setting == "auto" else int(setting)
    return max(1, min(p, ratio * n))


def shard_sizes(total: int, count: int) -> list[int]:
    """Balanced split of ``total`` slots into ``count`` shards (remainder first)."""
    size, extra = divmod(total, count)
    return [size + (1 if i < extra else 0) for i in range(count)]


@dataclass(frozen=True)
class ShardSpec:
    """One shard's static identity: slot share, re-key offset, artifact dir."""

    index: int
    size: int
    slot_offset: int  # cumulative slots of prior shards; sandbox_index re-key
    directory: Path

    @property
    def log_path(self) -> Path:
        return self.directory / "driver.log"

    @property
    def config_path(self) -> Path:
        return self.directory / "shard.yaml"

    @property
    def stress_path(self) -> Path:
        """This shard's stress marker; the driver unions them into one window."""
        return self.directory / "stress.lock"


@dataclass(frozen=True)
class ShardPlan:
    """The full split of one trial (P >= 1 for shardable modes; P == 1 is a
    single-shard plan so a ratio-1 trial shares the sharded layout with P >= 2
    trials; only non-shardable modes keep the legacy single-process path)."""

    mode: str
    ratio: int
    n: int
    specs: tuple[ShardSpec, ...]

    @property
    def count(self) -> int:
        return len(self.specs)


def plan_shards(*, trial_dir: Path, mode: str, ratio: int, n: int, setting: int | str) -> ShardPlan | None:
    """Build the shard plan for one trial, or ``None`` on the legacy path.

    Shardable modes get a plan at every P, including P == 1 (``auto`` at ratio
    1, or an explicit ``--shards 1``): running a single-shard trial through the
    sharded machinery keeps its layout (``shard-00/`` + ``merged/`` +
    driver-owned ``<trial_dir>/vm_monitor/``) byte-consistent with P >= 2
    trials, so agents comparing oversub ratios read one fixed structure and
    the host collector is owned by the driver at every ratio. Only
    non-shardable modes (lifecycle, always P=1) keep the legacy
    single-process path -- their pause/resume fleet is not split across
    processes, and a 1-shard wrap would rewrite it for no benefit.

    For P <= ratio the split is GROUP-ALIGNED: the trial's ``ratio`` groups of
    N slots are distributed over the shards, so every shard size is an integer
    multiple of N. Each shard then replays the whole first-N pool a whole
    number of times, and the merged per-trajectory run counts are exactly the
    single-process counts (slot k -> pool[k % N] in every shard). P > ratio
    falls back to a slot-level split -- totals are preserved but per-trajectory
    run counts redistribute (a shard smaller than the pool cannot cover every
    trajectory), so it is flagged with a warning.
    """
    if mode not in SHARDABLE_MODES:
        if isinstance(setting, int) and setting > 1:
            logger.warning(
                "mode %s cannot be sharded (shardable: %s); running single-process",
                mode,
                ", ".join(sorted(SHARDABLE_MODES)),
            )
        return None
    p = resolve_shard_count(setting, mode=mode, ratio=ratio, n=n)
    if p <= ratio:
        sizes = [groups * n for groups in shard_sizes(ratio, p)]
    else:
        logger.warning(
            "shards=%d > ratio=%d: shard sizes fall below the N=%d trajectory pool, so "
            "per-trajectory run counts redistribute vs a single-process trial (totals "
            "preserved). Use shards <= ratio for exact per-trajectory parity.",
            p,
            ratio,
            n,
        )
        sizes = shard_sizes(ratio * n, p)
    specs, offset = [], 0
    for i, size in enumerate(sizes):
        specs.append(ShardSpec(index=i, size=size, slot_offset=offset, directory=trial_dir / f"shard-{i:02d}"))
        offset += size
    return ShardPlan(mode=mode, ratio=ratio, n=n, specs=tuple(specs))


# ---------------------------------------------------------------------------
# Per-shard config + command
# ---------------------------------------------------------------------------


def build_shard_config(trial_cfg: dict, spec: ShardSpec, *, count: int, owns_collector: bool = True) -> dict:
    """Derive one shard's kernel config from the (unsharded) trial config.

    The shard is a self-contained kernel run over ``spec.size`` slots:
    ``total_count`` / ``round_size`` shrink to the shard's share, artifacts go
    to the shard dir, the docker container prefix is suffixed so parallel
    shards never collide on ``{prefix}-{index}`` names (the suffix keeps
    ``startswith(base)`` true, so prefix-based cleanup still reaches every
    shard), and the pacing knobs (``launch_interval_sec``, ``create_batch.interval``,
    ``task_batch.interval``) scale by P so P independently-pacing shards keep the
    GLOBAL average create/dispatch rate of the single-process trial.
    ``trajectory_limit`` stays untouched (the issue pool is global by design --
    every shard replays the same first-N pool; with group-aligned shard sizes
    the merged run set matches the single-process trial per trajectory, not
    just in total). ``running_concurrency`` clamps to the shard's own
    ``total_count``: on the P > ratio slot split a shard falls below N, and the
    kernel's config ``validate`` rejects ``running_concurrency > total_count``
    (the knob is a no-op for the shardable modes' admission).

    The ``monitor`` block splits the host collector from the stress marker:
    every shard writes its own ``stress.lock`` (the driver brackets their
    union), no shard spawns a collector, and shard 0 keeps the fleet gauge and
    the fixed ``<trial_dir>/vm_monitor/`` output path, so host data never hides
    under ``shard-00/``.
    """
    cfg = copy.deepcopy(trial_cfg)
    cfg.setdefault("sandbox", {})["total_count"] = spec.size
    cfg.setdefault("test", {})["round_size"] = spec.size
    cfg.setdefault("report", {})["output_dir"] = str(spec.directory)
    replay = cfg.get("replay")
    if isinstance(replay, dict):
        # P > ratio splits below the N pool; running_concurrency (a no-op for
        # shardable modes' admission) must stay <= the shard's own total_count
        # or the kernel's config validate rejects the shard at startup.
        rc = replay.get("running_concurrency")
        if rc is not None and rc > spec.size:
            replay["running_concurrency"] = spec.size
    monitor = dict(cfg.get("monitor") or {})
    monitor["stress_file"] = str(spec.stress_path)
    if spec.index == 0 and not monitor.get("log_dir"):
        monitor["log_dir"] = str(spec.directory.parent / "vm_monitor")
    if owns_collector and str(monitor.get("enabled", "auto")).lower() != "false":
        monitor["external_collector"] = True
        if spec.index == 0:
            # Exactly one fleet gauge per trial: shard 0 is the only sampler
            # (the driver owns the collector but holds no provider handle, and
            # "auto" on an external-collector process samples nothing). An
            # explicit "false" opts out; everything else samples.
            if str(monitor.get("api_state", "true")).lower() != "false":
                monitor["api_state"] = "true"
        else:
            # Shards > 0 never poll the provider API: P simultaneous samplers
            # would multiply the control-plane list-sandbox load.
            monitor["api_state"] = "false"
    cfg["monitor"] = monitor
    docker = cfg.get("docker")
    if isinstance(docker, dict) and docker.get("container_prefix"):
        docker["container_prefix"] = f"{docker['container_prefix']}-s{spec.index}"
    replay = cfg.get("replay")
    if isinstance(replay, dict) and replay.get("launch_interval_sec"):
        replay["launch_interval_sec"] = replay["launch_interval_sec"] * count
    for section in ("create_batch", "task_batch"):
        block = cfg.get(section)
        if isinstance(block, dict) and block.get("interval"):
            block["interval"] = block["interval"] * count
    return cfg


def write_shard_configs(trial_cfg: dict, plan: ShardPlan, *, owns_collector: bool = True) -> list[Path]:
    """Materialize every shard's ``shard.yaml``; returns the config paths."""
    paths = []
    for spec in plan.specs:
        spec.directory.mkdir(parents=True, exist_ok=True)
        # A prior attempt killed hard (group deadline kill, Ctrl+C, OOM) leaves
        # shard-XX/stress.lock behind. The driver's bracket polls marker
        # EXISTENCE, and each shard clears its own marker only at monitor start
        # (after create + warmup) -- so without this unlink a stale marker opens
        # the host window on the driver's first poll, mislabeling create/warmup
        # as stress and masking the "crashed shard cannot take the host report
        # down" guarantee.
        spec.stress_path.unlink(missing_ok=True)
        cfg = build_shard_config(trial_cfg, spec, count=plan.count, owns_collector=owns_collector)
        spec.config_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
        paths.append(spec.config_path)
    return paths


def build_shard_command(
    bench_core_bin: list[str],
    spec: ShardSpec,
    *,
    provider: str,
    no_vm_monitor: bool,
    scenario: str | None = None,
) -> list[str]:
    """One shard's bench-core argv; only shard 0 keeps its monitor flags.

    No shard spawns a host collector -- the driver runs one for the whole trial
    (see :func:`start_host_monitor`) -- so every shard's config carries
    ``monitor.external_collector`` and publishes its stress marker regardless.
    Shard 0 still keeps its monitor settings un-suppressed: it is the process
    that holds a provider handle and therefore the one that can poll the
    control-plane gauge. A second collector on the same host would double-sample
    the same /proc counters and race on the tool lock dir.

    ``scenario`` (the oversub driver's dataset selection) is passed through so
    a sharded trial resolves the same trajectory pool as a single-process one --
    the per-shard configs carry no trajectory pointer keys.
    """
    cmd = [*bench_core_bin, "--config", str(spec.config_path), "--provider", provider]
    if scenario:
        cmd.extend(["--scenario", scenario])
    if no_vm_monitor or spec.index > 0:
        cmd.append("--no-vm-monitor")
    return cmd


# ---------------------------------------------------------------------------
# Driver-owned host collector
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ProviderHint:
    """The provider facts ``MonitorController`` reads, without an SDK or a handle.

    The driver shells out to bench-core and owns no provider, so it cannot poll
    the control plane: ``list_sandbox_states`` returning None ends the sampler
    thread on its first poll. Shard 0 keeps that gauge (``monitor.api_state:
    true`` in its derived config), where a real provider handle exists.
    """

    name: str
    vmm_type: str | None

    def list_sandbox_states(self):
        return None


def start_host_monitor(
    trial_cfg: dict, trial_dir: Path, *, provider: str, test_duration: int, timeout_sec: int | None = None
) -> MonitorController | None:
    """Spawn the driver-owned host collector for one sharded trial.

    One collector per host: it samples while the union of the shards' stress
    markers is non-empty and exports to ``<trial_dir>/vm_monitor/``. Returns
    None when the collector is not applicable (no VMM for this provider, no
    binary, ``monitor.enabled: false``) -- the shards then simply publish
    markers nobody reads.

    ``timeout_sec`` is the shard group's deadline: the collector's sampling
    back-stop must cover the WHOLE union window (inter-shard start skew + the
    trial), which ``test_duration`` alone understates whenever skew is large
    (exec_only: every shard runs a full create_all first). The group deadline
    is the sound upper bound -- the window cannot outlive it.
    """
    raw = dict(trial_cfg.get("monitor") or {})
    log_dir = Path(raw.get("log_dir") or trial_dir / "vm_monitor")
    raw.update(
        log_dir=str(log_dir),
        stress_file=str(log_dir / "stress.lock"),
        external_collector=False,  # this process IS the collector
    )
    cfg = SimpleNamespace(
        monitor=MonitorConfig.from_raw(raw),
        output_dir=str(trial_dir),
        test_duration=max(int(test_duration), int(timeout_sec or 0)),
    )
    controller = MonitorController(cfg, _ProviderHint(f"{provider}-driver", provider_vmm_type(provider)))
    controller.start()
    return controller if controller.proc is not None else None


class ShardStressBracket:
    """Mirror the union of the shards' stress markers onto one collector lock.

    Each shard's kernel publishes ``monitor.stress_file`` for its own stress
    window; the driver-owned collector must sample from the FIRST shard's start
    to the LAST shard's end. This poller opens the collector's lock on the
    first marker it sees and holds it until ``stop()``, so a gapped union (one
    shard finishing while another is still creating) or a shard killed
    mid-stress never cuts the window short.
    """

    def __init__(self, marker_paths: list[Path], controller: MonitorController, poll_sec: float = 0.5) -> None:
        self._markers = list(marker_paths)
        self._controller = controller
        self._poll = poll_sec
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._opened = False

    @property
    def opened(self) -> bool:
        """True once a shard marker was seen, i.e. the collector began sampling.

        Read after :meth:`stop` (which joins the thread). An unopened bracket
        means no shard reached its stress phase: there is no window to export
        and nothing for the collector to preserve.
        """
        return self._opened

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="shard-stress-bracket", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """End the window; the collector then exports on its own."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(5.0, self._poll * 4))
            self._thread = None

    def _run(self) -> None:
        # Open on the first marker, then latch: the window covers [first shard
        # marker, driver stop]. A transition to an empty union (gapped starts,
        # a killed shard's kernel clearing its marker) must NOT end the sampling
        # -- the collector is single-shot and would export before the tail
        # shards' stress even begins.
        while not self._stop.is_set():
            if self._stop.wait(self._poll):
                break
            if not self._opened and any(p.exists() for p in self._markers):
                self._opened = True
                self._controller.begin_stress()
        if self._opened:
            self._controller.end_stress()


# ---------------------------------------------------------------------------
# Subprocess-group supervision
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GroupResult:
    """Outcome of one sharded launch: per-shard rc + how the group ended."""

    returncodes: list[int]
    interrupted: bool
    timed_out: bool

    def trial_return_code(self) -> int:
        """The rc a single-process trial would have shown (130 = user interrupt)."""
        if self.interrupted:
            return 130
        return next((rc for rc in self.returncodes if rc != 0), 0)


class ShardGroupRunner:
    """Launch P kernel subprocesses under one shared trial deadline.

    Same supervision semantics as the driver's single-process
    ``_run_subprocess`` -- terminate -> grace -> kill -- applied to the whole
    group: a deadline breach or Ctrl+C tears down EVERY shard (an orphaned
    shard would keep stressing the host and corrupt the next trial).
    """

    def __init__(self, commands: list[list[str]], log_paths: list[Path], timeout_sec: int) -> None:
        if len(commands) != len(log_paths):
            raise ValueError("commands and log_paths must align")
        self.commands = commands
        self.log_paths = log_paths
        self.timeout_sec = timeout_sec

    def run(self) -> GroupResult:
        procs: list[subprocess.Popen] = []
        logs = []
        interrupted = timed_out = False
        try:
            try:
                for cmd, log_path in zip(self.commands, self.log_paths):
                    log_path.parent.mkdir(parents=True, exist_ok=True)
                    logs.append(open(log_path, "w", encoding="utf-8"))
                    procs.append(subprocess.Popen(cmd, stdout=logs[-1], stderr=subprocess.STDOUT))
            except KeyboardInterrupt:
                # Ctrl+C inside the launch window: already-launched shards must
                # not be orphaned stressing the host while the driver exits.
                interrupted = True
                self._terminate_all(procs)
            except Exception:
                # A launch failure (missing interpreter, fd exhaustion) must
                # not leave the already-started shards stressing the host.
                self._terminate_all(procs)
                raise
            if not interrupted:
                deadline = time.monotonic() + self.timeout_sec if self.timeout_sec else None
                for proc in procs:
                    remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                    try:
                        proc.wait(timeout=remaining)
                    except subprocess.TimeoutExpired:
                        timed_out = True
                        logger.warning("shard group deadline (%ss) breached; tearing down all shards", self.timeout_sec)
                        self._terminate_all(procs)
                        break
                    except KeyboardInterrupt:
                        interrupted = True
                        self._terminate_all(procs)
                        break
        finally:
            for stream in logs:
                stream.close()
        returncodes = [p.returncode if p.returncode is not None else -1 for p in procs]
        return GroupResult(returncodes=returncodes, interrupted=interrupted, timed_out=timed_out)

    @staticmethod
    def _terminate_all(procs: list[subprocess.Popen]) -> None:
        """terminate -> grace -> kill every still-alive process, then reap all."""
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=_TERMINATE_GRACE_SEC)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=_POSTKILL_GRACE_SEC)
                except subprocess.TimeoutExpired:
                    logger.error("shard process %s did not exit after kill", proc.pid)


_MERGE_EXPORTS = {
    "ShardArtifacts",
    "collect_shard_artifacts",
    "merge_lifecycle_series",
    "merge_run_summaries",
    "merge_trajectory_indexes",
    "render_merged_obs",
    "write_merged_artifacts",
}


def __getattr__(name: str):
    """Lazily re-export the merge layer split into :mod:`bench_core.oversub_merge`."""
    if name in _MERGE_EXPORTS:
        from bench_core import oversub_merge

        return getattr(oversub_merge, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
