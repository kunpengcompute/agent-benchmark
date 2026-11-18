"""AENV provider backed by the Go worker client (``aenv-go``).

Same target as :mod:`env_provider.aenv` (an AENV server driven through its
E2B-compatible REST surface) but all HTTP requests are executed by the
resident Go worker (``go-client/``) instead of the e2b SDK: connection-pool
reuse, no per-request SDK client overhead. Python keeps replay policy
(trajectory interpretation, delay, admission, retry) exactly as before --
this is the client-swap port of ``agent-benchmark``'s ``--client go``.

Inheritance strategy: subclass :class:`env_provider.aenv.AenvProvider` and
override only the seams that go through a client:

  * the manager's SDK call points (create/list/connect) are redirected to the
    Go worker via the manager's INSTANCE-SCOPED ``sandbox_cls`` seam -- the
    handle-table/batching/readiness plumbing is reused unchanged, and no
    module-global ``Sandbox`` symbol is ever swapped (two provider instances
    in one process cannot cross-route, and teardown needs no restore);
  * ``pause`` / ``resume`` / ``create_one`` / ``kill_one`` override the AENV
    methods whose bodies call ``beta_pause`` / ``Sandbox.connect`` directly.

No E2B fallback: a missing/failed/mismatched Go worker raises, mirroring the
original client's hard semantics (silently degrading would corrupt a
client-comparison benchmark).
"""
from __future__ import annotations

import logging
import threading

from bench_core.config import KernelConfig
from env_provider import SandboxInstance
from env_provider.aenv import AenvProvider
from env_provider.aenv_go.bridge import (
    GoClientError,
    GoSandboxAPI,
    GoTransportError,
    binary_metadata,
    binary_path,
)
from env_provider.e2b.config import Config

logger = logging.getLogger(__name__)


def build_provider(config: KernelConfig, raw_config: dict) -> "AenvGoProvider":
    """Construct an AenvGoProvider from a raw YAML dict (kernel smoke path).

    Same shape as ``env_provider.aenv.build_provider``: kernel config already
    built. The e2b-style block is read from ``aenv_go:`` with a KEY-BY-KEY
    fall back to the ``aenv:`` block (the go block needs only its overrides --
    ``go_client_bin`` etc.; everything else inherits the aenv settings, so
    configs need no duplication and an empty ``aenv_go: {}`` is equivalent to
    the aenv block).
    """
    stop_event = threading.Event()
    if raw_config:
        base = dict(raw_config.get("aenv") or {})
        go_block = dict(raw_config.get("aenv_go") or {})
        merged_env = {**base.pop("env", {}), **go_block.pop("env", {})}
        merged = {**base, **go_block}
        if merged_env:
            merged["env"] = merged_env
        go_config = Config.from_raw({"aenv": merged}, block="aenv")
        binary = go_block.get("go_client_bin") or None
    else:
        go_config = Config()
        binary = None
    return AenvGoProvider(config, go_config, stop_event, binary=binary)


class _GoPaginator:
    """Duck-typed e2b paginator: the manager flattens via has_next/next_items."""

    def __init__(self, rows: list) -> None:
        self._rows = list(rows)

    @property
    def has_next(self) -> bool:
        return bool(self._rows)

    def next_items(self) -> list:
        rows, self._rows = self._rows, []
        return rows


def _build_shim_cls(api: GoSandboxAPI) -> type:
    """Build the ``sandbox_cls`` bound to THIS provider's worker API.

    The manager calls the class the way it calls the e2b SDK's module-level
    ``Sandbox``: ``cls.create(resolved, timeout=..., envs=..., metadata=...)``,
    ``cls.list()`` (paginator), ``cls.connect(id)`` -- so each seam is a
    method-defined-on-the-class (staticmethod keeps the call shape), closing
    over the provider's shared :class:`GoSandboxAPI`. Instance-scoped: the
    class is handed to THIS manager via ``sandbox_cls`` and never published
    on any module global.
    """

    class _GoSandboxShim:
        """Routes the manager's create/list/connect seams to the Go worker."""

        @staticmethod
        def create(template, timeout=86400, envs=None, metadata=None):  # noqa: ARG004
            return api.create_sandbox(template=template, metadata=metadata, timeout=timeout)

        @staticmethod
        def list():
            # The manager's _external_id/_attach read ``.sandbox_id`` off each
            # listed row (an attribute on e2b SDK objects); raw dicts from the
            # worker would fall through to str(dict) and corrupt the
            # cleanup/detect paths. Wrap rows in GoSandbox, which exposes
            # sandbox_id and routes connect/kill via the worker.
            from env_provider.aenv_go.bridge import GoSandbox

            rows = api.list_by_metadata({})
            return _GoPaginator([GoSandbox(api, row) for row in rows])

        @staticmethod
        def connect(sandbox_id):
            return api.connect_sandbox(sandbox_id)

    return _GoSandboxShim


class AenvGoProvider(AenvProvider):
    """AENV provider whose client is the resident Go worker (no e2b SDK on the wire)."""

    name = "aenv-go"
    default_replay_mode = "lifecycle"

    def __init__(self, kernel_config: KernelConfig, config: Config,
                 stop_event: threading.Event, *, binary: str | None = None) -> None:
        # Validate + record binary identity BEFORE anything else: a missing or
        # stale worker must fail fast (no E2B fallback, hard semantics).
        self.go_binary = binary_path(binary)
        self.go_metadata = binary_metadata(binary)
        super().__init__(kernel_config, config, stop_event)
        self._go_api: GoSandboxAPI | None = None
        self._shim_lock = threading.Lock()

    # ------------------------------------------------------------ worker wiring
    @property
    def go_api(self) -> GoSandboxAPI:
        """Lazily built shared worker API (one resident worker per provider)."""
        with self._shim_lock:
            if self._go_api is None:
                self._go_api = GoSandboxAPI(str(self.go_binary))
            return self._go_api

    @property
    def manager(self):
        # E2BProvider.manager property, extended: the Go shim class is passed
        # to THIS manager instance via the sandbox_cls seam (no module globals
        # are swapped; see _build_manager) before the first create/probe/list.
        if self._manager is None:
            self._manager = self._build_manager()
        return self._manager

    def _build_manager(self):
        from env_provider.e2b.manager import SandboxManager

        # Instance-scoped client swap: the shim class closes over THIS
        # provider's shared GoSandboxAPI, so a second provider instance in
        # the same process gets its own manager + shim and cannot cross-route;
        # after _close_worker only THIS manager's shim is affected.
        with self._shim_lock:
            if self._go_api is None:
                self._go_api = GoSandboxAPI(str(self.go_binary))
            return SandboxManager(
                self._kernel_config, self._config, self._stop_event, sandbox_cls=_build_shim_cls(self._go_api)
            )

    # ------------------------------------------------------------ client metadata
    def client_metadata(self) -> dict:
        return dict(self.go_metadata)

    # ------------------------------------------------------------ lifecycle overrides
    def pause(self, inst: SandboxInstance) -> None:
        """Memory-snapshot via the worker's pause (same REST call as beta_pause)."""
        state = self.manager.sandbox_states.get(inst.index)
        if state is None or state.sandbox_obj is None:
            raise RuntimeError(f"No AENV handle for sandbox index {inst.index}")
        state.sandbox_obj.beta_pause()

    def resume(self, inst: SandboxInstance) -> None:
        """Restore from snapshot via the worker's resume; swap the fresh handle in."""
        state = self.manager.sandbox_states.get(inst.index)
        if state is None:
            raise RuntimeError(f"No AENV handle for sandbox index {inst.index}")
        state.sandbox_obj = self.go_api.connect_sandbox(inst.id)

    # ------------------------------------------------------------ teardown
    def cleanup_all(self) -> None:
        try:
            super().cleanup_all()
        finally:
            self._close_worker()

    def cleanup_existing(self) -> int:
        try:
            return super().cleanup_existing()
        finally:
            self._close_worker()

    def _close_worker(self) -> None:
        if self._go_api is not None:
            try:
                self._go_api.close()
            except Exception:  # noqa: BLE001 - teardown is best-effort
                logger.warning("Go worker close failed during teardown", exc_info=True)
            self._go_api = None


__all__ = ["AenvGoProvider", "build_provider", "GoClientError", "GoTransportError"]
