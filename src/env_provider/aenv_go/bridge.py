"""Synchronous Go-worker bridge (thread-safe facade over one persistent worker).

Adapted from ``agent-benchmark/agent_replay/clients/go.py`` (asyncio) to the
sync provider model of ``env_provider``: bench-core runs providers in worker
threads (no event loop), so the bridge spawns ONE resident Go worker subprocess
and multiplexes requests over its stdin/stdout line protocol from any number
of threads.

Protocol (v2, unchanged from the original):
  request  {id, method, params, timeout_sec}        (one JSON line on stdin)
  response {id, result | error, error_kind, worker_elapsed_sec}  (stdout line)
  cancel   {"id": 0, "method": "cancel", "params": {"id": N}}  (best-effort)

Semantics preserved verbatim from the async original:
  - no implicit E2B fallback: a missing binary / protocol mismatch / dead
    worker raises (GoClientError / GoTransportError) instead of degrading
  - structured error kinds: timeout / transport / api
  - worker timing (``worker_elapsed_sec``) is validated on every response
  - binary identity (SHA256 + protocol version) recorded via ``binary_metadata``
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import shlex
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

WORKER_PROTOCOL = 2
DELIVERY_GRACE_SEC = 5
CANCEL_SEND_TIMEOUT_SEC = 1
_READ_BUF_LIMIT = 256 << 20  # 256MiB readline cap, mirrors the async original


class GoClientError(RuntimeError):
    """The Go worker reported an API-level error (error_kind == "api")."""


class GoTransportError(ConnectionError):
    """The Go worker process/pipe failed (error_kind == "transport")."""


def binary_path(binary: str | None = None) -> Path:
    """Resolve the Go worker binary; raise (no E2B fallback) when absent."""
    path = Path(binary or os.environ.get("AGENT_BENCHMARK_GO_BIN")
                or Path(__file__).resolve().parents[3] / "go-client" / "bin" / "agent-benchmark-go")
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(
            f"Go worker not found: {path}; run bash go-client/build.sh or set "
            "AGENT_BENCHMARK_GO_BIN; no E2B fallback was used"
        )
    return path


def binary_metadata(binary: str | None = None) -> dict:
    """Identity of the Go worker binary (client metadata for the report)."""
    path = binary_path(binary)
    stat = path.stat()
    return {
        "client": "go",
        "go_worker_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "go_worker_protocol": WORKER_PROTOCOL,
        "client_timing": "go-bridge-await-v1",
    }


def _validate_response(message: dict) -> None:
    """Strict response validation, mirroring the async reader's checks."""
    if not isinstance(message, dict) or type(message.get("id")) is not int or message["id"] <= 0:
        raise GoTransportError("Invalid Go worker response ID")
    elapsed = message.get("worker_elapsed_sec")
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
        raise GoTransportError("Invalid Go worker timing")
    error = message.get("error")
    if error is not None and (not isinstance(error, str)
                              or message.get("error_kind") not in ("timeout", "transport", "api")):
        raise GoTransportError("Invalid Go worker error response")
    if not error and "result" not in message:
        raise GoTransportError("Missing Go worker result")


class GoBridge:
    """One resident Go worker subprocess + a response-dispatch reader thread.

    ``call(method, params, timeout)`` is blocking and thread-safe: any provider
    worker thread may issue requests concurrently; responses are routed back
    by request id through ``concurrent.futures.Future``-style events.
    """

    def __init__(self, binary: str | None = None) -> None:
        self._binary = binary_path(binary)
        self._process: subprocess.Popen | None = None
        self._pending: dict[int, tuple[threading.Event, dict | None, BaseException | None]] = {}
        self._write_lock = threading.Lock()
        self._start_lock = threading.Lock()
        self._next_id = 0
        self._id_lock = threading.Lock()
        self._failed: BaseException | None = None
        self._reader: threading.Thread | None = None
        self._closed = False

    # ------------------------------------------------------------ process mgmt
    def _start(self) -> None:
        with self._start_lock:
            if self._failed is not None:
                raise GoTransportError(f"Go worker failed; refusing implicit restart: {self._failed}")
            if self._closed:
                raise GoTransportError("Go worker closed; refusing implicit restart")
            if self._process is not None:
                if self._process.poll() is not None:
                    raise GoTransportError("Go worker exited; refusing implicit restart")
                return
            try:
                self._process = subprocess.Popen(
                    [str(self._binary)],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=None,
                    bufsize=_READ_BUF_LIMIT,
                )
            except OSError as exc:
                raise GoTransportError(f"Go worker failed to start: {exc}") from exc
            self._reader = threading.Thread(target=self._read_loop, name="go-bridge-reader", daemon=True)
            self._reader.start()

    def _read_loop(self) -> None:
        """Dispatch worker stdout lines to the waiting request threads."""
        try:
            assert self._process is not None and self._process.stdout is not None
            for line in self._process.stdout:
                try:
                    message = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    raise GoTransportError("Invalid Go worker JSON")
                _validate_response(message)
                entry = self._pending.pop(message["id"], None)
                if entry is not None:
                    event, _, _ = entry
                    error = message.get("error")
                    if error is not None:
                        kind = message.get("error_kind")
                        if kind == "timeout":
                            exc: BaseException = TimeoutError(error)
                        elif kind == "transport":
                            exc = GoTransportError(error)
                        else:
                            exc = GoClientError(error)
                        entry = (event, None, exc)
                    else:
                        entry = (event, message, None)
                    # Publish through the shared slot + wake the waiter. The
                    # entry tuple is replaced atomically (GIL refcount), then
                    # the event is set -- the waiter re-reads the slot after
                    # waking, so no lost-wakeup race.
                    self._pending[message["id"]] = entry
                    event.set()
            raise GoTransportError("Go worker connection closed")
        except BaseException as exc:  # noqa: BLE001
            self._fail_all(exc)

    def _fail_all(self, exc: BaseException) -> None:
        self._failed = exc
        with self._id_lock:
            pending = list(self._pending.items())
            self._pending.clear()
        for _, entry in pending:
            event, _, old = entry
            if old is None:
                entry_slot = (event, None, exc)
                # Re-set via the shared dict is pointless (cleared); wake directly.
                # The waiter checks a per-call copy of the failure.
                event.set()

    # ------------------------------------------------------------ request path
    def call(self, method: str, params: dict, timeout: float = 120) -> dict:
        """Blocking request; returns the full worker response message."""
        started = time.perf_counter()
        request_id: int | None = None
        event: threading.Event | None = None
        try:
            self._start()
            with self._id_lock:
                self._next_id += 1
                request_id = self._next_id
                event = threading.Event()
                self._pending[request_id] = (event, None, None)
            self._write({"id": request_id, "method": method, "params": params, "timeout_sec": timeout})
            if not event.wait(timeout + DELIVERY_GRACE_SEC if timeout else None):
                raise TimeoutError(f"Go worker response not delivered within {timeout}s")
            _, response, exc = self._pending.pop(request_id, (event, None, GoTransportError("response lost")))
            if exc is not None:
                raise exc
            return response  # type: ignore[return-value]
        except BaseException:
            if request_id is not None:
                self._pending.pop(request_id, None)
                # Best-effort in-flight cancel, mirroring the async original.
                try:
                    self._write({"id": 0, "method": "cancel", "params": {"id": request_id}})
                except (OSError, RuntimeError, TimeoutError, GoTransportError):
                    pass
            raise

    def _write(self, payload: dict) -> None:
        line = json.dumps(payload, separators=(",", ":")) + "\n"
        with self._write_lock:
            process = self._process
            if process is None or process.poll() is not None or process.stdin is None:
                raise GoTransportError("Go worker is not running")
            try:
                process.stdin.write(line.encode())
                process.stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                raise GoTransportError(f"Go worker pipe write failed: {exc}") from exc

    # ------------------------------------------------------------ teardown
    def close(self) -> None:
        self._closed = True
        process = self._process
        if process is None:
            return
        try:
            if process.stdin is not None:
                process.stdin.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        finally:
            self._fail_all(GoTransportError("Go worker bridge closed"))


class GoSandboxAPI:
    """Method facade over :class:`GoBridge`, mirroring the old ``clients/go.py`` API.

    The objects it returns are duck-typed to the seams the e2b SandboxManager
    and AenvProvider touch: ``sandbox_id`` / ``commands.run`` / ``beta_pause``
    / ``connect`` / ``kill`` / ``Sandbox.create``-style static create. This lets
    AenvGoProvider reuse the manager's handle-table plumbing unchanged.
    """

    def __init__(self, binary: str | None = None) -> None:
        self.bridge = GoBridge(binary)

    def invoke(self, method: str, params: dict, timeout: float = 120) -> Any:
        return self.bridge.call(method, params, timeout)["result"]

    # -------------------------------------------------------------- sandbox ops
    def create_sandbox(self, *, template: str, metadata: dict | None = None,
                       timeout: float = 86400, request_timeout: float = 120) -> "GoSandbox":
        data = self.invoke("create", {"template": template, "metadata": metadata or {},
                                      "timeout": timeout}, request_timeout)
        return GoSandbox(self, data)

    def connect_sandbox(self, sandbox_id: str, *, timeout: float = 86400,
                        request_timeout: float = 120) -> "GoSandbox":
        data = self.invoke("resume", {"sandbox_id": sandbox_id, "timeout": timeout}, request_timeout)
        if data.get("sandboxID") != sandbox_id:
            raise GoClientError("Resume returned a different sandbox ID")
        return GoSandbox(self, data)

    def kill_sandbox(self, sandbox_id: str, *, request_timeout: float = 120) -> Any:
        return self.invoke("delete", {"sandbox_id": sandbox_id}, request_timeout)

    def list_by_metadata(self, metadata: dict, request_timeout: float = 120) -> list[dict]:
        rows = self.invoke("list", {"metadata": metadata}, request_timeout) or []
        return list(rows)

    def close(self) -> None:
        self.bridge.close()


class GoSandbox:
    """Duck-typed e2b-Sandbox stand-in backed by the Go worker."""

    def __init__(self, api: GoSandboxAPI, data: dict) -> None:
        self.api = api
        self.data = data
        self.sandbox_id = data["sandboxID"]
        self.commands = GoCommands(self)

    def beta_pause(self, *, request_timeout: float = 120) -> Any:
        return self.api.invoke("pause", {"sandbox_id": self.sandbox_id}, request_timeout)

    def connect(self, *, timeout: float = 86400, request_timeout: float = 120) -> "GoSandbox":
        return self.api.connect_sandbox(self.sandbox_id, timeout=timeout, request_timeout=request_timeout)

    def kill(self) -> Any:
        return self.api.kill_sandbox(self.sandbox_id)


class GoCommands:
    """``commands.run`` seam: start + wait, translating to a ``CommandResult``-like object.

    The e2b manager's ``_exec_probe`` and E2BProvider.exec call
    ``handle.commands.run(cmd, user="root", timeout=N)`` and read
    ``exit_code`` / ``stdout`` / ``stderr`` off the result. The Go worker's
    command path is two-phase (command_start -> command_wait), but this facade
    collapses them back into the blocking run shape those callers expect.
    """

    def __init__(self, sandbox: "GoSandbox") -> None:
        self.sandbox = sandbox

    # The Go worker always runs "/bin/bash -l -c <cmd>" as root; the e2b SDK
    # call sites in this codebase pass user="root" exclusively, so a non-root
    # request is rejected loudly rather than silently mis-executed.
    def run(self, cmd: str, *, timeout: float = 0, user: str = "root",
            cwd: str | None = None, env: dict | None = None):  # noqa: ARG002
        if user != "root":
            raise ValueError("Go client commands require user=root")
        # cwd: the worker's bash -l -c has no first-class cwd param; emulate
        # with a quoted cd prefix (the replay config's default workdir is "/").
        command = f"cd {shlex.quote(cwd)} && {cmd}" if cwd and cwd != "/" else cmd
        # env: replay config defaults to {}; bash -l already inherits the
        # sandbox env, so explicit vars are injected via an export prefix.
        if env:
            exports = " ".join(f"{k}={shlex.quote(v)}" for k, v in env.items())
            command = f"export {exports} && {command}"
        data = self.sandbox.api.invoke(
            "command_start",
            {"sandbox": self.sandbox.data, "command": command,
             "command_timeout_sec": timeout if timeout else 0},
            0,  # start itself: no worker deadline (stream ctx owns the deadline)
        )
        wait_timeout = timeout if timeout else 0
        result = self.sandbox.api.bridge.call(
            "command_wait", data, wait_timeout
        )["result"]
        return _CommandResult(**result) if isinstance(result, dict) else _CommandResult(
            exit_code=-1, stdout="", stderr=str(result))


class _CommandResult:
    """Minimal result object: ``exit_code`` / ``stdout`` / ``stderr`` attrs."""

    def __init__(self, exit_code: int, stdout: str = "", stderr: str = "") -> None:
        self.exit_code = int(exit_code)
        self.stdout = stdout or ""
        self.stderr = stderr or ""
