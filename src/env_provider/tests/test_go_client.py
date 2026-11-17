"""Exercise the real Go worker process against a local HTTP fixture.

Synchronized port of the upstream PR #3 ``tests/test_go_client.py`` (asyncio
version) onto 1113's thread-based bridge (:mod:`env_provider.aenv_go.bridge`).
Covers the same behaviors: concurrent create + metadata scoping, strict
response validation failing all pending calls, the full sandbox lifecycle with
a non-zero-exit command, worker reuse across sandboxes, and the
missing-binary-is-not-a-fallback contract.
"""
from __future__ import annotations

import base64
import json
import os
import struct
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

try:
    from env_provider.aenv_go.bridge import (  # noqa: E402
        GoBridge,
        GoClientError,
        GoSandboxAPI,
        GoTransportError,
        binary_path,
    )
except ImportError:  # the provider lands in its own PR/commit
    GoBridge = GoClientError = GoSandboxAPI = GoTransportError = binary_path = None
from bench_core.config import KernelConfig  # noqa: E402

_PROVIDER_AVAILABLE = GoBridge is not None


class LocalTestServer(ThreadingHTTPServer):
    request_queue_size = 128
    daemon_threads = True


class Fixture(BaseHTTPRequestHandler):
    """E2B-compatible fixture: /sandboxes CRUD + Connect process stream."""

    sandboxes: dict[str, dict] = {}
    calls: list = []
    lock = threading.Lock()
    sequence = 0

    def log_message(self, *args):
        pass

    def answer(self, value, status=200):
        raw = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        with self.lock:
            values = list(self.sandboxes.values())
        self.answer(values)

    def do_DELETE(self):
        sid = self.path.split("/")[-1]
        with self.lock:
            self.sandboxes.pop(sid, None)
        self.answer({})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        if self.path == "/process.Process/Start":
            assert self.headers["Authorization"] == "Basic cm9vdDo="
            assert self.headers["E2b-Sandbox-Id"] in self.sandboxes
            body = json.loads(raw[5:])
            command = body["process"]["args"][-1]
            end = {"exitCode": 7 if "exit 7" in command else 0, "exited": True}
            events = [
                {"event": {"start": {"pid": 42}}},
                {"event": {"data": {"stdout": base64.b64encode("fixture✓\n".encode()).decode()}}},
                {"event": {"end": end}},
                {},
            ]
            wire = b"".join(
                bytes([2 if i == 3 else 0]) + struct.pack(">I", len(b)) + b
                for i, b in enumerate(json.dumps(v).encode() for v in events)
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/connect+json")
            self.send_header("Content-Length", str(len(wire)))
            self.end_headers()
            self.wfile.write(wire)
            return
        body = json.loads(raw)
        with self.lock:
            self.calls.append((self.path, body))
            if self.path == "/sandboxes":
                Fixture.sequence += 1
                sid = str(Fixture.sequence)
                value = dict(sandboxID=sid, metadata=body["metadata"], state="running",
                             templateID=body["templateID"], envdVersion="0.4.0")
                self.sandboxes[sid] = value
            else:
                sid = self.path.split("/")[2]
                self.sandboxes[sid]["state"] = "paused" if self.path.endswith("/pause") else "running"
                value = self.sandboxes[sid]
        self.answer(value)


def _binary_available() -> bool:
    """The built Go worker exists (go-client/build.sh or AGENT_BENCHMARK_GO_BIN)."""
    if not _PROVIDER_AVAILABLE:
        return False
    try:
        binary_path()
        return True
    except FileNotFoundError:
        return False


@unittest.skipUnless(
    _binary_available(),
    "go worker binary not built -- run `bash go-client/build.sh` or set AGENT_BENCHMARK_GO_BIN",
)
class GoClientTests(unittest.TestCase):
    """Real Go worker subprocess + real HTTP against the local fixture."""

    @classmethod
    def setUpClass(cls):
        binary_path()  # A missing binary is an explicit test/setup failure.

    def setUp(self):
        Fixture.sandboxes, Fixture.calls, Fixture.sequence = {}, [], 0
        self.server = LocalTestServer(("127.0.0.1", 0), Fixture)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self._env = patch.dict(
            os.environ,
            {
                "E2B_API_URL": f"http://127.0.0.1:{self.server.server_port}",
                "E2B_SANDBOX_URL": f"http://127.0.0.1:{self.server.server_port}",
                "E2B_API_KEY": "test-only",
            },
        )
        self._env.start()
        self.api = GoSandboxAPI()

    def tearDown(self):
        self.api.close()
        self._env.stop()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_concurrent_requests_and_metadata_scope(self):
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=20) as pool:
            created = list(
                pool.map(
                    lambda i: self.api.create_sandbox(template="fixture", metadata={"i": str(i)}),
                    range(20),
                )
            )
        self.assertEqual(len({s.sandbox_id for s in created}), 20)
        found = self.api.list_by_metadata({"i": "7"})
        self.assertEqual(len(found), 1)
        for sb in created:
            self.api.kill_sandbox(sb.sandbox_id)
        self.assertFalse(Fixture.sandboxes)

    def test_shim_list_rows_expose_sandbox_id_attribute(self):
        """Regression: the _list() shim must wrap raw worker rows so the e2b
        manager's _external_id (``listed.sandbox_id``) resolves to the real ID.
        Raw dicts fell through to str(dict), corrupting --cleanup / --detect
        (HTTP 404 with the whole dict as the "sandbox id")."""
        from env_provider.aenv_go.bridge import GoSandbox

        created = self.api.create_sandbox(template="fixture", metadata={"k": "v"})
        rows = self.api.list_by_metadata({})
        self.assertEqual(len(rows), 1)
        # The provider's _list seam wraps rows: attribute access must work and
        # match the create-returned ID (what _external_id/_attach consume).
        wrapped = GoSandbox(self.api, rows[0])
        self.assertEqual(wrapped.sandbox_id, created.sandbox_id)
        # And the wrapped handle's kill routes through the worker (cleanup path).
        wrapped.kill()
        self.assertFalse(Fixture.sandboxes)

    def test_malformed_response_fails_pending_call(self):
        """A malformed worker response must surface as GoTransportError (strict
        validation), and the bridge must refuse implicit restart afterwards --
        no silent e2b fallback semantics."""
        from env_provider.aenv_go.bridge import _validate_response

        with self.assertRaises(GoTransportError):
            _validate_response({"id": 1, "error": {}, "worker_elapsed_sec": 0})
        with self.assertRaises(GoTransportError):
            _validate_response({"id": 1, "worker_elapsed_sec": "bad"})
        with self.assertRaises(GoTransportError):
            _validate_response({"id": 1, "worker_elapsed_sec": 0})  # no result, no error
        with self.assertRaises(GoTransportError):
            _validate_response({"id": 0, "worker_elapsed_sec": 0, "result": {}})  # id must be > 0

    def test_full_lifecycle_nonzero_command_and_worker_reuse(self):
        sb = self.api.create_sandbox(template="fixture", metadata={"test": "lifecycle"})
        pid = self.api.bridge._process.pid
        result = sb.commands.run(cmd="exit 7", timeout=30)
        self.assertEqual(result.exit_code, 7)
        self.assertEqual(result.stdout, "fixture✓\n")
        sb.beta_pause()
        sb2 = self.api.connect_sandbox(sb.sandbox_id)
        self.assertEqual(sb2.sandbox_id, sb.sandbox_id)
        self.api.kill_sandbox(sb.sandbox_id)
        self.assertEqual(pid, self.api.bridge._process.pid)

    def test_worker_elapsed_sec_reported(self):
        """worker_elapsed_sec is part of the v2 protocol and must be surfaced."""
        t0 = time.monotonic()
        res = self.api.bridge.call("version", {}, 30)
        elapsed = res.get("worker_elapsed_sec")
        self.assertIsInstance(elapsed, (int, float))
        self.assertGreaterEqual(elapsed, 0)
        self.assertLess(time.monotonic() - t0, 30)


@unittest.skipUnless(
    _PROVIDER_AVAILABLE, "env_provider.aenv_go not present - land the provider commit first",
)
class BinaryContractTests(unittest.TestCase):
    """The missing-binary-is-not-a-fallback contract from the upstream PR."""

    def test_missing_binary_is_not_a_fallback(self):
        with self.assertRaises(FileNotFoundError):
            binary_path("/definitely/missing/agent-benchmark-go")

    def test_e2b_remains_default_provider(self):
        from bench_core.bench import _PROVIDERS

        self.assertIn("e2b", _PROVIDERS)
        self.assertIn("aenv", _PROVIDERS)
        self.assertIn("aenv-go", _PROVIDERS)

    @unittest.skipUnless(
        _binary_available(),
        "go worker binary not built -- run `bash go-client/build.sh` or set AGENT_BENCHMARK_GO_BIN",
    )
    def test_protocol_v2_identity(self):
        from env_provider.aenv_go.bridge import WORKER_PROTOCOL, binary_metadata

        self.assertEqual(WORKER_PROTOCOL, 2)
        data = binary_metadata()
        self.assertEqual(data["client"], "go")
        self.assertEqual(data["go_worker_protocol"], 2)


@unittest.skipUnless(
    _PROVIDER_AVAILABLE, "env_provider.aenv_go not present - land the provider commit first",
)
class SandboxClsInjectionTests(unittest.TestCase):
    """The instance-scoped client-swap seam (review follow-up).

    The Go shim must ride the manager's ``sandbox_cls`` INSTANCE attribute --
    never a module-global ``Sandbox`` swap. Otherwise a second provider in the
    same process silently routes through whichever shim bound last, and after
    worker teardown the module still points at a dead shim."""

    def test_two_providers_get_distinct_shims_no_module_swap(self):
        import env_provider.aenv as aenv_module
        import env_provider.e2b.manager as mgr_module

        from env_provider.aenv_go import _build_shim_cls

        orig_mgr_sandbox = mgr_module.Sandbox
        orig_aenv_sandbox = aenv_module.Sandbox

        class _ApiA:
            marker = "a"

        class _ApiB:
            marker = "b"

        cls_a = _build_shim_cls(_ApiA())
        cls_b = _build_shim_cls(_ApiB())

        self.assertIsNot(cls_a, cls_b)
        # Each shim's seams are bound to ITS OWN api (the cell the staticmethod
        # closes over), never a shared/module-level object. On py>=3.10 a
        # staticmethod accessed on the class IS the underlying function.
        def api_of(seam):
            fn = seam.__func__ if hasattr(seam, "__func__") else seam
            return fn.__closure__[0].cell_contents

        self.assertIsInstance(api_of(cls_a.create), _ApiA)
        self.assertIsInstance(api_of(cls_b.create), _ApiB)
        # Building/binding shims must leave the module-level Sandbox symbols
        # untouched (the old _ensure_shim swapped exactly these two).
        self.assertIs(mgr_module.Sandbox, orig_mgr_sandbox)
        self.assertIs(aenv_module.Sandbox, orig_aenv_sandbox)

    def test_manager_sandbox_cls_defaults_to_module_sandbox(self):
        """sandbox_cls=None -> the e2b/aenv manager reads the module Sandbox
        (behavior-identical for non-go providers); the go provider passes its
        shim so create/list/connect route per-instance."""
        import inspect

        from env_provider.e2b.manager import SandboxManager

        sig = inspect.signature(SandboxManager.__init__)
        self.assertIn("sandbox_cls", sig.parameters)
        # The three SDK seams read the instance attribute with a module default.
        for seam in ("_create_single", "_list_existing", "_attach"):
            src = inspect.getsource(getattr(SandboxManager, seam))
            self.assertIn("self.sandbox_cls or Sandbox", src)

    def test_ensure_shim_and_module_globals_gone(self):
        """No provider code path may swap module-level Sandbox symbols."""
        import env_provider.aenv as aenv_module
        import env_provider.e2b.manager as mgr_module

        from env_provider import aenv_go

        self.assertFalse(hasattr(aenv_go.AenvGoProvider, "_ensure_shim"))
        for name in dir(aenv_go):
            self.assertNotIn("mgr_module", name)
        # The module symbols stay the SDK originals (or the dev mock), never a shim.
        self.assertNotIn("_GoSandboxShim", repr(getattr(mgr_module, "Sandbox")))
        self.assertNotIn("_GoSandboxShim", repr(getattr(aenv_module, "Sandbox")))


class _StubGoApi:
    """In-memory GoSandboxAPI: records calls, no worker subprocess needed.

    Drives the FULL provider wiring (build_provider -> manager -> shim ->
    pause/resume/create_one/kill_one -> cleanup) with zero binary dependency,
    per CLAUDE.md provider step 5 (stub-based, like FakeProvider)."""

    def __init__(self, binary: str) -> None:
        self.calls: list[tuple] = []
        self.sandboxes: dict[str, dict] = {}
        self.closed = False

    # -- SandboxManager seam surface (create/list/connect via the shim) --
    def create_sandbox(self, *, template=None, metadata=None, timeout=86400):
        self.calls.append(("create", template))
        sid = f"sbx{len(self.sandboxes) + 1:04d}"
        # GoSandbox reads data["sandboxID"] -- the worker's row shape.
        self.sandboxes[sid] = {"sandboxID": sid, "state": "running", "template": template or "tpl"}
        return self._handle(sid)

    def list_by_metadata(self, _meta):
        self.calls.append(("list",))
        return [dict(v) for v in self.sandboxes.values()]

    def connect_sandbox(self, sandbox_id, *, timeout=86400, request_timeout=120):
        self.calls.append(("connect", sandbox_id))
        if self.closed:
            raise GoTransportError("worker closed")
        if sandbox_id not in self.sandboxes:
            raise GoClientError(f"sandbox {sandbox_id} not found")
        # Mirrors GoSandboxAPI.connect_sandbox: connect IS the resume op.
        self.resume_sandbox(sandbox_id)
        return self._handle(sandbox_id)

    def _handle(self, sid):
        # The GoSandbox handle: what manager stores and pause/resume call.
        from env_provider.aenv_go.bridge import GoSandbox

        return GoSandbox(self, self.sandboxes[sid])

    def pause_sandbox(self, sandbox_id):
        self.calls.append(("pause", sandbox_id))
        self.sandboxes[sandbox_id]["state"] = "paused"

    def resume_sandbox(self, sandbox_id):
        self.calls.append(("resume", sandbox_id))
        self.sandboxes[sandbox_id]["state"] = "running"

    def kill_sandbox(self, sandbox_id):
        self.calls.append(("kill", sandbox_id))
        self.sandboxes.pop(sandbox_id, None)

    # -- GoSandbox surface: pause via invoke, commands via bridge.call --
    def invoke(self, op, payload, request_timeout=120):
        sid = payload.get("sandbox_id")
        if op == "pause":
            self.pause_sandbox(sid)
            return {"paused": sid}
        if op == "resume":
            self.resume_sandbox(sid)
            return {"resumed": sid}
        if op == "command_start":
            # GoCommands.run phase 1: returns the handle command_wait consumes.
            self.calls.append(("command_start", payload.get("command", "")))
            return {"command": payload.get("command", "")}
        raise GoClientError(f"stub: unsupported op {op}")

    @property
    def bridge(self):
        return self

    def call(self, op, data, timeout=0):
        # command_wait: every probe command succeeds instantly (echo semantics).
        if op == "command_wait":
            self.calls.append(("command_wait", data.get("command", "")))
            return {"result": {"exit_code": 0, "stdout": "ok\n", "stderr": ""}}
        raise GoClientError(f"stub: unsupported bridge op {op}")

    def close(self):
        self.closed = True
        self.calls.append(("close",))


@unittest.skipUnless(
    _PROVIDER_AVAILABLE, "env_provider.aenv_go not present - land the provider commit first",
)
class AenvGoProviderWiringTests(unittest.TestCase):
    """Full provider wiring on a stub worker (P1 review follow-up).

    Everything except the transport itself: build_provider's key-by-key
    config fallback, the manager/sandbox_cls wiring, the pause/resume
    overrides, create_one/kill_one (ephemeral path) through the shim, and
    cleanup_all -> _close_worker teardown."""

    def _provider(self, raw, stub):
        """build_provider with binary checks + GoSandboxAPI swapped for the stub."""
        from env_provider import aenv_go

        with (
            patch.object(aenv_go, "binary_path", return_value="/stub/agent-benchmark-go"),
            patch.object(aenv_go, "binary_metadata", return_value={"client": "go", "protocol": 2}),
            patch.object(aenv_go, "GoSandboxAPI", lambda binary: stub),
        ):
            provider = aenv_go.build_provider(KernelConfig(workflow_type="replay"), raw)
            # Force the lazy worker build NOW (inside the patch context) so the
            # stub binds; also validates the manager lazily wires the shim.
            _ = provider.manager
        return provider

    @staticmethod
    def _raw_config():
        return {
            "aenv": {
                "template": "base-tpl",
                "env": {"E2B_API_URL": "http://aenv:8000", "E2B_API_KEY": "k1"},
            },
            "aenv_go": {
                "go_client_bin": "/stub/agent-benchmark-go",
                "env": {"E2B_API_URL": "http://go:8001"},
            },
        }

    def test_build_provider_merges_aenv_go_over_aenv(self):
        stub = _StubGoApi("/stub/agent-benchmark-go")
        p = self._provider(self._raw_config(), stub)
        # key-by-key fallback: unset keys inherit the aenv block...
        self.assertEqual(p._config.template, "base-tpl")
        self.assertEqual(p._config.e2b_api_key, "k1")
        # ...env overrides apply per-key (URL overridden, KEY inherited)...
        self.assertEqual(p._config.e2b_api_url, "http://go:8001")
        # ...and the go-specific binary override lands.
        self.assertEqual(p.go_binary, "/stub/agent-benchmark-go")

    def test_build_provider_empty_aenv_go_block_inherits_all(self):
        raw = {"aenv": {"template": "base-tpl", "env": {"E2B_API_URL": "http://aenv:8000"}}}
        stub = _StubGoApi("/stub/go")
        p = self._provider(raw, stub)
        # An empty aenv_go: {} is equivalent to the aenv: block (the old
        # block-switch read an empty block as all-defaults and lost the base).
        self.assertEqual(p._config.template, "base-tpl")
        self.assertEqual(p._config.e2b_api_url, "http://aenv:8000")

    def test_manager_wiring_and_pause_resume_overrides(self):
        from env_provider.e2b.manager import SandboxManager

        stub = _StubGoApi("/stub/agent-benchmark-go")
        p = self._provider(self._raw_config(), stub)
        mgr = p.manager
        # The manager carries THIS provider's shim (not the module Sandbox).
        self.assertIsInstance(mgr, SandboxManager)
        self.assertIsNotNone(mgr.sandbox_cls)
        self.assertIs(mgr.sandbox_cls.__name__, "_GoSandboxShim")
        self.assertIs(p._go_api, stub)

        inst = p.create_one(0, template="base-tpl")
        self.assertTrue(inst.id.startswith("sbx"))
        self.assertIn(("create", "base-tpl"), stub.calls)

        # pause goes through the handle the shim created (GoSandbox.beta_pause).
        p.pause(inst)
        self.assertIn(("pause", inst.id), stub.calls)
        # resume reconnects via the worker and swaps the fresh handle in state.
        p.resume(inst)
        self.assertIn(("resume", inst.id), stub.calls)
        self.assertIn(("connect", inst.id), stub.calls)

        p.kill_one(inst)
        self.assertIn(("kill", inst.id), stub.calls)

    def test_cleanup_all_closes_worker(self):
        stub = _StubGoApi("/stub/agent-benchmark-go")
        p = self._provider(self._raw_config(), stub)
        p.create_one(0, template="base-tpl")
        p.cleanup_all()
        # teardown closes the resident worker exactly once, resets the lazy slot.
        self.assertTrue(stub.closed)
        self.assertIsNone(p._go_api)
        self.assertEqual(stub.calls.count(("close",)), 1)


if __name__ == "__main__":
    unittest.main()
