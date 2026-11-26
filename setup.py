"""Build the Go worker during ``pip install``.

``oversub-bench`` now defaults to the ``aenv-go`` provider, whose worker binary
must exist before the first trial. A plain ``git clone`` + ``pip install -e .``
never touched ``go-client/bin/``, so installs now build it here (build_py runs
for both ``pip install .`` and PEP 660 ``pip install -e .``).

Toolchain resolution (first match wins):
  1. ``AGENT_BENCHMARK_GO`` -- explicit path to a ``go`` binary;
  2. ``go`` on ``PATH`` when ``go version`` is >= 1.22;
  3. a cached bootstrap toolchain under
     ``$XDG_CACHE_HOME/agent-benchmark/go/go1.22.12`` (default ``~/.cache``),
     downloaded from golang.google.cn / mirrors.aliyun.com with the published
     sha256 verified before extraction. (GOTOOLCHAIN auto-download is not
     relied on: it is unavailable on the restricted networks this project
     targets.)

Escape hatches:
  ``AGENT_BENCHMARK_SKIP_GO_BUILD=1`` skips the hook entirely (Docker/E2B-only
  installs, air-gapped CI). Without it, a download or build failure fails the
  install loudly instead of leaving the default provider unusable.
"""
from __future__ import annotations

import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import NoReturn

from setuptools import setup
from setuptools.command.build_py import build_py

ROOT = Path(__file__).resolve().parent
GO_SRC = ROOT / "go-client"
BINARY_NAME = "agent-benchmark-go" + (".exe" if os.name == "nt" else "")
BINARY = GO_SRC / "bin" / BINARY_NAME

GO_VERSION = "1.22.12"
MIN_GO = (1, 22)
SKIP_ENV = "AGENT_BENCHMARK_SKIP_GO_BUILD"
GO_ENV = "AGENT_BENCHMARK_GO"

# sha256 of the official go1.22.12 archives (go.dev/dl JSON, cross-checked
# against mirrors.aliyun.com/golang).
_TOOLCHAIN_SHA256 = {
    ("linux", "amd64"): "4fa4f869b0f7fc6bb1eb2660e74657fbf04cdd290b5aef905585c86051b34d43",
    ("linux", "arm64"): "fd017e647ec28525e86ae8203236e0653242722a7436929b1f775744e26278e7",
    ("darwin", "amd64"): "e7bbe07e96f0bd3df04225090fe1e7852ed33af37c43a23e16edbbb3b90a5b7c",
    ("darwin", "arm64"): "416c35218edb9d20990b5d8fc87be655d8b39926f15524ea35c66ee70273050d",
    ("windows", "amd64"): "2ceda04074eac51f4b0b85a9fcca38bcd49daee24bed9ea1f29958a8e22673a6",
    ("windows", "arm64"): "6b9eaf160b155e02ffe9ed603f162ecc3264f6130c8fcf83bb77087f9807fdec",
}
_MIRRORS = (
    "https://golang.google.cn/dl/{filename}",
    "https://mirrors.aliyun.com/golang/{filename}",
)


def _say(message: str) -> None:
    print(f"[go-client] {message}", flush=True)


def _fail(message: str) -> NoReturn:
    raise SystemExit(
        f"[go-client] {message}\n"
        f"[go-client] Set {SKIP_ENV}=1 to skip the Go worker build "
        f"(Docker/E2B-only or offline installs), or build it manually with "
        f"`bash go-client/build.sh` and set {GO_ENV}."
    )


def _go_version_ok(go_bin: Path) -> bool:
    try:
        proc = subprocess.run(
            [str(go_bin), "version"], capture_output=True, text=True, timeout=60
        )
    except (OSError, subprocess.SubprocessError):
        return False
    match = re.search(r"\bgo(\d+)\.(\d+)", proc.stdout or "")
    return bool(match) and (int(match.group(1)), int(match.group(2))) >= MIN_GO


def _platform_key() -> tuple[str, str]:
    if sys.platform.startswith("linux"):
        goos = "linux"
    elif sys.platform == "darwin":
        goos = "darwin"
    elif sys.platform.startswith("win"):
        goos = "windows"
    else:
        _fail(f"unsupported platform for the Go toolchain bootstrap: {sys.platform}")
    machine = platform.machine().lower()
    goarch = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(machine)
    if goarch is None:
        _fail(f"unsupported architecture for the Go toolchain bootstrap: {machine}")
    return goos, goarch


def _cache_root() -> Path:
    base = os.environ.get("XDG_CACHE_HOME")
    root = Path(base) if base else Path.home() / ".cache"
    return root / "agent-benchmark" / "go"


def _bootstrap_toolchain() -> Path:
    """Download + verify + extract go1.22.12 into the cache; return its go binary."""
    goos, goarch = _platform_key()
    extension = "zip" if goos == "windows" else "tar.gz"
    filename = f"go{GO_VERSION}.{goos}-{goarch}.{extension}"
    expected = _TOOLCHAIN_SHA256[(goos, goarch)]
    install_dir = _cache_root() / f"go{GO_VERSION}"
    go_bin = install_dir / "bin" / ("go.exe" if goos == "windows" else "go")
    if go_bin.is_file() and _go_version_ok(go_bin):
        _say(f"using cached toolchain {go_bin}")
        return go_bin

    _say(f"no Go >= {MIN_GO[0]}.{MIN_GO[1]} on PATH; bootstrapping {filename}")
    cache = _cache_root()
    cache.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=cache) as tmp:
        tmp_dir = Path(tmp)
        archive = tmp_dir / filename
        for mirror in _MIRRORS:
            url = mirror.format(filename=filename)
            try:
                _say(f"downloading {url}")
                with urllib.request.urlopen(url, timeout=120) as response, open(archive, "wb") as out:
                    shutil.copyfileobj(response, out)
                break
            except OSError as exc:
                _say(f"download failed from {url}: {exc}")
        else:
            _fail(f"could not download {filename} from any mirror")
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        if digest != expected:
            _fail(f"sha256 mismatch for {filename}: got {digest}, want {expected}")

        extract_root = tmp_dir / "extract"
        extract_root.mkdir()
        if extension == "zip":
            with zipfile.ZipFile(archive) as bundle:
                bundle.extractall(extract_root)
        else:
            with tarfile.open(archive, "r:gz") as bundle:
                bundle.extractall(extract_root)
        install_dir.parent.mkdir(parents=True, exist_ok=True)
        if install_dir.exists():
            shutil.rmtree(install_dir)
        shutil.move(str(extract_root / "go"), str(install_dir))
    _say(f"installed toolchain at {install_dir}")
    return go_bin


def _resolve_go() -> Path:
    override = os.environ.get(GO_ENV)
    if override:
        path = Path(override).expanduser()
        if not _go_version_ok(path):
            _fail(f"{GO_ENV}={override} is not a Go >= {MIN_GO[0]}.{MIN_GO[1]} binary")
        return path
    on_path = shutil.which("go")
    if on_path and _go_version_ok(Path(on_path)):
        return Path(on_path)
    return _bootstrap_toolchain()


def _build_worker() -> None:
    if os.environ.get(SKIP_ENV) == "1":
        _say(f"{SKIP_ENV}=1: skipping the Go worker build")
        return
    go_bin = _resolve_go()
    BINARY.parent.mkdir(parents=True, exist_ok=True)
    # stdlib-only module: pin the toolchain and forbid module downloads so a
    # restricted network can never turn a local build into a network fetch.
    env = dict(os.environ, GOTOOLCHAIN="local", GOPROXY="off")
    command = [str(go_bin), "build", "-trimpath", "-o", str(BINARY), "."]
    _say(f"{' '.join(command)} (cwd={GO_SRC})")
    try:
        subprocess.run(command, cwd=GO_SRC, env=env, check=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"go build failed: {exc}")


class GoBuildCommand(build_py):
    """Build the worker, then ship it as package data for non-editable wheels."""

    def run(self) -> None:
        _build_worker()
        super().run()
        if os.environ.get(SKIP_ENV) == "1":
            return
        target = Path(self.build_lib) / "env_provider" / "aenv_go" / "bin" / BINARY_NAME
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(BINARY, target)
        target.chmod(0o755)


try:
    try:
        from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
    except ImportError:
        from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

    class GoBdistWheel(_bdist_wheel):
        """The wheel carries a platform-specific binary, so it is not pure."""

        def finalize_options(self) -> None:
            super().finalize_options()
            self.root_is_pure = False

        def get_tag(self) -> tuple[str, str, str]:
            _, _, platform_tag = super().get_tag()
            return "py3", "none", platform_tag

    _cmdclass = {"build_py": GoBuildCommand, "bdist_wheel": GoBdistWheel}
except ImportError:  # pragma: no cover - wheel is in build-system requires
    _cmdclass = {"build_py": GoBuildCommand}

setup(cmdclass=_cmdclass)
