"""CLI entry for the host-agnostic benchmark kernel.

Split out of :mod:`bench_core.bench` so the spine (``run_benchmark``) stays
importable without argparse. Installed as the ``bench-core`` console script.
"""
from __future__ import annotations

import argparse
import importlib
import logging
import time
from pathlib import Path
from typing import Any

from bench_core.bench import _PROVIDERS, run_benchmark
from bench_core.config import KernelConfig
from bench_core.task_runner.replay import ReplayConfig
from bench_core.utils import setup_logging
from env_provider import EnvironmentProvider


def load_config(path: str | Path, *, workflow_type_override: str | None = None) -> tuple[KernelConfig, dict[str, Any]]:
    """Load a :class:`KernelConfig` from a YAML file (unified schema).

    Reads the shared stress sections via :meth:`KernelConfig.from_raw`, so the
    kernel picks up ``create_batch`` / ``test`` / ``browser`` / ``sandbox``
    instead of running on defaults. Returns the config plus the raw YAML dict;
    backend blocks (``e2b:`` / ``docker:``) are passed through for the provider
    to read -- the kernel never reads them.

    ``workflow_type_override`` is applied to ``raw`` *before* ``from_raw`` (not
    post-hoc on the built config) so ``from_raw`` selects the right
    ``config_cls`` view; setting it on the built config later would leave the
    typed view bound to the YAML's workflow (RFC 0002 P2-1).
    """
    import yaml

    with open(path, encoding="utf-8") as handle:
        raw: dict[str, Any] = yaml.safe_load(handle) or {}

    if workflow_type_override is not None:
        raw["workflow_type"] = workflow_type_override
    return KernelConfig.from_raw(raw), raw


def _apply_monitor_override(config: KernelConfig, args) -> None:
    """Apply --vm-monitor / --no-vm-monitor CLI overrides to config.monitor.enabled."""
    if getattr(args, "no_vm_monitor", False):
        config.monitor.enabled = "false"
    elif getattr(args, "vm_monitor", "auto") != "auto":
        config.monitor.enabled = args.vm_monitor


def build_arg_parser() -> argparse.ArgumentParser:
    """Host-agnostic CLI. Provider packages add their own flags on top."""
    parser = argparse.ArgumentParser(description="Host-agnostic benchmark kernel")
    parser.add_argument("--config", help="YAML config path")
    parser.add_argument("--provider", default="fake", choices=list(_PROVIDERS))
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning"],
        help="root log level (default info; debug also surfaces per-step " "stderr for non-zero exits)",
    )
    parser.add_argument("-n", "--total-count", type=int)
    parser.add_argument("--workflow-type", choices=["browser", "coding", "document", "replay"])
    parser.add_argument("-bm", "--benchmark-mode", choices=["fixed", "round_robin"])
    parser.add_argument("--round-count", type=int)
    parser.add_argument("--round-size", type=int)
    parser.add_argument("--test-duration", type=int)
    parser.add_argument("--benchmark-percent", type=float)
    parser.add_argument("--create-only", action="store_true")
    parser.add_argument("--detect", action="store_true")
    parser.add_argument("--warmup-only", action="store_true")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="list + kill all existing sandboxes (teardown of a prior "
        "--create-only/--detect run that left them running), then exit",
    )
    parser.add_argument("-o", "--output-dir")
    parser.add_argument(
        "--report-format",
        choices=["txt", "xlsx", "both"],
        default=None,
        help="report output format (default: txt; xlsx/both add an openpyxl workbook)",
    )
    parser.add_argument(
        "--vm-monitor",
        choices=["auto", "true", "false"],
        default="auto",
        help="Override vm_monitor auto-enable: auto (default, by provider vmm_type), true, false.",
    )
    parser.add_argument(
        "--no-vm-monitor",
        action="store_true",
        help="Short-circuit vm_monitor off (overrides --vm-monitor and YAML).",
    )
    return parser


def _build_provider(name: str, config: KernelConfig, raw_config: dict[str, Any]) -> EnvironmentProvider:
    """Construct a provider by name.

    All providers live as submodules of ``env_provider`` (``e2b``, ``docker``,
    ``fake``, ``aenv``) and are lazy-imported here -- the kernel never imports a
    backend statically, so the layering rule (bench_core must not import
    e2b_bench / docker_bench) holds. ``env_provider``'s contract stays
    SDK-free; loading a provider submodule is what pulls in that backend's SDK.

    The name set is shared with ``_PROVIDERS`` (argparse choices + capability
    hint); keep the two in sync -- see CLAUDE.md "Adding a new provider".
    """
    if name == "fake":
        from env_provider.fake import FakeProvider

        return FakeProvider(count=config.total_count)
    if name == "e2b":
        from env_provider.e2b import build_provider
    elif name == "docker":
        from env_provider.docker import build_provider
    elif name == "aenv":
        from env_provider.aenv import build_provider
    elif name == "aenv-go":
        from env_provider.aenv_go import build_provider
    elif name == "cubesandbox":
        from env_provider.cubesandbox import build_provider
    else:
        raise ValueError(f"Unknown provider: {name}")
    return build_provider(config, raw_config)


def main() -> None:
    """CLI entry point (host-agnostic smoke via ``--provider fake``)."""
    args = build_arg_parser().parse_args()
    setup_logging(level=getattr(logging, args.log_level.upper()))

    if args.config:
        config, raw = load_config(args.config, workflow_type_override=args.workflow_type or None)
    else:
        config, raw = KernelConfig(), {}
        if args.workflow_type:
            # No --config path skips from_raw, so the typed view is not built; apply
            # the override directly (degenerate -- a real run uses --config).
            config.workflow_type = args.workflow_type

    # CLI overrides (only when explicitly set).
    if args.total_count is not None:
        config.total_count = args.total_count
    if args.benchmark_mode:
        config.benchmark_mode = args.benchmark_mode
    if args.round_count is not None:
        config.round_count = args.round_count
    if args.round_size is not None:
        config.round_size = args.round_size
    if args.test_duration is not None:
        config.test_duration = args.test_duration
    if args.benchmark_percent is not None:
        config.benchmark_percent = args.benchmark_percent
    if args.create_only:
        config.create_only = True
    if args.detect:
        config.detect_existing = True
    if args.warmup_only:
        config.warmup_only = True
    if args.cleanup:
        config.cleanup_only = True
    if args.output_dir:
        config.output_dir = args.output_dir
    if args.report_format:
        config.report_format = args.report_format
    _apply_monitor_override(config, args)

    # Stamp each run into its own subdir so the log, lifecycle series, report,
    # and vm_monitor outputs never overwrite a previous run (mirrors the
    # runs/<name>-<timestamp>/ layout). Applies to every workflow; the report
    # filename's own timestamp becomes redundant but harmless.
    run_stamp = time.strftime("%Y%m%d-%H%M%S")
    config.output_dir = str(Path(config.output_dir) / f"{config.filename_prefix}_{run_stamp}")

    # Attach a file handler once config (and CLI overrides) are resolved.
    # Stdout stays plaintext for live tailing; JSON lines go to the file only
    # for the lifecycle replay mode.
    log_dir = Path(config.output_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(log_dir / f"{config.filename_prefix}.log")
    setup_logging(
        level=getattr(logging, args.log_level.upper()),
        log_path=log_path,
        json_lines=isinstance(config.workflow_config, ReplayConfig)
        and config.workflow_config.replay_mode == "lifecycle",
    )

    provider = _build_provider(args.provider, config, raw)
    run_benchmark(config, provider)


if __name__ == "__main__":
    main()
