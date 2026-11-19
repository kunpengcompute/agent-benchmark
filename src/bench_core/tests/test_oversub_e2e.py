"""Unit tests for the oversub benchmark driver's pure helpers.

The driver's pure helpers are module-level functions in bench_core.oversub,
so pytest imports them directly (no conftest path hack). The subprocess +
main() path is covered by the --dry-run integration test (Task 7)."""

from __future__ import annotations

import csv

import json

from pathlib import Path

import yaml

import os

import subprocess

import sys

from bench_core.oversub import (
    build_trial_config,
    default_running_concurrency,
    parse_ratios,
)

from bench_core.oversub import compute_valid, parse_run_summary

from bench_core.oversub import aggregate_ratio_summary, trial_row, write_outputs

from bench_core.oversub import main

import pytest  # noqa: E402

from bench_core.oversub import (  # noqa: E402
    _coerce_cmd,
    _coerce_modes,
    _coerce_ratios,
    load_sweep_config,
)


def _summary(
    total=768,
    succeeded=768,
    failed=0,
    wall_sec=940.0,
    peak_active=380,
    maximum=384,
    mode="lifecycle",
    test_duration=1000,
):
    return {
        "replay_mode": mode,
        "test_duration": test_duration,
        "wall_sec": wall_sec,
        "throughput": {"total": total, "succeeded": succeeded, "failed": failed},
        "admission": {"maximum": maximum, "peak_active": peak_active} if peak_active is not None else None,
    }


def _trial_row(mode, ratio, repeat, *, wall=100.0, tps=10.0, peak=380, valid=True, failed=0, total=768):
    s = _summary(total=total, succeeded=total - failed, failed=failed, wall_sec=wall, peak_active=peak, mode=mode)
    s["throughput"]["tasks_per_sec"] = tps  # thread tps so throughput-degradation medians are non-zero
    return trial_row(
        mode=mode,
        ratio=ratio,
        repeat=repeat,
        running_concurrency=384,
        target_count=ratio * 384,
        summary=s,
        return_code=0,
        valid=valid,
        reused=False,
        trial_dir="t",
        run_summary_path="s.json",
    )


def _write_stub_bench_core(stub_path: Path, prefix: str):
    """Write a stub script that mimics bench-core: stamps a subdir, writes a
    canned run_summary.json + trajectories/index.json, exits 0. The driver
    invokes it via --bench-core-bin."""
    stub_path.write_text(
        f"""import json, sys, time
from pathlib import Path
import yaml
cfg = yaml.safe_load(open(sys.argv[sys.argv.index('--config')+1]))
pfx = cfg['report']['filename_prefix']
out = Path(cfg['report']['output_dir'])
stamp = time.strftime('%Y%m%d-%H%M%S')
run_dir = out / f'{{pfx}}_{{stamp}}'
run_dir.mkdir(parents=True, exist_ok=True)
summary = {{
  'schema_version': 2, 'workflow_type': 'replay',
  'replay_mode': cfg['replay']['mode'], 'provider': 'stub',
  'started_at': '2026-09-05T14:00:00+08:00',
  'completed_at': '2026-09-05T14:18:00+08:00',
  'started_epoch': 1000.0, 'completed_epoch': 2080.0, 'wall_sec': 1080.0,
  'test_duration': cfg['test']['duration'],
  'total_count': cfg['sandbox']['total_count'],
  'running_concurrency': cfg['replay']['running_concurrency'],
  'overcommit_ratio': cfg['sandbox']['total_count'] / cfg['replay']['running_concurrency'],
  'throughput': {{'total': cfg['sandbox']['total_count'],
                 'succeeded': cfg['sandbox']['total_count'],
                 'failed': 0, 'total_steps': 10,
                 'steps_per_sec': 0.01, 'tasks_per_sec': 0.7}},
  'admission': None, 'lifecycle_overhead': None,
  'paths': {{'report': None, 'obs_xlsx': None, 'lifecycle_series': None,
            'trajectory_index': None, 'vm_monitor_dir': None}},
  'error': None}}
(run_dir / f'{{pfx}}_run_summary.json').write_text(json.dumps(summary, indent=2)+'\\n')
(run_dir / 'trajectories').mkdir(parents=True, exist_ok=True)
idx = {{'n_trajectories': 1, 'trajectories': [
  {{'trajectory_id':'t0','sandbox_index':0,'n_steps':1,'n_failed':0,'n_timeout':0,
   'success_rate':1.0,'elapsed_sec':1.0,
   'time_breakdown_sec':{{'slice_total':0.8,'exec':0.5,'resume':0.1,'pause':0.2,
     'requested_delay':0.1,'create':0.05,'kill':0.05,'interaction_total':0.9,
     'slot_contention_wait':0.0,'resume_rate_pacing_wait':0.0,'pause_rate_pacing_wait':0.0,
     'running_slot_held':0.8}},
   'create_error_type':None,'kill_error_type':None,'file':'t0/replay_result.json'}}]}}
(run_dir / 'trajectories' / 'index.json').write_text(json.dumps(idx, indent=2)+'\\n')
""",
        encoding="utf-8",
    )


def test_dry_run_writes_trial_yamls_and_empty_outputs(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text(
        yaml.safe_dump(
            {
                "workflow_type": "replay",
                "sandbox": {"total_count": 4},
                "replay": {"running_concurrency": 4, "mode": "lifecycle"},
                "test": {"duration": 60, "round_size": 4, "round_count": 1, "benchmark_mode": "round_robin"},
                "report": {"output_dir": "results/replay", "filename_prefix": "rb", "format": "both"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    out_root = tmp_path / "sweep"
    rc = main(
        [
            "--config",
            str(base),
            "--provider",
            "aenv",
            "--ratios",
            "1,2",
            "--modes",
            "lifecycle",
            "--repeats",
            "1",
            "--test-duration",
            "60",
            "--dry-run",
            "--output-root",
            str(out_root),
        ]
    )
    assert rc == 0
    # Two trial.yaml files generated (ratio 1 + 2).
    yamls = sorted(out_root.glob("lifecycle/ratio-*/repeat-00/trial.yaml"))
    assert len(yamls) == 2
    # trial.yaml for ratio=2 carries total_count=8, round_size=8, round_count=0.
    cfg2 = yaml.safe_load(yamls[1].read_text(encoding="utf-8"))
    assert cfg2["sandbox"]["total_count"] == 8
    assert cfg2["test"]["round_size"] == 8
    assert cfg2["test"]["round_count"] == 1
    assert (out_root / "benchmark-report.json").exists()


def test_run_with_stub_aggregates_trials(tmp_path):
    """End-to-end driver run using a stub bench-core (no real kernel/SDK)."""
    stub = tmp_path / "stub_bench_core.py"
    _write_stub_bench_core(stub, "rb")

    base = tmp_path / "base.yaml"
    base.write_text(
        yaml.safe_dump(
            {
                "workflow_type": "replay",
                "sandbox": {"total_count": 4},
                "replay": {"running_concurrency": 4, "mode": "lifecycle"},
                "test": {"duration": 60, "round_size": 4, "round_count": 1, "benchmark_mode": "round_robin"},
                "report": {"output_dir": "results/replay", "filename_prefix": "rb", "format": "both"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    out_root = tmp_path / "sweep"
    rc = main(
        [
            "--config",
            str(base),
            "--provider",
            "aenv",
            "--ratios",
            "1,2",
            "--modes",
            "lifecycle",
            "--repeats",
            "1",
            "--test-duration",
            "60",
            "--output-root",
            str(out_root),
            "--bench-core-bin",
            sys.executable,
            str(stub),
            "--no-vm-monitor",
            "--cooldown-sec",
            "0",
            "--cleanup-between-trials",
            "off",  # stub always writes a summary even for --cleanup (semantically wrong); real bench-core handles --cleanup correctly.
        ]
    )
    assert rc == 0
    # Both trials valid (stub returns wall 1080 >= 0.9*60, total>0, no admission).
    with open(out_root / "trial-summary.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2  # 2 trials
    assert all(r["valid"] == "True" for r in rows)
    # trajectory-detail has 2 rows (one per stub trial); the stub seeds a
    # time_breakdown_sec so the breakdown columns must surface with its values.
    with open(out_root / "trajectory-detail.csv", newline="", encoding="utf-8") as f:
        trj = list(csv.DictReader(f))
    assert len(trj) == 2
    for r in trj:
        assert r["trajectory_id"] == "t0"
        assert r["exec_sec"] == "0.5"
        assert r["resume_sec"] == "0.1"
        assert r["pause_sec"] == "0.2"
        assert r["slice_total_sec"] == "0.8"
        assert r["create_sec"] == "0.05"


def _base_yaml_dict() -> dict:
    return {
        "workflow_type": "replay",
        "sandbox": {"total_count": 4},
        "replay": {"running_concurrency": 4, "mode": "lifecycle"},
        "test": {"duration": 60, "round_size": 4, "round_count": 1, "benchmark_mode": "round_robin"},
        "report": {"output_dir": "results/replay", "filename_prefix": "rb", "format": "both"},
    }


def test_coerce_ratios_list_and_string():
    assert _coerce_ratios([1, 2, 3]) == [1, 2, 3]
    assert _coerce_ratios("1,2,3") == [1, 2, 3]
    with pytest.raises(ValueError):
        _coerce_ratios([0, 1])  # non-positive
    with pytest.raises(ValueError):
        _coerce_ratios(5)  # wrong type


def test_coerce_modes_list_and_string():
    assert _coerce_modes(["lifecycle", "exec_only"]) == ["lifecycle", "exec_only"]
    assert _coerce_modes("lifecycle, exec_only") == ["lifecycle", "exec_only"]
    with pytest.raises(ValueError):
        _coerce_modes(5)


def test_coerce_cmd_scalar_not_char_spread():
    # A YAML scalar ``bench_core_bin: bench-core`` must become a one-element
    # argv list, NOT be spread into chars by ``[*args.bench_core_bin, ...]``.
    assert _coerce_cmd("bench-core") == ["bench-core"]
    assert _coerce_cmd(["python", "-m", "stub"]) == ["python", "-m", "stub"]
    with pytest.raises(ValueError):
        _coerce_cmd(5)


def test_load_sweep_config_rejects_unknown_keys(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("base_config: x.yaml\nrepeat: 1\n", encoding="utf-8")  # 'repeat' is a typo of 'repeats'
    with pytest.raises(ValueError, match="unknown sweep-config key"):
        load_sweep_config(p)


def test_shipped_example_sweep_config_loads():
    repo = Path(__file__).resolve().parents[3]  # repo root
    cfg = load_sweep_config(repo / "config" / "oversub" / "lifecycle-1to3.yaml")
    assert cfg["base_config"] == "config/common/replay.yaml"
    assert cfg["ratios"] == [1, 2, 3]
    assert cfg["modes"] == ["lifecycle", "exec_only"]
    # template should load too (every key must be known).
    load_sweep_config(repo / "config" / "oversub" / "template.yaml")


def test_main_default_trial_timeout_is_nonzero(tmp_path):
    """The built-in trial_timeout_sec default is nonzero: a safety net so a
    hung kernel subprocess cannot block the sweep indefinitely. A 1:1/384-
    sandbox run hung for ~24h because the shipped config set 0 (= off) and
    _run_subprocess did proc.wait(timeout=None). 0 stays a valid explicit
    opt-out; the default just is no longer 0."""
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump(_base_yaml_dict(), sort_keys=False), encoding="utf-8")
    sweep = tmp_path / "sweep.yaml"
    # no trial_timeout_sec key -> resolves to the built-in default
    sweep.write_text(
        yaml.safe_dump({"base_config": str(base), "ratios": [1], "modes": ["lifecycle"]}, sort_keys=False),
        encoding="utf-8",
    )
    out_root = tmp_path / "sweep"
    rc = main(["--sweep-config", str(sweep), "--dry-run", "--output-root", str(out_root)])
    assert rc == 0
    rep = json.loads((out_root / "benchmark-report.json").read_text(encoding="utf-8"))
    # built-in default (no CLI flag, no sweep-config value) is nonzero
    assert rep["configuration"]["trial_timeout_sec"] != 0
    assert rep["configuration"]["trial_timeout_sec"] > 0


def test_shipped_lifecycle_sweep_does_not_disable_trial_timeout():
    """The shipped 1:1/1:2/1:3 sweep must not force trial_timeout_sec: 0 (a
    hung kernel would block the sweep forever). Omitting it inherits the
    nonzero built-in default."""
    repo = Path(__file__).resolve().parents[3]
    cfg = load_sweep_config(repo / "config" / "oversub" / "lifecycle-1to3.yaml")
    assert cfg.get("trial_timeout_sec", 1) != 0


def test_main_sweep_config_drives_trials(tmp_path):
    """--sweep-config alone (base_config inside) drives the matrix via dry-run."""
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump(_base_yaml_dict(), sort_keys=False), encoding="utf-8")
    sweep = tmp_path / "sweep.yaml"
    sweep.write_text(
        yaml.safe_dump(
            {"base_config": str(base), "ratios": [1, 2], "modes": ["lifecycle"], "test_duration": 60},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    out_root = tmp_path / "sweep"
    rc = main(["--sweep-config", str(sweep), "--dry-run", "--output-root", str(out_root)])
    assert rc == 0
    yamls = sorted(out_root.glob("lifecycle/ratio-*/repeat-00/trial.yaml"))
    assert len(yamls) == 2
    cfg2 = yaml.safe_load(yamls[1].read_text(encoding="utf-8"))
    assert cfg2["sandbox"]["total_count"] == 8  # 2 * N(4)
    assert cfg2["test"]["round_count"] == 1  # one pass of the fleet
    # configuration recorded both base_config + sweep_config in the report.
    rep = json.loads((out_root / "benchmark-report.json").read_text(encoding="utf-8"))
    assert rep["configuration"]["base_config"] == str(base)
    assert rep["configuration"]["sweep_config"] == str(sweep)
    assert rep["configuration"]["ratios"] == [1, 2]
    assert rep["configuration"]["modes"] == ["lifecycle"]


def test_main_cli_overrides_sweep_config(tmp_path):
    """A CLI flag (--ratios 2) beats the sweep-config's ratios [1,2,3]."""
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump(_base_yaml_dict(), sort_keys=False), encoding="utf-8")
    sweep = tmp_path / "sweep.yaml"
    sweep.write_text(
        yaml.safe_dump(
            {"base_config": str(base), "ratios": [1, 2, 3], "modes": ["lifecycle"], "test_duration": 60},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    out_root = tmp_path / "sweep"
    rc = main(["--sweep-config", str(sweep), "--ratios", "2", "--dry-run", "--output-root", str(out_root)])
    assert rc == 0
    # Only one ratio-2 trial.yaml (CLI --ratios 2 overrode the YAML's [1,2,3]).
    yamls = sorted(out_root.glob("lifecycle/ratio-*/repeat-00/trial.yaml"))
    assert len(yamls) == 1
    cfg = yaml.safe_load(yamls[0].read_text(encoding="utf-8"))
    assert cfg["sandbox"]["total_count"] == 8
    rep = json.loads((out_root / "benchmark-report.json").read_text(encoding="utf-8"))
    assert rep["configuration"]["ratios"] == [2]


def test_main_config_flag_overrides_sweep_base_config(tmp_path):
    """--config beats the sweep-config's base_config pointer."""
    base_real = tmp_path / "real.yaml"
    base_real.write_text(yaml.safe_dump(_base_yaml_dict(), sort_keys=False), encoding="utf-8")
    base_decoy = tmp_path / "decoy.yaml"
    decoy = _base_yaml_dict()
    decoy["sandbox"]["total_count"] = 99  # if used, total_count would be 198
    base_decoy.write_text(yaml.safe_dump(decoy, sort_keys=False), encoding="utf-8")
    sweep = tmp_path / "sweep.yaml"
    sweep.write_text(
        yaml.safe_dump({"base_config": str(base_decoy), "ratios": [2], "modes": ["lifecycle"]}, sort_keys=False),
        encoding="utf-8",
    )
    out_root = tmp_path / "sweep"
    rc = main(["--sweep-config", str(sweep), "--config", str(base_real), "--dry-run", "--output-root", str(out_root)])
    assert rc == 0
    cfg = yaml.safe_load(sorted(out_root.glob("lifecycle/ratio-*/repeat-00/trial.yaml"))[0].read_text(encoding="utf-8"))
    # real.yaml (total_count 4) used, not decoy (99): 2*4 = 8, not 2*99.
    assert cfg["sandbox"]["total_count"] == 8


def test_main_sweep_scalar_bench_core_bin_reaches_cmd(tmp_path, caplog):
    """A scalar bench_core_bin in YAML must reach the dry-run cmd as one argv token
    (proves ``_coerce_cmd`` wiring: not char-split by ``[*args.bench_core_bin, ...]``)."""
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump(_base_yaml_dict(), sort_keys=False), encoding="utf-8")
    sweep = tmp_path / "sweep.yaml"
    sweep.write_text(
        yaml.safe_dump(
            {"base_config": str(base), "ratios": [1], "modes": ["lifecycle"], "bench_core_bin": "mybench"},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    out_root = tmp_path / "sweep"
    with caplog.at_level("INFO", logger="bench_core.oversub"):
        rc = main(["--sweep-config", str(sweep), "--dry-run", "--output-root", str(out_root)])
    assert rc == 0
    dry = [r.message for r in caplog.records if "[dry-run]" in r.message]
    assert dry, "expected a [dry-run] log line"
    # "[dry-run] mybench --config ...": mybench is a single token, not m / y / b / ...
    assert dry[0].split()[1] == "mybench"


def test_main_default_output_root_lands_under_results(tmp_path, monkeypatch):
    """With no --output-root, the sweep dir lives under results/oversub/ (the repo
    convention for benchmark outputs), never under runs/."""
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump(_base_yaml_dict(), sort_keys=False), encoding="utf-8")
    sweep = tmp_path / "sweep.yaml"
    sweep.write_text(
        yaml.safe_dump({"base_config": str(base), "ratios": [1], "modes": ["lifecycle"]}, sort_keys=False),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)  # so the relative results/ path stays inside tmp_path
    rc = main(["--sweep-config", str(sweep), "--dry-run"])
    assert rc == 0
    sweep_dirs = list((tmp_path / "results" / "oversub").glob("oversub-N4-*"))
    assert sweep_dirs, "default output_root must land under results/oversub/"
    assert list(
        sweep_dirs[0].glob("lifecycle/ratio-*/repeat-00/trial.yaml")
    ), "trial.yaml written under the default root"
    assert not (tmp_path / "runs").exists(), "nothing should be written under runs/"


def test_main_missing_config_returns_error(tmp_path):
    # No --config and no --sweep-config.
    assert main([]) == 2
    # Sweep-config without base_config and no --config.
    sweep = tmp_path / "sweep.yaml"
    sweep.write_text(yaml.safe_dump({"ratios": [1, 2]}, sort_keys=False), encoding="utf-8")
    assert main(["--sweep-config", str(sweep)]) == 2


def test_auto_compare_end_to_end_and_graceful(tmp_path):
    """_auto_compare writes comparison/ next to the sweep CSVs; a dir without
    the CSVs degrades to a warning (never raises -- the sweep must not fail)."""
    from bench_core.oversub import _auto_compare

    sweep = tmp_path / "sweep"
    write_outputs([_trial_row("lifecycle", 1, 1)], output_root=sweep)
    _auto_compare(sweep)
    assert (sweep / "comparison" / "comparison.xlsx").exists()
    assert (sweep / "comparison" / "comparison-ratio-summary.csv").exists()
    assert (sweep / "comparison" / "comparison-cross-ratio-trajectory.csv").exists()

    _auto_compare(tmp_path / "nonexistent")  # warning, no raise
    assert not (tmp_path / "nonexistent" / "comparison").exists()


def test_auto_compare_flag_defaults():
    from bench_core.oversub import _TRUE_DEFAULTS, build_arg_parser

    assert _TRUE_DEFAULTS["auto_compare"] is True
    assert build_arg_parser().parse_args([]).auto_compare is None  # sentinel; main() resolves
    assert build_arg_parser().parse_args(["--no-auto-compare"]).auto_compare is False


def test_provider_default_is_aenv_go():
    from bench_core.oversub import _TRUE_DEFAULTS, build_arg_parser

    assert _TRUE_DEFAULTS["provider"] == "aenv-go"
    assert build_arg_parser().parse_args([]).provider is None  # sentinel; main() resolves


def test_preflight_fails_fast_on_missing_go_worker(tmp_path, monkeypatch):
    from bench_core.oversub_run import preflight_provider

    monkeypatch.setenv("AGENT_BENCHMARK_GO_BIN", str(tmp_path / "missing"))
    with pytest.raises(SystemExit, match="Go worker"):
        preflight_provider("aenv-go", {})

    worker = tmp_path / "agent-benchmark-go"
    worker.write_text("#!/bin/sh\n")
    worker.chmod(0o755)
    monkeypatch.setenv("AGENT_BENCHMARK_GO_BIN", str(worker))
    preflight_provider("aenv-go", {})  # resolves: no raise
    preflight_provider("aenv", {})  # non-go providers are not checked
