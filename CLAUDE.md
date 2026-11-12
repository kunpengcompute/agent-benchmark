# agent-benchmark — Project Guide for AI Assistants

This file gives project-specific conventions, architecture overview, and working
guidelines for AI assistants working on this codebase.

## What is agent-benchmark

A performance-testing framework for virtualization scenarios. The **`bench_core`
kernel + `env_provider` contract** (under `src/`) is the host-agnostic core: one
stress profile drives any sandbox backend by swapping `--provider`, and the same
`config/common/*.yaml` runs on either e2b or docker. The **`oversub-bench`**
driver runs a set of overcommit-ratio trials against a backend and emits
degradation curves.

Scenarios / providers (all behind the SDK-free `env_provider` contract):
- **Fake** — in-memory, no SDK, drives the kernel in unit tests (`--provider fake`)
- **E2B Sandbox** — Firecracker microVMs via the E2B API (`--provider e2b`)
- **Docker Containers** — browser automation in containers (`--provider docker`)
- **AgentENV** — e2b SDK pointed at an AgentENV server; adds pause/resume + snapshots (`--provider aenv`)
- **CubeSandbox** — Cloud Hypervisor microVMs, full lifecycle (`--provider cubesandbox`)

The replay workflow (`exec_only` / `lifecycle` / `exec_ephemeral`) replays recorded
agent trajectories against a backend. The bundled 55-trajectory pool
(`artifacts/traces/replays`) + `artifacts/traces/image-map.json` manifest are the
default `replay.trajectory_dir` / `replay.template_manifest`, so a replay config
runs out of the box.

Host-level resource collection is provided by the bundled **`vm_monitor`**
package (`src/vm_monitor/`, registered as the `vm-monitor` CLI by
`pip install -e .`). The kernel calls it as a subprocess around the stress
phase; when the binary is absent the kernel skips host collection and
task-level metrics still produce. The kernel-side call layer
(`bench_core/observability/monitor.py`, oversub `start_host_monitor` /
`ShardStressBracket`, the `monitor:` config section, `--no-vm-monitor`) lives
in `bench_core`; the collector itself is `src/vm_monitor/`.

`agent_replay/` is an early replay implementation kept for reference only and
will be deprecated; new work goes in `src/`.

## Working Guidelines

Bias toward caution over speed. For trivial tasks, use judgment.

### Think before coding

Don't assume. Don't hide confusion. Surface tradeoffs.

- State assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### Simplicity first

Minimum code that solves the problem. Nothing speculative — no features beyond
what was asked, no abstractions for single-use code, no unrequested
configurability, no error handling for impossible scenarios. If you write 200
lines and it could be 50, rewrite it. Ask: "Would a senior engineer call this
overcomplicated?"

### Surgical changes

Touch only what you must. Clean up only your own mess.

- Don't "improve" adjacent code, comments, or formatting; don't refactor what isn't broken.
- Match existing style, even if you'd do it differently.
- Notice unrelated dead code → mention it, don't delete it.
- Remove imports/variables/functions **your** change orphaned; leave pre-existing dead code alone unless asked.

The test: every changed line traces directly to the user's request.

### Goal-driven execution

Define success criteria, loop until verified. Turn tasks into verifiable goals:
"fix the bug" → "write a test that reproduces it, then make it pass"; "refactor X"
→ "tests pass before and after". For multi-step tasks, state a short plan with a
`verify:` check per step.

## Build, Lint, Test Commands

> Requires **Python 3.11+** (`pyproject.toml` `requires-python = ">=3.11"`).
> Ruff `target-version = "py311"`, `line-length = 120`.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .          # registers bench-core + oversub-bench + oversub-compare + vm-monitor; pulls core deps

bench-core --provider fake --config config/common/browser.yaml --create-only -n 1   # smoke (no SDK)
python -m pytest                     # full suite
python -m pytest src/bench_core/tests src/env_provider/tests   # kernel + providers only
ruff check .                         # lint (target-version py311, line-length 120)
ruff format --check .                # format check
```

`pip install -e .` is sufficient — `pyproject.toml`'s `dependencies` declare the
core stack (`psutil`, `paramiko`, `flask`, `PyYAML`, `pandas`, `openpyxl`, `e2b`,
`aiohttp`); `e2b` is a core dependency pinned 2.46.0. Backend SDKs are otherwise
optional: `pip install docker` and/or the cubesandbox SDK only when using that
provider; `--provider fake` needs none.

`pyproject.toml` sets `pythonpath = ["src"]` for pytest, so tests resolve
`bench_core` / `env_provider` without an editable install. The `[project.scripts]`
entries are `bench-core = "bench_core.bench_cli:main"`,
`oversub-bench = "bench_core.oversub_run:main"`,
`oversub-compare = "bench_core.compare_sweeps:main"`,
`vm-monitor = "vm_monitor.cli:main"`.

## Architecture

### Layered architecture (src kernel)

```
┌─────────────────────────────────────────────────────────────┐
│  CLI / entry                                                 │
│  bench-core (bench_core.bench_cli.main) --provider {fake,e2b,docker,aenv,cubesandbox} │
│  oversub-bench (bench_core.oversub_run.main)                │
│  oversub-compare (bench_core.compare_sweeps.main)           │
└─────────────────────────────────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
┌────────────────────────┐   ┌──────────────────────────────┐
│  bench_core (kernel)   │   │  env_provider (contract)      │
│  run_benchmark spine    │◀──│  EnvironmentProvider ABC       │
│  stats / round_robin    │   │  SandboxInstance / Metrics    │
│  task_manager / runners │   │  CommandResult / SandboxStatus │
│  KernelConfig.from_raw  │   │  (SDK-free)                    │
└────────────────────────┘   └──────────────────────────────┘
                                          │
                ┌───────────────┬────────┴──────────┬────────────┐
                ▼               ▼                   ▼            ▼
         env_provider.e2b  env_provider.docker  env_provider.aenv  env_provider.cubesandbox
         (opt-in, pulls e2b SDK)  (pulls docker SDK)  (reuses e2b SDK)  (pulls cube SDK)
                                                          env_provider.fake (no SDK)
```

The kernel (`bench_core`) and every provider impl depend on the contract
(`env_provider`); neither owns it. The contract stays **SDK-free** —
`from env_provider import EnvironmentProvider` never imports the e2b, docker, or
cubesandbox SDK. Providers are **opt-in leaf submodules** loaded only by
`bench_core.bench._build_provider` when selected, so the kernel never loads a
backend it does not use. Adding a backend is one new `env_provider` submodule;
contract + kernel untouched.

**`bench_core` kernel** (`src/bench_core/`):
- `bench.py` — `run_benchmark(config, provider)` spine: `prepare_env → create/detect → (create-only | warmup-only | cleanup-only | benchmark) → stop → report`. `_build_provider` lazy-imports the selected provider submodule. `_promote` lifts provider `SandboxInstance` → kernel `BenchSandbox` (attaches workflow metrics).
- `bench_cli.py` — argparse entry; `__main__.py` wraps it.
- `config.py` — `KernelConfig`. `KernelConfig.from_raw(raw)` is the **single reader** of the shared stress sections (`sandbox` / `create_batch` / `task_batch` / `browser` / `coding` / `document` / `test` / `report` / `workflow_type`).
- `task_manager/{fixed,round_robin}.py` — the workflow dispatchers.
- `task_runner/{browser,coding,document,replay}.py` — the four workflows. Browser rides HTTP on top of a backend the provider starts; coding/document are pure `exec`; replay consumes the trajectory pool.
- `observability/` — `run_summary.py`, `obs_xlsx.py` (8-sheet observability workbook), `monitor.py` (host-level `vm-monitor` orchestration around the stress phase), `trajectory_export.py`, `lifecycle_reconstruct.py`.
- `payload/replay_payload.py` — trajectory loader (`load_pool`, `find_trajectories`, `load_trajectory`); manifest keys are `os.path.relpath(path, directory)`; manifest is authoritative.
- `oversub.py` / `oversub_run.py` / `oversub_merge.py` / `oversub_shards.py` / `compare_sweeps.py` — the oversub sweep driver, shard planner, artifact merger, and sweep-comparison tool.
- `admission.py` (lifecycle admission controller), `transients.py` (transient error detection), `schemas.py` (`BenchSandbox`, `ReplayMetrics`), `utils.py`, `workflow_registry.py`.

**`env_provider` contract** (`src/env_provider/__init__.py`): `EnvironmentProvider` ABC (`create_all`, `detect_existing`, `detect_from_ids`, `save_ids`, `check_alive`, `cleanup_all`, `cleanup_existing`, `prepare_env`, `prepare`, `exec`) + `SandboxInstance` / `CreationMetrics` / `CommandResult` / `SandboxStatus`. `exec()` is the **sole command primitive** — file writes go through `exec` as a heredoc, no separate upload method, so adding a provider is implementing `exec`. The provider keeps any SDK handle internally (`id → handle`); the kernel holds only the host-agnostic `SandboxInstance`. Optional capability Protocols gate lifecycle features: `LifecycleCapable` (`pause`/`resume`), `EphemeralCapable` (`create_one`/`kill_one`), `SnapshotSizeCapable` (`snapshot_sizes`) — `aenv`/`cubesandbox` satisfy these; `e2b`/`docker`/`fake` are exec-only.

**Shared backend infra** (`src/env_provider/_base.py`, `_ready.py`):
- `_ready.py` — `ReadyChecker`: workflow-driven poll-until-ready; backend supplies `_exec_probe`. browser=port scan, coding=`uname -a`, document=`document-bench-validate`. Constants `READY_MAX_WAIT=300`, `READY_INTERVAL=5`.
- `_base.py` — `BackendSandboxStatus` / `BackendCreationMetrics` + `BaseSandboxManager(ABC)` lifecycle template. Subclass supplies SDK seams (`_create_single`, `_list_existing`, `_external_id`, `_attach`, `_kill_one`, `_exec_probe`) + class attrs. `_ready_config` is a concrete base method returning shared constants — readiness is a workflow concern, so there are no per-backend readiness knobs and no `port_check` block in any YAML.

**Provider impls**: `env_provider/e2b/` (`config.py` `setup_e2b_env` + placeholder-credential fallback to `~/.e2b/config.json`, `manager.py`, `schemas.py`), `env_provider/docker/`, `env_provider/aenv/` (`AenvProvider` subclasses `E2BProvider` and reuses the E2B `SandboxManager` handle table + SDK — AENV has no separate SDK, it is the E2B SDK pointed at an AENV server via `E2B_API_URL`; adds `pause`/`resume`, `snapshot_sizes`, `create_one`/`kill_one`), `env_provider/cubesandbox/`, `env_provider/fake.py`.

**Config layering**: one `config/common/*.yaml` per workflow carries the per-backend blocks it drives; `--provider` selects which block `Config.from_raw` reads. Browser/coding/document configs carry **both** `e2b:` and `docker:` blocks; replay configs carry `e2b:` / `docker:` / `aenv:` / `cubesandbox:` blocks. The kernel reads only the shared sections. Credential placeholders (`your_e2b_access_token_here` / `your_e2b_api_key_here`) are treated as unset → fall back to `~/.e2b/config.json`.

### Replay workflow (replay modes)

`config/common/replay*.yaml` drives the replay workflow (`workflow_type: replay`):
deterministic replay of recorded trajectories against sandbox backends. Modes
(per `replay.mode`, defaults to the provider's `default_replay_mode`):

- `exec_only` — long-lived sandboxes, continuous exec of trajectory steps, no pause/resume. Baseline for pure exec-replay cost; used when the backend has no lifecycle capability (e2b/docker/fake).
- `lifecycle` — long-lived sandboxes: `create_all` → pause once → per-step `provider.resume()`/`pause()` (`LifecycleCapable`). Oversubscription = snapshot memory reuse — pause frees RAM, so `k×N` sandboxes fit in `running_concurrency = N` slots.
- `exec_ephemeral` — ephemeral `create_one`/`kill_one` per round (`EphemeralCapable`) with **no** pause/resume and **no** admission (k×N concurrent): the customer's real "spin up once → run → kill; to reuse, spin up again" workload.

The three modes differ by **sandbox lifecycle** (long-lived bare-exec / long-lived pause-resume / ephemeral bare-exec), not by "whether a rate limiter is attached". Oversubscription ratio: with a fixed host, `running_concurrency` stays at the baseline (N slots) and `total_count` scales to `k×N` for ratio `1:k`. Process sharding (`--shards`) splits one high-ratio trial across subprocesses to escape the GIL; only admission-free modes (`exec_ephemeral`/`exec_only`) shard. Merge yields a schema-faithful `run_summary.json` + `trajectories/index.json`; every downstream consumer (`compute_valid`, `trial_row`, ratio summaries, `compare_sweeps`, `--reuse`) is shard-unaware.

Outputs: a text report (`<output_dir>/<prefix>_<ts>.txt`), a JSONL lifecycle series (`<prefix>_lifecycle_series.jsonl`), and (with `report.format: xlsx|both`) an 8-sheet observability workbook (`<prefix>_obs.xlsx`). All series events are `time.time()`-stamped for direct join with external host samples.

## Code Conventions

### Python style

- PEP 8; type hints on public APIs; max line length **120**.
- f-strings for formatting; `pathlib.Path` over `os.path`.
- `from __future__ import annotations` at the top of kernel/provider modules (PEP 604 `int | None` in annotations).
- Ruff config in `pyproject.toml`: **`target-version = "py311"`**, `line-length = 120`. Do not lower it casually.
- English-only comments and commit messages.

### Naming & clean code

One bar: **the next reader (or you in three months) changes the code correctly without scrolling back for context.**

- **A name states intent, not type.** `data_row_count` over `n`. If a name needs a scroll to decode, the name is the defect.
- **A name must never lie.** A helper called `_bytes` that returns sectors is worse than a clumsy name.
- **Length scales with scope.** `i` is fine in a three-line loop; anything read 40 lines later or crossing a function boundary gets the full name.
- **Structure carries the meaning; comments carry the why.** Comments explain constraints and choices, never what the next line does.
- **No abstraction for symmetry.** A two-line helper that makes a list readable earns its place; a `SheetBuilder` class for one call site does not.
- **Consistency beats personal taste.** Match the surrounding file's idiom.

### Configuration

- All configs in YAML under `config/`; backend-agnostic workflows go in `config/common/` (one file per workflow, both `e2b:` + `docker:` blocks). Oversub sweep profiles go in `config/oversub/`.
- Template configs use `{{PLACEHOLDER}}` or `your_*_here` for dynamic values; the e2b provider treats credential placeholders as unset and falls back to `~/.e2b/config.json`.
- Never hardcode paths — use config or `.env`.
- Never declare per-backend readiness (port/timing) knobs in YAML — readiness is provider-transparent (kernel constants).

### Logging

- Python `logging` module, not `print`. Levels: DEBUG (verbose), INFO (progress), WARNING (issues), ERROR (failures).
- Streaming file logs use `buffering=1` (line buffering); include timestamps.

## Git / commit / PR rules (mandatory)

- **Branch first.** Never commit directly to `master`; cut a feature branch (`feat/...`).
- **Relevant files only.** `git add` only the files relevant to the change — never `git add -A` with unrelated churn.
- **Pre-commit before commit.** Run `ruff check` / `ruff format` on the staged set; fix failures before committing.
- **No Claude attribution.** Do not add `Co-Authored-By: Claude` or similar trailers. Do not add `🤖 Generated with ...` PR footers.
- **Commit messages** via heredoc `git commit -F - <<'EOF'`; Conventional Commit prefixes (`feat:`, `fix:`, `refactor:`, `docs:`, `ci:`, `chore:`).
- **Squash-merge is NOT the default for layered feature branches.** When a branch is built as a sequence of independently-runnable commits (e.g. a migration ladder), keep the layered history — do not squash.
- **IP & secret scrubbing.** Private `192.168.x.x` addresses are OK to commit; scrub public IPs. Never commit real E2B access tokens / API keys — leave placeholders.

## Entry Points

| Script | Purpose | CLI |
|--------|---------|-----|
| `bench-core` (`bench_core.bench_cli:main`) | host-agnostic kernel | `--config`, `--provider {fake,e2b,docker,aenv,cubesandbox}`, `--create-only`, `--detect`, `--warmup-only`, `--cleanup`, `-n`, `--test-duration`, `--no-vm-monitor`, `--log-level {debug,info,warning}` |
| `oversub-bench` (`bench_core.oversub_run:main`) | oversub sweep driver | `--sweep-config`, `--config`, `--provider`, `--ratios`, `--modes`, `--running-concurrency`, `--multiplier`, `--shards`, `--repeats`, `--test-duration`, `--output-root`, `--reuse`, `--dry-run`, `--no-vm-monitor` |
| `oversub-compare` (`bench_core.compare_sweeps:main`) | join sweep dirs into a comparison report | manifest-driven |
| `vm-monitor` (`vm_monitor.cli:main`) | host-level resource collector (Linux, run as root) | `-t`, `-i`, `--vmm {qemu,firecracker,cubesandbox}`, `--stress-file`, `--enable-capture`, `--host-proc`, `--log-rotation`, `raw-report <log_dir>` |

### bench-core phase ladder

`--create-only` and `--detect` both leave sandboxes running; finish with `--cleanup`. Tier validation: Tier 0 `--provider fake` (no SDK) → Tier 1 docker (local daemon) → Tier 2 e2b/aenv (cloud). See [docs/guide/bench-core-usage-zh.md](docs/guide/bench-core-usage-zh.md).

## Common Modifications

### Adding a new provider

1. Create `src/env_provider/<name>.py` (or `<name>/` package) implementing `EnvironmentProvider` (lifecycle + `exec`); keep an internal `id → handle` table.
2. If it has an SDK manager with create/detect/cleanup, subclass `BaseSandboxManager` (`_base.py`) and supply the SDK seams + class attrs; put config/schemas under `src/env_provider/<name>/`. For a lifecycle-capable backend that reuses an existing SDK, subclass that provider directly (see `env_provider/aenv/` — `AenvProvider` subclasses `E2BProvider` and only adds the capability methods, no new manager).
3. Register the provider in **two** places in `bench_core.bench` (both must be kept in sync):
   - the `_PROVIDERS` roster — drives the `--provider` argparse `choices` and the capability hint;
   - the `_build_provider` dispatch (lazy import — the kernel never imports a backend statically).
4. Add `config/common/*.yaml` blocks (`<name>:`) as needed — the kernel reads only shared sections.
5. Add unit tests under `src/env_provider/tests/` (drive via `FakeProvider`-style stubs; no live SDK needed).

### Adding a new workflow

1. Add a `task_runner/<workflow>.py` + dispatch in `bench.py`.
2. If it needs a readiness probe, extend `ReadyChecker` (`_ready.py`) — keep it provider-transparent (no per-backend knobs).
3. Add `config/common/<workflow>-*.yaml` (both backend blocks).

## File Locations

```
src/bench_core/              host-agnostic kernel + oversub driver
src/env_provider/            provider contract + e2b/docker/aenv/cubesandbox/fake impls
src/vm_monitor/              host-level resource collector (vm-monitor CLI: base + sampling,
                             procfs, vmm/ adapters, capture/, export/ incl. export/raw/)
config/common/               backend-agnostic workflow configs (one file per workflow)
config/oversub/              oversub sweep profiles + compare manifest
config/tools/                getfre_config.yaml (getfre collector template)
artifacts/traces/replays/    55 bundled trajectories (.replay.json)
artifacts/traces/image-map.json   trajectory -> template manifest (default template_manifest)
artifacts/images/            image manifest + download-images.sh + build_template.py
docs/guide/                  bench-core, oversub-bench and vm-monitor usage guides (EN + zh)
docs/dev/                    metrics-reference.md (kernel + vm_monitor metrics & formulas)
agent_replay/                early replay impl, kept for reference (to be deprecated)
```

bench-core reports go to each config's `report.output_dir` + `filename_prefix`. Create-only emits a creation-timing report (P50/P95/P99 of create + ready-check + total startup). Oversub trials land under `<output_root>/<mode>/ratio-<NN>/repeat-<NN>/` with `trial-summary.csv` / `ratio-summary.csv` / `trajectory-detail.csv` and the obs workbook.

## Related Documentation

- [docs/guide/bench-core-usage-zh.md](docs/guide/bench-core-usage-zh.md) / [docs/guide/bench-core-usage.md](docs/guide/bench-core-usage.md) — install→config→CLI→cleanup→troubleshooting (CN / EN)
- [docs/guide/oversub-bench-usage-zh.md](docs/guide/oversub-bench-usage-zh.md) / [docs/guide/oversub-bench-usage.md](docs/guide/oversub-bench-usage.md) — oversub sweep driver + oversub-compare (CN / EN)
- [docs/guide/vm-monitor-usage-zh.md](docs/guide/vm-monitor-usage-zh.md) / [docs/guide/vm-monitor-usage.md](docs/guide/vm-monitor-usage.md) — vm-monitor collector CLI, `.env` tool paths, capture modes (CN / EN)
- [docs/dev/metrics-reference.md](docs/dev/metrics-reference.md) — bench_core kernel + vm_monitor sheet metrics, `/proc` & `/sys` collection sources, calculation formulas
