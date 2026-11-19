# agent-benchmark

Agent 负载压测套件：同一份压力配置通过 `--provider` 在 E2B / AgentENV / CubeSandbox 三个受支持后端之间切换，采集沙箱的创建与执行耗时、主机资源指标；超分（overcommit）扫描驱动按不同超分比自动跑一组 trial 并输出退化曲线。以下命令均在仓库根目录执行。

| 目录 | 内容 | 从哪看 |
| --- | --- | --- |
| `src/` | `bench-core` 内核（3 个受支持后端）+ `oversub-bench` 扫描驱动 | [快速开始](#快速开始) |
| `artifacts/` | 55 条轨迹、镜像清单、轨迹 → 模板映射 | [数据集与模板](#数据集与模板) |
| `agent_replay/` | 早期回放实现，保留仅供参考，后续会 deprecate | — |
| `docs/guide/` | 中英文使用指南 | [文档](#文档) |

## 数据集与模板

仓库自带一套可直接用的负载：55 条 Agent 轨迹，以及它们对应的镜像与模板映射。

- `artifacts/traces/swe-bench/replays/`：55 条轨迹（`.json` / `.traj`）。
- `artifacts/traces/swe-bench/image-map.json`：每条轨迹对应的 template 名。
- `artifacts/images/manifest.csv`：55 组 ARM64 / x86_64 镜像地址。

**模板**就是把这些镜像灌进 AgentENV 后得到的一份快照。在 AgentENV 主机上装 Docker 和
`aenv` CLI（先 `aenv auth` 配好服务地址与密钥），并保证目标 Registry 已启动、允许推送且
AgentENV 能访问，然后：

```bash
bash artifacts/images/download-images.sh              # 按本机架构下载对应的 55 个镜像
python artifacts/images/build_template.py --dry-run   # 预览将执行的命令
python artifacts/images/build_template.py             # 构建全部 55 个模板
```

`build_template.py` 读本地当前架构的镜像，并校验与 `image-map.json` 一致。常用参数：
`--template` 只建指定模板（可重复）、`--registry` 改推送前缀（默认
`127.0.0.1:5000/swerebench`）、`--timeout` 单模板等待秒数（默认 1800）、`--list` 只列清单。

> 只有 AENV 路径需要模板；`fake` 与 `docker` 后端不需要，可以直接跳到下面的快速开始。

## 快速开始

需要 Python 3.11+。

### 1. 冒烟：一条命令，不需要任何后端（约 1 分钟）

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .        # e2b 是核心依赖（pin 2.46.0），随包安装

bench-core --provider fake --config config/common/browser.yaml --create-only -n 1
```

`fake` 后端在内存里模拟沙箱：**不需要 SDK、不需要服务器、不需要模板、不需要轨迹**。这条
命令能跑通，说明内核、CLI、配置解析都已就绪，剩下的只是接一个真后端。

### 2. AgentENV 小规模验证（推荐的真后端路径）

前提：上一节的模板已建好，AgentENV 服务已启动。改 `config/common/replay-ephemeral.yaml`
的 `aenv:` 块三处：`template` 写上一节建好的模板名；`E2B_ACCESS_TOKEN` / `E2B_API_KEY`
贴上密钥（保留 `your_*_here` 占位符则回落到 `~/.e2b/config.json`）；`E2B_API_URL` /
`E2B_SANDBOX_URL` 指向服务端。然后跑一个小集群：

```bash
bench-core --provider aenv --config config/common/replay-ephemeral.yaml -n 8 --test-duration 600
```

`-n 8` 是必须的：该配置默认声明 384 个沙箱，不加会按 384 个去申请。跑通后 `-n`、
`--test-duration` 随规模放大即可。

第一次只求连通性时，把 `replay.delay_scale` 临时改成 `0.02`（思考时间压缩 50 倍，几十秒
就能看到 `success`），出真实数字前必须还原 `1.0`。缩放为 1.0 时轨迹自带思考时间——仓库
这 55 条里最短一条约 3.5 分钟、中位约 20 分钟——窗口短于一条轨迹的时长时步骤照跑、但
`success` 是 0：不是没跑起来，是没跑完。

轨迹默认就读上一节那 55 条：`--scenario swe-bench`（默认值）把 `replay.trajectory_dir` /
`replay.template_manifest` 解析到 `artifacts/traces/swe-bench/{replays,image-map.json}`
（glob 取自同目录的 `dataset.yaml`），模板名与上一节构建出来的名字一一对应，无需改任何东西。
换成自己的数据时用 `--scenario <name>` 指向 `artifacts/traces/<name>/`（webArena / OSWorld
将来各自一个场景目录，配置零 fork）；或显式改 `replay.trajectory_dir` / `replay.template_manifest`；
`template_manifest` 不设则所有轨迹退回 `aenv.template`。

手边没有 AgentENV 时，可以先用本地 docker 做同样的连通性验证：`pip install docker`，
把 `config/common/docker-browser.yaml` 的 `docker.image` 换成本地已有镜像，再跑
`bench-core --provider docker --config config/common/docker-browser.yaml --create-only -n 2`。

### 3. 超分扫描：按超分比出一组曲线

```bash
oversub-bench --provider aenv --sweep-config config/oversub/exec-ephemeral-1to3.yaml
```

在 sweep 配置里改 `ratios` 调超分比、改 `test_duration` 调每轮时间窗（真实曲线至少 2 小时，
内网长跑用过 600000 秒），在 base 配置的 `monitor.skip` 里关掉不需要的采集。先加 `--dry-run`
可以只看它生成什么、不创建沙箱。五步逐条说明见
[oversub-bench 指南 §1.1](docs/guide/oversub-bench-usage-zh.md)。

## bench-core 套件

- **后端**：`fake`（内存，无 SDK）、`docker`、`e2b`、`aenv`（e2b SDK 打 AENV 服务端，带
  pause/resume）、`aenv-go`（同 AENV 服务端，改走常驻 Go worker 客户端，见
  [go-client/README.md](go-client/README.md)；配置在 `aenv_go:` 块逐键回退 `aenv:` 块）、
  `cubesandbox`。切后端只换 `--provider`，配置文件的共享段不变。
- **工作流**：browser、coding、document，以及轨迹回放 `replay`
  （`exec_only` / `lifecycle` / `exec_ephemeral`）。
- **阶段阶梯**：`--create-only` / `--detect` 都会留下运行中的沙箱，收尾用 `--cleanup`。从
  `fake` → `docker` → `e2b`/`aenv` 逐级验证，比一上来就连生产后端省时间。
- **主机资源采集**由内置的 `vm-monitor` 程序承担（`src/vm_monitor/`，随 `pip install -e .`
  注册）；二进制缺失时内核跳过主机采集，任务级指标照常产出。

## 文档

| 文档 | 内容 |
| --- | --- |
| [bench-core 使用指南（中文）](docs/guide/bench-core-usage-zh.md) | 安装、CLI、配置、后端、replay 模式、报告字段 |
| [bench-core usage guide (EN)](docs/guide/bench-core-usage.md) | 英文同内容 |
| [oversub-bench 使用指南（中文）](docs/guide/oversub-bench-usage-zh.md) | 五步上手、sweep 配置、读结果、故障排查 |
| [oversub-bench usage guide (EN)](docs/guide/oversub-bench-usage.md) | 英文同内容 |
| [vm-monitor 使用指南（中文）](docs/guide/vm-monitor-usage-zh.md) | 宿主机资源采集 CLI、`.env` 工具路径、采集模式 |
| [vm-monitor usage guide (EN)](docs/guide/vm-monitor-usage.md) | 英文同内容 |
| [Metrics Reference](docs/dev/metrics-reference.md) | 内核与 vm_monitor 指标定义、`/proc`+`/sys` 采集来源与公式 |

## 开发

```bash
python -m pytest src/bench_core/tests src/env_provider/tests   # 内核与 provider
python -m pytest                                               # 全量
ruff check src config && ruff format --check src config
```
