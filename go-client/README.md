# 可选 Go 请求客户端（aenv-go provider）

Go 客户端把对 AENV 服务端（E2B 兼容 REST）的请求——沙箱创建、连接、暂停、恢复、删除、
列表查询和 Guest 命令——交给一个常驻 Go worker 进程执行，绕开 e2b SDK 的每请求
HTTP 客户端开销。Python 侧保留全部回放策略（轨迹解释、delay、并发名额、重试/取消），
只替换执行客户端。这是 `agent-benchmark` 仓库 `--client go`（PR #3）向本仓库
（agent-benchmark_1113）provider 体系的移植。

## 构建和运行

需要 Go 1.22+，Python 依赖安装步骤不变。Go 部分只有标准库依赖。在仓库根目录执行：

```bash
bash go-client/build.sh

# 默认客户端仍是 e2b SDK（--provider aenv）
bench-core --provider aenv --config config/common/replay.yaml

# 相同配置，切换到 Go 客户端；请使用独立输出目录
bench-core --provider aenv-go --config config/common/replay.yaml
```

Go 二进制默认路径是仓库内 `go-client/bin/agent-benchmark-go`。也可以指定：

```bash
# 配置文件 aenv_go: 块内（不存在时回落 aenv: 块）
go_client_bin: /absolute/path/agent-benchmark-go
# 或环境变量
export AGENT_BENCHMARK_GO_BIN=/absolute/path/agent-benchmark-go
```

Go 缺失、异常退出或协议不匹配时明确报错（`FileNotFoundError` / `GoTransportError`），
**不自动回退 e2b**——静默降级会污染客户端对比基准。`--provider aenv` 入口及依赖不删除。
Go 二进制不提交到仓库；在目标机本地构建，或用标准 Go 的 `GOOS`、`GOARCH` 交叉编译。

当前 worker 协议版本为 2。更新本分支后应重新构建 Go 程序；旧二进制会被协议检查拒绝。
结构化错误类别区分超时、传输失败和 API 拒绝（`TimeoutError` / `GoTransportError` /
`GoClientError`），不根据服务端错误文案猜测类别。

### 三种回放模式下的行为

`aenv-go` 与 `aenv` 共用同一份配置和回放内核，`--provider` 是唯一差异：

- **exec_only**：长寿命沙箱纯 exec，命令 start/wait 走 Go worker 的 Connect 流；
- **lifecycle**：pause/resume 由 worker 的 snapshot/restore 调用执行；
- **exec_ephemeral**：每轮 create → exec → kill 全部经 worker。

API 地址和密钥继续复用配置文件 `aenv:` 块的 `E2B_API_URL`、`E2B_API_KEY`、
`E2B_SANDBOX_URL`、`E2B_DOMAIN` 等现有设置（`aenv_go:` 块可覆盖，未设则回落）。

## 与上游 PR #3 的差异（有意为之）

- **同步桥接**：上游 `clients/go.py` 基于 asyncio；本仓库 provider 在
  `ThreadPoolExecutor` 工作线程中运行（无事件循环），桥接层
  （`src/env_provider/aenv_go/bridge.py`）改写为一个常驻 worker + 读线程分发 +
  per-request Event 槽位的同步模型，协议（v2）与语义（严格响应校验、结构化错误、
  不回退）逐条保留；
- **接入方式**：上游 CLI 是 `--client go`，本仓库接入 provider 注册表
  （`--provider aenv-go`），配置文件共享段不变；
- **计时口径**：走本仓库统一的 trajectory_details 指标体系，worker 侧耗时
  （`worker_elapsed_sec`）用于响应校验与问题定位，不单独成列。

## 结果与计时

- 每个 provider 进程复用一个常驻 Go worker 和 HTTP 连接池，不为每次请求启动子进程；
- provider 的 `client_metadata()` 报告客户端身份：Go 二进制 SHA256、worker 协议
  版本、计时口径，用于实验配置留痕与 `--reuse` 场景的客户端一致性校验；
- Go 测试不自动删除其他客户端的数据；orphan 清理策略与 `--provider aenv` 完全一致；
- 未知结果的 create 由现有 metadata 核对逻辑处理，Go HTTP 层不会自动重试 POST。

每个 worker 输入消息上限为 8 MiB；单个 Connect 帧、API 响应或累计命令输出上限为
128 MiB。超限明确报错，不截断后宣称成功。命令非零退出按业务结果记录；断流或缺少
退出事件不会被伪装成成功。

## 验证

从仓库根目录运行：

```bash
(cd go-client && go test -race ./...)
bash go-client/build.sh
python -m pytest src/env_provider/tests
# 端到端用例需要真实 go 二进制：
LIFECYCLE_TEST_GO_BIN=$PWD/go-client/bin/agent-benchmark-go \
  python -m pytest src/env_provider/tests
```

Python 测试（`src/env_provider/tests/test_go_client.py`）运行真实 Go 进程和本地
HTTP/Connect 测试服务，覆盖多请求并发、metadata 隔离、严格响应校验、四阶段、非零
退出、断流、worker 复用与协议身份。测试服务不创建真实虚拟机——真实性能收益需在
真实 AENV 服务上固定模板、并发、服务配置、日志与预热方式，交替先后对比
`--provider aenv` 与 `--provider aenv-go` 的完整回放指标。
