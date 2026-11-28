# webArena 轨迹回放数据集(scenario: webarena)

28 条 webArena 浏览器 Agent 轨迹,逐步真实执行(模拟 agent 工具调用负载),
覆盖一对真实站点:GitLab(:8023)+ Magento(:7770)。

| 项 | 值 |
| --- | --- |
| 轨迹数 | 28(`replays/*.replay.json`) |
| 总步数 | 410(每条轨迹开头已补 4 步登录) |
| 模板 | `browser-webarena`(28 条轨迹共用) |
| 站点 | 规范占位 `http://127.0.0.1:8023`(GitLab)、`http://127.0.0.1:7770`(Magento)；真实 host 回放时传入 |
| 判定 | `success = (exit_code == 0 and not timed_out)` |

## 快速开始

```bash
# 1) 启动站点容器(镜像已推送公仓)
docker run -d --name webarena-gitlab -p 8023:8023 -e SITE_HOST=<站点IP> \
  swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/webarena-gitlab:latest
docker run -d --name webarena-verified-shopping -p 7770:80 -e SITE_HOST=<站点IP> \
  swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/webarena-verified-shopping:latest

# 2) 构建浏览器模板
aenv pull swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/ubuntu-openclaw-chromium:webarena-arm64 \
  --name browser-webarena --cpu 2 --memory 4096 --timeout 1800

# 3) 回放(8 沙箱)
REPLAY_TARGET_HOST=<站点IP> bench-core --provider aenv --config config/common/webarena-replay.yaml -n 8

# 4) 压测(1:1 / 1:2 / 1:3 超分)
REPLAY_TARGET_HOST=<站点IP> oversub-bench --sweep-config config/oversub/webarena-exec-ephemeral-1to3.yaml
```

## 站点容器说明

镜像已推送至华为云 SWR 公仓,内置自动配置,无需手动修改:

- **GitLab**: 启动时自动检测宿主机 IP 更新 `external_url`。`SITE_HOST` 环境变量可指定 IP(大网小网均支持),不设则用 docker 网关。
- **Magento**: 启动时自动修正 DB `base_url` + 每 5 分钟 cron 自愈。`SITE_HOST` 同上。
- **站点预检**: bench-core 启动前自动检测 `:8023` 和 `:7770` 可达性,不通则 fail-fast。

## 浏览器模板说明

镜像已推送至公仓,内置 agent-browser 0.43.0 + 加固 shim,客户直接 `aenv pull` 即可:
- CDP 冷启动重试 / SPA 元素重试 / daemon 崩溃恢复 / ERR_ABORTED 重试
- exit code 归一化(0.43.0 成功返回 exit=2,shim 归一化为 0)
- `AGENT_BROWSER_PROFILE` 浏览器 session 持久化

## 验证记录

28 条轨迹 / 410 步 / 100% 通过率(14 路并行,delay_scale=1.0)。

## 部署备注（x86 实测，2026-11）

以下为 x86（<AENV_HOST>）上回放时的站点部署实测记录，供复现参考。

### GitLab（:8023）

- **启动脚本**: 容器内置 `auto-external-url.sh`，启动时根据 `SITE_HOST` 自动更新 `external_url`。
- **启动耗时**: 30–60s（首次拉起较慢，后续 restart 较快）。
- **登录凭据**: `root` / `password`（webarena 默认）。
- **端口**: 8023。
- **可忽略告警**: 启动日志可能出现 `file-max` / `too many open files` 类 warning，属容器内 ulimit 限制提示，不影响回放，可忽略。

### Magento（:7770）

- **启动脚本**: 容器内置 `wa_baseurl_fix.sh`（启动时修正 DB `base_url`）+ 每 5 分钟 cron 自愈。
- **启动耗时**: 60–90s（MySQL 初始化 + Magento env 预热）。
- **登录凭据**: customer 账号（webarena 默认 customer 凭据）。
- **端口**: 7770。
- **可忽略告警**: 启动日志可能出现 `env-ctrl` / `env` 相关 error，属 Magento 启动早期阶段噪声，cron 自愈后消失，可忽略。
- **MySQL 重初始化**: 若 `base_url` 长期不生效或站点 500，删除容器 + 数据卷重新 `docker run` 即触发 MySQL 重新初始化；`wa_baseurl_fix.sh` 会在首启阶段重写 `base_url`。
