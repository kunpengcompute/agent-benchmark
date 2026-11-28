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

**no-op 步骤说明**(410 步中的 15 步,设计如此,非缺陷):
- 11 个空 action 是轨迹**终止步**(agent 输出最终答案,不做浏览器操作;原始
  `_history.json` 中 `is_done: true`、action 为空数组)。
- 4 个 `#` 注释步是 agent 通过**元素坐标/index** 的交互(无语义选择器,无法转成
  可复现的 agent-browser 命令),保留为注释描述;回放时是 bash 注释 no-op。
两者 exit_code=0 计成功,是轨迹数据形态的一部分。

## 快速开始

```bash
# 1) 启动站点容器(镜像已推送公仓)
docker run -d --name webarena-gitlab -p 8023:8023 -e SITE_HOST=<站点IP> \
  swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/webarena-gitlab:latest
docker run -d --name webarena-verified-shopping -p 7770:80 -e SITE_HOST=<站点IP> \
  swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/webarena-verified-shopping:latest

# 2) 构建浏览器模板（x86 主机把 tag 换成 :x86_64）
aenv pull swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/webarena-browser:arm64 \
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

**覆盖说明**: multiplier M 下每个 slot 连跑 M 条轨迹,但 distinct 覆盖约为
min(slot 数, 池大小)——`-n 14 × M=2` 实测只覆盖 15/28 条。逐条轨迹验证用
`multiplier: 1` + `-n 28`;吞吐压测用 M=2。

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
- **base_url 缓存**: 若 DB 中 `web/*/base_url` 已是正确值但首页仍 302 跳旧 IP，是 Magento 页面缓存未刷新：`docker exec <容器> rm -rf /var/www/magento2/var/cache`。

## 部署备注（同机部署）

一般压测将站点与 AENV 分两台机器部署。若同机部署，注意以下三点（均为实测）：

- **AgentENV 宿主 IP egress 黑名单**: AgentENV 默认禁止沙箱访问宿主 IP（防止沙箱
  回打宿主服务）。站点与 AENV 同机时需放行站点端口：
  `iptables -I INPUT -s 10.11.0.0/16 -i aenv-+ -p tcp -m multiport --dports 8023,7770 -j ACCEPT`
  （`veth-+` 接口同理）。分机部署无需此项。
- **客户端系统代理**: 客户端机器带系统 HTTP 代理（如本机 127.0.0.1:7890）时，
  e2b SDK 的 API 流量需 `NO_PROXY=<aenv-host>`；站点预检已内置直连探测，不走代理。
- **镜像内 shim 行尾**: 仓库检出在 Windows（autocrlf）时，烧 shim 进镜像务必用
  `git cat-file blob` 提取的 LF 版本——CRLF 的 shebang（`bash\r`）会让每步 rc=127。

