# Docker 部署与高级验收

部署资产权威来源为 `Dockerfile`、`deploy/compose.advanced.yaml` 和
`scripts/run_advanced_acceptance.py`。

> Last verified: 2026-08-05，Docker Engine 29.6.2，Linux/Python 3.11.9，
> image `sha256:8c9d3581807f4419cf5776cbc3590e61452afd1d4b38720bd68016609f29ca8c`。
> Core 与 HA E2E 均通过。

## Profiles

| 服务 | Core | HA |
|---|---:|---:|
| Kafka SSL broker | 1 | 1 |
| MinIO | 1 | 4×2 drives |
| S3 HAProxy | - | 1 |
| JobManager | 1 | 2 active/passive |
| JobManager HAProxy | 1 | 1 |
| Worker | 3 | 3 |
| Prometheus | 1 | 1 |

JobManager/Worker 使用非 root 用户、只读根文件系统、`cap_drop: ALL` 和
`no-new-privileges`。PKI、Token 和 S3 凭据位于临时 `run/pki`，通过 Docker
Secret 以文件挂载。

## Core 验收

```powershell
.\scripts\run_advanced_core_acceptance.ps1 `
  -PythonCommand .\.venv\Scripts\python.exe
```

流程：

1. 生成临时 PKI/Secret 并启动 Core。
2. 运行基础 WordCount 回归。
3. 运行显式 At-least-once 中级回归。
4. 运行 Exactly-once 无故障 baseline。
5. 在 `before_barrier` arm gate 后 SIGKILL 承载状态算子的 Worker 子进程。
6. 在 `before_decision` PREPARED gate 后再次注入 Worker 故障。
7. 对三组 manifest-visible rows 比较多重集。
8. 验证 Kafka 两 partitions lag=0、pending transaction 受控。
9. 删除作业、容器、网络、卷和临时 Secret。

最终结果：

```text
advanced_core_acceptance=passed
before_barrier recovery=13.870s
before_decision recovery=15.598s
committed output diff=0
kafka lag=0
containers=0 networks=0 volumes=0 secrets_removed=true
```

## HA 验收

```powershell
.\scripts\run_advanced_ha_acceptance.ps1 `
  -PythonCommand .\.venv\Scripts\python.exe
```

流程：

1. 启动 4 MinIO、S3 proxy、2 JobManagers、3 Workers、Kafka 和 Prometheus。
2. 验证 ACTIVE=1、STANDBY=1。
3. 验证正确 Token/CA 路径；无/错 Token、错 CA、错误内部身份被拒绝。
4. 在 checkpoint 1 `after_decision`、FINALIZED 前终止 active JobManager。
5. standby 获取新 epoch，完成 finalize，并把旧 active 重启为 standby。
6. 删除一个 MinIO 容器；以无 9000 监听的 holder 保留其旧 IP，避免 Docker 把
   peer 地址重分配给 Worker。
7. 等待 S3 proxy 摘除 backend，并完成 checkpoint 2 读写。
8. 在降级存储下 SIGKILL Worker，恢复到 attempt 2 并完成 checkpoint 3。
9. 验证 manifest rows、lag、Prometheus targets/rules 和最终清理。

最终结果：

```text
advanced_ha_acceptance=passed
jobmanager takeover=25.828s (SLO <=30s)
storage checkpoint=28.988s
worker recovery=19.755s (SLO <=60s)
checkpoint manifests=1,2,3
committed output diff=0
kafka lag=0
prometheus targets=5 rules=8
containers=0 networks=0 volumes=0 secrets_removed=true
```

## 证据

| 文件 | 内容 |
|---|---|
| `reports/advanced-core-acceptance.log` | Core 命令 transcript |
| `reports/advanced-core-runtime.log` | Core 服务 JSON 日志 |
| `reports/advanced-core-evidence.json` | Core 场景、状态、output、lag、清理 |
| `reports/advanced-ha-acceptance.log` | HA 命令 transcript |
| `reports/advanced-ha-runtime.log` | HA 服务 JSON 日志 |
| `reports/advanced-ha-evidence.json` | HA 接管、存储、Worker、指标、清理 |
| `reports/*-failure.json` | 注入目标、容器状态和 UTC 时间 |

验证器只读取 output manifest 引用的 fragments，不把 glob 目录结果当作 committed
真值。报告不得包含私钥、Bearer Token 或 S3 access key。

## 常用参数

```text
-SkipBuild
-KeepEnvironment
-LogPath <path>
-RuntimeLogPath <path>
-EvidencePath <path>
```

只有源代码和镜像完全一致时才可使用 `-SkipBuild`。最终候选应先构建一次，然后让
Core/HA 复用同一 image ID。

## 手工观察

```powershell
.\.venv\Scripts\python.exe scripts\generate_dev_pki.py --output run\pki --force
docker compose -f deploy\compose.advanced.yaml --profile core up -d --wait
docker compose -f deploy\compose.advanced.yaml --profile core ps
docker compose -f deploy\compose.advanced.yaml --profile core down -v
Remove-Item -Recurse -Force run\pki
```

管理入口是 `https://localhost:8080`，必须提供 CA、客户端证书和
`PYSTREAM_EXTERNAL_TOKEN_FILE`。不要把 Secret 值放入命令行或环境日志。

## 成功与兼容标记

高级脚本的成功标记是 `advanced_core_acceptance=passed` 和
`advanced_ha_acceptance=passed`。中级历史验收仍使用
`compose_project_resources=0`；高级结构化证据使用
`resource_cleanup={containers:0,networks:0,volumes:0}`。

## 运行限制

- Kafka 是单 broker，脚本不注入 broker 故障。
- 全栈位于单 Docker Desktop 主机，不证明跨主机 HA。
- 事务 File Sink 位于单共享 output volume，不证明卷丢失容灾。
- MinIO holder 是验收辅助容器，只提供连接拒绝以保留故障节点网络身份，不提供
  存储服务。
- 在线范围依赖使镜像尚未达到 byte-for-byte 可复现。
