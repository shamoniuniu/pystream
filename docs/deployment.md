# Docker 部署与中级验收

部署资产权威来源为 `Dockerfile`、`deploy/compose.yaml` 和
`scripts/run_intermediate_acceptance.ps1`。

> Last verified: 2026-07-30，Docker Engine 29.6.2，Python 3.11.9，
> `pystream:0.2.0`。初级兼容与中级故障恢复 E2E 均通过。

## 拓扑与资源

- Kafka 3.9.1：单 Broker KRaft，`words` 和 `intermediate-words` 各 2 partitions。
- JobManager：`http://localhost:8080`。
- Worker 1/2/3：每个 4 slots，共 12 slots。
- 共享卷：artifacts、checkpoints、output、Kafka 数据。
- Worker 独立 work 卷。

JobManager/Worker 使用 UID/GID 10001、只读根文件系统、`cap_drop: ALL` 和
`no-new-privileges`。镜像预创建并授权 `/data/checkpoints`，使非 root Worker
可以写共享快照。

## 标准验收

本机 Docker Compose 插件在长时间验收中可能挂起，因此标准脚本使用原生 Docker
命令创建与清理固定资源，并用
`.pystream/intermediate-docker-resources.tsv` 记录资源。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts\run_intermediate_acceptance.ps1 `
  -PythonCommand .\.venv\Scripts\python.exe
```

可用参数：

```text
-KeepEnvironment
-LogPath reports/intermediate-acceptance.log
-RuntimeLogPath reports/intermediate-runtime.log
-EvidencePath reports/intermediate-failure-evidence.json
```

默认流程：

1. 精确删除 ledger 中旧资源并确认资源为 0。
2. 创建 network、7 个 volumes 和 6 个长期/初始化容器。
3. 验证 Kafka、JobManager、3 Worker healthy。
4. 运行初级 WordCount，验证 `apple=2, pie=1` 和跨 Worker HASH。
5. 运行中级 event-time/retract baseline。
6. 显式触发 Checkpoint 1。
7. 写入 recovery 窗口并等待追加 Sink 输出。
8. 在承载状态算子的 Worker 内执行：

   ```sh
   kill -9 $(cat /proc/1/task/1/children)
   ```

   它终止 tini 的业务子进程，使 `restart: unless-stopped` 自动拉起容器。
9. 验证 RestartCount、StartedAt、incarnation、recovery attempts、attempt 和统一
   restored checkpoint。
10. 显式触发恢复后 Checkpoint 2。
11. 验证 baseline 精确一次、recovery 窗口允许重复、Kafka lag=0、无输入丢失。
12. 取消作业并删除所有容器、网络和卷。

成功标记：

```text
intermediate_acceptance=passed
compose_project_resources=0
```

## 验收专用 Checkpoint 控制

示例默认周期仍为 `10s`。验收提交工具在临时作业副本中将周期覆盖为 `1h`，并通过
`POST /v1/jobs/{job_id}/checkpoint` 在精确边界触发 Checkpoint。这样故障必定位于
Checkpoint 1 之后、Checkpoint 2 之前，不依赖 Docker CLI 速度。

该覆盖不会修改 `examples/intermediate/job.yaml`。

## At-least-once 预期

baseline 输出：

```text
2026/07/29T00:00:05,1,1
2026/07/29T00:00:05,2,1
```

recovery 输出在故障恢复后各出现两次，其中一次是允许的追加 Sink 重放：

```text
2026/07/29T00:00:10,1,1
2026/07/29T00:00:10,2,2
```

Kafka 最终：

```text
partition 0: committed=6, end=6, lag=0
partition 1: committed=4, end=4, lag=0
```

这证明输入无丢失且故障边界允许重复，不证明 Exactly-once。

## Compose 手工启动

Compose 仍可用于日常观察：

```powershell
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml ps
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/v1/workers
```

初级工具示例：

```powershell
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/produce_wordcount.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/submit_wordcount.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/wait_for_window.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/verify_wordcount.py
```

手工停止：

```powershell
docker compose -f deploy/compose.yaml down
docker compose -f deploy/compose.yaml down -v
```

`down -v` 删除 Kafka、Checkpoint 和输出证据，不可恢复。

## 运行证据

- `reports/intermediate-acceptance.log`：完整主流程。
- `reports/intermediate-runtime.log`：JobManager/Worker JSON 日志。
- `reports/intermediate-failure-evidence.json`：RestartCount、incarnation、attempt、
  恢复点和状态 trace。
- `reports/intermediate-acceptance.md`：验收摘要。

## 运行限制

- Checkpoint 会暂停 Source 并 drain 全图；大状态或慢 Sink 会延长暂停。
- checkpoint 命名卷允许跨 Worker 恢复，但共享卷和单 JobManager 都是单故障域。
- `restart: unless-stopped` 不会把 `docker kill` 视为自动恢复场景，因此标准故障
  注入杀容器内业务子进程。
- Docker CLI 可能在请求已生效后超时；脚本通过 inspect 验证真实状态并有限重试。
