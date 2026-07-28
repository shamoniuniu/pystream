# 故障排查

本文面向部署和演示执行者。先确认故障层级，再查看健康端点、任务状态和 JSON
日志；不要在未定位原因前反复重启并覆盖证据。

## 最短诊断路径

```powershell
docker compose -f deploy/compose.yaml ps
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/v1/workers
Invoke-RestMethod http://localhost:8080/v1/jobs/<job_id>
docker compose -f deploy/compose.yaml logs --since 10m jobmanager worker-1 worker-2 worker-3
```

每行服务日志是 JSON，稳定定位字段为：

```text
timestamp level component job_id operator_id subtask worker_id event
```

建议先按 `job_id`，再按 `event` 或 `operator_id/subtask` 过滤。

## 服务无法启动

### `docker` 不存在

症状：PowerShell 报 `docker is not recognized`。

处理：安装并启动 Docker Desktop，启用 Linux containers，重新打开终端后运行
`docker version` 和 `docker compose version`。无 Docker 时只能运行离线测试，
不能完成多容器验收。

### JobManager 不健康

```powershell
docker compose -f deploy/compose.yaml logs jobmanager
docker compose -f deploy/compose.yaml port jobmanager 8080
```

检查主机 8080 端口冲突、制品卷写权限和 Python 启动错误。

### Worker 显示 degraded

访问 Worker 容器内 `/health`，重点查看：

- `heartbeat_error`：最近一次 JobManager 心跳错误。
- `data_plane.active_connections`：当前数据连接。
- `runtime.errors`：本地任务累计错误。

Worker 会继续尝试心跳；超过 JobManager 心跳超时时，运行作业会失败。

## 提交失败

### YAML 校验失败

```powershell
.\.venv\Scripts\python -m pystream validate examples/wordcount/job.yaml
```

错误会包含 `operators[3].window` 等字段路径。常见原因：

- 未知字段或拼写错误。
- Source 有上游、Sink 有下游。
- 图存在环或引用不存在的算子。
- Reduce 的任一上游路径未经过 KeyBy。
- 算子 UDF、connector 或 window 与 type 不匹配。

### 制品被拒绝

常见原因：SHA-256 不匹配、ZIP 损坏、缺少 `job.yaml`、路径穿越、符号链接、
文件数量/大小超限。重新使用 `pystream package` 构建，不要手工修改 ZIP。

### 资源不足

JobManager 返回 HTTP 409 并说明 required/available slots。WordCount 需要 10 个
物理任务，默认 3 Worker 共 12 slots。检查：

```powershell
Invoke-RestMethod http://localhost:8080/v1/workers
```

不要只增加 Source 并发度；所有算子并发度之和必须不超过健康 slots。

## 作业进入 FAILED

查询状态：

```powershell
.\.venv\Scripts\python -m pystream status <job_id> --json
```

定位首个 `status=FAILED` 的 task 和 `error`。

### `task_failed`

表示 UDF、算子、Source、Sink 或运行循环异常。日志中有 job/operator/subtask 和
异常文本。若是 UDF，先在独立 Python 测试中验证输入、返回值和同步签名。

### `connection_failed`

表示 HELLO、协议帧、异常 EOF 或远端连接错误。检查：

- upstream/downstream task 所在 Worker 是否健康。
- 9000 内部端口是否被 Compose 网络阻断。
- 两端 job_id/task_id 是否来自同一物理图。
- 是否有 Worker 在运行中被停止。

第一阶段不会重连；此事件使作业失败是预期行为。

### `heartbeat_failed`

Worker 无法联系 JobManager。检查 Compose DNS、JobManager 健康和 8080 端口。
短暂错误会重试；超时后作业失败。

### `bad_record_skipped`

仅在 `bad_record_policy=skip` 出现。日志包含 topic、partition、offset 和错误。
修复输入生产者；不要把跳过记录计入正确性结果。

## 窗口没有输出

1. 确认作业仍为 RUNNING。
2. 检查 Source `records_read`、Task `records_in/out` 是否增长。
3. 检查 Reduce `operator_metrics.state_entries/active_windows`。
4. 等待完整处理时间窗口结束；示例为 10 秒，默认配置为 300 秒。
5. 检查 `window_triggered` 事件和 `emitted_records`。
6. 检查 Sink `operator_metrics.records_written` 和输出卷。

处理时间以 Worker Clock 为准，不读取消息中的事件时间。

## 结果错误或重复

- APPLE 和 apple 未合并：检查 Map UDF 是否转小写，KeyBy 是否返回 `word`。
- 同 key 分散：检查边是否为 HASH，并确认 UDF 返回稳定 JSON key。
- 历史结果混入：先运行 producer 重建 topic，并清理旧作业输出。
- 故障后重复：第一阶段 File Sink 追加写且无 Checkpoint/事务，这是已知限制；
  清理输出并重新执行完整演示。
- 行顺序不同：跨 key 没有全局顺序，按 `(window_end, word)` 比较。

## 背压定位

Worker `/tasks` 中查看：

```text
input_queue_depth / input_queue_capacity
output_queue_depth / output_queue_capacity
max_output_queue_depth
batches_out
records_in / records_out
```

队列接近容量且下游记录数增长缓慢表示背压正在传播。容量必须保持有界；不要通过
无限增大队列掩盖慢 Sink 或不可达下游。

## 清理失败

先用 CLI 或脚本取消作业，再停止 Compose。若只需保留证据：

```powershell
docker compose -f deploy/compose.yaml down
```

只有确认不再需要 Kafka/制品/输出后才执行：

```powershell
docker compose -f deploy/compose.yaml down -v
```

## 需要保留的证据

出现新故障时记录时间、job_id、task_id、Worker、操作步骤、状态 JSON、相关日志、
输入和输出文件。若形成新的稳定排障步骤，应在同一修复变更中更新本文。
