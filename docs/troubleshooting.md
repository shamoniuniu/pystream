# 故障排查

先保留状态和日志，再重启。标准证据位于 `reports/intermediate-*.log/json`。

## 最短诊断路径

```powershell
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/v1/workers
Invoke-RestMethod http://localhost:8080/v1/jobs/<job_id>
docker logs pystream-jobmanager-1
docker logs pystream-worker-1-1
```

日志稳定字段：

```text
timestamp level component job_id operator_id subtask worker_id event
```

## Docker 命令挂起

症状：`docker start/create/inspect/logs` 长时间无输出。

- 不并行发起更多 Docker 枚举命令。
- 标准验收脚本为每个原生命令设置硬超时。
- `start` 超时后 inspect：目标仍为 `created` 才重试；已是 running/exited 则继续。
- create 成功后立即写 resource ledger，失败时只删除已知资源。
- 最终必须出现 `compose_project_resources=0`。

Compose 插件挂起时使用 `scripts/run_intermediate_acceptance.ps1`，不要改用无界
`docker ps -aq`、network/volume 全量枚举。

## 服务或提交失败

### JobManager/Worker 不健康

检查：

- 主机 8080 是否冲突。
- checkpoint/artifact/output 卷是否可写。
- Worker `/health` 的 `heartbeat_error` 和 runtime errors。
- Worker 是否以 UID/GID 10001 运行。

`PermissionError: /data/checkpoints` 表示镜像没有为非 root 用户初始化挂载点权限，
必须重建包含 `/data/checkpoints` chown 的镜像。

### 资源不足

状态 API 返回 required/available slots。中级示例有 12 个物理任务，默认集群正好
提供 12 slots；任一 Worker 未注册都会阻止整图部署。

### YAML/制品拒绝

```powershell
.\.venv\Scripts\python -m pystream validate examples/intermediate/job.yaml
```

检查未知字段、Reduce keyed 输入、event-time execution、retract UDF、ZIP 摘要、
路径穿越和符号链接。

## 事件时间无输出

1. 检查 Source `records_read`、partition assignment 和 event time 解析。
2. 检查 `current_watermark` 与每输入 idle/active 状态。
3. 确认两 partitions 都有推进 Watermark 的 clock 记录。
4. 检查 `late_record_dropped`；`event_time <= watermark` 不会进入窗口。
5. 检查 Reduce active windows 和 `window_triggered`。

多输入 Watermark 取活跃输入最小值，不是最大值。

## 二级聚合错误或重复

- baseline 出现重复：优先检查 Source 是否发生动态 group rebalance；多并发必须使用
  确定性 manual partition assignment。
- count distribution 错误：检查 UPDATE_BEFORE/UPDATE_AFTER 顺序和 retract UDF。
- baseline 只能出现一次；recovery 窗口在 At-least-once 故障边界允许重复。
- 行顺序不稳定，验证必须比较多重集。

## Checkpoint 失败

状态字段：

```text
checkpoint.next_id
checkpoint.last_completed_id
checkpoint.consecutive_failures
```

检查顺序：

1. Source 是否 pause 并广播 DRAIN。
2. 所有输入是否收齐同一 checkpoint/attempt DRAIN。
3. snapshot SHA、大小、schema 和 task set 是否通过。
4. manifest 是否最后原子写入。
5. Source 是否只在 manifest 成功后 commit/resume。

损坏高版本 manifest 应被忽略并回退前一完整版本。连续失败达到配置阈值后进入恢复。

显式触发：

```powershell
Invoke-RestMethod -Method Post `
  http://localhost:8080/v1/jobs/<job_id>/checkpoint
```

Checkpoint 是停流操作；慢 Sink、大状态或 Docker I/O 延迟会增加暂停时间。

## Worker SIGKILL 后未恢复

不要使用：

```powershell
docker kill --signal SIGKILL pystream-worker-1-1
```

Docker 将其视为人工停止，`restart: unless-stopped` 不会自动拉起。标准注入为：

```powershell
docker exec pystream-worker-1-1 /bin/sh -c `
  "kill -9 `$(cat /proc/1/task/1/children)"
```

应验证：

- RestartCount 增加、StartedAt 改变。
- incarnation 改变。
- `recovery.attempts` 增加；不要求轮询必然采到短暂 RECOVERING。
- 最终作业 RUNNING，所有 Task attempt 一致、恢复点一致。

新 Worker 注册后端口可能尚未完全 ready，首次恢复部署可失败并进入下一 attempt；
只要未耗尽 `max_attempts` 且最终一致，即为正常重试路径。

## Kafka lag 验证失败

lag 验证分离两个 consumer：

- 无 group 的 metadata consumer 跟踪 topic 并读取 partitions/end offsets。
- 无订阅的 group consumer 只读取 committed offsets，避免加入业务组触发 rebalance。

`Topic ... not found in cluster metadata` 在 broker 刚启动时可能瞬态出现；验证脚本
有 30 秒有界 metadata 刷新。超时后仍为空才判失败。

## 清理失败

标准脚本只删除 ledger 中固定容器、network 和 volumes。若进程中断，重新运行脚本
会先读取 ledger 清理。不要删除不属于 `pystream` 的 Docker 资源。

出现新故障时保留时间、job_id、attempt、checkpoint、Worker/incarnation、状态
JSON、运行日志和输入/输出多重集。
