# PyStream 中级功能规格

## 状态

- Change ID: `build-pystream-intermediate`
- 基线: `v0.1.0` / `24384c14a7c7f95db350016e623421d5d37af674`
- 开发分支: `feature/intermediate-v0.2`
- 权威设计: `.trae/documents/pystream-intermediate-development-plan.md`
- 范围: 事件时间与有限乱序、Retract、自动恢复、At-least-once

## Goals

1. 保持现有 `pystream/v1` 处理时间作业兼容。
2. 支持 RFC3339 事件时间、滚动窗口、有限乱序 Watermark 和空闲输入检测。
3. 支持 `INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE` Changelog 及 Reduce retract。
4. 支持周期性停流协调 Checkpoint、共享卷状态快照和 Kafka offset 快照。
5. Worker 失败后由 Compose 拉起进程，JobManager 整作业重新调度并恢复状态。
6. 以结构化日志、测试、Docker 故障注入和 Git 回退记录提供可复核证据。

## Non-goals

- Exactly-once、事务 Sink、持续流对齐 Barrier。
- JobManager 高可用、跨主机共享存储容灾、动态扩缩容、多租户。
- SQL、Join、CEP、滑动窗口、会话窗口和 Side Output。

## Public YAML

### Execution

```yaml
execution:
  event_time:
    max_out_of_orderness: 2s
    idle_timeout: 30s
  checkpoint:
    interval: 10s
    timeout: 30s
    max_consecutive_failures: 3
  restart:
    max_attempts: 3
    delay: 2s
```

`execution` 不存在时保持第一阶段逐条 offset commit 和 fail-fast 行为。

### Source Event Time

```yaml
config:
  connector: kafka
  topic: words
  value_format: json
  event_time:
    pointer: /event_time
    format: rfc3339
```

- JSON Pointer 遵循 RFC 6901。
- 有时区 RFC3339 时间统一转换为 UTC；naive 时间和 leap second 拒绝。
- 提取或解析失败遵循 `bad_record_policy=fail|skip`。

### Window And Retract

```yaml
window:
  type: tumbling
  time_characteristic: event
  size: 5s
emit_mode: changelog
```

- `emit_mode=final|changelog`，默认 `final`。
- `changelog` 首次输出 INSERT，更新依次输出 UPDATE_BEFORE、UPDATE_AFTER。
- DataStream 传播 changelog 属性；消费可能撤回输入的 Reduce 必须配置二参数
  `retract_udf`。
- `retract_udf` 返回 `null` 删除当前 key/window 状态。

### Generic File Sink

```yaml
config:
  connector: file
  format: csv
  columns:
    - /headers/window_end
    - /payload/count
    - /payload/word_count
```

未配置 columns 时保持 `window_end,word,count`。

## Event-time Semantics

1. Source 按 topic/partition 维护最大事件时间和最后活动单调时间。
2. 分区 candidate 为 `max_event_time - max_out_of_orderness`。
3. Source Task Watermark 是活跃已观测分区 candidate 的最小值。
4. TaskRuntime Watermark 是活跃入通道 Watermark 的最小值。
5. 全部输入 idle 时不推进；恢复活跃时不得回退。
6. `event_time <= current_watermark` 为迟到记录，默认丢弃并计数。
7. 事件时间窗口只在 Watermark 到达 window end 时触发。

## Data-plane Semantics

- 内部协议版本为 2，不支持协议 v1/v2 混跑。
- DATA 使用现有 FORWARD/REBALANCE/HASH。
- WATERMARK 和 CHECKPOINT_DRAIN 使用独立 CONTROL frame，广播全部物理下游通道。
- CONTROL 不能越过前序 DATA，不能进入业务 UDF。
- HELLO、部署、状态上报和快照均携带 attempt_id，拒绝旧 attempt。

## Checkpoint Semantics

1. JobManager 递增 checkpoint_id，按下游到上游 arm 全部 Task。
2. Source 暂停消费并在前序 DATA 后广播 CHECKPOINT_DRAIN。
3. 非 Source 收齐全部入通道 drain 后写状态并继续广播。
4. Task 状态使用版本化规范 JSON、SHA-256、大小上限和原子替换。
5. JobManager 收齐执行图全部 Task descriptor 后最后写 manifest。
6. 只有合法 manifest 对应的完整 Checkpoint 可以恢复。
7. manifest 完成后 Source 提交精确 partition offset 并恢复消费。
8. timeout/校验错误 abort 当前 Checkpoint；连续失败达到上限触发恢复。

Checkpoint 目录：

```text
<root>/<job_id>/checkpoint-<id>/attempt-<id>/tasks/<sha256(task_id)>.json
<root>/<job_id>/checkpoint-<id>/manifest.json
```

Source partition 状态在 manifest 中按 operator 合并。恢复后 partition 即使分配给不同
Source subtask，也从同一 next offset 和 Watermark 基线恢复。

## Recovery Semantics

```text
RUNNING -> RECOVERING -> DEPLOYING -> RUNNING
RECOVERING/DEPLOYING -> RECOVERING
RECOVERING -> FAILING -> FAILED
RUNNING/RECOVERING -> CANCELLING -> CANCELLED
```

- 任一任务、连接或 Worker 心跳故障触发一次整作业恢复。
- 停止旧任务、abort Checkpoint、释放 slot，等待健康 Worker 和配置 delay。
- 从最高合法完整 Checkpoint 恢复；无 Checkpoint 时从初始状态恢复。
- WorkerManager 对相同 attempt 幂等，对更高 attempt 替换，对更低 attempt 拒绝。
- 超过 max attempts 后 FAILED；cancel 优先并停止后续重试。
- File Sink 仍为追加写，Checkpoint 后故障可能产生重复，但不得丢失输入。

## Observability

必须提供：

- Watermark: `watermark_emitted`、`watermark_advanced`、`input_idle`、
  `input_active`、`late_record_dropped`
- Retract: `changelog_emitted`、`retract_applied`、`retract_state_deleted`
- Checkpoint: `checkpoint_started`、`source_paused`、`task_snapshot_written`、
  `checkpoint_completed`、`checkpoint_aborted`
- Recovery: `recovery_started`、`state_restored`、`task_redeployed`、
  `recovery_completed`、`recovery_exhausted`

字段至少包含 attempt_id、checkpoint_id、duration_ms、state_bytes 和 records_in/out；
不得记录业务 payload、凭据或 token。

## Acceptance

- 初级 WordCount E2E 不回归。
- 中级 demo 对有限乱序输出正确，对迟到数据计数并丢弃。
- apple 计数从 1 更新到 2 时，二级聚合撤回 count=1 并加入 count=2。
- 完整 Checkpoint 覆盖全部任务，损坏/不完整/旧 attempt 快照不会恢复。
- SIGKILL 状态任务所在 Worker 后，容器重启、Worker 重注册、作业恢复 RUNNING。
- 恢复后无输入丢失，允许追加 Sink 重复。
- Python 3.11 Ruff、格式、全量测试通过，分支覆盖率 >= 80%，Docker 验收通过。
