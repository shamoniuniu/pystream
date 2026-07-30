# YAML、UDF 与运行协议参考

公共契约的权威来源是 `src/pystream/api/models.py`、
`artifact/udf.py`、`common/records.py` 和 `runtime/protocol.py`。

## 作业包

ZIP 根目录必须包含 `job.yaml`。UDF 使用 `module:function` 引用，并与 YAML
位于同一作业目录。Worker 只加载包内普通 `.py` 文件；UDF 是可信代码，不提供
沙箱，也不能在 Worker 上在线安装依赖。

```powershell
pystream validate examples/intermediate/job.yaml
pystream package examples/intermediate --output-dir .pystream/artifacts
pystream submit .pystream/artifacts/<bundle>.zip
```

## YAML v1

`api_version` 仍为 `pystream/v1`。中级能力通过可选字段扩展，旧 WordCount 不配置
`execution` 时继续使用处理时间和 fail-fast 行为。

```yaml
api_version: pystream/v1
job:
  name: event-time-retract

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

持续时间接受正整数加 `ms`、`s`、`m` 或 `h`。`restart.delay` 额外允许 `0ms`。
未知字段全部拒绝。

### 算子

| type | 必需字段 | 中级约束 |
|---|---|---|
| `source` | `config` | Kafka JSON，可配置校验器和事件时间提取 |
| `map` | `udf` | 保留 `change_kind` |
| `key_by` | `udf` | key 必须是 JSON 值，出边使用稳定 HASH |
| `reduce` | `udf`、`window` | keyed 输入；可配置 `emit_mode` 和 `retract_udf` |
| `sink` | `config` | File CSV；必须能消费上游 changelog 能力 |

`parallelism` 范围为 1-1024。Source 多并发时按
`partition % source_parallelism == subtask_index` 静态分配；parallelism 不能
超过 topic 可用 partition。

### Kafka Source

| 字段 | 默认 | 说明 |
|---|---|---|
| `connector` | 必填 `kafka` | 连接器判别字段 |
| `topic` | 必填 | Kafka topic |
| `value_format` | `json` | 当前只支持 JSON |
| `bootstrap_servers` | `kafka:9092` | broker 地址 |
| `group_id` | `pystream-<job>-<operator>` | Checkpoint 后提交精确 next offset |
| `bad_record_policy` | `fail` | `fail` 或 `skip` |
| `validator` | null | 可选同步一参 UDF |
| `event_time.pointer` | 无 | RFC 6901 JSON Pointer |
| `event_time.format` | `rfc3339` | 当前只支持带时区 RFC3339 |

启用 Checkpoint 后，Source pause、保存每 partition next offset、事件时间基线和
Watermark；manifest 完成后才提交 offset 并恢复消费。恢复时按 manifest 显式
seek。未启用 Checkpoint 的旧作业维持原显式 commit/fail-fast 行为。

### File Sink

| 字段 | 默认 | 说明 |
|---|---|---|
| `connector` | 必填 `file` | 连接器判别字段 |
| `format` | `csv` | 当前只支持 CSV |
| `output_path` | `/data/output` | 输出根目录 |
| `columns` | 旧 WordCount 三列 | JSON Pointer 列表 |

文件位于 `<output_path>/<job_id>/<operator_id>/part-<subtask>.csv`。Sink 追加并
立即 flush，不参与事务；故障恢复允许重复。

### 窗口与 Changelog

窗口固定为 tumbling、epoch 对齐、左闭右开。

| 字段 | 取值 |
|---|---|
| `time_characteristic` | `processing` 或 `event` |
| `size` | 正持续时间 |
| `emit_mode` | `final` 或 `changelog` |

事件时间窗口在合并 Watermark 到达 window end 时触发。记录满足
`event_time <= current_watermark` 时视为迟到并丢弃。

`emit_mode=changelog` 的 Reduce 严格输出：

1. 首次状态：`INSERT`。
2. 更新：`UPDATE_BEFORE` 后 `UPDATE_AFTER`。
3. 下游 Reduce 使用 `retract_udf(accumulator, old_value)` 撤回旧贡献。
4. retract 返回 null 时删除空状态。

## Python UDF

UDF 必须同步且返回严格 JSON 值；NaN/Infinity 不允许。

```python
def add_bucket(left, right):
    return {"count": left["count"], "word_count": left["word_count"] + 1}


def remove_bucket(left, right):
    remaining = left["word_count"] - 1
    return None if remaining == 0 else {"count": left["count"], "word_count": remaining}
```

- Map：`fn(payload) -> payload | None`
- KeyBy：`fn(payload) -> key`
- Reduce add：`fn(accumulator, payload) -> accumulator`
- Reduce retract：`fn(accumulator, payload) -> accumulator | None`
- Source validator：`fn(payload) -> payload`

## RecordEnvelope

| 字段 | 当前语义 |
|---|---|
| `message_type` | `DATA`；控制消息使用独立 CONTROL frame |
| `record_id` | Kafka `topic:partition:offset` |
| `payload` / `key` | 严格 JSON |
| `processing_time` / `event_time` | UTC ISO-8601；event time 可为空 |
| `change_kind` | INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE |
| `checkpoint_id` | Checkpoint 控制上下文，可为空 |
| `headers` | 来源、窗口和业务元数据 |

## 数据面协议 v2

编码为 `4-byte big-endian body length + UTF-8 JSON body`，默认 body 上限
8 MiB。

| 帧 | 用途 |
|---|---|
| `HELLO` | 声明 job、task 和 `attempt_id`，旧 attempt 被 fencing |
| `DATA_BATCH` | 有序数据批次 |
| `CONTROL` | WATERMARK、CHECKPOINT_DRAIN 等控制消息 |
| `HEARTBEAT` | 严格递增 sequence |
| `END_OF_STREAM` | 正常结束 |
| `ERROR` | 远端错误 |

同一物理通道内 DATA/CONTROL 保序；控制消息广播到逻辑边的全部物理通道。
多输入 Watermark 取活跃输入最小值，idle 输入暂不阻塞推进，重新 active 后不得
使 Watermark 回退。

## HTTP

JobManager 主要端点：

| Method | Path | 用途 |
|---|---|---|
| GET | `/health` | 控制面摘要 |
| GET | `/v1/workers` | Worker、incarnation、slot 和心跳 |
| POST | `/v1/jobs` | 上传 `application/zip` |
| GET | `/v1/jobs/{job_id}` | 作业、attempt、Checkpoint 和物理任务状态 |
| POST | `/v1/jobs/{job_id}/checkpoint` | 同步触发一次完整停流 Checkpoint |
| POST | `/v1/jobs/{job_id}/cancel` | 取消并释放资源 |

Worker 主要端点包括 `/tasks/deploy`、任务 stop/status，以及
arm/trigger/wait/complete/abort Checkpoint 控制接口。所有请求携带 attempt，
低 attempt 请求不得影响新 Runtime。

HTTP 管理端点只面向可信实验网络，不提供认证或 TLS。

## 日志

每行是 UTF-8 JSON，稳定字段为：

```text
timestamp level component job_id operator_id subtask worker_id event message
```

关键事件包括 `source_partitions_assigned`、`watermark_advanced`、
`late_record_dropped`、`checkpoint_completed`、`job_recovery_started` 和
`worker_re_registered`。`PYSTREAM_LOG_LEVEL` 控制最低级别，默认 `INFO`。
