# YAML、UDF 与运行协议参考

公共契约的权威来源是 `src/pystream/api/models.py`、`artifact/udf.py`、
`common/records.py` 和 `runtime/protocol.py`。

## 作业包

ZIP 根目录必须包含 `job.yaml`。UDF 使用 `module:function` 引用，并与 YAML
位于同一作业目录。UDF 是可信代码，不提供沙箱或在线依赖安装。

```powershell
pystream validate examples/advanced/job.yaml
pystream package examples/advanced --output-dir .pystream/artifacts
pystream submit .pystream/artifacts/<bundle>.zip
```

## YAML v1

`api_version` 仍为 `pystream/v1`：

```yaml
api_version: pystream/v1
job:
  name: exactly-once-counts

execution:
  delivery_guarantee: exactly_once
  event_time:
    max_out_of_orderness: 2s
    idle_timeout: 30s
  checkpoint:
    interval: 10s
    timeout: 30s
    max_consecutive_failures: 3
  restart:
    max_attempts: 6
    delay: 2s
```

兼容规则：

- 缺少 `execution`：保持基础 fail-fast 和逐条 offset commit。
- `execution` 存在但未配置 guarantee：默认 `exactly_once`。
- `delivery_guarantee: at_least_once`：使用 DRAIN 和 append File Sink。
- Exactly-once 作业的全部 Sink 必须声明事务能力，否则 Graph 校验失败。

持续时间接受正整数加 `ms`、`s`、`m` 或 `h`；未知字段拒绝。

## 算子与连接器

| type | 必需字段 | 关键约束 |
|---|---|---|
| `source` | `config` | Kafka JSON、可选 validator/event time |
| `map` | `udf` | 保留 `change_kind` |
| `key_by` | `udf` | key 必须是严格 JSON 值 |
| `reduce` | `udf`、`window` | keyed 输入；可配 changelog/retract |
| `sink` | `config` | CSV File；Exactly-once 时使用事务布局 |

Kafka Source 按 `partition % source_parallelism == subtask_index` 分配；Source
parallelism 不能超过 topic partitions。Exactly-once complete 只提交 Barrier
时冻结的 next offsets，abort 不提交。

事务 File Sink 布局：

```text
<output>/<job_id>/<operator>/
  pending/attempt-<attempt>/tx-<uuid>/part-<subtask>.csv
  committed/checkpoint-<checkpoint>/part-<subtask>.csv
  manifests/checkpoint-<checkpoint>.json
```

目录扫描不代表可见结果。消费者必须读取 manifest 并复验 fragment 的相对路径、
identity、SHA-256 和 size。

## Python UDF

UDF 必须同步并返回严格 JSON 值；NaN/Infinity 不允许。

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

## 数据面协议

编码为 `4-byte big-endian body length + UTF-8 JSON body`，默认 body 上限 8 MiB。

| 帧 | 用途 |
|---|---|
| `HELLO` | job/task/attempt/coordinator epoch 握手 |
| `DATA_BATCH` | 有序数据批次 |
| `CONTROL` | WATERMARK、DRAIN、BARRIER、CHECKPOINT_COMPLETE |
| `HEARTBEAT` | 严格递增 sequence |
| `END_OF_STREAM` | 正常结束 |
| `ERROR` | 远端错误 |

同一物理通道内 DATA/CONTROL 保序。Barrier 不进入 UDF；收到 Barrier 的输入由
连接 gate 阻塞 post-barrier 读取，直到 Task 完成全输入对齐。

## HTTP

JobManager 端点：

| Method | Path | 用途 |
|---|---|---|
| GET | `/health/live` | 进程存活 |
| GET | `/health/ready` | 依赖就绪 |
| GET | `/health/leader` | 仅 ready active 返回 200 |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/v1/workers` | Worker、incarnation、slot、epoch |
| POST | `/v1/jobs` | 上传 ZIP |
| GET | `/v1/jobs/{job_id}` | 作业、attempt、phase、decided/finalized |
| POST | `/v1/jobs/{job_id}/checkpoint` | 触发 Checkpoint |
| POST | `/v1/jobs/{job_id}/cancel` | 取消作业 |

外部管理端点要求 HTTPS 与 Bearer Token。内部 JobManager/Worker HTTP 要求 mTLS，
写请求还受 active role、attempt 和 coordinator epoch fencing。

## 确定性验收 Hook

测试 Hook 仅在 `PYSTREAM_ENABLE_TEST_HOOKS=true` 或
`--enable-test-hooks` 时注册；生产默认关闭并返回 404。

```text
POST /test/checkpoint-hooks/{hook}/arm
GET  /test/checkpoint-hooks
POST /test/checkpoint-hooks/{hook}/release
```

合法 hook：

- `before_barrier`：全 Task 已 arm，Source 尚未注入 Barrier。
- `before_decision`：snapshot/PREPARED 完成，durable decision 尚未写入。
- `after_decision`：decision 已持久化，finalize 尚未开始。

release body 为 `{"action":"continue"}` 或 `{"action":"fail"}`。接口仍要求 HAProxy
证书身份和外部 Bearer Token，不是安全绕过。

## 状态与日志

作业状态的 checkpoint 字段至少包含：

```text
next_id active_id phase last_completed_id last_decided_id last_finalized_id
```

稳定日志字段为：

```text
timestamp level component job_id operator_id subtask worker_id event message
```

关键事件包括 Barrier 对齐、checkpoint decision/finalize、事务状态、leader lease、
作业恢复和认证拒绝。日志不得包含 Token、私钥、access key 或业务 payload。
