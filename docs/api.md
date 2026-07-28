# YAML、UDF 与运行协议参考

本文是作业作者和集成开发者的字段级参考。公共契约的权威来源是
`src/pystream/api/models.py`、`artifact/udf.py`、`common/records.py` 和
`runtime/protocol.py`。

## 作业包

ZIP 根目录必须包含 `job.yaml`。Python UDF 文件与它位于同一作业目录，引用格式
为 `module:function`。CLI 为 ZIP 生成清单和 SHA-256；Worker 只加载当前作业
目录中的普通 `.py` 文件。

```powershell
pystream validate examples/wordcount/job.yaml
pystream package examples/wordcount --output-dir .pystream/artifacts
pystream submit .pystream/artifacts/<bundle>.zip
```

作业包不能在 Worker 上在线安装依赖。UDF 只能使用标准库、作业包内模块和镜像已
安装依赖。UDF 被视为可信代码，不提供沙箱。

## YAML v1

```yaml
api_version: pystream/v1
job:
  name: wordcount
operators:
  - id: words
    type: source
    parallelism: 2
    config:
      connector: kafka
      topic: words
      value_format: json
      bootstrap_servers: kafka:9092
      bad_record_policy: fail
  - id: normalize
    type: map
    parallelism: 2
    udf: wordcount_udfs:normalize
  - id: by_word
    type: key_by
    parallelism: 2
    udf: wordcount_udfs:word_key
  - id: totals
    type: reduce
    parallelism: 3
    udf: wordcount_udfs:add_counts
    window:
      type: tumbling
      time_characteristic: processing
      size: 10s
  - id: output
    type: sink
    parallelism: 1
    config:
      connector: file
      format: csv
      output_path: /data/output
edges:
  - from: words
    to: normalize
  - from: normalize
    to: by_word
  - from: by_word
    to: totals
  - from: totals
    to: output
```

### 顶层字段

| 字段 | 约束 |
|---|---|
| `api_version` | 第一阶段只接受 `pystream/v1` |
| `job.name` | 1-128 字符 |
| `operators` | 至少 2 个；ID 唯一 |
| `edges` | 至少 1 条；端点存在且图无环 |

未知字段全部拒绝。Source 入度必须为 0，Sink 出度必须为 0，其他算子必须有上游
和下游。作业至少包含一个 Source 和一个 Sink。

### 算子

| type | 必需字段 | 约束 |
|---|---|---|
| `source` | `config` | 只能使用 Kafka 配置，不接受 UDF/window |
| `map` | `udf` | 同步一参函数 |
| `key_by` | `udf` | 同步一参函数，返回 JSON key |
| `reduce` | `udf`、`window` | 同步二参函数；所有上游必须 keyed |
| `sink` | `config` | 只能使用 File 配置，不接受 UDF/window |

`parallelism` 为 1-1024 的整数。普通边在并发度相同的情况下为 FORWARD，否则为
REBALANCE；keyed stream 的出边为 HASH。

### Kafka Source 配置

| 字段 | 默认 | 说明 |
|---|---|---|
| `connector` | 必填 `kafka` | 连接器判别字段 |
| `topic` | 必填 | Kafka topic |
| `value_format` | `json` | 第一阶段只支持 JSON |
| `bootstrap_servers` | `kafka:9092` | broker 地址 |
| `group_id` | `pystream-<job>-<operator>` | 所有 Source subtask 共享 |
| `bad_record_policy` | `fail` | `fail` 或 `skip` |

Consumer 设置 `enable_auto_commit=False`。调用 `commit()` 只是显式提交 offset；
第一阶段未将 offset 与状态快照关联，不能据此宣称恢复语义。

### File Sink 配置

| 字段 | 默认 | 说明 |
|---|---|---|
| `connector` | 必填 `file` | 连接器判别字段 |
| `format` | `csv` | 第一阶段只支持 CSV |
| `output_path` | `/data/output` | 输出根目录 |

实际文件为
`<output_path>/<job_id>/<operator_id>/part-<subtask>.csv`。每条窗口结果写
`window_end,word,count`，无表头，并立即 flush。

### 窗口配置

- `type`: 固定 `tumbling`。
- `time_characteristic`: 固定 `processing`。
- `size`: 正整数加 `ms`、`s`、`m` 或 `h`，默认 `300s`。

窗口 epoch 对齐、左闭右开；空窗口不输出。

## Python UDF

UDF 必须是同步函数，并返回严格 JSON 可序列化值；NaN/Infinity 不允许。

```python
def normalize(value):
    return {"word": value["word"].lower(), "count": value["count"]}


def word_key(value):
    return value["word"]


def add_counts(left, right):
    return {"word": left["word"], "count": left["count"] + right["count"]}
```

- Map：`fn(payload) -> payload | None`。返回 `None` 表示丢弃记录。
- KeyBy：`fn(payload) -> key`。key 必须可 JSON 序列化。
- Reduce：`fn(accumulator, payload) -> accumulator`。

引擎在作业专属 Python 命名空间中加载 UDF；同名模块不会跨作业复用缓存。包内
模块应使用相对导入。

## RecordEnvelope

| 字段 | 第一阶段 |
|---|---|
| `message_type` | `DATA` |
| `record_id` | Kafka `topic:partition:offset` |
| `payload` | JSON 值 |
| `key` | KeyBy 前为 null，之后为 JSON 值 |
| `processing_time` | UTC ISO-8601 |
| `event_time` | null，后续启用 |
| `change_kind` | `INSERT` |
| `checkpoint_id` | null，后续启用 |
| `headers` | JSON object，包含来源/窗口元数据 |

## TCP 数据帧

编码为 `4-byte big-endian body length + UTF-8 JSON body`，协议版本为 1。
默认 body 上限 8 MiB，单 DATA_BATCH 默认最多 1000 条。

| 帧 | 用途 |
|---|---|
| `HELLO` | 首帧，声明 job/upstream/downstream task |
| `DATA_BATCH` | 记录数组 |
| `HEARTBEAT` | 严格递增 sequence |
| `END_OF_STREAM` | 正常结束 |
| `ERROR` | 远端错误 |

同一通道内保持发送顺序；跨通道没有全局顺序保证。

## HTTP 与 CLI

用户 CLI：

```text
validate JOB_YAML
package SOURCE_DIR [--output-dir DIR]
submit BUNDLE [--jobmanager-url URL]
status JOB_ID [--json]
cancel JOB_ID [--wait-timeout SEC]
```

JobManager 主要端点：

| Method | Path | 用途 |
|---|---|---|
| GET | `/health` | 控制面摘要 |
| GET | `/v1/workers` | Worker/slot/心跳 |
| POST | `/v1/jobs` | 上传 `application/zip` |
| GET | `/v1/jobs/{job_id}` | 作业和物理任务状态 |
| POST | `/v1/jobs/{job_id}/cancel` | 取消并释放资源 |

Worker 主要端点：

| Method | Path | 用途 |
|---|---|---|
| GET | `/health` | 心跳、连接和聚合运行指标 |
| GET | `/tasks` | 全部 RuntimeSnapshot |
| GET | `/tasks/{task_id}` | 单任务指标和错误 |
| POST | `/tasks/deploy` | JobManager 部署任务 |
| DELETE | `/tasks/{task_id}` | 停止任务 |

HTTP 管理端点面向可信实验网络，第一阶段不提供认证或 TLS。

## 日志 schema

每行是一个 JSON object，至少包含：

```json
{
  "timestamp": "2026-07-26T12:00:00+00:00",
  "level": "INFO",
  "component": "task_runtime",
  "job_id": "job-id",
  "operator_id": "totals",
  "subtask": 0,
  "worker_id": "worker-2",
  "event": "window_triggered",
  "message": "处理时间窗口已触发"
}
```

不适用的定位字段为 null。`PYSTREAM_LOG_LEVEL` 控制最低级别，默认 `INFO`。
