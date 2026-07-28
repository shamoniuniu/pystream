# 架构与数据流

本文面向需要理解 PyStream 运行机制和失败边界的开发者。代码和测试是当前状态的
权威来源；本文解释其组织方式，不引入额外语义。

## 部署拓扑

```text
CLI
  |
  | HTTP/ZIP
  v
JobManager :8080
  |  register / heartbeat / deploy / status / artifact
  +--------------------+--------------------+
  v                    v                    v
Worker-1             Worker-2             Worker-3
:8081 control        :8081 control        :8081 control
:9000 data           :9000 data           :9000 data
  \_____________________|____________________/
                        |
                      Kafka :9092
                        |
                 shared output volume
```

JobManager 持有作业、物理执行图、Worker 资源和制品元数据。Worker 下载制品并在
本地启动 TaskRuntime。Kafka 是外部无界输入，CSV 文件是第一阶段输出。

## 控制流

1. `pystream package` 校验 YAML，将作业目录构建为带清单和 SHA-256 的 ZIP。
2. `pystream submit` 通过 HTTP 上传 ZIP。
3. JobManager 复验摘要、安全解压并构建逻辑 `StreamGraph`。
4. 每个逻辑算子按 `parallelism` 展开为 `TaskInstance`。
5. 调度器先做全量 slot 预检，再确定性分配 Worker；资源不足不会部分部署。
6. JobManager 按 Sink 到 Source 的逆拓扑顺序部署，确保下游先监听。
7. Worker 下载同一不可变制品，隔离加载当前任务所需 UDF，启动 TaskRuntime。
8. CLI 通过状态 API 查询任务所在 Worker、slot、状态和错误。

任一部署失败时，JobManager 停止已部署任务、释放所有 slot，并将作业置为
`FAILED`。取消作业同样按反向部署顺序停止任务。

## 数据流

WordCount 的默认逻辑链路：

```text
Kafka Source(2)
  -> Map normalize(2)       FORWARD
  -> KeyBy word(2)          FORWARD
  => Reduce window(3)       HASH
  -> File Sink(1)           REBALANCE
```

- FORWARD：上下游并发度相同，连接相同 subtask 编号。
- REBALANCE：上游按轮询选择下游 subtask。
- HASH：将 key 转为排序键的规范 JSON，计算 SHA-256 后对下游并发度取模。
- 分支：每条逻辑边独立发送。
- 合流：多个上游写入目标 TaskRuntime 的同一个有界输入队列；不提供 Join 语义。

Task 间使用 TCP 长连接。每帧是 4 字节大端长度加 UTF-8 JSON body。连接先发送
HELLO 确认 job/upstream/downstream 身份，再传 DATA_BATCH、HEARTBEAT、
END_OF_STREAM 或 ERROR。

## 背压

每个出通道持有有界队列，唯一发送协程按批次写入，并等待 `writer.drain()`。
目标 Worker 的 TaskRuntime 同样使用有界合流队列。因此下游处理变慢时：

```text
下游 input queue 满
  -> TCP 接收变慢
  -> writer.drain 等待
  -> output queue 满
  -> 上游 send 等待
```

系统不会通过无限队列隐藏过载。Worker `/health` 和 `/tasks` 暴露当前队列深度、
容量、历史峰值、批次数和输入/输出记录数。

## 处理时间窗口

Reduce 只消费 keyed stream。状态键为 `(TimeWindow, canonical_key)`：

- 默认窗口 300 秒，示例为便于演示使用 10 秒。
- 窗口按 Unix epoch 对齐，区间为左闭右开 `[start, end)`。
- TaskRuntime 周期调用 `on_timer()`。
- 当前 UTC Clock 到达窗口结束时间后输出非空 key，并立即删除该窗口状态。
- 输出 `headers.window_start/window_end` 使用 `YYYY/MM/DDTHH:MM:SS`。
- 空窗口不输出；不同窗口不累计。

## 记录与状态

`RecordEnvelope` 保存 record_id、payload、key、processing_time、headers，并预留
event_time、message_type、change_kind 和 checkpoint_id。第一阶段：

- `message_type=DATA`
- `change_kind=INSERT`
- `event_time=null`
- `checkpoint_id=null`

算子状态只在 Worker 内存中。Source record_id 使用
`topic:partition:offset`，便于日志定位，但它本身不提供去重或恢复。

## 信任边界

- JobManager/Worker 在可信实验网络内，第一阶段无认证和 TLS。
- Python UDF 是可信代码，可在 Worker 进程中执行，不提供沙箱。
- ZIP 解压仍防止绝对路径、`..`、符号链接、文件数量/大小超限和摘要不匹配。
- Kafka payload 和 UDF 输出经过 JSON 及类型校验。
- File Sink 路径按 job/operator/subtask 隔离，拒绝路径逃逸。

## 失败模型

第一阶段采用 fail-fast：

| 失败 | 行为 |
|---|---|
| 坏 Kafka 记录 | 按 `bad_record_policy` 跳过并记录，或使 Source 失败 |
| UDF/算子异常 | Task 上报失败，JobManager 停止整作业 |
| TCP 异常 EOF/协议错误 | 下游 Task 失败，不把数据缺口当正常结束 |
| Worker 心跳超时 | 使用该 Worker 的运行作业失败并释放 slot |
| 文件写入失败 | Sink Task 失败并传播 |
| JobManager 失败 | 无高可用；控制面不可用 |

系统不会自动重启、恢复内存状态或回退 Kafka offset。因此当前不承诺
At-least-once 或 Exactly-once；完整边界见 [阶段路线](roadmap.md)。

## 依赖方向

```text
common <- api <- control
common <- runtime <- worker
common <- operators <- runtime
artifact <- control/worker/cli
observability <- service/control/runtime/worker/operators
cli -> api + artifact + HTTP client
```

`common` 不反向依赖控制面、运行时或算子。JobManager 不导入 WordCount UDF。
传输适配器位于边缘，领域模型不依赖 HTTP。

## 可观测检查点

- JobManager `/health`：Worker 和作业摘要。
- JobManager `/v1/workers`：地址、slot、心跳和健康状态。
- JobManager `/v1/jobs/{job_id}`：物理任务位置、状态和错误。
- Worker `/health`：心跳、数据连接及聚合运行指标。
- Worker `/tasks`：每个任务的通道、队列、记录、批次、错误和算子状态指标。
- 标准错误流：一行一个 JSON 日志；按 job/operator/subtask/worker/event 查询。
