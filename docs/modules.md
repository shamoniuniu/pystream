# 模块说明

本文面向引擎维护者。模块 docstring 提供就近摘要；本文说明跨模块职责和依赖。

## `pystream.api`

- 解析严格 `pystream/v1` YAML，构造配置模型和 `StreamGraph`。
- 校验 DAG、keyed 属性、parallelism 与 delivery guarantee。
- `execution` 作业默认 Exactly-once；不具备事务能力的 Sink 在调度前拒绝。
- 不依赖 control/runtime/worker。

## `pystream.artifact`

- 确定性构建 ZIP、生成清单/SHA-256、安全解压和隔离加载可信 UDF。
- 拒绝摘要错误、路径穿越、链接、超限和非法模块。
- artifact 可由 Local 或 S3 repository 持久化。

## `pystream.common`

- 提供 `RecordEnvelope`、`MessageType`、`ChangeKind` 等稳定共享模型。
- 严格验证 UTC 时间和 JSON 值。
- 只依赖标准库，不反向依赖业务模块。

## `pystream.runtime`

- 实现协议 v2、DATA/CONTROL 保序、attempt/epoch HELLO fencing、Shuffle 和有界队列。
- 维护 per-input Watermark 与 Barrier gate。
- all-input alignment 后 snapshot/forward/unblock，失败/abort/stop 一律释放 gate。
- 协议或连接错误使 Task 失败并交由控制面恢复。

## `pystream.operators`

- 提供 Source、Map、KeyBy、Reduce、Kafka、File Sink 和算子生命周期。
- Kafka Source 维护 checkpoint-specific frozen offsets。
- Reduce 支持事件时间、changelog/retract 和状态 round-trip。
- File Sink 支持 ACTIVE/PREPARED/COMMITTED/ABORTED 事务与幂等 finalize。
- UDF 是可信进程内代码，不提供强隔离。

## `pystream.checkpoint`

- 定义 task snapshot、transaction descriptor、decision 和 finalized schema。
- Local/S3 store 严格校验 identity、task set、SHA、size 和 schema version。
- immutable object、manifest-last、损坏回退和 durable latest manifest 是恢复基础。
- DECIDED 不可逆；FINALIZED 可以由接管 leader 重试。

## `pystream.storage`

- 封装 S3 TLS、请求、条件写、错误映射与指标。
- `If-None-Match` 用于不可变对象，ETag `If-Match` 用于 CAS pointer/lease。
- 409/412 映射为明确并发冲突，不被重试逻辑吞掉。
- 不包含 Job 或 Checkpoint 领域决策。

## `pystream.control`

- 维护 Job/Task/Worker 状态、物理图、slot、部署、取消、Checkpoint 和恢复。
- `CheckpointCoordinator` 编排 arm、Barrier、PREPARED、DECIDED 和 FINALIZED。
- `JobManager` 持久化 metadata revisions，并保证 decided/finalized 单调。
- leader controller 使用 S3 lease 和 coordinator epoch 提供 active/passive 接管。
- 同一算子 subtasks 并发部署，算子之间保持下游优先。
- `testing.py` 提供 opt-in 确定性验收 hook；默认不注册测试路由。

## `pystream.worker`

- 负责 incarnation 注册/心跳、attempt/epoch-aware 部署和 TaskRuntime 生命周期。
- 提供 arm/trigger/wait/complete/abort Checkpoint 控制接口。
- active 切换后自动重新注册；低 epoch 和旧 attempt 请求被拒绝。
- 制品复验、算子组装或 Runtime 错误转为明确任务失败。

## `pystream.security`

- 创建 client/server SSL context、校验证书 SAN/CN 身份。
- 提供恒定时间 Bearer 比较和 file-only Secret loader。
- 管理 Token 与 metrics Token 分离。
- 不记录 Secret 值；调用方只持有启动时读取的内存值。

## `pystream.observability`

- 输出单行 JSON 日志和 Prometheus 指标。
- 指标覆盖 leader/lease、Barrier/Checkpoint、transaction/finalize、recovery、
  object store、TLS/auth 和证书到期。
- 只提供 read model，不修改领域状态机。

## `pystream.cli` 与 `pystream.client`

- 提供 validate/package/submit/status/cancel/checkpoint。
- 客户端从文件加载 CA、证书、key 和外部 Token。
- 验收客户端额外提供 arm/status/release checkpoint hook。
- CLI 不导入 JobManager 领域实现，机器读取使用 JSON 状态。

## `pystream.service`

- 组装 JobManager/Worker、repository、TLS、auth、metrics 和 leader 依赖。
- 入口为 `python -m pystream.service jobmanager|worker`。
- `--enable-test-hooks` 或对应环境变量只用于明确的验收环境。
- 只做依赖注入，不复制领域逻辑。

## 依赖规则

1. `common` 不依赖其他业务模块。
2. `api` 不依赖 control/runtime/worker。
3. operators 不发起 JobManager 控制请求。
4. control 通过端口调用 Worker、artifact、metadata 和 checkpoint store。
5. storage 不理解 Job/Checkpoint 状态机。
6. CLI 不导入控制面领域实现。
7. observability 只读取指标状态，不改变业务状态。
8. security 不接触业务 payload。

修改公开接口、失败语义、状态 schema 或依赖方向时，必须同步更新本文和对应测试。
