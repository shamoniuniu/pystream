# 模块说明

本文面向引擎维护者。每个顶层模块的职责、接口、数据流、依赖和失败边界如下；
模块 docstring 提供就近摘要，本文是跨模块关系的权威说明。

## `pystream.api`

- 职责：解析严格 `pystream/v1` YAML，构造 `JobDefinition`、`DataStream` 和
  `StreamGraph`，完成字段、DAG、keyed 属性和分区校验。
- 入口：`load_job_definition`、`load_stream_graph`、`build_stream_graph`。
- 输入/输出：YAML 文件 -> 不可变配置模型和规范化逻辑图。
- 依赖：Pydantic、PyYAML；不依赖控制面或运行时。
- 失败：`JobConfigError` 包含 YAML 路径；未知字段、环、错误端点和非法 Reduce
  在调度前失败。
- 扩展：新增公共算子或配置必须升级契约兼容性检查，不能静默改变 v1。

## `pystream.artifact`

- 职责：确定性构建 ZIP、生成清单/SHA-256、安全解压、隔离加载可信 UDF。
- 入口：`build_job_bundle`、`verify_job_bundle`、`extract_job_bundle`、
  `UDFLoader`。
- 输入/输出：作业目录 <-> 不可变 ZIP；`module:function` -> 可调用对象。
- 依赖：标准库；只读取 API 约定，不依赖调度。
- 失败：摘要、路径、链接、大小、签名或 JSON 契约错误阻止部署。
- 扩展：未来可加入依赖封装，但不能绕过清单和作业命名空间。

## `pystream.common`

- 职责：提供跨控制面、运行时和算子共享的稳定记录信封。
- 入口：`RecordEnvelope`、`MessageType`、`ChangeKind`。
- 输入/输出：业务 JSON 与系统元数据 <-> 可序列化记录对象。
- 依赖：仅标准库；禁止反向依赖其他 PyStream 业务模块。
- 失败：非 UTC 时间、不可 JSON 序列化值、未知字段或枚举触发
  `RecordValidationError`。
- 扩展：event time、changelog 和 checkpoint 字段已启用；后续事务 Sink 仍需保持
  向后兼容。

## `pystream.control`

- 职责：维护 Job/Task/Worker 状态，展开物理图、调度 slot、编排部署/取消、
  协调 Checkpoint 与整作业恢复，并暴露 JobManager HTTP API。
- 入口：`JobManager`、`CheckpointCoordinator`、`JobManagerHttpService`、
  `build_execution_graph`、`SlotScheduler`、`LocalArtifactRepository`。
- 输入/输出：逻辑 StreamGraph、Worker 注册、任务状态 -> 物理执行图和作业状态。
- 依赖：API、Artifact、可替换 `WorkerGateway`/`ArtifactRepository` 端口。
- 失败：资源不足时原子拒绝；中级作业在重试预算内整作业恢复，耗尽后失败并释放
  slot；旧 attempt 请求被 fencing。
- 扩展：Exactly-once 和 JobManager HA 仍属于后续阶段。

## `pystream.runtime`

- 职责：协议 v2、attempt 握手、DATA/CONTROL 保序、Shuffle、有界队列、
  Watermark/DRAIN 合并、TaskRuntime snapshot/restore 和失败传播。
- 入口：`TaskRuntime`、`DataPlaneServer`、`BoundedDataChannel`、
  `ShuffleRouter`、协议帧函数。
- 输入/输出：`TaskDeployment` 和 RecordEnvelope -> 跨 Task 数据批次与状态快照。
- 依赖：Common、Control 的部署 DTO、Operators 接口。
- 失败：协议、握手、异常 EOF、队列发送或算子异常使任务进入 `FAILED`。
- 扩展：WATERMARK 和 CHECKPOINT_DRAIN 已启用；持续流 barrier 对齐尚未实现。

## `pystream.operators`

- 职责：统一算子生命周期、Clock、Map、KeyBy、处理时间/事件时间 Reduce、
  Changelog/Retract、Kafka Source 和 CSV File Sink。
- 入口：`OperatorContext`、`BaseOperator`、`KafkaJsonSource`、
  `MapOperator`、`KeyByOperator`、`ReduceWindowOperator`、
  `FileSinkOperator`。
- 输入/输出：RecordEnvelope -> 零到多条 RecordEnvelope；外部 Kafka/文件。
- 依赖：API 连接器配置、Common 记录、注入的 UDF 和 Clock。
- 失败：生命周期、迟到记录、坏记录、UDF、Kafka、快照和文件错误均使用明确异常。
- 扩展：Source/Reduce 已支持状态快照；Sink 事务方法仍明确不支持。

## `pystream.worker`

- 职责：进程 incarnation 注册和心跳、attempt-aware 部署、Checkpoint 控制接口、
  制品缓存、UDF/算子组装、TaskRuntime 生命周期和失败上报。
- 入口：`WorkerTaskManager`、`WorkerHttpService`、`HttpJobManagerClient`、
  `HttpWorkerGateway`。
- 输入/输出：TaskDeployment -> RuntimeSnapshot；Worker 状态 -> JobManager。
- 依赖：Artifact、API、Operators、Runtime，以及控制面端口 DTO。
- 失败：制品不匹配、任务类型不一致、启动失败或运行失败转为 `WorkerTaskError`
  并上报。
- 扩展：自动恢复已由 JobManager 编排；进程级强隔离和 Worker 内局部恢复未实现。

## `pystream.observability`

- 职责：配置统一 JSON 日志和标准定位字段；指标值由各拥有状态的模块维护。
- 入口：`configure_logging`、`JsonLogFormatter`、`log_event`。
- 输入/输出：Python LogRecord -> 单行 UTF-8 JSON。
- 依赖：仅标准库；其他模块可依赖它，它不读取或改变业务状态。
- 失败：不可直接 JSON 编码的额外字段通过字符串回退，不中断任务。
- 扩展：后续可增加指标导出适配器，但状态所有权仍留在 Runtime/Operator。

## `pystream.cli` 与 `pystream.client`

- 职责：提供 validate/package/submit/status/cancel 用户流程和同步 HTTP 客户端；
  客户端也可显式触发一次完整 Checkpoint。
- 入口：`pystream`、`python -m pystream`、`JobManagerClient`。
- 输入/输出：本地路径/Job ID -> 人类可读摘要或稳定退出码。
- 依赖：公开 API、Artifact 和 HTTP client；不导入 JobManager 领域实现。
- 失败：配置、制品、HTTP 和等待超时转换为非零退出码和中文错误。
- 扩展：新增命令必须保持脚本友好的错误码；机器读取使用 `status --json`。

## `pystream.service`

- 职责：容器进程入口，组装 JobManager 或 Worker 的具体依赖并配置日志。
- 入口：`python -m pystream.service jobmanager|worker`。
- 输入/输出：命令行参数 -> 前台 aiohttp 服务。
- 依赖：Control、Worker、Runtime、Observability。
- 失败：非法端口/间隔在启动前拒绝；运行错误由服务日志和进程退出体现。
- 扩展：这里只做依赖注入，不应复制领域逻辑。

## 依赖规则

1. `common` 不依赖其他业务模块。
2. `api` 不依赖 control/runtime/worker。
3. 算子不发起 JobManager 控制请求。
4. JobManager 通过端口调用 Worker，不持有 Worker 进程内对象。
5. CLI 不导入控制面领域实现。
6. Observability 只格式化事件，不修改状态机或记录。

修改任一模块的公开接口、输入输出、失败语义或依赖方向时，必须同步更新本文件和
对应契约/单元测试。
