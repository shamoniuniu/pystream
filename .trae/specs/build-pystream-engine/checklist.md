# 基础阶段验收清单

## 范围与工程基线

- [x] 当前目录已初始化 Git，现有题目 Markdown/PDF 未移动、覆盖或删除
- [x] 项目名为 PyStream、包名为 `pystream`，使用 Python 3.11 和 `pyproject.toml`
- [x] 项目可在干净 Python 3.11 环境安装，CLI 帮助可正常运行
- [x] `.gitignore` 排除虚拟环境、缓存、构建物、运行数据、输出和本地制品
- [x] 基础阶段未误实现或宣称 At-least-once/Exactly-once；中高级能力仅保留兼容接口和文档路线

## YAML API 与 DAG

- [x] `pystream/v1` YAML 可表达 Source、Map、KeyBy、Reduce、Sink、窗口、edges 和每算子并发度
- [x] YAML 被解析为 `JobDefinition`、`DataStream` 和无环 `StreamGraph`
- [x] 合法线性 DAG、分支 DAG 和多上游合流 DAG 均能通过校验
- [x] 环、重复 ID、未知字段、未知算子、非法并发度、缺失 UDF 和非法 keyed 关系均在调度前拒绝
- [x] 配置错误包含可定位的 YAML 字段路径和修复信息
- [x] 逻辑 DAG 能确定性展开为包含 operator/subtask 的物理执行图

## 作业制品与 UDF

- [x] CLI 能将 `job.yaml` 和 Python UDF 打包为带清单与 SHA-256 的 ZIP
- [x] JobManager 以 job_id 和摘要不可变保存制品，Worker 下载后重新验证摘要
- [x] 安全测试证明绝对路径、`..`、符号链接、超限文件和摘要篡改均被拒绝
- [x] `module:function` 可正确加载 Map、KeySelector 和 Reduce UDF
- [x] 两个作业包含同名模块时不会发生 `sys.modules` 串扰
- [x] UDF 返回不可 JSON 序列化对象时产生明确任务错误

## 控制面、调度与生命周期

- [x] JobManager、Worker 均提供健康检查，3 个 Worker 能注册 slots 和数据地址
- [x] CLI 支持 `validate`、`package`、`submit`、`status`、`cancel`
- [x] 作业状态只按规格允许的状态转换变化，错误原因可查询
- [x] 调度前检查总 slots；资源不足时无部分任务被启动
- [x] 合法作业的 TaskInstances 被均衡分配到至少两个 Worker
- [x] 部署按下游到上游顺序进行，任一失败会停止已启动任务并释放 slots
- [x] 取消 RUNNING 作业会关闭任务和连接、释放 slots，并进入 CANCELLED
- [x] 第一阶段 Worker 失联或任务连接断开时，JobManager 保持存活且作业进入 FAILED

## 数据通道与 Shuffle

- [x] TCP 帧使用 4-byte 大端长度前缀和版本化 JSON body
- [x] 半包、粘包、非法 JSON、超限帧、错误 HELLO 身份和断连均有自动化测试
- [x] FORWARD 在等并发度情况下按对应 subtask 路由
- [x] REBALANCE 在并发度不同时轮询分发，确定性测试无丢失、无重复
- [x] HASH 使用 key 的规范 JSON 和 SHA-256，相同 key 跨进程/重启始终路由到同一下游 subtask
- [x] 每条 DAG 分支独立发送，多个上游通道可合流但不宣称 Join
- [x] 慢下游测试证明队列有界且上游产生背压，内存不会随输入无限增长
- [x] 同一上游到同一下游通道内的记录顺序得到保持

## Source、算子、窗口与 Sink

- [x] Kafka Source 使用同一作业消费组并支持多 SourceTask 分区消费
- [x] 每条 Kafka 记录生成 `topic:partition:offset` record_id 和 UTC processing_time
- [x] 非法输入按 `bad_record_policy=fail|skip` 执行，日志包含 topic、partition、offset
- [x] Map 能调用自定义 UDF 完成一对一转换和显式丢弃
- [x] KeyBy 能调用自定义 KeySelector，并将输出标记为 keyed
- [x] Reduce 只接受 keyed stream，并按 key/window 隔离状态调用自定义 UDF
- [x] 处理时间窗口可配置，默认 300 秒、epoch 对齐、按 UTC 窗口结束触发
- [x] 可控 Clock 测试覆盖窗口左闭右开边界、跨窗口、空窗口和触发后状态清理
- [x] 文件 Sink 按 job/operator/subtask 隔离写入并及时刷新
- [x] 并行 Sink 使用不同分片文件，不并发写同一文件
- [x] 文件写入失败会向上游传播并使作业失败

## WordCount 端到端行为

- [x] 示例 Kafka 输入使用 `{"word":"APPLE","count":1}` JSON 契约
- [x] `APPLE` 和 `apple` 被归一化为同一 `apple` key
- [x] 同一窗口中的 count 正确求和，不跨窗口累计
- [x] 输出为无表头 `window_end,word,count` CSV
- [x] `window_end` 使用 UTC `YYYY/MM/DDTHH:MM:SS` 并表示窗口结束时间
- [x] 测试窗口下的 `APPLE,1`、`pie,1`、`apple,1` 结果与预期逐行集合一致
- [x] 作业状态或日志能证明 Source、Map、KeyBy、Reduce、Sink 分布在至少两个 Worker
- [x] 日志或通道指标能证明 KeyBy 到 Reduce 发生跨 Worker HASH Shuffle

## Docker 与可复现部署

- [x] 用户已安装 Docker Desktop，`docker` 和 `docker compose` 命令可用
- [x] Docker 镜像固定 Python 3.11，使用非 root 用户运行
- [x] Compose 包含健康的 Kafka KRaft、1 个 JobManager、3 个 Worker 和命名输出卷
- [x] 启动流程会创建满足 Source 并发度的 Kafka topic 分区
- [x] `docker compose up` 后所有服务在规定时间内健康
- [x] 数据生产、作业提交、等待窗口、结果验证和清理流程可重复执行
- [x] `docker compose down` 后无遗留运行容器；按文档选择保留或清理数据卷

## 可观测性与文档

- [x] 日志至少包含 timestamp、level、component、job_id、operator_id、subtask、worker_id、event
- [x] 状态信息可定位每个 TaskInstance 的 Worker、端口、状态和最后错误
- [x] 连接、队列、输入、输出、错误和窗口触发具有可查询日志或指标
- [x] 根 README 包含模块概览、评分项映射、部署启动、WordCount 验证和当前限制
- [x] 架构文档解释控制流、数据流、部署图、Shuffle、窗口和失败行为
- [x] `docs/modules.md` 覆盖每个顶层模块的职责、接口、输入输出、依赖和失败行为
- [x] 每个顶层 Python 模块具有与实现一致的中文模块 docstring
- [x] API 文档说明 YAML 字段、UDF 签名、输入输出契约和配置错误
- [x] 部署与排障文档说明 Docker/Kafka 前置条件、常见失败和日志定位
- [x] 文档解释第一阶段为何不提供恢复语义，并准确描述后续 At-least-once/Exactly-once 路线

## 测试、质量与性能

- [x] 格式检查通过
- [x] 静态检查通过
- [x] 全部单元测试通过
- [x] 全部非 Docker 集成测试通过
- [x] 核心 `src/pystream` 行覆盖率不低于 80%
- [x] Docker Kafka 集成测试通过
- [x] Docker 多 Worker 端到端 WordCount 测试通过
- [x] 慢 Sink 背压测试通过
- [x] Worker 终止故障行为测试通过
- [x] 性能脚本记录环境、输入规模、分区、并发度、窗口、总耗时、吞吐、p50 和 p95 延迟
- [x] 性能报告不使用未经硬件条件约束的固定吞吐门槛
- [x] 所有测试和验证命令均在 README 或验证文档中可直接找到

## 后续兼容性

- [x] RecordEnvelope 已保留 `event_time`、`message_type`、`change_kind` 和 `checkpoint_id`
- [x] Channel 能区分 DATA 与未来控制消息，不把未知控制消息当业务 payload
- [x] Operator 生命周期存在状态快照/恢复扩展接口，但第一阶段实现明确为 no-op 或 unsupported
- [x] Sink 生命周期存在 begin/pre-commit/commit/abort 扩展边界，但第一阶段只使用普通追加写
- [x] 基础阶段完成后，第二阶段可在不破坏 `pystream/v1` 样例作业的前提下新增 Watermark、Retract 和 Checkpoint
