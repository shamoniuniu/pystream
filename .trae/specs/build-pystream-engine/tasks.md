# Tasks

## 执行范围

本任务清单只实施第一阶段“基础流式系统”。`spec.md` 中第二阶段和第三阶段属于已固定的后续路线，不在本 change-id 中实现；基础阶段验收完成后分别创建新的 change-id。

- [x] Task 1: 建立 PyStream 工程基线
  - [x] SubTask 1.1: 在当前目录初始化本地 Git 仓库，保留现有题目 Markdown/PDF，添加适合 Python、测试、构建、IDE、Docker 输出和运行数据的 `.gitignore`
  - [x] SubTask 1.2: 创建 Python 3.11 `src` 布局、`pyproject.toml`、CLI 入口和运行/开发依赖分组
  - [x] SubTask 1.3: 配置格式检查、静态检查、pytest、pytest-asyncio 和 coverage，设置核心包行覆盖率 80% 门槛
  - [x] SubTask 1.4: 创建 `pystream` 顶层包和模块边界骨架，每个顶层模块写中文模块 docstring
  - [x] SubTask 1.5: 验证 Python 3.11 容器或环境中项目可安装，CLI 可显示帮助，基础质量命令可运行

- [x] Task 2: 实现 YAML 作业 API、DataStream 和 DAG 校验
  - [x] SubTask 2.1: 定义版本化 `JobDefinition`、`OperatorSpec`、`EdgeSpec`、窗口配置、连接器配置和严格字段校验
  - [x] SubTask 2.2: 定义 `DataStream`、分区属性、`StreamGraph` 和拓扑排序
  - [x] SubTask 2.3: 实现 Source/Map/KeyBy/Reduce/Sink 约束、并发度约束、环检测、重复 ID、端点和 keyed stream 传播校验
  - [x] SubTask 2.4: 输出包含 YAML 字段路径的可操作错误，不允许未知字段静默生效
  - [x] SubTask 2.5: 添加合法 DAG、分支、合流、并发度变化和全部主要非法配置的契约测试

- [x] Task 3: 实现 ZIP 作业制品和隔离 UDF 加载
  - [x] SubTask 3.1: 实现作业目录打包、清单生成、SHA-256 摘要和不可变制品命名
  - [x] SubTask 3.2: 实现安全解压，拒绝绝对路径、`..`、符号链接、超限文件和摘要不匹配
  - [x] SubTask 3.3: 实现 `module:function` UDF 解析、job 目录隔离加载、可调用校验和模块缓存清理
  - [x] SubTask 3.4: 定义并校验 Map、KeySelector、Reduce UDF 的调用约定和 JSON 可序列化返回值
  - [x] SubTask 3.5: 添加正常打包/加载、制品篡改、ZIP 路径穿越和跨作业同名模块隔离测试

- [x] Task 4: 实现公共记录、数据帧和 Shuffle 路由
  - [x] SubTask 4.1: 实现 `RecordEnvelope` 和预留的 message type、event time、change kind、checkpoint 字段
  - [x] SubTask 4.2: 实现 4-byte 大端长度前缀 JSON 帧的编码、增量解码、版本和帧大小校验
  - [x] SubTask 4.3: 实现 `HELLO`、`DATA_BATCH`、`END_OF_STREAM`、`ERROR`、`HEARTBEAT` 帧
  - [x] SubTask 4.4: 实现 FORWARD、REBALANCE 和基于规范 JSON + SHA-256 的 HASH 路由
  - [x] SubTask 4.5: 实现有界异步队列、批量发送、`writer.drain()` 背压和连接资源清理
  - [x] SubTask 4.6: 添加半包、粘包、超限帧、错身份、顺序、哈希稳定、重平衡无丢失/重复和慢消费者测试

- [x] Task 5: 实现基础算子和处理时间窗口
  - [x] SubTask 5.1: 定义统一 Operator/Task 接口、生命周期和 Clock 注入点
  - [x] SubTask 5.2: 实现 Map 一对一映射与显式丢弃语义
  - [x] SubTask 5.3: 实现 KeyBy key 提取、key 合法性检查和 keyed stream 输出
  - [x] SubTask 5.4: 实现按 key、window 隔离的 Reduce 内存状态和用户 Reduce UDF 调用
  - [x] SubTask 5.5: 实现可配置、默认 300 秒、epoch 对齐、UTC 窗口结束触发的处理时间滚动窗口
  - [x] SubTask 5.6: 确保空窗口不输出、窗口触发后清理内存状态，并提供状态大小日志/指标
  - [x] SubTask 5.7: 使用可控 Clock 添加边界、跨窗口、大小写归一化、空窗口和状态清理测试

- [x] Task 6: 实现 Kafka Source 和文件 Sink
  - [x] SubTask 6.1: 实现 Kafka JSON Source 配置、consumer group、并行分区消费和手动 offset 模式
  - [x] SubTask 6.2: 为输入生成 `topic:partition:offset` record_id 和 UTC processing_time
  - [x] SubTask 6.3: 实现 `bad_record_policy=fail|skip`，错误日志必须包含 topic、partition、offset
  - [x] SubTask 6.4: 实现按 job/operator/subtask 隔离的 CSV 文件 Sink、刷新、关闭和错误传播
  - [x] SubTask 6.5: 实现 WordCount `window_end,word,count` 无表头输出格式和并行 Sink 分片命名
  - [x] SubTask 6.6: 使用 fake consumer/临时目录完成连接器单元测试；Docker 可用后补 Kafka 集成测试

- [x] Task 7: 实现 JobManager 控制面、执行图和调度器
  - [x] SubTask 7.1: 实现 Job、Worker、TaskInstance、slot 和作业状态机模型
  - [x] SubTask 7.2: 实现 Worker 注册、心跳、资源视图和健康检查
  - [x] SubTask 7.3: 将逻辑 DAG 按并发度展开为物理执行图，生成端点、通道和路由规则
  - [x] SubTask 7.4: 实现确定性均衡调度、资源预检查和至少跨两个 Worker 的放置
  - [x] SubTask 7.5: 实现下游优先部署、失败回滚、取消、slot 释放和状态聚合
  - [x] SubTask 7.6: 实现作业制品持久保存和 Worker 摘要校验下载接口
  - [x] SubTask 7.7: 添加状态转换、资源不足、调度稳定性、部署顺序和回滚测试

- [x] Task 8: 实现 Worker 和 TaskRuntime
  - [x] SubTask 8.1: 实现 Worker HTTP 服务、注册/心跳、任务部署/停止和状态查询
  - [x] SubTask 8.2: 实现制品下载、隔离工作目录、UDF 加载和任务生命周期管理
  - [x] SubTask 8.3: 实现数据端口监听、HELLO 握手、上游连接、输入合流和下游通道建立
  - [x] SubTask 8.4: 将 Source、Map、KeyBy、Reduce/Window、Sink 接入统一 TaskRuntime
  - [x] SubTask 8.5: 实现任务异常向 Worker/JobManager 传播；第一阶段断连或 Worker 丢失使作业 FAILED
  - [x] SubTask 8.6: 添加 loopback 多 Runtime 集成测试，证明跨端口 Map、HASH Shuffle、Reduce 和 Sink 数据流

- [x] Task 9: 实现 CLI 和基础作业操作流程
  - [x] SubTask 9.1: 实现 `validate`，展示 DAG 摘要、总 tasks、分区方式和配置错误
  - [x] SubTask 9.2: 实现 `package`，输出制品路径、文件清单和摘要
  - [x] SubTask 9.3: 实现 `submit`，上传 ZIP 并返回 job_id 和初始状态
  - [x] SubTask 9.4: 实现 `status`，显示作业状态、算子、subtask、Worker 和错误原因
  - [x] SubTask 9.5: 实现 `cancel`，等待终态并给出资源释放结果
  - [x] SubTask 9.6: 添加 CLI 参数、失败退出码、HTTP 错误和端到端 mock 控制面测试

- [x] Task 10: 提供 WordCount 示例和 Docker 多容器集群
  - [x] SubTask 10.1: 创建 WordCount `job.yaml` 和 UDF，输入为 `{"word":"APPLE","count":1}`，输出为题目要求的 CSV
  - [x] SubTask 10.2: 创建 Python 3.11 多阶段 Docker 镜像和非 root 运行用户
  - [x] SubTask 10.3: 创建 Kafka KRaft、JobManager、3 个 Worker、命名输出卷和健康检查的 Compose 配置
  - [x] SubTask 10.4: 配置足够的 Worker slots、Kafka topic 分区和算子并发度，使 TaskInstances 分布到至少两个 Worker
  - [x] SubTask 10.5: 创建可重复执行的数据生产、作业提交、等待窗口、结果校验和清理脚本
  - [x] SubTask 10.6: Docker 可用后验证 Compose 启停、服务健康、跨 Worker Shuffle、窗口输出和取消释放资源

- [x] Task 11: 完成可观测性与中文项目文档
  - [x] SubTask 11.1: 统一日志字段，至少包含 timestamp、level、component、job_id、operator_id、subtask、worker_id、event
  - [x] SubTask 11.2: 为 JobManager、Worker 和任务提供健康/状态信息，并记录连接、队列、输入、输出、错误和窗口触发指标
  - [x] SubTask 11.3: 编写根 README，说明项目目标、评分项映射、快速开始、验证入口和当前阶段限制
  - [x] SubTask 11.4: 编写架构与数据流文档，解释控制流、数据流、Shuffle、窗口和第一阶段失败语义
  - [x] SubTask 11.5: 编写逐模块文档，列出职责、入口接口、输入输出、依赖方向、错误处理和后续扩展点
  - [x] SubTask 11.6: 编写 YAML/UDF API、Docker 部署、WordCount 验证、测试和故障排查文档
  - [x] SubTask 11.7: 文档明确第一阶段不承诺恢复语义，并说明中级/高级路线与 At-least-once/Exactly-once 论证边界

- [x] Task 12: 执行基础阶段系统验收与性能测量
  - [x] SubTask 12.1: 运行格式、静态检查、单元测试、非 Docker 集成测试和覆盖率门槛
  - [x] SubTask 12.2: Docker 可用后运行 Kafka/JobManager/3 Worker 端到端测试并保存关键日志与结果摘要
  - [x] SubTask 12.3: 验证并发度不一致时 REBALANCE 无丢失/重复，相同 key 经 HASH 到同一 ReduceTask
  - [x] SubTask 12.4: 验证题目 WordCount 大小写不敏感、窗口结束时间和计数正确
  - [x] SubTask 12.5: 注入慢 Sink，验证有界队列和背压；终止 Worker，验证 JobManager 存活且第一阶段作业进入 FAILED
  - [x] SubTask 12.6: 运行可配置性能脚本，记录环境、输入规模、分区、并发度、窗口、吞吐和 p50/p95 延迟，不设置硬性吞吐门槛
  - [x] SubTask 12.7: 对照 `checklist.md` 逐项验收，未通过项转为修复任务并重新验证

- [x] Task 13: 修复独立验收发现的基础语义与验证缺口
  - [x] SubTask 13.1: 让 Kafka Source 的可配置 payload 校验错误统一遵循 `bad_record_policy=fail|skip`，覆盖合法 JSON 但字段/类型非法的 WordCount 输入
  - [x] SubTask 13.2: 在 Channel/TaskRuntime 中隔离非 DATA 控制消息，确保 WATERMARK/BARRIER 不进入第一阶段业务算子
  - [x] SubTask 13.3: 增加慢 File Sink 的完整 TaskRuntime 背压测试，证明真实 Sink 处理变慢时输入队列保持有界
  - [x] SubTask 13.4: 消除 WordCount 等待脚本的部分结果竞态，并增强验证脚本对 KeyBy 到 Reduce 跨 Worker HASH 通道证据的检查
  - [x] SubTask 13.5: 运行专项测试、全仓测试、覆盖率和 Ruff，并重新验证对应 checklist 失败项

## Task Dependencies

| Task | Depends on | 可并行说明 |
|---|---|---|
| Task 1 | 无 | 首先完成 |
| Task 2 | Task 1 | 工程基线后独立 |
| Task 3 | Task 1, Task 2 的 UDF/配置契约 | 可与 Task 4、Task 7 主体并行 |
| Task 4 | Task 1, Task 2 的分区模型 | 可与 Task 3、Task 5、Task 7 主体并行 |
| Task 5 | Task 1, Task 2 的算子配置 | 可与 Task 3、Task 4、Task 7 主体并行 |
| Task 6 | Task 2, Task 4 的记录契约, Task 5 的算子接口 | Source 与 Sink 可彼此并行 |
| Task 7 | Task 1, Task 2 | 控制面主体可与 Task 3/4/5 并行，制品下载部分等待 Task 3 |
| Task 8 | Task 3, Task 4, Task 5, Task 6, Task 7 | 集成关键路径 |
| Task 9 | Task 2, Task 3, Task 7 | 可在 Task 8 后半段并行 |
| Task 10 | Task 6, Task 7, Task 8, Task 9 | Docker 验收依赖用户安装 Docker |
| Task 11 | Task 2-10 对应模块 | 文档随模块增量编写，最终统一校验 |
| Task 12 | Task 1-11 | 最终验收；Docker 子项受环境前置条件约束 |
| Task 13 | Task 6, Task 8, Task 10, Task 12 的独立验收 | 修复完成后重新执行 Task 12.7 |

## 后续路线（不在本任务清单执行）

第一阶段全部验收通过后：

1. 新建第二阶段 change-id，实现事件时间、Watermark、Retract、状态快照、自动恢复和 At-least-once。
2. 第二阶段全部验收通过后，新建第三阶段 change-id，实现对齐 Barrier、系统内 Exactly-once、事务文件 Sink 和端到端故障证明。
