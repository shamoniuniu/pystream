# PyStream 简易流计算系统三阶段总规格

## Why

当前工作区只有课程题目，没有实现代码、工程结构或可执行验证流程。需要从零实现一个以 Python 为核心、能在 Docker 多容器环境中真实运行的分布式流计算系统，并通过清晰的模块文档和测试证据覆盖课程的基础、中级和高级评分项。

本规格固定三阶段总体架构和关键语义。本轮只实施第一阶段“基础流式系统”；第二阶段“事件时间、Retract、At-least-once 故障恢复”和第三阶段“Exactly-once”必须在基础阶段验收完成后分别创建新的 change-id 实施。

## What Changes

- 在当前目录建立项目名为 `PyStream`、Python 包名为 `pystream` 的 Python 3.11 工程，并保留现有题目 Markdown/PDF 文件。
- 使用 YAML 描述作业 DAG、算子并发度、窗口和连接关系，使用 Python UDF 实现自定义 Map、KeyBy 和 Reduce 逻辑。
- CLI 将 `job.yaml` 与 UDF 打包为 ZIP 并提交给 JobManager；JobManager 校验、保存并向 Worker 分发作业包。
- 使用 JobManager + 多 Worker 的控制面，以及基于 `asyncio` 长连接 TCP 的数据面。
- 通过 Kafka Source、文件 Sink、Map、KeyBy、Reduce 和处理时间滚动窗口完成分布式 WordCount。
- 使用 Docker Compose 提供 Kafka、1 个 JobManager 和默认 3 个 Worker 的可复现集群。
- 为每个顶层模块提供中文职责、接口、数据流和失败行为说明，并为 Python 模块提供模块级 docstring。
- 提供单元测试、集成测试、端到端验证和不设硬性吞吐门槛的性能测试脚本与报告模板。
- 在统一记录信封、状态后端、控制事件和 Sink 提交接口中预留第二、第三阶段扩展点，但本轮不实现中高级行为。
- **BREAKING**：无。项目当前没有既有 API 或代码。

## Impact

- Affected specs: 作业 API、DAG 编排、算子运行时、任务调度、跨节点 Shuffle、Kafka 接入、文件输出、窗口语义、故障恢复路线、Exactly-once 路线、工程文档与测试。
- Affected code: 当前目录下后续新增的 `src/pystream/`、`tests/`、`examples/`、`docs/`、`deploy/`、`scripts/`、`pyproject.toml`、Docker 构建与 Compose 配置。
- Runtime dependencies: Python 3.11、Kafka、Docker Desktop/Linux 容器、Docker Compose。
- Current environment note: 当前机器尚未提供 `docker` 命令，完整多容器验收必须在用户安装 Docker 后执行；Git 可用，本机默认 Python 为 3.13，不作为目标运行时。

## 范围与成功标准

### 当前实施范围

| 阶段 | 范围 | 本 change-id 状态 |
|---|---|---|
| 第一阶段 | 基础 API、DAG、并发度、调度、Kafka Source、文件 Sink、Map、KeyBy、Reduce、处理时间滚动窗口、Shuffle、WordCount、文档与测试 | 实施 |
| 第二阶段 | 事件时间窗口、有限乱序 Watermark、Retract、自动故障恢复、At-least-once | 仅固定架构与需求，后续 change-id |
| 第三阶段 | 系统内 Exactly-once、事务文件 Sink、端到端 Exactly-once | 仅固定架构与需求，后续 change-id |

### Goals

- 真实启动多个独立容器，并通过网络在不同 Worker 之间传输记录。
- 用户仅通过 YAML 和 Python UDF 即可描述并提交一个流式 DAG。
- 基础 WordCount 在大小写不敏感、并发度不一致和 5 分钟处理时间滚动窗口下输出正确结果。
- 模块边界、数据契约和扩展接口足以承载后续 Watermark、Checkpoint Barrier、Retract 和事务提交。
- 所有核心行为可通过自动化测试或明确的手工验证命令复现。

### Non-goals

- 不以生产级 Flink 替代品为目标。
- 第一阶段不恢复崩溃任务、不恢复状态，也不承诺 At-least-once 或 Exactly-once。
- 不实现 JobManager 高可用、跨机房容灾、动态扩缩容、资源隔离、多租户或 Web 管理界面。
- 不支持 SQL、Join、CEP、滑动窗口、会话窗口或任意外部 Sink。
- 不沙箱化用户 UDF；系统只接受可信用户提交的代码。
- 不承诺固定吞吐或延迟数值，只提供可复现测量结果。

### Constraints

- 目标解释器为 Python 3.11，项目依赖由 `pyproject.toml` 声明并通过 pip 安装；本机可自行使用 Conda 创建 Python 3.11 环境。
- Kafka 使用 Docker Compose 内的单 Broker KRaft 模式，版本在实现时固定并记录。
- 默认集群为 1 个 JobManager、3 个 Worker；每个 Worker 默认 4 个 task slots。
- 控制面采用 HTTP/JSON；数据面采用长度前缀 UTF-8 JSON 帧的 `asyncio` TCP 长连接。
- 所有容器使用同一 Docker 网络；文件 Sink 和后续 Checkpoint 使用命名卷。
- 用户提供的 payload 和 UDF 返回值必须可 JSON 序列化。

## 架构决策记录

### ADR-001：PyStream 基础架构

- **Status**: Accepted for specification
- **Decision question**: 如何以最小但真实的分布式实现覆盖课程基础要求，同时避免阻塞后续状态一致性能力？
- **Responsibility**: 用户负责 Docker 前置环境和最终验收；`tests/`、Compose 健康检查与文档验证步骤负责持续校验；各模块责任记录在 `docs/modules.md`。

#### Forces

| 驱动力 | 原因 |
|---|---|
| 真实分布式行为 | 评分要求多节点调度、逻辑分发和节点间 Shuffle，单进程模拟不足以证明这些能力。 |
| 教学可解释性 | 需要能直接讲清 DAG、调度、分区、网络传输和窗口触发，不能把核心能力完全交给现成流框架。 |
| 可复现性 | Docker Compose 应一次启动 Kafka 和多个计算节点，减少环境差异。 |
| 可演进性 | 基础阶段的数据与控制契约必须允许后续加入 Watermark、Barrier、状态快照和事务 Sink。 |
| 实现成本 | 当前先拿到基础功能，不能提前实现完整生产级调度器和容错系统。 |

#### Decision

采用“模块化控制面 + 独立 Worker 容器 + 自研异步数据面”的结构：

1. CLI 负责校验本地输入、构建 ZIP 作业包、提交、查询和取消作业。
2. JobManager 负责作业存储、DAG 校验、任务图展开、槽位调度、部署编排和状态聚合。
3. Worker 负责下载并安全解压作业包、加载 UDF、启动 TaskRuntime、暴露数据端口和上报状态。
4. TaskRuntime 使用 TCP 长连接和有界队列传递批量记录，通过 `await writer.drain()` 形成基础背压。
5. Kafka 和文件系统作为外部输入、输出依赖；第一阶段状态仅存内存。
6. 公共记录信封从第一阶段起保留 `event_time`、`message_type`、`checkpoint_id` 和 `change_kind` 字段，未启用字段必须为 `null` 或默认值。

#### Alternatives

| 方案 | 结论 | 原因 |
|---|---|---|
| 单进程内模拟多个节点 | Rejected | 无法充分证明跨节点网络 Shuffle、逻辑分发和节点故障边界。 |
| 全部使用 HTTP 传递数据 | Rejected | 高频流式记录的连接复用、背压和持续传输表达较弱。 |
| 全部使用 gRPC | Rejected for phase 1 | 强契约有价值，但 `.proto`、生成代码和流生命周期增加当前阶段成本。 |
| 直接使用现成流计算框架 | Rejected | 核心调度、Shuffle 和窗口能力无法体现为项目自主实现。 |
| 共享目录直接加载 UDF | Rejected | 不能充分展示“提交后分发计算逻辑代码”。 |

#### Consequences

| Positive | Negative |
|---|---|
| 控制面易于用 HTTP 工具调试。 | 需要维护 HTTP 和 TCP 两套协议。 |
| 数据面可以清晰展示分帧、路由、背压和跨节点连接。 | 自研协议必须处理半包、粘包、断连和资源清理。 |
| ZIP 作业包满足逻辑分发要求。 | 任意 Python UDF 是可信代码执行边界，不能面向不可信用户。 |
| 统一信封和任务接口可扩展至后续一致性阶段。 | 提前保留字段会带来少量第一阶段复杂度。 |
| Docker Compose 容易复现多节点拓扑。 | 当前环境未安装 Docker，完整验收暂时依赖外部前置条件。 |

#### Reversibility

- HTTP 控制面与 TCP 序列化属于可逆决策；若协议维护成本明显高于收益，可在保持内部接口不变时替换为 gRPC。
- YAML、记录信封和状态接口属于中等成本决策；只有在样例作业无法表达或后续 Barrier 无法兼容时才重新设计。
- Checkpoint 与事务 Sink 语义属于高成本决策；第三阶段只有在 Barrier 原型和故障测试通过后才正式冻结。

## 系统地图

### 控制流

```text
User
  -> CLI: package/submit/status/cancel
  -> JobManager HTTP API
       -> Artifact Store
       -> DAG Validator
       -> Scheduler
       -> Worker HTTP API: deploy/stop
       <- Worker registration/heartbeat/task status
```

### 数据流

```text
Kafka
  -> SourceTask
  -> MapTask
  -> KeyByTask
  => HASH Shuffle over TCP
  -> ReduceWindowTask
  -> SinkTask
  -> Shared output volume
```

`->` 表示普通流转，`=>` 表示由 KeyBy 产生的确定性哈希分区。DAG 分支会为每条下游边独立路由；多个上游进入同一普通算子时语义为合流，不提供 Join。

### 依赖方向

```text
common <- api/config <- control
common <- api/config <- runtime <- operators
common <- artifact
cli -> api/config + control client + artifact
```

`common` 不得反向依赖 `control`、`runtime`、`operators` 或 `cli`。JobManager 不导入具体 WordCount UDF。

### 信任边界

- CLI 到 JobManager、JobManager 到 Worker 位于课程实验的可信 Docker 网络，第一阶段不实现认证和 TLS。
- ZIP 作业包是可信代码，但解压逻辑仍必须拒绝绝对路径、`..` 路径穿越、符号链接和超限文件。
- Kafka 消息和 UDF 输出视为不可信数据，必须经过格式、类型和可序列化校验；坏记录记录错误并按配置失败作业或跳过。
- 输出卷和后续 Checkpoint 卷被视为可靠本地持久存储；第三阶段依赖同一文件系统内原子重命名。

## 有界上下文

| 上下文 | 责任与本地检查 | 模型/语言 | 上游 | 下游 | 关系与转换面 |
|---|---|---|---|---|---|
| 作业定义 | `pystream.api`；YAML schema tests | JobDefinition、OperatorSpec、EdgeSpec、DataStream、StreamGraph | 用户配置 | JobManager、Runtime | 对用户为 customer/supplier；将 YAML 转为规范化逻辑图 |
| 作业制品 | `pystream.artifact`；ZIP 安全测试 | JobBundle、ArtifactManifest | CLI | JobManager、Worker | 与控制面 partnership；将本地目录转为带摘要的不可变制品 |
| 控制面 | `pystream.control`；状态机与调度测试 | Job、ExecutionGraph、TaskInstance、WorkerSlot | CLI、Worker 注册 | Worker、Artifact Store | 对 Runtime 为 customer/supplier；逻辑图转换为物理执行图 |
| 运行时 | `pystream.runtime`；协议、连接和背压测试 | RecordEnvelope、Frame、Channel、TaskRuntime | 控制面、上游 Task | 算子、下游 Task | 对控制面 conformist；把部署描述转换为进程内任务与网络通道 |
| 算子 | `pystream.operators`；算子契约测试 | Source、Map、KeyBy、Reduce、Window、Sink | Runtime、UDF | Runtime、外部系统 | 与 Runtime 使用 shared kernel 中的记录信封；适配 Kafka/文件模型 |
| 可观测与验证 | `pystream.observability`、`tests/`、`scripts/` | 日志事件、指标、测试报告 | 所有上下文 | 用户、文档 | 对各上下文为 conformist；不改变业务数据 |

## 交互方式决策

| 交互 | 使用位置 | 选择原因 | 不适用位置 |
|---|---|---|---|
| 同步 HTTP 请求/响应 | 提交、查询、取消、部署命令、制品下载 | 低频、易调试、明确超时和响应 | 高频逐条数据 |
| 异步 TCP 流 | Task 间数据批次、未来 Watermark/Barrier | 长连接、低开销、自然背压 | 用户管理操作 |
| Kafka 消费流 | 外部无界输入 | 满足题目输入和分区消费要求 | 内部控制命令 |
| 文件追加/阶段提交 | 第一阶段普通 Sink；第三阶段事务 Sink | 与题目输出和本地持久文件假设一致 | 低延迟查询 |
| 批处理 | 仅用于 TCP 帧内微批和性能测试数据生产 | 降低序列化与系统调用成本，不改变逐记录语义 | 作业整体执行模式 |

## 运行时依赖采用标准

| 依赖 | 支持与调试 | 变更方式 | 失败/降级路径 | 退出路径 |
|---|---|---|---|---|
| Kafka | Compose 日志、健康检查、消费组和 topic 检查脚本 | 固定镜像版本并集中配置 | 第一阶段标记 Source/Job 失败；第二阶段恢复后 seek | Source 接口允许替换外部输入实现 |
| Docker Compose | `docker compose ps/logs` 与健康检查 | Compose 文件和 `.env` | 单元测试可脱离 Docker；完整集成测试不能降级 | Worker/JobManager 均可作为普通 Python 进程运行，但本轮不提供正式本机集群脚本 |
| HTTP 服务库 | 请求日志、健康端点、pytest client | 封装在 control transport | 控制请求失败即部署回滚或作业失败 | 保持 service interface 后可替换传输 |
| asyncio TCP | 协议日志、连接指标、loopback 集成测试 | 版本化 Frame 协议 | 第一阶段断连使作业失败；后续重连并恢复 | 保持 Channel 接口后可替换为 gRPC |
| 共享文件卷 | 文件树、摘要和原子操作测试 | 路径集中配置 | 第一阶段 Sink 失败使作业失败 | Sink 接口允许新增其他实现 |

## 关键数据与协议契约

### YAML 作业定义

作业包根目录必须包含 `job.yaml`，UDF 模块位于同一 ZIP 内。基础结构如下：

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
    parallelism: 2
    udf: wordcount_udfs:add_counts
    window:
      type: tumbling
      time_characteristic: processing
      size: 300s
  - id: output
    type: sink
    parallelism: 1
    config:
      connector: file
      format: csv
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

规则：

- `api_version` 第一阶段只接受 `pystream/v1`。
- 算子 ID 在作业内唯一；图必须无环；Source 入度为 0；Sink 出度为 0。
- 所有算子并发度为正整数，物理任务总数不能超过注册 Worker 的可用 slots。
- `reduce` 必须消费 keyed stream；`key_by` 的下游边强制使用 HASH 分区。
- 普通边在上下游并发度相同时默认 FORWARD，否则默认 REBALANCE。
- 分支合法；多上游仅代表合流；不提供 Join 语义。
- 未知字段默认拒绝，避免配置拼写错误静默生效。

### ZIP 作业包

- CLI 从用户指定目录构建 ZIP，并生成文件清单、SHA-256 摘要、总大小和入口配置。
- JobManager 以 `job_id/artifact_sha256.zip` 形式不可变保存。
- Worker 下载后再次验证摘要，并解压到独立 `job_id` 目录。
- UDF 引用格式固定为 `module:function`；模块只能从当前作业目录加载，禁止从其他作业目录复用模块缓存。
- 第一阶段不做依赖在线安装；作业 UDF 只能使用 Python 标准库和 PyStream 镜像已经声明的依赖。

### 记录信封

| 字段 | 第一阶段语义 |
|---|---|
| `message_type` | `DATA`；后续增加 `WATERMARK`、`BARRIER`、`CHECKPOINT_COMPLETE` |
| `record_id` | Kafka `topic:partition:offset`，在输入侧唯一 |
| `payload` | JSON 可序列化业务对象 |
| `key` | KeyBy 前为 `null`，KeyBy 后为 JSON 标量或结构 |
| `processing_time` | Source 接收记录时生成的 UTC 时间 |
| `event_time` | 第一阶段为 `null`，第二阶段启用 |
| `change_kind` | 第一阶段固定 `INSERT`，第二阶段增加更新前后与删除 |
| `checkpoint_id` | 第一阶段为 `null`，后续由 Barrier 设置 |
| `headers` | 追踪和来源元数据，不参与业务聚合 |

### 数据面帧

- 每个 TCP 消息为 `4-byte big-endian length + UTF-8 JSON body`。
- 协议至少包含 `HELLO`、`DATA_BATCH`、`END_OF_STREAM`、`ERROR`、`HEARTBEAT` 帧。
- `HELLO` 校验 job、上游 task、下游 task 和协议版本。
- `DATA_BATCH` 包含有上限的记录数组；长度超限、JSON 非法或身份不匹配必须关闭连接并报告任务失败。
- 每个下游通道使用有界队列；写端等待 `drain()`，不得无限缓存。
- 同一上游到同一下游通道保持发送顺序；第一阶段不承诺跨通道全局顺序。

### 控制面与作业状态

最低接口能力：

- CLI：`package`、`submit`、`status`、`cancel`、`validate`。
- JobManager：健康检查、提交、状态查询、取消、Worker 注册/心跳、制品下载。
- Worker：健康检查、部署任务、停止任务、任务状态查询。

作业状态：

```text
SUBMITTED -> VALIDATING -> DEPLOYING -> RUNNING
                   \-> REJECTED
DEPLOYING/RUNNING -> FAILING -> FAILED
RUNNING -> CANCELLING -> CANCELLED
```

第一阶段 Worker 心跳丢失或数据连接断开会使相关作业进入 `FAILED`，不会自动恢复。部署必须先启动下游监听端，再启动上游；任一部署失败时停止已经启动的任务并释放 slots。

## ADDED Requirements

### Requirement: Python 工程与版本管理

系统 SHALL 在当前目录建立 Python 3.11 `src` 布局工程，使用 `pyproject.toml` 声明运行、开发和测试依赖，并初始化本地 Git 仓库。

#### Scenario: 保留题目文件

- **WHEN** 初始化项目结构和 Git 仓库
- **THEN** 现有题目 Markdown/PDF 保持原路径和内容，不被移动、覆盖或删除

#### Scenario: 可安装工程

- **WHEN** 在 Python 3.11 环境执行项目安装
- **THEN** `pystream` 包和 CLI 入口可导入/运行，开发依赖可支持测试、覆盖率和静态检查

### Requirement: YAML DAG API 与 DataStream 抽象

系统 SHALL 将 YAML 作业定义解析为 `JobDefinition` 和无环 `StreamGraph`，并以 `DataStream` 表示算子输出及其分区属性。

#### Scenario: 合法 DAG

- **WHEN** 用户提交包含 Source、Map、KeyBy、Reduce、Sink 和显式 edges 的合法 YAML
- **THEN** 系统生成拓扑有序的逻辑图，并保留每个算子的并发度、UDF、连接器和窗口配置

#### Scenario: 非法 DAG

- **WHEN** YAML 存在环、重复 ID、未知算子、非法并发度、缺失 UDF、Reduce 未消费 keyed stream 或未知字段
- **THEN** 提交在调度前被拒绝，并返回包含配置路径的可操作错误

### Requirement: ZIP 作业包与 UDF 分发

系统 SHALL 将 YAML 与 Python UDF 构建为带 SHA-256 清单的 ZIP 制品，并由 JobManager 保存、Worker 下载和隔离加载。

#### Scenario: 自定义 Map/KeyBy/Reduce

- **WHEN** YAML 引用 ZIP 内的 `module:function`
- **THEN** Worker 在当前 job 的模块命名空间中加载可调用对象，并按算子契约调用

#### Scenario: 非法制品

- **WHEN** ZIP 摘要不匹配、路径穿越、绝对路径、符号链接、文件超限或 UDF 不可调用
- **THEN** Worker 拒绝部署且 JobManager 将作业标记为失败，不在目标目录外写文件

### Requirement: 分布式控制面与 CLI

系统 SHALL 提供 JobManager、Worker 和 CLI，使用户能够提交、查询和取消持续运行的流作业。

#### Scenario: Worker 注册与调度

- **WHEN** 3 个 Worker 启动并各自注册 slots 和数据地址
- **THEN** JobManager 能查看可用资源，并将物理 TaskInstances 分配到至少两个 Worker

#### Scenario: 提交作业

- **WHEN** 用户通过 CLI 提交合法 ZIP 作业包
- **THEN** CLI 返回 job_id，作业依次进入 VALIDATING、DEPLOYING 和 RUNNING，状态接口展示任务位置

#### Scenario: 取消作业

- **WHEN** 用户取消 RUNNING 作业
- **THEN** JobManager 停止全部任务、关闭连接、释放 slots，并将作业标记为 CANCELLED

### Requirement: 算子并发度与物理执行图

系统 SHALL 将每个逻辑算子按 `parallelism` 展开为独立 TaskInstance，并使用可用 slots 进行确定性、可解释的均衡调度。

#### Scenario: 上下游并发度不同

- **WHEN** 上游并发度为 2、下游并发度为 3
- **THEN** 执行图为所有需要的通道生成端点和路由规则，记录不会因并发度不一致而丢失

#### Scenario: 资源不足

- **WHEN** TaskInstance 总数超过可用 slots
- **THEN** 部署在启动任务前失败并报告所需/可用 slots，不进行部分运行

### Requirement: 跨节点数据通道与 Shuffle

系统 SHALL 通过版本化 TCP 协议在 Worker 之间发送批量记录，并支持 FORWARD、REBALANCE 和 HASH 三种路由。

#### Scenario: KeyBy 哈希分区

- **WHEN** KeyBy 为记录生成 key
- **THEN** 系统基于 key 的规范 JSON 表示计算稳定 SHA-256，并按下游并发度取模；相同 key 始终到达同一下游 subtask

#### Scenario: 普通重平衡

- **WHEN** 非 keyed 边的上下游并发度不同
- **THEN** 上游按轮询将记录分发给下游 subtasks，测试输入的每条记录恰好发送一次

#### Scenario: 背压

- **WHEN** 下游处理速度低于上游
- **THEN** 有界队列和 TCP drain 使上游等待，内存不会随输入无限增长

### Requirement: Kafka Source

系统 SHALL 使用 Kafka consumer group 从配置 topic 持续读取 JSON 消息，并为每条记录生成可追踪的 RecordEnvelope。

#### Scenario: 并行消费

- **WHEN** Source 并发度为 2 且 topic 至少有 2 个分区
- **THEN** 两个 SourceTask 使用同一作业消费组分配分区，记录 ID 包含 topic、partition 和 offset

#### Scenario: 非法 JSON

- **WHEN** Kafka 消息不是合法 JSON 或不符合样例输入契约
- **THEN** Source 按 `bad_record_policy` 执行 `fail` 或 `skip`，并输出包含来源 offset 的结构化日志

### Requirement: Map 算子

系统 SHALL 对每条输入记录调用用户 Map UDF，并支持一对一输出以及显式丢弃记录。

#### Scenario: 大小写归一化

- **WHEN** WordCount Map 收到 `{"word":"APPLE","count":1}`
- **THEN** 输出 payload 的 word 为 `apple`，count 保持为整数 1

### Requirement: KeyBy 算子

系统 SHALL 调用用户 KeySelector UDF 生成 key，并将 DataStream 标记为 keyed。

#### Scenario: 同 key 汇聚

- **WHEN** `APPLE` 和 `apple` 已由 Map 归一化为 `apple`
- **THEN** KeyBy 为二者生成相同 key，且二者被 HASH 到相同 ReduceTask

### Requirement: Reduce 与处理时间滚动窗口

系统 SHALL 在 keyed stream 上调用用户 Reduce UDF，并支持基于处理时间、epoch 对齐、窗口结束触发的可配置滚动窗口。

“滚动窗口”在本规格中等同于互不重叠的 tumbling window。默认大小为 300 秒；测试和演示允许使用秒级配置。窗口只统计本窗口内记录，不跨窗口累计。

#### Scenario: 窗口聚合

- **WHEN** 同一 5 分钟窗口内收到 `APPLE,1` 和 `apple,1`
- **THEN** 归一化 key `apple` 在窗口结束时输出 count 2

#### Scenario: 窗口时间

- **WHEN** 记录的处理时间落入 `[12:00:00, 12:05:00)`
- **THEN** 输出窗口触发时间为 UTC `YYYY/MM/DDTHH:MM:SS` 格式的 `12:05:00`

#### Scenario: 空闲窗口

- **WHEN** 某个窗口没有任何记录
- **THEN** 不输出空结果

### Requirement: 文件 Sink

系统 SHALL 将上游结果追加写入输出卷中按 job、operator、subtask 隔离的文件，并在写入后刷新可见数据。

#### Scenario: WordCount CSV

- **WHEN** Sink 收到窗口结果
- **THEN** 无表头输出 `window_end,word,count`，文件路径可从作业状态或文档中确定

#### Scenario: 并行 Sink

- **WHEN** Sink 并发度大于 1
- **THEN** 每个 subtask 只写自己的分片文件，避免并发写同一文件

### Requirement: 流式 WordCount 示例

系统 SHALL 提供可直接打包、提交和验证的 WordCount 示例作业。

#### Scenario: 题目样例

- **WHEN** Kafka 依次收到 `APPLE,1`、`pie,1`、`apple,1` 的等价 JSON，且记录落入对应测试窗口
- **THEN** 输出包含正确的 UTC 窗口结束时间、`apple`/`pie` 小写 key 和窗口内计数

#### Scenario: 分布式演示

- **WHEN** 示例按默认 Compose 集群运行
- **THEN** Source、Map、KeyBy、Reduce、Sink 的 TaskInstances 分布在至少两个 Worker，日志和状态接口能证明跨 Worker Shuffle

### Requirement: 可观测性、文档和验证

系统 SHALL 提供人可读控制台日志、稳定结构化字段、健康检查、模块文档、部署验证、自动化测试和性能测量脚本。

#### Scenario: 模块解释

- **WHEN** 阅读任一顶层模块
- **THEN** 模块 docstring 和 `docs/modules.md` 说明其职责、入口接口、输入输出、依赖方向和失败行为

#### Scenario: 自动化质量门

- **WHEN** 运行项目检查
- **THEN** 格式/静态检查、单元测试和非 Docker 集成测试通过，核心包行覆盖率不低于 80%

#### Scenario: Docker 端到端验证

- **WHEN** Docker 前置条件满足并运行验证流程
- **THEN** Compose 服务健康，WordCount 结果正确，跨节点 Shuffle 有证据，作业可取消且资源释放

#### Scenario: 性能测试

- **WHEN** 用户运行性能脚本并提供记录数、分区数、并发度和窗口大小
- **THEN** 脚本记录输入规模、总耗时、吞吐、端到端延迟统计和测试环境，不使用硬编码通过门槛

## 第二阶段预留需求

### Requirement: 事件时间与有限乱序

系统 SHALL 在后续 change-id 中从可配置字段提取事件时间，使用 `max_observed_event_time - max_out_of_orderness` 生成 Watermark，并在 Watermark 到达窗口结束时触发事件时间滚动窗口。

#### Scenario: 有限乱序

- **WHEN** 乱序记录仍晚于当前 Watermark
- **THEN** 记录进入其事件时间所属窗口并参与最终结果

#### Scenario: 迟到记录

- **WHEN** 记录事件时间早于或等于已推进的 Watermark
- **THEN** 默认丢弃并记录 late-record 指标；可配置 side output 作为扩展

### Requirement: Retract Changelog

系统 SHALL 在后续 change-id 中支持 `INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER`、`DELETE` 变更类型，使下游聚合能撤回旧值并应用新值。

#### Scenario: 二级聚合

- **WHEN** 某个 word 的 count 从 1 更新为 2
- **THEN** 下游“不同 count 对应多少个 word”先撤回 count=1 的旧贡献，再加入 count=2 的新贡献

### Requirement: 自动故障恢复与 At-least-once

系统 SHALL 在后续 change-id 中检测 Worker 失联、重新拉起或接纳替代 Worker、重建数据连接、从最近完成快照恢复状态，并从快照 offset 重新消费 Kafka。

#### Scenario: Worker 崩溃

- **WHEN** 运行中的 Worker 被终止
- **THEN** 计算任务自动重新部署，连接重建，状态恢复，输入数据不丢失但允许重放导致重复输出

## 第三阶段预留需求

### Requirement: Barrier Checkpoint 与系统内 Exactly-once

系统 SHALL 在后续 change-id 中实现对齐的 Checkpoint Barrier、算子状态快照、Kafka offset 快照和一致恢复。

#### Scenario: Checkpoint 后故障

- **WHEN** 数据在最近完成 Checkpoint 后被处理但尚未包含在新完成 Checkpoint 中，随后发生故障
- **THEN** 状态回滚到最近完成 Checkpoint，Source 从对应 offset 重放，恢复后的逻辑状态等价于每条输入只生效一次

### Requirement: 事务文件 Sink 与端到端 Exactly-once

系统 SHALL 在后续 change-id 中将每个 Checkpoint 的输出写入独立临时文件，在 Checkpoint 完成后原子提交，并在恢复时删除未提交文件。

#### Scenario: 提交前故障

- **WHEN** Sink 已预写当前 Checkpoint 输出但全局 Checkpoint 尚未完成
- **THEN** 恢复时未提交输出不可见且被清理，重放不会产生可见重复

#### Scenario: 端到端证明

- **WHEN** 在处理固定输入时多次注入 Worker 故障
- **THEN** 最终已提交文件与无故障基准逐记录一致，无丢失、无重复

## Fitness Functions

| 属性 | 指标 | 阈值/规则 | 数据源 | 频率 | 失败响应 | 本地检查路径 |
|---|---|---|---|---|---|---|
| 依赖方向 | 非法跨层 import 数 | 0 | 架构测试/静态分析 | 每次测试 | 阻止验收并修正 import | `tests/architecture/` |
| YAML 公共契约 | v1 样例解析与错误快照 | 所有兼容样例通过；未知字段拒绝 | 配置契约测试 | 每次测试 | 阻止验收 | `tests/contract/` |
| Shuffle 正确性 | 丢失、重复、错分区记录数 | 确定性测试均为 0 | 协议与集成测试 | 每次测试 | 阻止验收 | `tests/integration/` |
| WordCount 正确性 | 实际行与期望行差异 | 0；比较时不依赖跨 key 行顺序 | E2E 输出文件 | 每次发布候选 | 阻止验收 | `scripts/verify_wordcount` |
| 基础背压 | 队列最大深度与内存增长 | 队列不超过配置上限；持续慢消费不无限增长 | 慢 Sink 测试 | 每次测试 | 阻止验收 | `tests/integration/` |
| 模块隔离 | 单 Worker 退出对控制面影响 | JobManager 存活；第一阶段作业进入 FAILED | 故障注入测试 | 每次发布候选 | 阻止验收 | `scripts/fail_worker` |
| 性能基线 | 吞吐、p50/p95 延迟、记录总数 | 成功处理指定输入且无丢失；不设硬吞吐线 | 性能脚本和报告 | 每次里程碑 | 记录回归并分析，不因硬件数值单独失败 | `scripts/benchmark` |
| 测试覆盖 | 核心包行覆盖率 | `>= 80%` | coverage 报告 | 每次测试 | 阻止验收 | `pytest` 配置 |
| 文档完整性 | 顶层模块缺失说明数 | 0 | 文档清单测试/人工检查 | 每次里程碑 | 补充文档后再验收 | `docs/modules.md` |

## Risk Register

| 风险 | 可能性 | 影响 | 缓解措施 | 责任/记录 |
|---|---|---|---|---|
| 当前无 Docker，无法立即完成真实多容器验证 | 高 | 高 | 先完成可脱离 Docker 的测试；把 Compose E2E 保留为发布阻断项，用户安装后执行 | 用户；`docs/deployment.md` |
| 自研 TCP 协议出现半包、断连或无限缓存 | 中 | 高 | 长度前缀、帧上限、有界队列、超时、loopback 集成测试 | Runtime；`tests/integration/` |
| UDF ZIP 路径穿越或模块串扰 | 中 | 高 | 安全解压、摘要校验、job 目录隔离、清理 `sys.modules` 命名空间 | Artifact；安全测试 |
| 定时窗口测试不稳定 | 中 | 中 | 注入 Clock、使用秒级虚拟/可控时间、避免真实等待 5 分钟 | Operators；窗口测试 |
| Kafka 分区数小于 Source 并发度导致空闲任务 | 中 | 低 | 提交时警告并在示例部署中创建足够分区 | Source；部署文档 |
| 基础接口无法承载后续 Barrier/Watermark | 中 | 高 | 从 v1 起保留控制消息、状态后端和 Sink lifecycle 接口；第二阶段前做兼容性评审 | Architecture；本规格 |
| 任意 UDF 阻塞事件循环 | 中 | 中 | 文档要求短时同步 UDF；监测处理时长；后续可引入 executor | Operators；模块文档 |
| 总范围过大导致基础阶段无法闭环 | 中 | 高 | tasks/checklist 只包含第一阶段；中高级必须另开 change-id | 本规格与 `tasks.md` |

## 决策表

| 决策点 | 默认 | 已拒绝替代 | 例外触发条件 |
|---|---|---|---|
| 部署拓扑 | Docker 多容器 | 仅本机模拟 | Docker 长期不可用且用户明确要求本机回退 |
| 作业入口 | YAML + Python UDF | 纯链式 API、双 API | 后续课程明确强制链式编程接口 |
| 控制/数据通信 | HTTP + asyncio TCP | 全 HTTP、全 gRPC | TCP 维护成本或协议缺陷无法通过测试 |
| 作业分发 | ZIP 上传/下载 | 镜像预置、共享目录 | 制品大小或依赖隔离需求要求镜像化 |
| 处理时间窗口 | 可配置、默认 300 秒、epoch 对齐 | 固定窗口、本地时区 | 题目验收明确要求其他边界 |
| 时间格式 | UTC `YYYY/MM/DDTHH:MM:SS` | 机器本地时区 | 用户明确要求带时区 RFC3339 |
| 性能验收 | 记录结果，不设硬门槛 | 固定 1000 records/s | 硬件条件和评分门槛被明确提供 |
| Exactly-once 路线 | Barrier + 事务文件 | 仅系统内 Exactly-once | 外部 Sink 类型改变或文件系统无原子重命名 |

## Follow-up Checks

1. 用户安装 Docker Desktop 并启用 Linux 容器后，执行 Compose、Kafka、多 Worker 和跨节点 WordCount 的完整端到端验收。
2. 第一阶段验收后，为第二阶段单独建立 change-id，并在实现前验证 RecordEnvelope、Channel、StateBackend 和 Sink lifecycle 对 Watermark/Barrier/Changelog 的兼容性。
