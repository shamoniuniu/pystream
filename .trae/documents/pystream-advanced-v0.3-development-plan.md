# PyStream 高级功能 v0.3 开发计划

## Summary

### 目标

在中级完成提交 `64b875bbdd45f9d4cd4718422adacbbfcc1e4001` 上，以独立
`feature/advanced-v0.3` 分支分阶段完成：

1. 持续流对齐 Checkpoint Barrier。
2. Kafka offset、算子状态和事务 Sink 的系统内 Exactly-once。
3. 事务文件 Sink 与端到端 Exactly-once。
4. 4 节点分布式对象存储，持久化 artifact、Checkpoint、提交决定和
   JobManager 元数据。
5. 双 JobManager active/passive、带 fencing epoch 的租约和 30 秒内自动接管。
6. 内部 mTLS、外部 HTTPS + Bearer Token、Docker Secret/文件凭据。
7. Prometheus 指标、告警规则、恢复与 Checkpoint SLO。
8. Worker、active JobManager、单对象存储节点及三个事务提交窗口的 Docker
   故障实证。

### 成功标准

- 缺少 `execution` 的 v0.1 基础作业行为不变。
- 配置 `execution` 且未声明 `delivery_guarantee` 的作业默认升级为
  `exactly_once`。
- 显式 `delivery_guarantee: at_least_once` 继续走 v0.2 停流 DRAIN 和追加
  File Sink，保证中级回归可复现。
- Exactly-once Source 只在注入 Barrier 的短边界内暂停，不等待全图
  Checkpoint 完成；Checkpoint 期间正常数据流继续。
- 多输入 Task 在每条输入通道内保持 DATA/BARRIER 顺序，收到 Barrier 后停止读取
  该通道，继续处理未对齐通道；全部输入对齐后才快照、转发 Barrier 并解除通道门控。
- 全局 Checkpoint 决定前的失败不会产生可见事务输出；决定后的失败只能重试
  finalize，不能反向 abort 已决定 Checkpoint。
- 固定输入在无故障、Worker 故障、Sink 预提交后故障、Checkpoint 决定后故障、
  active JobManager 故障和单对象存储节点故障下，最终已提交输出逐记录一致，
  无丢失、无重复。
- active JobManager 退出后 30 秒内由 standby 接管；Worker 故障后 60 秒内作业
  恢复 RUNNING。
- 任何时刻最多一个可写 JobManager；旧 leader epoch 的部署、状态写入和
  Checkpoint 请求被拒绝。
- 单个对象存储节点退出时，artifact、元数据、最新完整 Checkpoint 和提交决定仍可读，
  且存储集群保持可写。
- 未携带有效外部 Token、无有效内部客户端证书、证书过期或服务身份不匹配的请求被拒绝。
- Python 3.11 下 Ruff、格式、全量测试通过，无 skip，分支覆盖率 `>= 80%`。
- `core` 与 `ha` 两套 Docker E2E 均通过，最终容器、网络、卷和临时 Secret 清零。
- 每个里程碑有开发日志、验收证据、独立 commit、Git note、远端备份和
  `git revert <sha>` 回退方式。

### 受众

- 课程评分与答辩人员：关注 Barrier、Exactly-once 和故障证明。
- 后续维护者：关注事务决定恢复、HA fencing、证书与对象存储运维边界。
- 本地开发者：需要可在 Docker Desktop/Linux containers 中重复运行 core/ha 验收。

### 范围外

- Active-active JobManager。
- Kafka broker 故障容忍；Kafka 仍视为外部可靠输入系统，本地 demo 使用单 broker。
- File Sink 所在 Docker 主机或输出卷丢失；文件系统必须支持同文件系统原子 rename。
- 跨主机、跨可用区或跨地域容灾；4 个对象存储节点仍运行于同一 Docker 主机。
- 动态扩缩容、Savepoint、状态重分片、多租户、SQL、Join、CEP。
- 不可信 UDF 沙箱或 UDF 子进程隔离；本轮继续使用可信 UDF 模型。
- 对外发布镜像、合并 `main` 或创建 v0.3.0 tag，除非用户在高级验收后另行批准。

## Current State Analysis

### Git 与制品状态

- 当前分支：`feature/intermediate-v0.2`。
- 当前完成提交与远端：`64b875bbdd45f9d4cd4718422adacbbfcc1e4001`。
- `main`、`origin/main`、`v0.1.0^{}` 均为
  `24384c14a7c7f95db350016e623421d5d37af674`。
- 包版本为 `0.2.0`，中级 Docker E2E、SIGKILL 恢复和 At-least-once 已通过。
- 当前 `git status` 将 3 个报告文件标为修改，但 `git diff` 无文本差异，仅出现
  LF/CRLF 提示。执行前必须重新核对 blob/hash；不得通过 checkout/reset 丢弃潜在
  用户内容。确认无语义 diff 后才允许建立高级基线。

### 已有可复用扩展点

- `RecordEnvelope` 已有 `message_type` 与 `checkpoint_id`。
- `MessageType` 已预留 `BARRIER`、`CHECKPOINT_COMPLETE`。
- `CONTROL` frame 与 DATA 分离，同一 TCP 通道严格保序。
- `BoundedDataChannel` 有有界队列和 `writer.drain()` 背压。
- `TaskRuntime` 已按物理输入 identity 合并数据，并维护 Watermark/Checkpoint
  输入状态。
- `BaseOperator` 已有 `snapshot_state/restore_state`。
- `FileSinkOperator` 已预留 begin/pre-commit/commit/abort 事务方法。
- Checkpoint Store 已具备规范 JSON、SHA、大小上限、不可变 Task snapshot、
  manifest-last 和损坏回退。
- Source 已能 pause、snapshot 精确 next offset、显式 seek 和按 Checkpoint commit。
- attempt fencing、Worker incarnation 和整作业恢复已实证。

### 需要改造的关键缺口

- 当前 Source 在整个 Checkpoint 周期暂停，并发送 `CHECKPOINT_DRAIN`；不是持续流
  Barrier。
- `TaskRuntime.accept_control()` 只入共享队列后立即返回，无法在单通道 Barrier 后
  自然阻塞后续帧。
- Source 恢复消费后会清除当前 checkpoint partition 集合，无法在持续消费期间延迟
  提交 Barrier 时刻的精确 offset。
- File Sink 直接 append/flush，快照仍按“无状态算子”处理。
- Checkpoint 只有“manifest 完成”概念，没有 `DECIDED -> FINALIZED` 两阶段。
- 当前协调器在 manifest 写入后逐 Task complete；若进程在两者之间退出，没有机制
  恢复 Sink commit 决定。
- artifact、JobRun、Checkpoint Store 均依赖单实例内存或单共享卷。
- JobManager 没有持久元数据、leader lease、coordinator epoch 或 standby 模式。
- Worker 只在启动时注册，尚不能在 active JobManager 切换后自动重新注册。
- HTTP/TCP/Kafka demo 当前无 TLS，管理 API 无认证，凭据没有 Secret 文件契约。
- 指标只有状态字段与 JSON 日志，没有 Prometheus 导出和告警规则。

## Architecture Decision Record

### Context

第三阶段要求在 Worker 故障和 Checkpoint 重放下实现系统内及端到端
Exactly-once。用户进一步确认本轮分阶段实现完整 v0.3 平台能力，包括冗余对象
存储、双 JobManager 主备、全故障 E2E、认证/TLS/Secret 和 Prometheus。

### Decision Drivers

| Driver | Priority | Evidence | Tradeoff |
|---|---:|---|---|
| 端到端无丢失无重复 | 最高 | 原题第三阶段 requirement | 需要两阶段提交和决定恢复 |
| 数据流持续 | 高 | v0.2 Checkpoint 暂停直接增加延迟 | Barrier 对齐会把压力反馈给较快输入 |
| v0.2 可回归 | 高 | 已有 At-least-once 证据 | 需要双模式而非删除 DRAIN 路径 |
| 控制面可接管 | 高 | 当前单 JobManager 是故障域 | 引入持久元数据、租约和 fencing |
| 存储单节点容忍 | 高 | 当前共享卷是单故障域 | 引入分布式对象存储和新运行依赖 |
| 本地可验收 | 高 | Docker Desktop 曾出现 CLI/WSL 波动 | core/ha 拆分并保留资源 ledger |
| 安全边界明确 | 中 | 当前无认证/TLS | 增加证书和 Secret 运维成本 |

### Options Considered

| Option | Benefits | Costs | Risks | Decision |
|---|---|---|---|---|
| 保持停流 DRAIN + append Sink | 最简单 | 无法 Exactly-once | 继续重复、暂停 | Rejected |
| Barrier + 仅系统内 Exactly-once | 状态正确 | 输出仍可重复 | 不满足端到端要求 | Rejected |
| Barrier + 事务文件 + 单 JM/单卷 | 满足课程最低高级项 | 控制/存储仍单点 | 用户要求的平台化未完成 | Rejected |
| Barrier + 事务文件 + 分布式对象存储 + 主备 JM | 同时满足高级语义和平台故障目标 | 实现与运维复杂度最高 | 租约、finalize、TLS 必须严格验证 | Selected |
| 双活 JobManager | 无主备切换 | 需要多写一致性 | 状态机与提交决定冲突风险高 | Rejected |
| 自研三副本存储 | 教学代码自主 | 需实现复制/修复/quorum | 数据一致性风险超出项目目标 | Rejected |

### Decision

- 包版本升级到 `0.3.0`。
- 保持 `api_version: pystream/v1`，新增字段为向前可解析扩展。
- `execution` 存在时，`delivery_guarantee` 默认 `exactly_once`。
- `delivery_guarantee: at_least_once` 显式保留 v0.2 DRAIN/append 路径。
- Exactly-once 路径使用 aligned Barrier 和事务 File Sink。
- Checkpoint 使用 `DECIDED` 与 `FINALIZED` 两阶段；决定不可逆。
- artifact、Checkpoint、作业元数据和 leader lease 使用 S3 兼容对象存储。
- 本地 HA profile 使用 4 个分布式 MinIO 节点和一个存储负载均衡入口。
- 两个 JobManager 使用对象存储 ETag CAS lease，只允许 active 处理写请求。
- 外部入口经 HAProxy 路由到 active；内部 PyStream HTTP/TCP 使用 mTLS。
- 对外管理 API 使用 HTTPS + Bearer Token。
- core profile 验证 Exactly-once；ha profile 验证主备、4 节点存储、安全和组合故障。

### Status

`Accepted for implementation`。用户已逐项确认范围、分支、兼容、故障域、存储、
主备、profile、输出布局、安全边界、SLO 和故障矩阵。

### Consequences

**Positive**

- Exactly-once 从接口预留升级为可运行、可故障复现的能力。
- Checkpoint 决定、Sink 可见性和 Source offset 形成同一恢复边界。
- active JobManager 和单个对象存储节点退出不再中断已承诺服务。
- Secret、TLS、Token 和指标成为显式契约。

**Negative**

- Docker HA 栈达到 12 个以上容器，启动和故障测试耗时增加。
- Barrier 对齐期间快输入会通过 TCP 门控产生背压。
- `execution` 作业默认语义从 At-least-once 升级为 Exactly-once。
- 引入 S3 SDK、Prometheus client、HAProxy、MinIO、证书生成和 Secret 管理。
- 输出读取方必须以 committed output manifest 为可见性真值，不能直接 glob pending。

### Reversibility

| Decision | Door | Undo Cost | Reconsideration Trigger |
|---|---|---:|---|
| 新高级分支与 0.2 tag | Two-way | 低 | 基线校验失败 |
| delivery 默认 Exactly-once | Two-way | 低 | 0.2 迁移成本不可接受 |
| Barrier 控制协议 | One-way after release | 高 | 对齐无法保持有界或协议死锁 |
| DECIDED/FINALIZED 格式 | One-way after state exists | 高 | 崩溃窗口无法证明 |
| S3 对象键/metadata schema | One-way after release | 高 | 条件写或 HA 行为不满足 |
| MinIO/HAProxy 本地依赖 | Two-way via ports | 中 | 资源/许可/维护成本不可接受 |

## System Map

```text
External CLI
  | HTTPS + Bearer Token
  v
HAProxy :8080
  | active health routing + internal mTLS
  +-----------------------+
  v                       v
JobManager A          JobManager B
active or standby     standby or active
  | leader lease / metadata / artifacts / checkpoint decisions
  v
S3 load balancer -> MinIO 1..4 (distributed erasure set)

Active JobManager
  | mTLS control, coordinator_epoch, attempt_id
  v
Worker 1..3
  | asyncio TLS TCP, DATA/CONTROL ordered frames
  +---- Source -> Map -> KeyBy -> Reduce -> Transactional File Sink
  |                                   |
  |                                   v
  |                         shared file output volume
  |                         pending -> committed + manifest
  v
Kafka SSL Source (external dependency in the fault model)

Prometheus
  | mTLS/token scrape
  +-- JobManagers / Workers / HAProxy / object store
```

### Data And Control Flow

1. CLI 向 HAProxy 提交 artifact；active JobManager 将 artifact 不可变写入对象存储，
   再持久化 Job metadata revision。
2. active 调度任务，部署请求携带 `coordinator_epoch` 和 `attempt_id`。
3. Worker 下载 artifact，建立 mTLS 数据通道并处理 DATA。
4. Checkpoint 时 active arm 全图，再触发 Source 注入 Barrier。
5. Barrier 沿全部物理通道广播；每条连接在 `accept_control(BARRIER)` 后等待对应 gate，
   因而不会把 post-barrier DATA 提前送入 Runtime。
6. Task 收齐全部输入 Barrier 后快照、转发 Barrier、解除 gates。
7. Sink pre-commit 当前事务并立即开始下一个事务。
8. active 收齐快照后写不可变 `decision.json`，该写入是提交决定。
9. active 幂等 finalize Sink、提交 Source offset、发布 output manifest，最后写
   `finalized.json`。
10. 任何 leader 接管都先扫描 DECIDED 未 FINALIZED Checkpoint 并继续 finalize，
    然后恢复作业。

### Trust Boundaries

| Boundary | Authentication | Encryption | Authorization |
|---|---|---|---|
| External CLI -> HAProxy/JM | Bearer Token | Server TLS | 管理 API token |
| HAProxy -> JobManager | client certificate | mTLS | cert identity + active role |
| JobManager -> Worker | service certificate | mTLS | coordinator epoch + cert identity |
| Worker -> JobManager | service certificate | mTLS | worker cert CN 与 worker_id 匹配 |
| Worker -> Worker data plane | service certificate | mTLS | HELLO job/task/attempt + cert |
| PyStream -> object store | access key secret | TLS | bucket policy/prefix |
| PyStream -> Kafka | client certificate | mTLS/SSL | broker ACL outside PyStream |
| Prometheus -> metrics | scrape token/client cert | TLS | read-only metrics |

## Interaction Style Decision

| Surface | Style | Reason | Rejected Alternative |
|---|---|---|---|
| DATA/BARRIER | Ordered persistent stream | 顺序、背压、低开销 | 逐记录 HTTP |
| Worker control | Synchronous HTTPS | 需要明确 arm/complete/abort 结果 | 异步无确认命令 |
| Job metadata | Immutable revisions + CAS pointer | 可审计、可接管 | 单个可覆盖 JSON |
| Leader election | Lease object + ETag CAS | 单 active、可 fencing | DNS/启动顺序选主 |
| Checkpoint/output visibility | Decision/finalized manifests | 崩溃后可重放 finalize | 文件存在即视为可见 |
| Metrics | Prometheus pull | 本地标准化、可告警 | 仅解析日志 |

## Bounded Context Map

| Context | Responsibility / Check Path | Model | Upstream | Downstream | Relationship / Translation |
|---|---|---|---|---|---|
| API/Graph | `tests/contract/` | YAML/StreamGraph | 用户作业 | Control | anti-corruption：严格 YAML -> 内部图 |
| Control/HA | `tests/unit/control/` | JobRun/leader epoch | API、metadata | Worker、Checkpoint | customer/supplier；持久 revision |
| Runtime | `tests/integration/` | DATA/CONTROL/channel identity | Control、上游 Task | Operator、下游 Task | conformist to deployment |
| Checkpoint | `tests/unit/checkpoint/` | task snapshot/decision/finalized | Runtime | Recovery、Sink | shared kernel：checkpoint identity |
| Transaction Sink | `tests/unit/operators/` | active/prepared/committed tx | Runtime Barrier | File output reader | translation：RecordEnvelope -> CSV |
| Object Storage | store contract tests | S3 object/ETag | Control、Worker | HA recovery | anti-corruption：S3 -> repository ports |
| Security | security contract tests | cert identity/token/secret | CLI/services | all protected edges | shared policy, no business payload |
| Observability | metrics tests | counters/histograms/rules | all contexts | Prometheus/operator | separate read model |

## Public API And Contracts

### YAML

```yaml
api_version: pystream/v1

execution:
  # execution 存在时默认 exactly_once；中级回归必须显式 at_least_once
  delivery_guarantee: exactly_once
  checkpoint:
    interval: 10s
    timeout: 30s
    max_consecutive_failures: 3
  restart:
    max_attempts: 3
    delay: 2s
```

规则：

- `delivery_guarantee` 取 `at_least_once|exactly_once`。
- `execution` 缺失时保持 v0.1 fail-fast/逐条 commit。
- `at_least_once` 使用现有 DRAIN、共享状态和 append Sink。
- `exactly_once` 强制启用 Barrier、事务 File Sink、决定/finalize。
- 当前只有 File Sink 声明 exactly-once capability；未来新增不兼容 Sink 时 Graph
  必须拒绝 exactly-once 作业。
- `examples/intermediate/job.yaml` 显式写 `at_least_once`。
- 新增 `examples/advanced/job.yaml` 使用默认或显式 `exactly_once`。

### Status API

`GET /v1/jobs/{job_id}` 增加：

```json
{
  "delivery_guarantee": "exactly_once",
  "coordinator_epoch": 4,
  "checkpoint": {
    "active_id": 8,
    "last_decided_id": 7,
    "last_finalized_id": 7,
    "phase": "IDLE"
  },
  "leader": {
    "instance_id": "jobmanager-a",
    "epoch": 4,
    "role": "active"
  }
}
```

新增只读端点：

- `GET /health/live`：进程存活。
- `GET /health/ready`：依赖可用；standby 可 ready。
- `GET /health/leader`：仅 active 返回 200，供 HAProxy。
- `GET /metrics`：受内部认证保护的 Prometheus 文本。

standby 对写请求返回 503 和 active hint；HAProxy 正常情况下不会路由到 standby。

### Checkpoint State Machine

```text
IDLE
  -> ARMED
  -> ALIGNING
  -> SNAPSHOTTED/PREPARED
  -> DECIDED
  -> FINALIZING
  -> FINALIZED

ARMED/ALIGNING/PREPARED -> ABORTED -> RECOVERING
DECIDED/FINALIZING      -> FINALIZING (retry only; never ABORTED)
```

不变量：

- checkpoint ID 和 coordinator epoch 单调递增且不复用。
- 同一 Task/attempt/checkpoint 只接受一次每输入 Barrier。
- snapshot 只包含该输入 Barrier 之前的 DATA。
- `decision.json` 只在任务全集、摘要和 prepared transaction 全部验证后写入。
- decision 写入后，abort API 必须拒绝。
- finalized 可重复执行，结果相同。

### Object Keys

```text
pystream/
  artifacts/<sha256>.zip
  control/leader.json
  jobs/<job_id>/current.json
  jobs/<job_id>/revisions/<revision:020d>.json
  checkpoints/<job_id>/<checkpoint:020d>/
    attempts/<attempt:08d>/tasks/<task_sha256>.json
    decision.json
    finalized.json
```

- immutable 对象使用 `If-None-Match: *`。
- `leader.json`、`current.json` 使用 ETag + `If-Match` CAS。
- 409/412 视为并发冲突，不得静默覆盖。
- schema、SHA、大小和 identity 校验沿用并扩展现有规则。

### Transaction File Layout

```text
/data/output/<job_id>/<sink_operator>/
  pending/attempt-<attempt>/tx-<uuid>/part-<subtask>.csv
  committed/checkpoint-<checkpoint>/part-<subtask>.csv
  manifests/checkpoint-<checkpoint>.json
```

- pending 与 committed 必须在同一文件系统。
- pre-commit 执行 flush + fsync + close，并记录 size/SHA/path。
- finalize 使用确定性目标名和 `os.replace`；目标已存在时复验 size/SHA 后幂等成功。
- 所有 sink fragment 完成后才原子写 output manifest。
- 读取方只读取 manifest 引用的 committed fragments；pending 和孤儿 fragment 不可见。
- 恢复时清理未被任何 DECIDED checkpoint 引用的 pending transaction。

## Barrier Alignment Design

1. `arm_checkpoint()` 为每个输入创建未设置的 gate，并初始化 barrier set。
2. Source trigger 短暂 pause，等待当前 record 完成，捕获 Barrier 边界 offset/state，
   将 BARRIER 排在所有前序 DATA 后，立即恢复 Source。
3. `DataPlaneServer` 读取 CONTROL/BARRIER 后调用 `accept_control()`；该方法先把 Barrier
   放入 Runtime 有界队列，再等待该 identity 的 gate。
4. 因为每条 TCP connection 有独立 handler，该输入停止读取 post-barrier 帧，
   其他输入仍继续。
5. Runtime 处理 Barrier 前已入队的 DATA；收到某输入 Barrier 后登记，不把 Barrier
   传入 UDF。
6. 收齐全部输入后，Sink pre-commit、Task snapshot、向全部物理下游广播 BARRIER，
   设置 descriptor/ready，再开启所有 gates。
7. timeout/abort/stop/failure 必须开启全部 gates，避免连接泄漏或死锁。
8. Source 为每个 checkpoint 保存冻结 offset mapping；后续消费不改变待提交 mapping。
9. complete 用冻结 mapping 提交 Kafka；abort 删除 mapping；恢复仍以 decision
   snapshot 显式 seek。

## Transaction And Recovery Design

### Sink Lifecycle

- Task open：创建 active transaction。
- Barrier aligned：pre-commit active transaction，关联 checkpoint，立即创建下一个
  active transaction。
- Pre-decision failure：abort prepared transaction，整作业立即从上一个 DECIDED/
  FINALIZED checkpoint 恢复；不能仅恢复消费继续运行。
- Decision success：prepared transaction 成为必须完成的提交决定。
- Finalize：移动分片、写 output manifest；重复调用安全。
- Recovery：先 finalize 所有 DECIDED 未 FINALIZED checkpoint，再恢复算子状态和
  Source offset。

### Coordinator Ordering

1. arm tasks。
2. trigger source barriers。
3. wait all task snapshots/prepared descriptors。
4. validate task set、attempt、epoch、hash、size、transaction descriptors。
5. conditional write `decision.json`。
6. send complete/finalize to all tasks。
7. verify sink fragments and commit frozen Source offsets。
8. atomically publish output manifest。
9. conditional write `finalized.json`。

步骤 5 之前失败：abort + immediate job recovery。
步骤 5 之后失败：持久记录 finalize pending，由当前或新 leader 重试。

## HA Design

### Survivability Statement

- 可承受：一个 JobManager 进程退出、一个 MinIO 节点退出、任一 Worker 进程退出。
- 继续提供：已提交作业状态、artifact、最近决定 Checkpoint、事务 finalize 和恢复。
- 允许退化：切换期间管理写请求短暂 503；Barrier/作业恢复期间吞吐下降。
- 不承诺：Docker 主机、Kafka broker、输出卷整体丢失。

### Leader Lease

- 默认 lease TTL `10s`，renew interval `3s`，standby poll `1s`。
- leader object：`instance_id`、`epoch`、`issued_at`、`expires_at`。
- 初次创建用 `If-None-Match:*`；续租/接管用 `If-Match:<etag>`。
- 续租连续失败或不能访问对象存储多数服务入口时，active 在 TTL 内主动 step down。
- 接管者 CAS 成功后 epoch +1，先恢复 metadata/finalize，再开放 leader health。
- 所有部署、Worker 状态、Checkpoint snapshot/decision 写入携带 epoch。
- Worker 拒绝低于已见 epoch 的请求；对象存储 current pointer 拒绝 stale ETag。

### Metadata Recovery

持久 revision 包含：

- canonical job definition 与 artifact SHA。
- delivery guarantee。
- Job status、attempt、next checkpoint、last decided/finalized。
- recovery counters、last failure、coordinator epoch。
- 事务 finalize backlog。

接管步骤：

1. 获取 lease 和新 epoch。
2. 加载所有 current pointer/revision，校验 schema/SHA。
3. 扫描并完成 DECIDED 未 FINALIZED checkpoint。
4. 等待 Worker 重新注册；Worker heartbeat 遇到 leader 变化/404/epoch 更新时自动注册。
5. 对 RUNNING/DEPLOYING/RECOVERING 作业统一进入 RECOVERING，attempt +1。
6. 从最近 DECIDED checkpoint 重部署。
7. 全任务 RUNNING 后开放 leader health 并恢复周期 Checkpoint。

## Runtime Dependency Adoption

| Dependency | Needed Capability | Failure Mode | Fallback/Exit | Adoption Gate |
|---|---|---|---|---|
| S3 SDK | TLS、conditional put/get/list | timeout、409/412、endpoint fail | repository ports 保留 Local 实现 | fake contract + MinIO integration |
| 4-node MinIO | erasure redundancy | 单节点退出、quorum loss | core 可单节点；HA 失败即保护模式 | 单节点故障读写 E2E |
| HAProxy | active JM 与 S3 endpoint routing | proxy exit/misroute | 直连诊断端口 | leader-only health test |
| Prometheus client/server | metrics + rules | scrape failure | JSON 日志/status 保留 | metrics contract |
| Python ssl/aiohttp TLS | mTLS HTTP | cert expiry/CN mismatch | 无明文生产 fallback | negative auth tests |
| asyncio SSL streams | mTLS data plane | handshake/rotation failure | task fail + recovery | loopback TLS integration |

对象存储镜像、HAProxy、Prometheus 和 Python base 均固定版本与 digest；依赖范围仍需
lock/wheelhouse 后才声明可复现镜像。

## Observability And SLO

新增指标至少包括：

- `pystream_leader_role`、`pystream_leader_epoch`、`pystream_lease_renew_failures_total`
- `pystream_checkpoint_phase`、`pystream_checkpoint_duration_seconds`
- `pystream_barrier_alignment_seconds`、`pystream_barrier_inputs_blocked`
- `pystream_checkpoint_decisions_total`、`pystream_checkpoint_finalize_retries_total`
- `pystream_sink_transactions{state=active|prepared|committed|aborted}`
- `pystream_job_recovery_duration_seconds`
- `pystream_object_store_requests_total{operation,status}`
- `pystream_tls_handshake_failures_total`、`pystream_auth_rejections_total`

告警规则：

- active JobManager 数量 `!= 1` 持续 15 秒。
- lease 剩余时间低于 2 个 renew interval。
- DECIDED 未 FINALIZED 超过 30 秒。
- Checkpoint 连续失败达到配置上限。
- 对象存储 endpoint/quorum 不可用。
- 恢复超过 JM 30 秒或 Worker 60 秒目标。
- TLS 证书剩余有效期低于 7 天。

## Fitness Functions

| Property | Metric | Threshold/Rule | Source | Cadence | Failure Response | Check Path |
|---|---|---|---|---|---|---|
| 依赖方向 | 非法 import | 0 | architecture test | 每提交 | 阻止提交 | `tests/architecture/` |
| v1 契约 | 基础/中级样例差异 | 0 | contract tests | 每提交 | 阻止提交 | `tests/contract/` |
| Barrier 顺序 | post-barrier record 进入 snapshot | 0 | state-machine integration | 每提交 | 阻止提交 | `tests/integration/` |
| 对齐有界 | queue > capacity / gate 泄漏 | 0 | runtime metrics/tests | 每提交 | abort/fix | `tests/integration/` |
| 状态一致性 | state/offset checkpoint mismatch | 0 | restore tests | 每提交 | 阻止提交 | `tests/unit/checkpoint/` |
| 输出一致性 | baseline 与 fault diff | 0 rows | Docker core E2E | 每里程碑 | 阻止验收 | `verify_advanced.py` |
| 提交幂等 | 重复 finalize 产生额外文件 | 0 | sink tests | 每提交 | 阻止提交 | `tests/unit/operators/` |
| 单 leader | active count | exactly 1 | Prometheus/E2E | 持续/候选 | 保护模式 | HA acceptance |
| JM failover | 恢复时间 | `<=30s` | fault evidence | 候选 | 阻止验收 | HA acceptance |
| Worker recovery | 恢复时间 | `<=60s` | fault evidence | 候选 | 阻止验收 | core/HA acceptance |
| 存储容错 | 单节点退出后读写 | 100% 成功 | MinIO E2E | 候选 | 阻止验收 | HA acceptance |
| 安全 | 未授权/坏证书成功数 | 0 | security tests | 每提交 | 阻止提交 | `tests/security/` |
| Secret | 仓库/日志凭据命中 | 0 | secret scan | 每提交 | 清除并轮换 | quality gate |
| 覆盖率 | branch coverage | `>=80%` | pytest-cov | 每提交 | 阻止提交 | Python 3.11 |
| 清理 | 项目资源残留 | 0 | resource ledger/inspect | 每 E2E | 强制清理 | acceptance scripts |

## Risk Register

| Risk | Likelihood | Impact | Mitigation | Record / Owner |
|---|---|---|---|---|
| Barrier gate 死锁 | 中 | 高 | 所有 abort/stop/fail 开 gate；状态机与超时测试 | Runtime tests |
| Source Barrier offset 漂移 | 中 | 高 | 冻结 per-checkpoint mapping；延迟 commit 明确参数化 | Source tests |
| decision 后错误地 abort | 低 | 致命 | phase guard；DECIDED 只允许 finalize retry | Checkpoint tests |
| Sink partial visibility | 中 | 高 | fragment 不作为真值；manifest-last | Sink/E2E |
| active/standby split-brain | 低 | 致命 | ETag CAS、TTL、epoch fencing、step-down | HA evidence |
| 对象存储条件写不兼容 | 低 | 高 | 启动 capability probe；409/412 contract tests | Store integration |
| 单 MinIO 节点退出导致 quorum loss | 低 | 高 | 4 节点×2 drive，启动前验证 topology | HA acceptance |
| 默认 Exactly-once 破坏中级回归 | 中 | 高 | 中级样例显式 at_least_once；双路径契约测试 | Contract tests |
| mTLS 证书过期/身份错误 | 中 | 高 | 证书有效期指标、负面测试、轮换 runbook | Security docs |
| Secret 泄露到日志/仓库 | 低 | 高 | file secrets、redaction、扫描、禁止 payload | Security gate |
| Docker Desktop CLI/WSL 抖动 | 高 | 中 | 原生 Docker 硬超时、ledger、profile 拆分 | Acceptance log |
| 范围过大导致阶段失控 | 中 | 高 | 7 个独立里程碑、每步远端回退点 | Development log |

### Residual Risk / Explicit Exceptions

| Exception | Residual Impact | Monitoring | Revisit Trigger |
|---|---|---|---|
| 单 Kafka broker | broker 退出停止输入 | source errors/lag | 需要 Kafka HA 评分或生产部署 |
| 单 Docker 主机 | 主机退出全栈不可用 | deployment health | 跨主机部署 |
| 单共享 File output volume | 卷丢失最终输出丢失 | file/output manifest checks | 输出存储改为对象存储 |
| 可信 UDF | 恶意/阻塞 UDF 可影响 Worker | UDF duration/task failure | 引入不可信用户 |

## Proposed Changes

### Milestone 0：治理、高级基线与规格

文件：

- `.trae/specs/build-pystream-advanced/spec.md`（新增）
- `.trae/specs/build-pystream-advanced/tasks.md`（新增）
- `.trae/specs/build-pystream-advanced/checklist.md`（新增）
- `.trae/documents/pystream-advanced-v0.3-development-plan.md`
- `reports/advanced-development.md`（新增）

步骤：

1. 核对当前 3 个报告文件的 blob/hash；有语义变化则停下保护，不自动还原。
2. 运行 Python 3.11 全量门禁和中级 Docker smoke，确认 `64b875b` 可作为基线。
3. 按 release event policy 完成 reproducibility/readiness review 和 receipt。
4. 在 `64b875b` 创建 annotated `v0.2.0` 并推送。
5. 从 `64b875b` 创建并推送 `feature/advanced-v0.3`。
6. 固化 spec/tasks/checklist、开发日志和 ADR。

提交建议：`chore: establish advanced development baseline`

### Milestone 1：公共契约与状态机

修改：

- `pyproject.toml`、`src/pystream/__init__.py`
- `src/pystream/api/models.py`
- `src/pystream/api/graph.py`
- `src/pystream/common/records.py`
- `src/pystream/checkpoint/models.py`
- `src/pystream/control/models.py`
- `src/pystream/control/ports.py`
- 对应 contract/model tests

内容：

- 版本升级 `0.3.0`。
- 新增 `DeliveryGuarantee`、`CheckpointPhase`、`CoordinatorRole`。
- `ExecutionConfig.delivery_guarantee` 在 execution 存在时默认 exactly_once。
- Graph 验证 Sink capability。
- snapshot/decision/finalized schema 升级并保留版本拒绝逻辑。
- deployment、Worker API、状态上报增加 `coordinator_epoch`。
- 添加完整状态转换表和非法转换测试。

提交建议：`feat: define advanced consistency contracts`

### Milestone 2：持续流 Barrier 对齐

修改：

- `src/pystream/operators/connectors.py`
- `src/pystream/runtime/channel.py`
- `src/pystream/runtime/server.py`
- `src/pystream/runtime/task.py`
- `src/pystream/control/checkpoint.py`
- `src/pystream/worker/manager.py`
- `src/pystream/worker/http.py`
- runtime/source/control tests

内容：

- 保留 DRAIN 实现，新增 guarantee strategy 分派。
- Source 短暂停顿注入 Barrier 后立即 resume。
- Source 保存 checkpoint-specific frozen offsets。
- `accept_control(BARRIER)` 入队后等待 per-input gate。
- all-input alignment 后 snapshot/forward/unblock。
- timeout、abort、cancel、task failure 一律解除 gates。
- 指标记录 alignment duration、blocked inputs、source pause duration。
- 覆盖双输入快慢通道、Barrier 重复/回退/乱序、断连、abort、背压。

提交建议：`feat: add aligned streaming checkpoints`

### Milestone 3：事务 File Sink 与两阶段提交

修改/新增：

- `src/pystream/operators/connectors.py`
- `src/pystream/checkpoint/models.py`
- `src/pystream/checkpoint/store.py`
- `src/pystream/control/checkpoint.py`
- `src/pystream/runtime/task.py`
- `scripts/verify_advanced.py`（新增）
- sink/checkpoint/state-machine tests

内容：

- 实现 active/prepared/committed/aborted transaction。
- pre-commit fsync 并快照 transaction descriptor。
- decision manifest 与 finalized marker。
- finalize 幂等 rename/hash 检查。
- output manifest-last 可见性。
- DECIDED 后异常只允许 retry。
- 作业启动/恢复清理无决定引用的 pending transaction。
- 三个确定性 test hooks 仅在 `--enable-test-hooks` 时启用。

提交建议：`feat: add transactional file sink commits`

### Milestone 4：对象存储与持久元数据

新增/修改：

- `src/pystream/checkpoint/ports.py`（新增）
- `src/pystream/checkpoint/s3_store.py`（新增）
- `src/pystream/control/metadata.py`（新增）
- `src/pystream/control/artifacts.py`
- `src/pystream/storage/`（新增低层 S3 client/config/models）
- `src/pystream/service.py`
- `pyproject.toml`
- store/artifact/metadata tests
- `deploy/compose.advanced.yaml`（新增）

内容：

- 抽象 Local/S3 store ports。
- S3 immutable put、ETag CAS、严格错误映射和 capability probe。
- artifact、checkpoint、job revision/current pointer 迁移到对象存储实现。
- Local 实现继续服务单元测试与显式兼容部署。
- advanced core 使用单节点 S3 兼容服务；ha profile 使用 4 节点×2 drives。
- 镜像全部 pin tag+digest，凭据从 Secret file 读取。

提交建议：`feat: add durable object-backed state`

### Milestone 5：双 JobManager 主备与接管

新增/修改：

- `src/pystream/control/leader.py`（新增）
- `src/pystream/control/metadata.py`
- `src/pystream/control/manager.py`
- `src/pystream/control/http.py`
- `src/pystream/control/models.py`
- `src/pystream/worker/http.py`
- `src/pystream/service.py`
- `deploy/haproxy/advanced.cfg`（新增）
- leader/metadata/takeover tests

内容：

- lease acquire/renew/step-down 与 epoch fencing。
- JobManager active/standby role。
- 所有 JobRun 关键转换持久化 revision。
- standby 接管、decision finalize、Worker 重注册和整作业恢复。
- HAProxy leader health routing。
- 旧 epoch 命令和状态写入拒绝。
- fake clock/CAS race/双 contender/lease loss 测试。

提交建议：`feat: add fenced jobmanager failover`

### Milestone 6：mTLS、Token、Secret 与 Prometheus

新增/修改：

- `src/pystream/security/`（新增）
- `src/pystream/observability/metrics.py`（新增）
- HTTP clients/services、data channel/server、Kafka source、service CLI
- `scripts/generate_dev_pki.py`（新增）
- `deploy/prometheus/prometheus.yml`（新增）
- `deploy/prometheus/rules.yml`（新增）
- `.gitignore`
- security/metrics tests

内容：

- Python SSL context factory，证书 SAN/CN 与 service identity 校验。
- asyncio TCP mTLS。
- aiohttp internal mTLS。
- external HTTPS/Bearer Token，使用 `hmac.compare_digest`。
- Kafka SSL client context。
- `--*-secret-file`/Docker Secret 契约；不接受明文 CLI secret。
- liveness/readiness/leader health 与 `/metrics`。
- Prometheus rules 和证书到期指标。
- 无 token、错 token、错 CA、错 CN、过期证书、secret redaction 测试。

提交建议：`feat: secure and observe the advanced cluster`

### Milestone 7：core/ha Docker 验收与文档

新增/修改：

- `examples/advanced/job.yaml`
- `examples/advanced/exactly_once_udfs.py`
- `scripts/run_advanced_core_acceptance.ps1`
- `scripts/run_advanced_ha_acceptance.ps1`
- `scripts/inject_advanced_failure.py`
- `scripts/verify_advanced.py`
- `scripts/cleanup_advanced.py`
- `deploy/compose.advanced.yaml`
- README 与 `docs/*`
- `reports/advanced-*`

core profile：

1. 生成临时 PKI/Secret。
2. 启动 Kafka、单对象存储、单 JM、3 Workers、Prometheus。
3. 运行基础与显式 At-least-once 中级回归。
4. 运行 Exactly-once 无故障基准。
5. 在 Barrier 前和 prepared/decision 前窗口 SIGKILL Worker。
6. 等待恢复 `<=60s`。
7. 比较 committed output 与基准逐记录一致，lag=0。

ha profile：

1. 启动 4 MinIO、S3 LB、HAProxy、2 JM、3 Workers、Kafka、Prometheus。
2. 验证 active count=1 和 mTLS/Token 负面路径。
3. 在 decision 后/finalize 前终止 active JM。
4. 验证 standby `<=30s` 接管、epoch 增加、finalize 完成。
5. 终止一个 MinIO 节点，继续读取 artifact/metadata/checkpoint 并完成新写入。
6. 再次注入 Worker 故障并验证 Exactly-once。
7. 保存 metrics、状态、lease、checkpoint、output manifests 和容器证据。
8. 清理全部容器、网络、卷、Secret 和 ledger，资源计数为 0。

提交建议：`test: prove advanced exactly-once and failover`

## Verification Plan

### 每个里程碑

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pytest
git diff --check
```

最终门禁必须在 Python 3.11 Linux 容器执行；宿主 Python 3.13 仅作补充。

### 专项测试

- 配置默认升级与显式 At-least-once 回退。
- Barrier per-channel ordering/gating。
- all-input alignment、timeout、abort、cancel、断连。
- Source frozen offset 不被 post-barrier 消费推进。
- Sink begin/precommit/decision/finalize/abort 全状态转换。
- crash before decision 与 after decision 的不同恢复动作。
- output manifest 是唯一可见性真值。
- S3 If-None-Match/If-Match 409/412/timeout。
- leader lease 双 contender、renew loss、stale epoch。
- metadata revision 损坏/回退。
- mTLS 与 Token 正/负路径。
- Prometheus 指标名、label 基数和告警规则。

### 最终验收证据

- Python 3.11 测试数、覆盖率、Ruff、format。
- core/ha 完整 transcript 与运行 JSON 日志。
- 每个故障窗口前后 checkpoint phase、attempt、epoch。
- active JM 接管耗时与 Worker 恢复耗时。
- MinIO 单节点故障前后读写证据。
- baseline 与 fault committed rows diff=0。
- Kafka committed/end/lag。
- pending transaction 清理与 output manifest。
- TLS/Token 拒绝证据，不含凭据值。
- 最终资源清零。

## Git, Review And Rollback

1. 不使用 `git reset --hard` 或 `git checkout --` 丢弃工作树。
2. tag/commit 前按 event policy：
   - stage 单独命令；
   - 审查 exact staged diff；
   - 展示结构化 review；
   - 单独写 receipt；
   - 再执行 tag/commit。
3. 每个 milestone 单独 commit、Git note、push
   `origin/feature/advanced-v0.3` 与 `refs/notes/commits`。
4. 每条开发日志包含 UTC、任务、文件、测试、故障、决策、SHA 和
   `git revert <sha>`。
5. 单步回退：`git revert <milestone-sha>`。
6. 完整退回中级：从 `v0.2.0` 新建分支或 revert 高级 commits。
7. 高级验收前不合并 `main`、不创建 v0.3.0 tag、不发布镜像。

## Assumptions And Fixed Decisions

| Item | Decision |
|---|---|
| 开发方式 | 分阶段全实现 |
| 分支 | 从 `64b875b` 新建 `feature/advanced-v0.3` |
| 中级标签 | 创建并推送 annotated `v0.2.0` |
| API | 保持 `pystream/v1` |
| 默认语义 | execution 作业默认 exactly_once，可显式 at_least_once |
| Barrier | per-input aligned gate，无无界 post-barrier buffer |
| Sink | checkpoint fragments + manifest |
| Checkpoint | DECIDED/FINALIZED，不可逆决定 |
| 存储 | 4 节点分布式 S3 兼容对象存储 |
| 存储数据 | artifact + checkpoint + JM metadata |
| JobManager | 双实例 active/passive |
| 安全 | 内部 mTLS、外部 HTTPS + Token、file secrets |
| 可观测性 | Prometheus + rules + JSON logs |
| SLO | JM `<=30s`，Worker `<=60s` |
| 故障矩阵 | Barrier 前、prepared/decision 前、decision/finalize 前 |
| Docker | core 与 ha 两个 profile |
| 发布 | 不合并 main、不发布镜像，验收后另行批准 |

## Plan Completion Gate

执行器只有在以下条件全部满足后才能标记高级阶段完成：

1. tasks/checklist 无未完成项。
2. 所有 fitness functions 通过。
3. 三个提交窗口和 Worker/JM/存储故障 E2E 通过。
4. Exactly-once committed output 与无故障基准 diff=0。
5. HA/SLO、安全、Secret、指标和清理证据完整。
6. 文档只声明已实证能力，并列出 Kafka/主机/输出卷剩余故障域。
7. 功能分支、Git notes 和全部回退点已推送。
