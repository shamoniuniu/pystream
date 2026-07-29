# PyStream 中级功能开发计划

## Summary

### 目标

在初级版本 `24384c14a7c7f95db350016e623421d5d37af674` 上完成课程定义的全部中级功能：

1. 事件时间滚动窗口、有限乱序 Watermark、多输入 Watermark 合并与空闲输入检测。
2. 原生 Changelog/Retract 计算，使二级聚合在上游结果更新时保持正确。
3. Worker 进程自动拉起、整作业连接重建、状态与 Kafka offset 恢复，并提供可复核的 At-least-once 证据。
4. 全过程保留开发日志、结构化运行日志、验收报告和可远程获取的 Git 回退点。

### 成功标准

- 原有 `pystream/v1` 作业不修改即可运行，初级 WordCount 回归结果不变。
- 事件时间 demo 接受 Watermark 之后仍合法的有限乱序记录，丢弃 `event_time <= watermark` 的迟到记录并记录指标/日志。
- 多输入任务使用“活跃输入 Watermark 的最小值”，Watermark 只单调前进；空闲输入超过配置时间后不再阻塞窗口。
- Changelog 模式按首次 `INSERT`、更新 `UPDATE_BEFORE + UPDATE_AFTER` 输出；下游 Reduce 使用 `retract_udf` 撤回旧贡献，最终“每种 count 对应多少个 word”结果正确。
- 周期性停流协调快照把 Source offset、Watermark/窗口状态、Reduce 状态写入同一个已完成 Checkpoint；不完整快照永不用于恢复。
- 杀死承载状态任务的 Worker 进程后，Compose 自动拉起进程，作业进入 `RECOVERING`，从最近完成 Checkpoint 整作业重新调度并回到 `RUNNING`。
- 恢复后固定输入无丢失；Checkpoint 之后、故障之前的输出允许重复，文档不宣称 Exactly-once。
- Python 3.11 下 Ruff、格式检查、全量测试通过，核心包分支覆盖率保持 `>= 80%`；Docker 三 Worker 故障注入验收通过。
- `main` 保持初级稳定版本；中级工作位于 `feature/intermediate-v0.2`，每个里程碑有独立提交、远端备份和回退说明。

### 受众

- 课程评分与答辩人员：需要可执行 demo、设计论证、故障证据和日志。
- 后续高级阶段开发者：需要在不推翻中级接口的前提下继续实现非停流 Barrier 和事务 Sink。

### 范围外

- Exactly-once、事务文件 Sink、持续数据流中的对齐 Barrier。
- JobManager 高可用、跨主机共享存储容灾、多机房、动态扩缩容、多租户。
- SQL、Join、CEP、滑动/会话窗口、Side Output、外部 Sink 扩展。
- 不可信 UDF 沙箱、认证、TLS、生产级 SLO。

## Current State Analysis

### 仓库与 Git

- `main` 与 `origin/main` 均指向唯一初始提交 `24384c1`，工作树干净。
- 远端为私有仓库 `https://github.com/shamoniuniu/pystream.git`。
- 当前没有 Git 标签；初级版本尚无独立回退标签。
- `.gitignore` 已忽略运行时 `checkpoints/`、`logs/`、`output/`、`work/`，但 `reports/` 可提交，适合保存开发与验收日志。

### 已有扩展点

- `RecordEnvelope` 已有 `event_time`、`message_type`、`change_kind`、`checkpoint_id`。
- `MessageType` 已预留 `WATERMARK`、`BARRIER`、`CHECKPOINT_COMPLETE`。
- `BaseOperator` 已预留 `snapshot_state/restore_state`。
- `FileSinkOperator` 已预留事务方法，但本轮不启用。
- 数据面按通道保持顺序，输出队列有界，并能通过 `drain()` 传播背压。
- Compose 已为 PyStream 服务配置 `restart: unless-stopped`，可作为 Worker 进程监督器。
- 运行日志已统一为单行 JSON，标准字段包含 component、job、operator、subtask、worker 和 event。

### 必须改造的现状

- YAML 只允许处理时间窗口，没有事件时间提取、Watermark、Checkpoint 和重试配置。
- 控制消息仍被装入 `DATA_BATCH`；`TaskRuntime` 收到任何非 DATA 消息都会失败。
- Source 每成功发送一条记录就提交 Kafka offset，offset 会领先于持久算子状态，不能用于状态恢复。
- Reduce 状态只存在内存字典中，关闭时直接清空，快照/恢复接口只抛异常。
- Job/Task 状态机把 `FAILED` 作为终态，没有 attempt、`RECOVERING`、最近 Checkpoint 或恢复耗尽信息。
- Worker 重注册可以刷新地址，但 WorkerManager 保留旧 Runtime，无法按更高 attempt 安全部署同一逻辑任务。
- File Sink 固定写 `window_end,word,count`，不能表达 Retract demo 的通用列。
- JobManager 仅在内存保存作业运行信息；本轮只承诺 Worker 故障恢复，不承诺 JobManager 重启恢复。

## Assumptions & Decisions

### 已确认的用户决策

| 决策 | 选择 |
| --- | --- |
| 中级范围 | 一次完成事件时间、Retract、自动恢复与 At-least-once |
| Checkpoint | 停流协调快照，不提前实现持续流对齐 Barrier |
| 恢复粒度 | 任一任务/Worker 故障后整作业一致恢复 |
| 状态存储 | JobManager 和所有 Worker 共享 Docker 命名卷 |
| Retract API | Reduce 可选产生 Changelog；消费 Changelog 的 Reduce 配置 `retract_udf` |
| 事件时间 | Source 使用 JSON Pointer 提取 RFC3339 时间，多输入最小 Watermark并支持空闲检测 |
| 重试 | 作业级可配置，默认最多 3 次 |
| Git | `v0.1.0` 基线标签 + `feature/intermediate-v0.2` + 小步提交 |
| 合并 | 完成后保留功能分支，用户确认前不合并 `main` |
| 日志 | 开发日志、结构化运行日志、验收报告全部保留 |

### 默认参数

| 参数 | 默认值 | 测试覆盖 |
| --- | --- | --- |
| Checkpoint interval | `10s` | 单元/集成测试使用毫秒级显式配置 |
| Checkpoint timeout | `30s` | 覆盖超时、abort、下次成功 |
| Restart max attempts | `3` | 覆盖首次成功和耗尽失败 |
| Restart delay | `2s` | 测试注入零/短延迟 |
| Watermark max out-of-orderness | 作业显式配置；demo `2s` | 覆盖边界值 |
| Watermark idle timeout | `30s` | 覆盖空闲排除与恢复活跃 |
| 单任务快照上限 | `64 MiB` | 覆盖超限拒绝 |

### 兼容性

- 保持 `api_version: pystream/v1`；新增字段全部可选，默认行为等价于初级版本。
- 未配置 `execution` 时不启动周期 Checkpoint/自动恢复，处理时间作业行为不变。
- `Reduce.emit_mode` 默认 `final`；是否需要 `retract_udf` 由输入流是否可能包含撤回消息决定。
- File Sink `columns` 缺省为现有三列，旧 WordCount 文件格式不变。
- 版本化快照只使用规范 JSON，不使用 pickle；未知版本、摘要不符或字段缺失直接拒绝。

## Public API And Contracts

### YAML

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

operators:
  - id: words
    type: source
    parallelism: 2
    config:
      connector: kafka
      topic: words
      value_format: json
      event_time:
        pointer: /event_time
        format: rfc3339

  - id: normalize
    type: map
    parallelism: 2
    udf: event_time_retract_udfs:normalize

  - id: by_word
    type: key_by
    parallelism: 2
    udf: event_time_retract_udfs:word_key

  - id: word_totals
    type: reduce
    parallelism: 2
    udf: event_time_retract_udfs:add_word_counts
    emit_mode: changelog
    window:
      type: tumbling
      time_characteristic: event
      size: 5s

  - id: to_count_bucket
    type: map
    parallelism: 1
    udf: event_time_retract_udfs:to_count_bucket

  - id: by_count
    type: key_by
    parallelism: 1
    udf: event_time_retract_udfs:count_key

  - id: count_distribution
    type: reduce
    parallelism: 1
    udf: event_time_retract_udfs:add_bucket
    retract_udf: event_time_retract_udfs:remove_bucket
    emit_mode: final
    window:
      type: tumbling
      time_characteristic: event
      size: 5s

  - id: output
    type: sink
    parallelism: 1
    config:
      connector: file
      format: csv
      columns:
        - /headers/window_end
        - /payload/count
        - /payload/word_count

edges:
  - from: words
    to: normalize
  - from: normalize
    to: by_word
  - from: by_word
    to: word_totals
  - from: word_totals
    to: to_count_bucket
  - from: to_count_bucket
    to: by_count
  - from: by_count
    to: count_distribution
  - from: count_distribution
    to: output
```

约束：

- `execution.event_time` 仅在至少一个事件时间窗口存在时允许并要求配置。
- 每条能到达事件时间窗口的 Source 路径都必须配置 `config.event_time`。
- JSON Pointer 遵循 RFC 6901；不存在、非字符串、无时区或非法 RFC3339 时间进入现有 `bad_record_policy=fail|skip`。
- 时间可带任意合法 offset，进入系统后统一转换为 UTC；无时区时间和 leap second 被拒绝。
- `max_out_of_orderness >= 0`、`idle_timeout > 0`，所有持续时间沿用 `ms/s/m/h`。
- `emit_mode=final|changelog` 只定义输出模式；Graph 同时传播 `DataStream.changelog` 属性。
- 任一 Reduce 只要存在可能携带 `UPDATE_BEFORE/DELETE` 的上游，就必须配置二参数同步
  `retract_udf`；只消费 INSERT 的 Reduce 禁止配置，避免未使用配置掩盖 DAG 错误。
- `retract_udf(accumulator, value)` 返回新 accumulator；返回 `null` 表示删除当前 key/window 状态。
- Sink `columns` 是针对 `RecordEnvelope.to_dict()` 的 JSON Pointer 列表，缺失列使任务失败，不静默写空值。

### 控制消息和数据面

- 新增 `MessageType.CHECKPOINT_DRAIN`，与高级阶段预留的 `BARRIER` 分离。
- 新增 `FrameType.CONTROL`，只承载一个非 DATA `RecordEnvelope`。
- 内部数据面 `PROTOCOL_VERSION` 从 1 升为 2；中级集群要求所有 Worker 使用同一镜像，
  不支持 v1/v2 混跑，版本不符在 HELLO 阶段明确失败。
- `BoundedDataChannel` 必须先发送控制消息之前的全部 DATA，再发送 CONTROL；控制消息之后的数据不得越过它。
- DATA 继续按 FORWARD/REBALANCE/HASH 选择目标；WATERMARK 和 CHECKPOINT_DRAIN 必须
  向该逻辑边的每条物理输出通道各广播一次，不能经过单目标数据路由。
- `DataPlaneServer` 将 DATA 和 CONTROL 分别投递给 `accept_records` 与 `accept_control`，业务 UDF 永远看不到控制消息。
- `ChannelIdentity` 增加 `attempt_id`；旧 attempt 的连接、状态上报和快照写入均被拒绝。

### 状态与 Checkpoint 文件

```text
/data/checkpoints/
  <job_id>/
    checkpoint-00000000000000000001/
      attempt-00000001/
        tasks/
          <sha256(task_id)>.json
      manifest.json
```

- Task 文件包含 schema version、job/task/attempt/checkpoint、operator/source state、SHA-256 和字节数。
- 每个文件先写同目录临时文件、flush + fsync，再原子替换。
- JobManager 收齐并验证当前 attempt 的所有 Task descriptor 后，最后原子写 `manifest.json`。
- 只有存在合法 manifest、任务集合与执行图完全一致、全部摘要复验通过的 Checkpoint 才是“已完成”。
- 临时文件、无 manifest 目录、旧 attempt 文件和损坏文件在恢复时忽略并记录；清理由后续成功 Checkpoint 或作业结束执行。

## Distributed Data And Consistency Plan

### Data Classification

| Data | Source Of Truth | Consistency Requirement | Staleness Allowed | Repair Path |
| --- | --- | --- | --- | --- |
| Kafka 输入与 next offset | Kafka + 最近完成 manifest | 恢复 offset 不得领先于同一 Checkpoint 的算子状态 | 最多一个 Checkpoint 周期 | 按 manifest 显式 seek；缺 manifest 从初始 group 位置启动 |
| Reduce/window/changelog 状态 | 运行内存；持久真值为最近完成 Task snapshot | 单 Task/attempt 单写，恢复整作业使用同一 checkpoint_id | 最多一个 Checkpoint 周期 | 摘要验证后 restore；损坏则退回前一个完整 Checkpoint |
| Watermark 与空闲输入状态 | Runtime 内存 + Task snapshot | 单调不回退；恢复到 Checkpoint 值后继续推进 | 允许恢复到旧 Watermark并重放 | 重放产生重复但不丢数据；迟到判断使用恢复后的 Watermark |
| File Sink 可见输出 | 追加文件 | At-least-once，允许 Checkpoint 后故障边界重复 | 不允许丢失，允许重复 | 用户按业务键去重；事务提交留给高级阶段 |
| Checkpoint manifest | 共享卷上的原子 manifest | 单调 checkpoint_id、完整任务集合、摘要一致 | 不允许读取半成品 | 忽略不完整目录，使用最近一个合法 manifest |
| 开发/验收证据 | Git 提交、`reports/` | 每条记录关联任务、测试和 commit SHA | 每个任务完成时更新 | `git revert` 单提交或切回 `v0.1.0` |

### Operation Consistency Matrix

| Operation | Read/Write Path | Consistency Model | Failure Behavior | User Contract |
| --- | --- | --- | --- | --- |
| 读取输入 | Kafka -> Source -> DATA | 分区内有序；显式 offset | 完成 Checkpoint 后按 manifest seek | 无丢失，可能重放 |
| 推进 Watermark | Source max event time -> 各通道 -> min(active inputs) | 单调、通道有序 | 恢复旧值后重新推进 | 有限乱序内记录进入正确窗口 |
| 更新聚合 | DATA/Changelog -> keyed state | 每个 key 由一个 subtask 单写 | 恢复旧状态并重放 | 允许重复影响，不允许静默丢状态 |
| 完成 Checkpoint | pause -> drain -> task snapshots -> manifest | 全任务同一 checkpoint_id | 任一失败不写 manifest并恢复运行/触发恢复 | 只有完整 manifest 可用于恢复 |
| 故障恢复 | latest manifest -> 全图重部署 | 整作业一致恢复 | 超过 max attempts 后 FAILED | 状态接口展示 attempt 和原因 |
| 写文件 | Sink append + flush | At-least-once side effect | 故障边界可能重复行 | 本轮明确不保证 Exactly-once |

### Storage Decision

| Option | Decision | Rejected Alternatives | Reversal/Isolation Path |
| --- | --- | --- | --- |
| 共享命名卷 + JSON 快照 + 原子 manifest | Adopted | Worker 本地卷无法跨 Worker；HTTP 上传增加大状态传输；pickle 不安全且不稳定 | `CheckpointStore` 端口隔离，后续可替换对象存储 |
| 停流 `CHECKPOINT_DRAIN` | Adopted | 独立本地快照无法证明无丢失；持续流 Barrier 属于高级阶段 | 保持控制消息接口，高级阶段替换为非停流 Barrier |
| 追加 File Sink | Retained for phase 2 | 事务文件属于 Exactly-once | Sink 事务接口保持未启用 |

### Replication And Conflict Resolution

| Flow | Replication Method | Conflict Rule | Reconciliation | Alert |
| --- | --- | --- | --- | --- |
| Task snapshot | 共享卷单份持久化 | Application-level：只接受当前 attempt；旧 attempt 拒绝，不做 LWW/CRDT | manifest 校验任务全集和摘要 | `checkpoint_snapshot_rejected` |
| Worker 状态上报 | Worker -> 单 JobManager | Application-level：job/task/attempt 必须匹配 | 重复同状态幂等，过期上报忽略并记录 | `stale_attempt_report` |
| Checkpoint 完成 | 单 JobManager 写 manifest | checkpoint_id 严格递增，已完成 manifest 不覆盖 | 启动/恢复时扫描并取最高合法 ID | `checkpoint_manifest_invalid` |

读己之写由单 JobManager 协调器读取自己原子提交的 manifest 保证；不引入 quorum。

### Sharding, Hot Keys, And Tenant Routing

| Surface | Shard/Partition Rule | Hot-Key Risk | Tenant Routing | Mitigation |
| --- | --- | --- | --- | --- |
| 业务状态 | 沿用 canonical JSON + SHA-256 HASH 到 subtask | 单一热门 key 仍可能倾斜 | 本项目无租户 | 记录 state_entries；不在本轮改变分区算法 |
| Checkpoint 文件 | job/checkpoint/task 分片 | 单 Task 大状态文件 | job_id 隔离 | 64 MiB 上限、摘要、原子写 |
| Kafka | 沿用 topic partition / consumer group | 分区倾斜 | job/source group 隔离 | demo 使用 2 分区；记录每分区 offset |

### Transaction, Outbox, Saga, Or Reconciliation

| Operation | Pattern | Failure Handling | Repair Path |
| --- | --- | --- | --- |
| Task 快照提交 | 原子文件替换 | 半写临时文件不可见 | 删除临时文件 |
| 全局 Checkpoint | manifest-last 协调提交 | 缺任一 Task descriptor 就 abort | 使用前一完整 Checkpoint |
| Kafka offset 提交 | manifest 完成后提交并显式保存 next offset | commit 失败不删除 manifest，恢复仍按 manifest seek | 重试/整作业恢复 |
| File Sink | 非事务 append | 允许故障边界重复 | 文档说明去重；高级阶段实现事务 |

### Time, Clock, And Ordering

| Path | Clock Source | Skew Bound | Leap/DST Behavior | Lease/TTL Rule | Logical Clock Needed? |
| --- | --- | --- | --- | --- | --- |
| event_time | 输入 RFC3339，内部 UTC | 由 max_out_of_orderness 限定乱序，不假设机器钟 | offset 归一化 UTC；naive/leap second 拒绝 | 不适用 | Watermark 时间戳单调 |
| processing_time | `SystemClock.now()` UTC | 仅用于日志/兼容处理时间窗口 | UTC，无 DST 分支 | 不适用 | 否 |
| idle/checkpoint/retry timeout | `time.monotonic()`；测试注入 ManualClock | 不受墙钟回拨影响 | 不适用 | 到期使用 `>=` | 否 |
| Checkpoint/attempt | JobManager 递增整数 | 不依赖墙钟 | 不适用 | 不复用 ID | 是，整数 generation |

### At-Rest Data Quality

| Data Class | Golden Record | Reconciliation Cadence | Anomaly Signal | Repair Owner |
| --- | --- | --- | --- | --- |
| Task snapshot | manifest 中 descriptor | 每次完成/恢复 | SHA、size、schema、task set 不符 | Worker 写入，JobManager 验证 |
| Source offsets | manifest 的 partition->next_offset | 每次完成/恢复 | offset 缺分区、负值、seek 失败 | Source/Checkpoint coordinator |
| Window state | versioned JSON snapshot | snapshot/restore 测试 + 每次恢复 | 非法 key/window/RecordEnvelope | Operator |
| 开发证据 | Git SHA + report entry | 每个里程碑 | 日志无 SHA、测试命令或结果 | 执行 Agent |

### Correctness Verification

| Invariant | Check | Cadence | Repair |
| --- | --- | --- | --- |
| Watermark 单调且等于活跃输入最小值 | 单元 + 多通道集成 | 每次相关提交 | 阻止提交 |
| `event_time <= watermark` 不进入状态 | 边界测试 + late metric/log | 每次相关提交 | 阻止提交 |
| Changelog before/after 成对且顺序固定 | 算子与协议测试 | 每次相关提交 | 阻止提交 |
| 完成 manifest 覆盖执行图全部 Task | Store/Coordinator 测试 | 每次相关提交 | abort Checkpoint |
| offset 不领先于状态 | 停流 drain 集成测试 | 每次相关提交 | 不写 manifest |
| 旧 attempt 不能写状态或上报成功 | 状态机/HTTP 测试 | 每次相关提交 | fencing 拒绝 |
| 故障后无输入丢失 | Docker 固定输入故障注入 | 中级里程碑验收 | 阻止验收 |
| 初级行为不回归 | 原 284 项测试 + WordCount E2E | 每个里程碑/最终 | `git revert` 该里程碑 |

## Proposed Changes

### 0. 建立规格、日志与 Git 回退基线

文件：

- 新建 `.trae/specs/build-pystream-intermediate/spec.md`
- 新建 `.trae/specs/build-pystream-intermediate/tasks.md`
- 新建 `.trae/specs/build-pystream-intermediate/checklist.md`
- 新建 `reports/intermediate-development.md`

步骤：

1. 批准计划后再次验证工作树干净、`main == origin/main == 24384c1`；若漂移则停止并报告。
2. 按 Staff Engineer Mode 的发布事件要求先完成可复核检查，再在 `24384c1` 创建并推送 annotated tag `v0.1.0`。
3. 从该标签创建并推送 `feature/intermediate-v0.2`，后续不直接修改 `main`。
4. 将本计划转成独立中级 spec/tasks/checklist，任务按下述里程碑逐项勾选。
5. 开发日志每条包含：UTC 时间、任务 ID、变更摘要、命令、测试结果、问题/决策、commit SHA、`git revert <sha>` 回退方式。

### 1. 扩展 YAML/API 与公共契约

修改：

- `src/pystream/api/models.py`
- `src/pystream/api/graph.py`
- `src/pystream/api/__init__.py`
- `src/pystream/common/records.py`
- `src/pystream/artifact/udf.py`
- `tests/contract/test_job_api.py`
- `tests/unit/test_records.py`
- `tests/unit/test_artifact.py`

实现：

- 新增 `ExecutionConfig`、`EventTimeExecutionConfig`、`CheckpointConfig`、`RestartConfig`。
- Source 新增 `EventTimeExtractorConfig(pointer, format=rfc3339)`。
- Window 支持 `time_characteristic=processing|event`。
- Reduce 新增彼此独立的 `emit_mode=final|changelog` 和 `retract_udf` 契约；
  `DataStream` 新增 changelog 属性并由 Graph 校验消费能力。
- File Sink 新增非空、数量有界的 `columns` JSON Pointer 列表，默认保持旧三列。
- 新增 `UDFKind.RETRACT`，签名与 Reduce 相同。
- Graph 校验事件时间窗口所有 Source 路径、changelog/retract 配对、持续时间和不兼容字段。
- 新增公共 RFC 6901 JSON Pointer 读取辅助函数，统一供 Source 和 Sink 使用；拒绝非法转义和数组越界。

### 2. 实现事件时间、Watermark 与控制消息通道

修改：

- `src/pystream/operators/clock.py`
- `src/pystream/operators/base.py`
- `src/pystream/operators/window.py`
- `src/pystream/operators/connectors.py`
- `src/pystream/runtime/protocol.py`
- `src/pystream/runtime/channel.py`
- `src/pystream/runtime/server.py`
- `src/pystream/runtime/task.py`
- `src/pystream/worker/manager.py`
- `tests/unit/operators/test_operators.py`
- `tests/unit/operators/test_connectors.py`
- `tests/unit/test_protocol.py`
- `tests/integration/test_data_channel.py`
- `tests/integration/test_task_runtime.py`

实现：

- Source 在解码和 payload validator 后提取 event_time，归一化为 UTC；按
  topic/partition 分别维护 max event time 与最后活动单调时间，再以活跃分区 candidate
  的最小值发出 Source Task Watermark，避免一个 consumer 暂时持有多个分区时过早关窗。
- `Clock` 增加单调时间接口；ManualClock 同时可控 UTC 与 elapsed time，禁止倒退。
- Channel 将 DATA 批处理与 CONTROL 严格分帧，保证同通道顺序和背压。
- Watermark/Checkpoint 控制消息广播全部物理出通道；HASH/REBALANCE 只用于 DATA。
- TaskRuntime 记录每个入通道的 Watermark、最后活动单调时间和 idle 状态；只向下游传播大于上次输出的 `min(active watermarks)`。
- 全部输入 idle 时不凭空推进 Watermark；恢复活跃的输入若产生旧数据，按当前 Watermark 判断迟到。
- Operator 增加 `on_watermark()`；处理时间仍走 `on_timer()`，事件时间窗口只由 Watermark 关闭。
- `ReduceWindowOperator` 按 `event_time` 分窗，`event_time <= current_watermark` 时丢弃并增加 `late_records`。
- 增加 `watermark_emitted`、`watermark_advanced`、`input_idle`、`input_active`、`late_record_dropped` JSON 日志。

停止条件：

- Watermark 回退、控制消息进入 UDF、空闲通道导致永久阻塞、初级处理时间测试回归，任一出现均不进入下一里程碑。

### 3. 实现 Changelog/Retract

修改：

- `src/pystream/operators/window.py`
- `src/pystream/operators/core.py`
- `src/pystream/operators/connectors.py`
- `src/pystream/worker/manager.py`
- `examples/` 下新增中级 demo UDF 与 YAML
- 对应 API/UDF/operator/connector 测试

实现：

- `final` 模式完全保留现有窗口关闭后单次输出。
- `changelog` 模式首次状态输出 `INSERT`；更新先深拷贝并输出 `UPDATE_BEFORE`，再输出 `UPDATE_AFTER`。
- 下游 Reduce 对 `INSERT/UPDATE_AFTER` 使用 add UDF，对 `UPDATE_BEFORE/DELETE` 使用 `retract_udf`。
- `retract_udf` 返回 `null` 时删除状态；对不存在状态 retract 或 UDF 非法返回值立即失败。
- UPDATE_BEFORE/UPDATE_AFTER 可能因新旧 key 不同被 HASH 到不同下游 task，因此配对完整性
  在 Changelog 产生端验证，下游不能错误要求两个消息出现在同一通道。
- Map、KeyBy 和 Shuffle 保留 `change_kind`，不得吞掉或改写。
- 事件时间窗口关闭只输出最终模式结果或清理 changelog 模式状态，不额外发 DELETE，避免下游在同一窗口关闭前撤销全部最终贡献。
- File Sink 使用 columns 提取通用 CSV，覆盖旧默认格式和中级 count-distribution 格式。
- 增加 `changelog_emitted`、`retract_applied`、`retract_state_deleted` 日志与计数。

验收序列：

```text
apple count 1 -> INSERT(count=1)
apple count 2 -> UPDATE_BEFORE(count=1), UPDATE_AFTER(count=2)
pie count 1   -> INSERT(count=1)
最终分布       -> count=1 有 1 个 word；count=2 有 1 个 word
```

### 4. 实现版本化状态快照与停流协调 Checkpoint

新建：

- `src/pystream/checkpoint/__init__.py`
- `src/pystream/checkpoint/models.py`
- `src/pystream/checkpoint/store.py`
- `src/pystream/control/checkpoint.py`
- `tests/unit/checkpoint/test_store.py`
- `tests/unit/control/test_checkpoint.py`

修改：

- `src/pystream/operators/base.py`
- `src/pystream/operators/window.py`
- `src/pystream/operators/connectors.py`
- `src/pystream/runtime/task.py`
- `src/pystream/control/ports.py`
- `src/pystream/control/manager.py`
- `src/pystream/control/http.py`
- `src/pystream/worker/models.py`
- `src/pystream/worker/manager.py`
- `src/pystream/worker/http.py`
- `src/pystream/service.py`

协议：

1. JobManager 为 RUNNING 作业生成递增 checkpoint_id。
2. 先按下游到上游对全部 Task 执行 `arm`，创建当前 attempt 的 Checkpoint context。
3. 再对 Source 执行 `trigger`：暂停消费，在已发 DATA 之后发送 `CHECKPOINT_DRAIN`。
4. 非 Source 对每个入通道封存 drain；收齐全部入通道后，前序数据必已处理，写本地 Task snapshot，然后向所有下游传播 drain。
5. Sink 收齐 drain 后 flush 并写无状态 snapshot。
6. JobManager 轮询 descriptor；在 timeout 内收齐全部当前 attempt descriptor 后复验并原子写 manifest。
7. manifest 完成后通知所有 Task `complete`；Source 提交对应 offset 并恢复消费，其他 Task 清除 drain context。
8. timeout/校验失败时 `abort`，删除/忽略当前不完整目录并恢复 Source；连续失败达到阈值后触发整作业恢复。

状态内容：

- Stateless Map/KeyBy/Sink：版本、attempt、checkpoint 和空状态。
- Source：每个已分配 topic/partition 的 next offset、max event time、last activity 和 last
  emitted watermark；Checkpoint 期间 assignment/generation 变化即 abort，不能提交混合分配快照。
- JobManager 在 manifest 中按 Source operator 合并所有 subtask 的 partition 状态并验证无重复/
  缺失；恢复后各 Source 等待新的 consumer assignment，只 seek 自己当前持有的 partition，
  因此 partition 即使换到另一 subtask 也使用同一 next offset 和 Watermark 基线。
- Reduce：所有 window/key、accumulator、representative envelope、current watermark、changelog mode。
- Runtime：每入通道 Watermark/idle 元数据；恢复后所有通道初始视为 active，避免持久化单调时钟绝对值。
- 未配置 `execution` 的旧作业继续采用发送后逐条 commit 和 fail-fast；启用 Checkpoint 的作业
  禁止逐条 commit，只在完整 manifest 之后提交该 Checkpoint 的精确 partition offsets。

### 5. 实现整作业自动恢复和 attempt fencing

修改：

- `src/pystream/control/models.py`
- `src/pystream/control/execution.py`
- `src/pystream/control/scheduler.py`
- `src/pystream/control/manager.py`
- `src/pystream/control/checkpoint.py`
- `src/pystream/control/ports.py`
- `src/pystream/control/http.py`
- `src/pystream/worker/models.py`
- `src/pystream/worker/manager.py`
- `src/pystream/worker/http.py`
- `src/pystream/runtime/protocol.py`
- `src/pystream/runtime/task.py`
- `tests/unit/control/test_models_execution.py`
- `tests/unit/control/test_manager.py`
- `tests/unit/worker/test_worker_manager.py`
- `tests/unit/worker/test_worker_http.py`
- `tests/integration/test_task_runtime.py`

状态机：

```text
RUNNING -> RECOVERING -> DEPLOYING -> RUNNING
RECOVERING/DEPLOYING -> RECOVERING   # 下一次 attempt
RECOVERING -> FAILING -> FAILED      # 重试耗尽
RUNNING/RECOVERING -> CANCELLING -> CANCELLED
```

行为：

- TaskInstance 增加 `attempt_id`、`restored_checkpoint_id`；JobRun 增加当前 attempt、最近完整 Checkpoint、恢复次数和最后失败。
- 任一 Task 失败、连接异常或 Worker 心跳超时只触发一次 recovery leader。
- Recovery 停止所有可达旧任务、abort 当前 Checkpoint、释放 slot、等待 delay 和足够健康 slot。
- Compose 负责拉起退出的 Worker 进程；Worker 重注册后可重新参与调度。
- ExecutionGraph 为新 attempt 重置 Task 状态并重新绑定端点，保持逻辑 task_id，所有 HELLO/部署/状态上报携带 attempt_id。
- WorkerManager 接受更高 attempt 时清理旧 Runtime；相同 attempt 重复部署幂等；较低 attempt 拒绝。
- 选择最高合法完整 manifest；无完整 Checkpoint 时从初始状态启动。
- Worker 在建立数据流之前完成 Operator/Source restore；Source open 后按 snapshot 显式 seek。
- 所有任务成功部署后才回到 RUNNING；重试耗尽后执行完整清理并 FAILED。
- cancel 在 CHECKPOINTING/RECOVERING 中都具有优先权，必须停止后续自动重试。

状态接口新增：

```json
{
  "attempt": 1,
  "recovery": {
    "max_attempts": 3,
    "last_failure": "...",
    "last_completed_at": "..."
  },
  "checkpoint": {
    "status": "COMPLETED",
    "last_completed_id": 2,
    "duration_ms": 123,
    "state_bytes": 4567
  }
}
```

### 6. 部署、演示、日志与文档

修改：

- `deploy/compose.yaml`
- `pyproject.toml`
- `Dockerfile`（仅在新增文件复制或目录权限需要时）
- `README.md`
- `docs/api.md`
- `docs/architecture.md`
- `docs/deployment.md`
- `docs/modules.md`
- `docs/roadmap.md`
- `docs/testing.md`
- `docs/troubleshooting.md`
- `tests/contract/test_documentation.py`
- `tests/contract/test_deployment_assets.py`
- `tests/contract/test_demo_scripts.py`

新增：

- `examples/intermediate/event_time_retract_udfs.py`
- `examples/intermediate/job.yaml`
- `scripts/_intermediate_demo.py`
- `scripts/produce_intermediate.py`
- `scripts/submit_intermediate.py`
- `scripts/wait_for_checkpoint.py`
- `scripts/inject_worker_failure.py`
- `scripts/verify_intermediate.py`
- `scripts/cleanup_intermediate.py`
- `reports/intermediate-acceptance.md`

部署：

- 新增共享 `pystream-checkpoints` volume，挂载到 JobManager、3 个 Worker 和 tools 的 `/data/checkpoints`。
- JobManager/Worker 增加 `--checkpoint-root`；保持只读根文件系统、非 root、cap drop 和资源限制。
- 项目与 Compose 镜像版本更新为 `0.2.0`；不创建 `v0.2.0` Git 标签，直到功能分支获准合并/发布。
- 保持 `restart: unless-stopped`，故障脚本使用 SIGKILL 终止实际 Worker 容器进程，不使用 `docker compose stop`。

中级 demo：

1. 两个 Kafka 分区分别写入 `apple@12:00:01`、`pie@12:00:04`。
2. 等待包含两条记录状态的完整 Checkpoint。
3. 根据作业状态定位承载 Reduce 的 Worker，SIGKILL 该 Worker。
4. 验证容器 restart count 增加，Worker 重注册，作业经历 `RECOVERING -> RUNNING`，attempt 增加且 restored checkpoint 正确。
5. 写入乱序但合法的 `apple@12:00:03`，再推进 Watermark 并让一个输入进入 idle。
6. 写入已迟到记录，验证它被丢弃并出现指标/结构化日志。
7. 验证窗口输出为 `count=1 -> 1 word`、`count=2 -> 1 word`，状态接口与日志证明 Retract、Checkpoint 和恢复。

### 7. Git 提交与远端回退点

建议提交序列：

1. `chore: establish intermediate development baseline`
2. `feat: add event-time windows and watermarks`
3. `feat: add retract changelog processing`
4. `feat: add coordinated at-least-once checkpoints`
5. `feat: recover jobs from completed checkpoints`
6. `test: add intermediate failure recovery acceptance`
7. `docs: document intermediate guarantees and evidence`

每次提交严格执行：

1. 更新 task/checklist 和 `reports/intermediate-development.md`。
2. 只暂存当前里程碑文件。
3. 查看 staged diff，执行对应专项测试和全量质量门。
4. 按 `agent-pr-review` 流程输出审查结果并记录 receipt。
5. 单独执行 commit，再推送 `feature/intermediate-v0.2`。
6. 在开发日志记录 commit SHA 和安全回退命令 `git revert <sha>`。

不使用 `git reset --hard`。完整退回初级版本使用 `git switch main` 或从 `v0.1.0` 新建分支；用户批准前不 merge、不改写 `main` 历史。

## Verification

### 每个里程碑

```powershell
python -m ruff check src tests
python -m ruff format --check src tests
python -m pytest --no-cov <本里程碑测试路径>
```

### 最终离线质量门

在 Python 3.11 容器/项目镜像中执行：

```powershell
python -m ruff check src tests
python -m ruff format --check src tests
python -m pytest
```

要求：

- 原有测试全部通过，不以删除/弱化断言解决回归。
- 新增代码覆盖事件时间边界、Watermark 合并/idle、Retract 顺序、snapshot round-trip、损坏 manifest、attempt fencing、重试耗尽和取消竞态。
- 总分支覆盖率 `>= 80%`，无 skip；Windows 特权相关 skip 必须在 Linux/Python 3.11 容器补跑。

### Docker 验收

1. 全新构建镜像并 `docker compose up -d`。
2. 验证 Kafka、JobManager、3 Worker 健康和 12 slots。
3. 先运行初级 WordCount，证明向后兼容。
4. 运行中级 event-time/retract demo。
5. 等待 Checkpoint 完成，保存 manifest 摘要、作业状态和关键日志。
6. SIGKILL 状态任务所在 Worker，保存容器重启、Worker 重注册、attempt、恢复 Checkpoint 和连接重建证据。
7. 继续生产乱序/迟到数据并验证最终 CSV、late metric、Retract 日志和无数据丢失。
8. 验证重试耗尽、Checkpoint timeout/abort、恢复期间 cancel。
9. 执行 `docker compose down` 与 `down -v` 两种清理路径，确认无项目容器/网络/卷残留。

### 日志验收

运行日志至少覆盖：

- `watermark_emitted`、`watermark_advanced`、`input_idle`、`late_record_dropped`
- `changelog_emitted`、`retract_applied`
- `checkpoint_started`、`source_paused`、`task_snapshot_written`、`checkpoint_completed`、`checkpoint_aborted`
- `recovery_started`、`state_restored`、`task_redeployed`、`recovery_completed`、`recovery_exhausted`

公共字段外增加 `attempt_id`、`checkpoint_id`、`duration_ms`、`state_bytes`、`records_in/out`；不得记录业务 payload、凭据或 GitHub token。

### 最终交付检查

- `.trae/specs/build-pystream-intermediate/tasks.md` 和 checklist 无未完成项。
- `reports/intermediate-development.md` 能逐提交追溯实施、测试、问题和回退方式。
- `reports/intermediate-acceptance.md` 包含环境、输入、输出、状态迁移、Checkpoint、故障和清理证据。
- `git status` 干净；功能分支已推送并跟踪远端。
- `v0.1.0` 仍指向 `24384c1`；`main` 仍指向初级基线。
- 不声明 Exactly-once，文档明确共享卷、单 JobManager和追加 Sink 的限制。

## Risks And Stop Conditions

| 风险 | 控制 | 停止条件 |
| --- | --- | --- |
| 停流 Checkpoint 长时间阻塞 | timeout、状态大小指标、abort/resume | Source 无法恢复或在途数据无法证明排空 |
| idle 输入恢复后数据变迟到 | 文档、metric、可配置 idle timeout | Watermark 回退或已关闭窗口被重新打开 |
| 旧 attempt 污染新运行 | HELLO/HTTP/snapshot 全链路 attempt fencing | 任一旧连接/上报被接受 |
| 共享卷损坏/不完整 | manifest-last、SHA、schema、任务全集 | 损坏快照仍被选为恢复源 |
| File Sink 重复 | 明确 At-least-once、验收按“不丢失”而非“无重复” | 文档或 UI 误宣称 Exactly-once |
| 范围膨胀到高级阶段 | 保留事务 Sink/BARRIER 非启用 | 需要非停流对齐或事务提交才能继续时重新规划 |
