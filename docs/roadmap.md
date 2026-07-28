# 阶段语义与后续路线

本文用于区分“当前已实现”与“规划能力”，防止接口预留被误解为一致性承诺。
权威范围来自 `.trae/specs/build-pystream-engine/spec.md`。

## 第一阶段：已实现

- 严格 YAML DAG、UDF 作业包、并发度和 slot 调度。
- Kafka JSON Source、Map、KeyBy、Reduce 处理时间滚动窗口、File Sink。
- FORWARD、REBALANCE、跨节点 HASH Shuffle。
- 有界队列和 TCP drain 背压。
- Worker 心跳、任务/连接失败上报、整作业 fail-fast。
- 结构化日志、健康/状态端点、离线自动化测试。

### 当前交付语义

第一阶段发生故障时：

1. 失败任务报告 JobManager。
2. JobManager 停止其他任务、释放 slots。
3. 作业进入 `FAILED`。
4. 系统不自动重启任务、不恢复内存状态、不回退到一致 Kafka offset。

Kafka Consumer 关闭自动提交并不等于 At-least-once。File Sink 每条记录追加并
flush 也不等于 Exactly-once。重新提交可能重放输入并产生重复文件行。

## 已预留但未启用的接口

| 接口/字段 | 当前行为 | 未来用途 |
|---|---|---|
| `RecordEnvelope.event_time` | null | 事件时间 |
| `MessageType.WATERMARK` | 不作为当前数据语义处理 | Watermark |
| `MessageType.BARRIER` | 不作为当前数据语义处理 | Checkpoint 对齐 |
| `change_kind` | 固定 INSERT | Retract changelog |
| `checkpoint_id` | null | 快照归属 |
| `snapshot_state/restore_state` | 抛 UnsupportedStateOperation | 算子状态快照/恢复 |
| Sink begin/pre-commit/commit/abort | 抛 UnsupportedStateOperation | 事务文件提交 |

预留枚举或方法不代表功能通过测试。只有后续 change-id 的任务与 checklist 全部
完成后，文档才能将其标记为已实现。

## 第二阶段：规划

独立 change-id 将实现：

- 从配置字段提取事件时间。
- 有限乱序 Watermark：`max_event_time - max_out_of_orderness`。
- 事件时间滚动窗口和迟到记录策略。
- INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE Retract。
- 周期状态快照和 Kafka offset 快照。
- Worker 失联后的自动重新部署、连接重建和状态恢复。
- At-least-once：不丢失，但允许故障边界重放和重复输出。

第二阶段开始前必须先验证现有 RecordEnvelope、Channel、Operator 生命周期和
Sink 扩展边界的兼容性。

## 第三阶段：规划

独立 change-id 将实现：

- 对齐 Checkpoint Barrier。
- Kafka offset 与算子状态的一致快照。
- 最近完成 Checkpoint 恢复。
- File Sink 每 Checkpoint 临时文件、预提交、原子提交和 abort 清理。
- 故障注入下与无故障基准逐记录一致的端到端 Exactly-once 证明。

只有系统内状态不重复而 File Sink 仍重复，不能称为端到端 Exactly-once。

## 能力声明门槛

| 声明 | 必需证据 |
|---|---|
| 基础分布式 | 多 Worker 状态、跨 Worker 通道、正确 WordCount |
| At-least-once | Worker 故障后自动恢复，固定输入无丢失，允许重复 |
| 系统内 Exactly-once | 状态与 offset 一致恢复，逻辑状态无重复生效 |
| 端到端 Exactly-once | 事务 Sink，最终可见输出无丢失无重复 |

每个阶段上线后必须同步更新 README、架构、API、部署、测试和排障文档，并将本文件
相应能力从 planned 改为 implemented。
