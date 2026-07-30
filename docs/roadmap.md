# 阶段语义与后续路线

本文区分已实现能力和未来承诺。中级范围权威来源为
`.trae/specs/build-pystream-intermediate/spec.md`。

## v0.1.0 初级基线

- YAML DAG、UDF 作业包、slot 调度。
- Kafka Source、Map、KeyBy、处理时间 Reduce、File Sink。
- FORWARD/REBALANCE/跨 Worker HASH。
- 有界队列和 fail-fast。

初级基线由 Git tag `v0.1.0` 固定。

## v0.2.0 中级：已实现

- RFC3339 事件时间、有限乱序 Watermark、多输入 min/idle。
- 事件时间滚动窗口和迟到记录丢弃。
- Changelog/Retract 二级聚合。
- DATA/CONTROL 协议 v2 与 attempt fencing。
- 停流协调 Checkpoint。
- Kafka next offset、Watermark 和 Reduce 状态 snapshot/restore。
- 版本化 manifest-last、损坏回退。
- Worker incarnation、自动拉起、整作业重调度。
- At-least-once：输入无丢失，追加 Sink 允许故障边界重复。

### 已实证语义

受控 Worker 业务进程 SIGKILL 后：

1. 容器 RestartCount 增加并以新 incarnation 注册。
2. 作业 attempt/recovery attempts 增加。
3. 全部 Task 从同一完整 Checkpoint 恢复。
4. Kafka 两 partitions 最终 lag=0。
5. Checkpoint 前 baseline 精确一次。
6. Checkpoint 后 recovery 窗口发生允许的重复。

因此当前可以声明 At-least-once，不能声明 Exactly-once。

## 明确未实现

| 能力 | 当前边界 |
|---|---|
| 持续流 barrier 对齐 | 当前 Checkpoint 会 pause Source 并 drain 全图 |
| 事务 Sink | File Sink 普通追加和 flush |
| Exactly-once | 故障恢复可产生重复可见行 |
| JobManager HA | 单实例控制面 |
| 多副本 Checkpoint Store | 单共享命名卷 |
| 安全多租户 | 无认证、TLS、UDF 沙箱或租户隔离 |

## 后续候选

### v0.3 一致性与可用性

- 持续流 barrier 对齐和对齐超时。
- 事务/幂等 Sink，按 Checkpoint 预提交与原子发布。
- 无故障与故障输出逐记录一致的 Exactly-once 证明。
- Checkpoint Store 抽象到具备冗余的持久存储。
- JobManager 元数据恢复或 HA 设计。

### 运维增强

- 固定 Python 依赖 lock 和离线 wheelhouse，使镜像构建可复现。
- 指标导出、SLO 和恢复时间趋势。
- Checkpoint 暂停时间、状态大小和背压容量基准。
- 认证、TLS、secret 管理与 UDF 隔离。

## 能力声明门槛

| 声明 | 必需证据 |
|---|---|
| 基础分布式 | 多 Worker、跨 Worker HASH、正确 WordCount |
| At-least-once | Worker 故障恢复、统一恢复点、lag=0、允许重复 |
| 系统内 Exactly-once | 状态与 offset 一致恢复，逻辑状态不重复生效 |
| 端到端 Exactly-once | 事务 Sink，最终输出无丢失无重复 |

接口预留、枚举存在或单元测试通过都不足以升级能力声明；必须有完整运行证据并同步
更新 README、API、架构、部署、测试和排障文档。
