# 阶段语义与后续路线

本文区分已实现能力和未来承诺。高级范围权威来源为
`.trae/specs/build-pystream-advanced/spec.md`。

## v0.1.0 基础

- YAML DAG、UDF 作业包、slot 调度。
- Kafka Source、Map、KeyBy、处理时间 Reduce、File Sink。
- FORWARD/REBALANCE/跨 Worker HASH。
- 有界队列和 fail-fast。

## v0.2.0 中级

- 事件时间 Watermark、多输入 min/idle、滚动窗口和迟到丢弃。
- Changelog/Retract 二级聚合。
- 停流 DRAIN Checkpoint、状态/offset snapshot。
- Worker incarnation、attempt fencing 和自动恢复。
- At-least-once：输入无丢失，append Sink 在故障边界允许重复。

该路径在 0.3 中继续由显式 `delivery_guarantee: at_least_once` 保留。

## v0.3.0 高级：已实现并验收

- 持续流 aligned Barrier 与 per-input gate。
- Kafka frozen offset、算子状态和事务 Sink 同 checkpoint。
- PREPARED、DECIDED、FINALIZED 不可逆两阶段提交。
- output manifest-last 可见性和幂等 finalize。
- S3 artifact、Checkpoint 和 JobManager metadata revisions。
- 4 节点 MinIO 的单节点故障容忍。
- 双 JobManager lease、epoch fencing、protective step-down 和自动接管。
- 外部 HTTPS/Bearer、内部/数据面 mTLS、Kafka SSL、file-only Secret。
- Prometheus 指标和 8 条 SLO rules。

### 已实证语义

固定 Kafka 输入在以下场景的最终 manifest-visible output 多重集与无故障 baseline
完全一致：

1. `before_barrier` Worker SIGKILL。
2. PREPARED 后、`before_decision` Worker SIGKILL。
3. DECIDED 后、FINALIZED 前 active JobManager 退出。
4. 一个 MinIO 节点退出后继续 checkpoint 读写。
5. 接管并降级存储后再次 Worker SIGKILL。

所有场景最终 Kafka lag=0，committed output diff=0。接管 25.828 秒，组合故障下
Worker 恢复 19.755 秒，分别满足 30/60 秒 SLO。

## 兼容语义

| 配置 | 行为 |
|---|---|
| 无 `execution` | v0.1 fail-fast、逐条 offset commit |
| `execution.delivery_guarantee: at_least_once` | v0.2 DRAIN + append Sink |
| `execution` 且未声明 guarantee | 默认 Exactly-once |
| `delivery_guarantee: exactly_once` | Barrier + transaction + decision/finalize |

因此项目同时保留 At-least-once 兼容路径和已实证的端到端 Exactly-once 路径。

## 明确不承诺

| 能力 | 当前边界 |
|---|---|
| Kafka broker HA | 本地只有一个 broker |
| 跨主机容灾 | 所有容器位于单 Docker 主机 |
| File output volume HA | committed fragments/manifest 位于单共享卷 |
| Active-active JobManager | 当前为单 active、单 standby |
| 多租户/不可信 UDF | UDF 是可信进程内代码 |
| 动态扩缩容/状态重分片 | parallelism 在提交时固定 |
| byte-reproducible image | Python 依赖仍按在线范围解析 |

本地 4 MinIO 节点证明单节点数据服务容错，不等于跨机房或跨地域存储容灾。

## 后续候选

### 发布治理

- 在用户批准后决定是否把高级分支合并到 `main`。
- 只有明确发布操作才创建或移动发布 tag；当前 `v0.3.0` 不移动。
- 固定 Python lock/wheelhouse 和镜像构建 provenance。

### 可用性扩展

- Kafka 3 broker 与分区副本故障演练。
- 跨主机 Worker/JobManager/MinIO 编排。
- 把事务 output manifest/fragments 迁移到冗余对象存储。
- 证书在线轮换与长期 SLO 趋势。

### 流处理扩展

- Savepoint、动态扩缩容和状态重分片。
- Join、CEP、SQL 和 schema evolution。
- UDF 子进程/容器沙箱与资源配额。

## 能力声明门槛

| 声明 | 必需证据 |
|---|---|
| At-least-once | Worker 恢复、统一恢复点、lag=0、允许重复 |
| 系统内 Exactly-once | 状态与 offset 同 checkpoint、逻辑状态不重复 |
| 端到端 Exactly-once | 事务 Sink、manifest visibility、fault diff=0 |
| 控制面 HA | 单 active、接管 SLO、epoch fencing、decided finalize |
| 存储容错 | 单节点退出后 artifact/metadata/checkpoint 继续读写 |

接口预留、枚举存在或单元测试通过不足以升级能力声明；必须有 Docker E2E 证据和同步
文档。
