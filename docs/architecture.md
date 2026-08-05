# 架构与数据流

本文描述 PyStream 0.3.0 的控制流、数据流、Exactly-once 和 HA 边界。代码与测试是
行为权威来源。

## 部署拓扑

```text
CLI / acceptance tools
  | HTTPS + Bearer Token
  v
HAProxy :8080 ----- leader-only routing
  |                              Prometheus
  +-> JobManager A/B <------------ mTLS scrape
       | active/passive lease
       | metadata/artifact/checkpoint
       v
  S3 HAProxy -> MinIO 1..4 (2 drives each)
       |
       +-- mTLS control + coordinator epoch
       v
  Worker 1..3 <==== TLS data plane ====>
       |
       +-- Kafka SSL source
       +-- transactional File output volume
```

Core profile 使用单 JobManager、单 MinIO、3 Workers；HA profile 使用两个
JobManager 和 4 节点 MinIO。Kafka 和 File output 仍是单故障域。

## 部署与调度

1. CLI 构建带清单和 SHA-256 的 ZIP。
2. active JobManager 校验制品并不可变写入 artifact repository。
3. 逻辑算子按 parallelism 展开，调度器先做全量 slot 预检。
4. 算子之间保持下游优先部署；同一算子的 subtasks 并发部署。
5. Worker 按 attempt/epoch 隔离 Runtime 和 UDF 命名空间。
6. Source 按 `partition % parallelism == subtask_index` 确定性分配 Kafka partition。

## 数据面与背压

FORWARD 保持 subtask 编号，REBALANCE 轮询下游，HASH 对规范 JSON key 的 SHA-256
取模。协议 v2 将 DATA 与 CONTROL 分离；同一通道严格保序，HELLO 携带 attempt
和 coordinator epoch，旧连接被 fencing。

出通道和 Runtime 使用有界队列。Barrier 到达某输入后，连接 handler 等待该输入
gate，而不是无界缓存 post-barrier DATA；压力通过 TCP 和上游有界队列反向传播。

## 事件时间

Source 从 RFC3339 字段提取 event time：

```text
watermark = max_seen_event_time - max_out_of_orderness
```

多输入 Runtime 对活跃输入取 Watermark 最小值；idle 输入暂时退出计算，重新 active
后不能使全局 Watermark 回退。事件时间窗口在 Watermark 到达 window end 时触发。

## Aligned Checkpoint

Exactly-once 路径使用持续流 Barrier：

1. active JobManager 为当前 attempt/epoch 的所有 Task arm。
2. Source 短暂停顿，冻结每 partition 的 next offset 和时间状态。
3. Source 把 BARRIER 排在全部前序 DATA 后广播并立即恢复消费。
4. 输入收到 BARRIER 后停止读取该通道的 post-barrier 帧。
5. Task 继续处理其他未对齐输入的 pre-barrier DATA。
6. 全部输入对齐后 snapshot；Sink 同时 pre-commit 当前事务。
7. Task 转发 BARRIER 并解除所有输入 gate。
8. Coordinator 复验 task set、identity、SHA、size 和事务 descriptor。

显式 At-least-once 作业继续使用 v0.2 的 pause + DRAIN 路径。

## 事务输出与决定

File Sink 事务生命周期：

```text
ACTIVE -> PREPARED -> COMMITTED
ACTIVE/PREPARED -> ABORTED
```

Checkpoint 状态：

```text
ARMED -> ALIGNING -> PREPARED -> DECIDED -> FINALIZING -> FINALIZED
```

- `decision.json` 之前失败：abort 当前事务并从前一决定点恢复。
- `decision.json` 之后失败：决定不可逆，只能由当前或接管 leader 幂等 finalize。
- fragment 完成后才原子发布 output manifest。
- 读取方只读取 manifest 引用并通过 identity/SHA/size 校验的 committed fragments。
- Source offset、算子状态、Sink fragment 和 output manifest 属于同一 checkpoint。

## 持久元数据

S3 compatible repositories 持久化：

```text
pystream/artifacts/<sha256>.zip
pystream/control/leader.json
pystream/jobs/<job_id>/revisions/<revision>.json
pystream/jobs/<job_id>/current.json
pystream/checkpoints/<job_id>/<checkpoint>/decision.json
pystream/checkpoints/<job_id>/<checkpoint>/finalized.json
```

不可变对象使用 `If-None-Match: *`；leader/current 指针使用 ETag `If-Match` CAS。
每次恢复重新读取 durable latest manifest，不能让 takeover 缓存使
`last_decided`/`last_finalized` 回退。

## JobManager HA

- lease TTL 10 秒、renew 3 秒、standby poll 1 秒。
- 任意时刻只有一个可写 active；HAProxy 仅路由 `/health/leader` ready 的实例。
- 新 leader CAS 接管后 epoch 严格增加，先完成 DECIDED 未 FINALIZED，再恢复作业。
- Worker 保存最高 epoch，拒绝旧 epoch 的部署、状态和控制请求。
- 失去 lease 的 active 进入 protective/standby，不继续写控制状态。

最终 HA 验收在 checkpoint 1 DECIDED、FINALIZED 前终止 active；standby 25.83 秒
接管并完成 finalize。单 MinIO 节点退出后 checkpoint 2 成功，随后 Worker 故障从
checkpoint 2 恢复并完成 checkpoint 3，最终 committed output diff=0。

## 信任边界

- 外部管理入口：HTTPS + Bearer Token。
- HAProxy、JobManager、Worker HTTP 与 Worker 数据面：mTLS 和证书身份校验。
- Kafka：SSL client certificate。
- S3：TLS + access key file Secret。
- Prometheus：内部证书或独立 metrics token。
- Secret 只从文件读取，不进入日志、状态响应或验收报告。
- UDF 仍是可信进程内代码。

## 故障域

| 故障 | 已实证行为 |
|---|---|
| Worker 业务进程 SIGKILL | 整作业恢复，attempt 增加，Exactly-once output diff=0 |
| active JobManager 退出 | standby 接管，epoch 增加，完成不可逆 finalize |
| 一个 MinIO 节点退出 | artifact/metadata/checkpoint 继续读写 |
| Kafka broker 退出 | 不承诺 HA，输入不可用 |
| Docker 主机退出 | 不承诺跨主机容灾 |
| File output volume 丢失 | 不承诺输出卷容灾 |
