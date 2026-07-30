# 架构与数据流

本文描述 PyStream 0.2.0 的控制流、数据流、Checkpoint 和恢复边界。代码与测试是
行为权威来源。

## 部署拓扑

```text
CLI / acceptance tools
          |
          v
JobManager :8080 ---------------- shared artifacts/checkpoints volumes
  | deploy/status/checkpoint
  +--------------------+--------------------+
  v                    v                    v
Worker-1             Worker-2             Worker-3
:8081 control        :8081 control        :8081 control
:9000 data           :9000 data           :9000 data
  \_____________________|____________________/
                        |
                    Kafka :9092
                        |
                 shared output volume
```

JobManager 持有作业状态、执行图、Worker 资源、Checkpoint coordinator 和恢复
状态机。Worker 持有 TaskRuntime、算子状态和数据面连接。Kafka 是可重放输入，
共享 checkpoint 卷允许任务恢复到不同 Worker。

## 部署与调度

1. CLI 构建带清单和 SHA-256 的 ZIP。
2. JobManager 复验、安全解压并构建 `StreamGraph`。
3. 逻辑算子按 parallelism 展开，调度器先做全量 slot 预检。
4. 非 Source 任务按下游优先顺序部署。
5. 同一 Source 的 subtasks 并发部署，并按 partition 编号确定性静态分配。
6. Worker 下载同一制品，按 attempt 隔离 Runtime 和 UDF 命名空间。

Source 确定性分配消除了正常部署时消费组 rebalance 导致的跨 Runtime 重放：

```text
partition % source_parallelism == subtask_index
```

## 数据面

```text
Kafka Source(2)
  -> Map normalize(2)        FORWARD
  -> KeyBy word(2)           FORWARD
  => Reduce word_totals(2)   HASH + changelog
  -> Map bucket(1)
  -> KeyBy count(1)
  -> Reduce distribution(1)  retract + final
  -> File Sink(1)
```

- FORWARD：相同 subtask 编号。
- REBALANCE：轮询下游。
- HASH：规范 JSON key 的 SHA-256 对下游并发度取模。
- 分支：每条逻辑边独立发送。
- 合流：多个物理输入共享有界队列，但保留独立 Watermark/Checkpoint 输入状态。

协议 v2 将数据和控制帧分离。同一通道内严格保序，WATERMARK 和
CHECKPOINT_DRAIN 广播全部物理出通道。HELLO 携带 attempt，旧连接被拒绝。

## 背压

出通道和目标 Runtime 均使用有界队列：

```text
下游 input queue 满
  -> TCP 接收变慢
  -> writer.drain 等待
  -> output queue 满
  -> 上游 send 等待
```

系统不以无限缓冲隐藏过载。Worker 健康和任务状态公开队列深度、峰值、批次和
记录计数。

## 事件时间与 Watermark

Source 从 RFC3339 字段提取 event time，并按 partition 维护：

```text
watermark = max_seen_event_time - max_out_of_orderness
```

Runtime 对多输入取活跃输入 Watermark 最小值。idle 输入暂时退出 min 计算；
重新 active 时不能让全局 Watermark 回退。Reduce 在 Watermark 到达 window end
时触发事件时间窗口；`event_time <= watermark` 的迟到记录丢弃并记录指标/日志。

## Changelog 与 Retract

上游 changelog Reduce 在状态变化时发送 UPDATE_BEFORE/UPDATE_AFTER。Map、
KeyBy 和 Shuffle 保留 change kind。下游 Reduce 对旧值执行 retract，对新值
执行 add；retract 返回 null 时删除空状态。中级示例由此实现“单词计数分布”的
二级聚合。

## 停流 Checkpoint

Checkpoint 是 stop-the-world 协调，不是持续流 barrier 对齐：

1. JobManager 串行分配 checkpoint ID。
2. 所有 Task arm，Source pause。
3. Source 在已有 DATA 后广播 CHECKPOINT_DRAIN。
4. 多输入 Runtime 收齐全部当前 attempt 输入的 DRAIN。
5. Task 写版本化 JSON snapshot；Source 保存 partition next offset、事件时间和
   Watermark，Reduce 保存窗口/聚合/输入 Watermark。
6. JobManager 收齐执行图全部 descriptor，复验 SHA/大小/身份并最后原子写
   manifest。
7. manifest 成功后 Source 才提交 offset，所有 Task complete 并恢复消费。

任一超时或失败执行 abort；没有完整 manifest 的目录不能恢复。快照写入期间 Source
暂停，因此 Checkpoint 时间直接增加端到端延迟。

## 整作业恢复

Worker 进程启动生成唯一 incarnation。同一 worker ID 出现新 incarnation 时：

1. JobManager 将受影响的中级作业转入恢复流程。
2. 取消周期 Checkpoint，停止旧 attempt，释放全图 slots。
3. 选择最高合法完整 manifest；不存在时从初始状态恢复。
4. attempt 递增并重新调度全图。
5. Task 在建立输出连接前恢复状态；Source 按 partition seek。
6. 全部 Task RUNNING 后恢复周期 Checkpoint。

恢复部署失败会在 `max_attempts` 内重试；旧 attempt 的连接、状态上报、stop 和
Checkpoint 请求均被 fencing。状态 API 持久暴露 recovery attempts，客户端不必
依赖采到短暂的 RECOVERING 状态。

## 语义与故障域

| 失败/边界 | 行为 |
|---|---|
| Worker 业务进程 SIGKILL | 容器自动拉起，新 incarnation 触发整作业恢复 |
| Kafka/算子状态 | 从同一完整 manifest 恢复，无输入丢失 |
| File Sink | 普通追加写，Checkpoint 后故障可重放并产生重复 |
| 损坏/不完整快照 | 忽略并回退前一合法 manifest |
| JobManager 失败 | 无 HA；控制面和内存中的作业协调状态不可用 |
| 共享 checkpoint 卷失败 | 所有 Worker 的恢复源同时不可用 |

因此当前语义为 At-least-once，不是 Exactly-once。共享卷解决跨 Worker 可见性，
但共享卷和单 JobManager 仍是单故障域。

## 信任边界

- 控制面位于可信实验网络，无认证和 TLS。
- Python UDF 是可信代码，不提供沙箱。
- ZIP 防止路径穿越、符号链接、超限和摘要不匹配。
- Kafka payload、UDF 输出、snapshot 和 manifest 都经过结构/大小校验。
- File Sink 路径按 job/operator/subtask 隔离。
