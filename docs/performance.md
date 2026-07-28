# 性能基线

本文记录基础阶段的可复现性能测量。性能数据用于同环境回归比较，不是跨机器的
通过门槛。原始结构化结果位于 `reports/offline-benchmark.json`。

## 测量范围

离线基准使用真实的 Map、KeyBy、HASH Shuffle 路由、处理时间滚动窗口
Reduce、REBALANCE 和 CSV File Sink。它不启动 Kafka、TCP 数据通道、Docker
或 JobManager，因此不能替代多容器端到端性能测试。

延迟从每条记录创建开始，到其所属 key 的窗口结果经过合成窗口触发并刷新到 CSV
为止。它不包含真实 5 分钟窗口等待。较早进入同一窗口的记录会等待本轮其他输入，
所以该指标同时反映批次规模和本机算子处理成本。

## 复现命令

```powershell
.\.venv\Scripts\python scripts\benchmark.py `
  --records 50000 `
  --partitions 2 `
  --map-parallelism 2 `
  --key-parallelism 2 `
  --reduce-parallelism 3 `
  --sink-parallelism 1 `
  --window-seconds 300 `
  --word-cardinality 100 `
  --report reports\offline-benchmark.json
```

所有整数参数必须大于 0。`word-cardinality` 不能超过输入记录数。脚本在输出速度
前先校验 CSV 聚合计数、输出行数和延迟样本数；任一不守恒都会以非零状态失败。

## 当前基线

测量时间为 2026-07-27 UTC，运行环境为 Windows 11、12 逻辑 CPU、约 31.6 GiB
内存、Python 3.13.12。项目目标运行时仍是 Python 3.11；此结果只作为当前开发机
离线基线。

| 指标 | 结果 |
|---|---:|
| 输入/完成记录 | 50,000 / 50,000 |
| 输出窗口行 | 100 |
| 总耗时 | 5.959428 s |
| 吞吐 | 8,390.07 records/s |
| p50 延迟 | 2,787.685 ms |
| p95 延迟 | 5,745.805 ms |
| 错误/丢弃 | 0 / 0 |
| Reduce 分布 | 20,000 / 16,500 / 13,500 |

Docker 和 Compose 在本机不可用，原始报告中的版本字段为 `null`。安装 Docker
后还需要单独运行 Kafka、JobManager、3 Worker 的端到端测量，并保存网络、容器
资源和 Kafka 分区条件，不能把本离线结果当作集群吞吐。
