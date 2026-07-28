# 测试与性能测量

本文面向开发者和验收者。测试命令与覆盖率门槛的权威来源是
`pyproject.toml`，具体行为证据位于 `tests/`。

## 安装开发环境

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev]"
```

目标解释器是 Python 3.11。开发机上的其他兼容版本只能作为补充验证，不能替代
Python 3.11 容器验证。

## 质量门

```powershell
.\.venv\Scripts\python -m ruff check src tests
.\.venv\Scripts\python -m ruff format --check src tests
.\.venv\Scripts\python -m pytest
```

`pytest` 默认启用分支覆盖并要求 `src/pystream` 总行覆盖率不低于 80%。任一命令
失败都阻止基础阶段验收。

## 测试分层

| 层 | 路径 | 证明内容 |
|---|---|---|
| 契约 | `tests/contract/` | YAML/DAG、部署资产、文档入口和公共约束 |
| 单元 | `tests/unit/` | 制品、UDF、协议、路由、算子、控制面、Worker、CLI |
| 离线集成 | `tests/integration/` | TCP loopback、背压、多 Runtime WordCount、异常 EOF |
| Docker 集成 | 部署文档流程 | Kafka、JobManager、3 Worker、共享卷和跨容器 Shuffle |

离线集成测试使用本机 loopback 端口和 fake Kafka consumer，不需要 Docker。
Docker 集成不能由 fake 替代。

## 关键证据

- `test_job_api.py`：合法线性/分支/合流 DAG 与非法配置路径。
- `test_artifact.py`：摘要、路径穿越、链接、超限、UDF 隔离。
- `test_protocol.py`：半包、粘包、帧上限、身份和记录契约。
- `test_data_channel.py`：发送顺序、有界队列和慢消费者背压。
- `test_operators.py`：窗口边界、跨窗口、空窗口、状态清理。
- `test_connectors.py`：坏记录策略、record_id、File Sink 分片与错误传播。
- `test_task_runtime.py`：Map -> HASH -> Reduce -> Sink loopback 和异常 EOF。
- `test_manager.py`：资源预检、部署回滚、Worker 超时、作业失败。
- `test_deployment_assets.py`：固定镜像、Compose 服务与演示脚本。

## 选择性运行

```powershell
.\.venv\Scripts\python -m pytest --no-cov tests/contract
.\.venv\Scripts\python -m pytest --no-cov tests/unit
.\.venv\Scripts\python -m pytest --no-cov tests/integration
.\.venv\Scripts\python -m pytest --no-cov tests/integration/test_task_runtime.py -q
```

聚焦运行使用 `--no-cov`，避免全仓 80% 门槛把未加载模块计为未覆盖；最终验收仍
必须运行不带 `--no-cov` 的完整 `pytest`。

## Windows 符号链接

安全解压测试包含 ZIP 符号链接拒绝。在未开启开发者模式且无创建符号链接权限的
Windows 上，该单个测试可能显示 `skipped`；其他路径穿越与链接元数据测试仍必须
通过。Linux/Python 3.11 容器应执行完整测试。

## Docker 验收

按 [部署文档](deployment.md)执行完整流程。最少保存：

- `docker compose ps`
- JobManager `/health` 和 `/v1/workers`
- 作业状态 JSON
- `verify_wordcount.py` JSON 输出
- JobManager/Worker 结构化日志
- 停止一个 Worker 后的 `FAILED` 状态与 slot 释放

当前仓库没有把 Docker E2E 纳入普通 pytest，因为执行环境可能不提供 Docker。
正式验收机器必须单独执行，不能把静态 Compose 测试当成运行通过。

## 性能测量口径

基础阶段不设硬编码吞吐门槛。离线基准使用真实 Map、KeyBy、HASH、ReduceWindow
和 File Sink，并记录：

```text
timestamp
OS / CPU / memory
Python / Docker / Compose version
Kafka partitions
Source/Map/KeyBy/Reduce/Sink parallelism
window size
input records
completed records
duration seconds
throughput records/s
p50 latency ms
p95 latency ms
error/drop count
```

性能结果必须同时证明输入/输出记录正确，不能只报告速度。不同机器结果不可直接
作为通过/失败线；同一环境回归时应保留参数和原始报告。

运行默认 50,000 条基准：

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

结果解释、当前基线和范围限制见 [性能基线](performance.md)。该离线基准不包含
Kafka、TCP、Docker 和调度，不能替代多容器端到端性能测试。

## 结果解释

- `records_out` 是发送到所有逻辑分支的总次数，分支图中可能大于输入条数。
- `max_output_queue_depth <= output_queue_capacity` 是有界队列不变量。
- 同 key 的记录只应进入一个 Reduce subtask。
- 多 key 输出跨通道没有稳定全局顺序，验证应比较集合或按 key/window 排序。
- 第一阶段故障测试预期作业失败，而非恢复后继续运行。
