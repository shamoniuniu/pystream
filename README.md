# PyStream

PyStream 是一个用于课程实践的分布式流计算系统。项目使用 Python 3.11，以 YAML
描述 DAG，以 Python UDF 描述计算逻辑，由 JobManager、Worker、Kafka 和 S3
兼容对象存储完成调度、Shuffle、事件时间、Checkpoint 与故障恢复。

> 当前版本：`0.3.0` 高级阶段。Core/HA Docker E2E 已实证端到端
> Exactly-once、JobManager 自动接管、单 MinIO 节点容错和安全控制面。

## 已实现能力

| 能力 | 实现 |
|---|---|
| 作业 API 与 DAG | 严格 `pystream/v1` YAML、拓扑校验、分支/合流、每算子并发度 |
| 调度与 Shuffle | slot 预检、均衡放置、FORWARD/REBALANCE/跨 Worker HASH |
| 事件时间 | RFC3339、有限乱序 Watermark、多输入 min/idle、窗口、迟到丢弃 |
| Changelog | INSERT/UPDATE_BEFORE/UPDATE_AFTER、retract UDF、二级聚合 |
| Checkpoint | 持续流 aligned Barrier、输入 gate、冻结 Kafka offset、状态快照 |
| Exactly-once | 事务 File Sink、PREPARED/DECIDED/FINALIZED、manifest-last 可见性 |
| 持久状态 | S3 artifact、Checkpoint、作业 revision/current pointer、ETag CAS |
| 高可用 | 双 JobManager lease、coordinator epoch fencing、接管与 finalize 恢复 |
| 安全 | 外部 HTTPS + Bearer、内部/数据面 mTLS、Kafka SSL、file-only Secret |
| 可观测性 | JSON 日志、Prometheus 指标、8 条 SLO 告警规则 |

## 目录

```text
src/pystream/             引擎实现
examples/wordcount/       基础处理时间 WordCount
examples/intermediate/    显式 At-least-once 事件时间作业
examples/advanced/        Exactly-once 事务输出作业
deploy/compose.advanced.yaml  Core/HA 高级拓扑
scripts/                  提交、故障注入、验证和清理脚本
tests/                    契约、单元、集成和安全测试
reports/                  开发、测试和 Docker 验收证据
docs/                     API、架构、部署、测试与排障文档
```

## 本地开发

目标环境为 Python 3.11：

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev]"
.\.venv\Scripts\ruff check .
.\.venv\Scripts\ruff format --check .
.\.venv\Scripts\python -m pytest
```

pytest 默认启用 branch coverage，门槛为 80%。宿主机其他 Python 版本只能用于补充
验证，最终门禁在 Linux/Python 3.11 中执行。

## Docker E2E 验收

要求 Docker Desktop 使用 Linux containers。脚本会生成临时 PKI/Secret、启动
固定镜像、执行故障演练、保存结构化证据，并确认容器、网络、卷和 Secret 清零。

```powershell
.\scripts\run_advanced_core_acceptance.ps1 `
  -PythonCommand .\.venv\Scripts\python.exe

.\scripts\run_advanced_ha_acceptance.ps1 `
  -PythonCommand .\.venv\Scripts\python.exe
```

最终实测：

| 场景 | 结果 |
|---|---|
| Core `before_barrier` Worker 故障 | 13.87 秒恢复，output diff=0 |
| Core `before_decision` Worker 故障 | 15.60 秒恢复，output diff=0 |
| HA DECIDED 后 active JobManager 退出 | 25.83 秒接管，epoch 1 -> 2 |
| 单 MinIO 节点退出后新 Checkpoint | 28.99 秒完成 |
| 降级存储下 Worker 故障 | 19.76 秒恢复，output diff=0 |
| 最终 Kafka/Prometheus/清理 | lag=0，5 targets，8 rules，资源=0 |

完整流程和证据解释见 [部署与验证](docs/deployment.md)。

## 文档导航

- [文档索引与维护规则](docs/README.md)
- [架构与数据流](docs/architecture.md)
- [模块说明](docs/modules.md)
- [YAML、UDF 与运行协议参考](docs/api.md)
- [Docker 部署与验收](docs/deployment.md)
- [测试与性能测量](docs/testing.md)
- [性能基线与复现](docs/performance.md)
- [故障排查](docs/troubleshooting.md)
- [阶段语义与后续路线](docs/roadmap.md)

## 语义边界

- `execution` 存在时默认 `exactly_once`；显式 `at_least_once` 保留 v0.2 DRAIN 和
  append Sink 兼容路径；缺少 `execution` 时保持基础 fail-fast 行为。
- Exactly-once 读取方只把 output manifest 引用且通过 SHA/size 校验的 committed
  fragments 视为可见结果，不能扫描 pending/committed 目录推断真值。
- 本地 HA 可承受一个 JobManager、一个 MinIO 节点或一个 Worker 进程退出。
- Kafka 仍是单 broker；全部容器位于单 Docker 主机；File output 是单共享卷。
- Python UDF 是可信代码，不提供不可信代码沙箱、动态扩缩容或跨主机容灾。

高级规格和验收标准位于 `.trae/specs/build-pystream-advanced/`。`v0.3.0` tag
仍固定在 Milestone 5，不因本次验收移动；功能回退使用 `git revert <sha>`。
