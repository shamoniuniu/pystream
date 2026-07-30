# PyStream

PyStream 是一个用于课程实践的简易分布式流计算系统。项目使用 Python 3.11，
以 YAML 描述 DAG，以 Python UDF 描述计算逻辑，并由独立 JobManager、Worker
和 Kafka 容器完成调度、Shuffle、窗口聚合、Checkpoint 与故障恢复。

> 当前版本：`0.2.0` 中级阶段。已实现并实证 At-least-once；未实现
> Exactly-once、事务 Sink 或 JobManager HA。

## 已实现能力

| 能力 | 实现 |
|---|---|
| 作业 API 与 DAG | 严格 `pystream/v1` YAML、拓扑校验、分支/合流、每算子并发度 |
| 逻辑分发 | 带 SHA-256 清单的 ZIP，Worker 安全解压并隔离加载可信 UDF |
| 调度与 Shuffle | slot 预检、均衡放置、FORWARD/REBALANCE/跨 Worker HASH |
| 事件时间 | RFC3339 提取、有限乱序 Watermark、多输入 min/idle、滚动窗口、迟到丢弃 |
| Changelog | INSERT/UPDATE_BEFORE/UPDATE_AFTER、retract UDF、二级聚合 |
| Checkpoint | 停流 drain、版本化 JSON 快照、manifest-last、Kafka offset/Watermark/算子状态 |
| 自动恢复 | Worker incarnation、attempt fencing、整作业重调度、最近完整 Checkpoint 恢复 |
| 一致性 | Kafka 输入无丢失；故障边界允许追加 File Sink 重复，即 At-least-once |
| 可观测性 | JSON 日志、健康/状态端点、恢复/Checkpoint/队列/记录指标 |

## 目录

```text
src/pystream/             引擎实现
examples/wordcount/       初级处理时间 WordCount
examples/intermediate/    事件时间 + Retract 二级聚合
deploy/compose.yaml       Kafka、JobManager、3 Worker 集群
scripts/                  生产、提交、故障注入、验证和清理脚本
tests/                    契约、单元和 loopback 集成测试
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

宿主机其他兼容版本只能作为补充验证，最终门禁必须使用 Python 3.11。

## Docker E2E 验收

要求 Docker Desktop 使用 Linux containers。标准验收脚本使用固定资源名和
resource ledger，不依赖本机 Compose 插件的运行稳定性：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts\run_intermediate_acceptance.ps1 `
  -PythonCommand .\.venv\Scripts\python.exe
```

脚本依次验证：

1. 初级 WordCount 兼容性。
2. 中级事件时间与 Retract 输出。
3. 显式完整 Checkpoint。
4. 单 Worker 业务进程 `SIGKILL`、容器自动拉起和新 incarnation 注册。
5. 整作业恢复、attempt 递增和统一恢复点。
6. 恢复后 Checkpoint、Kafka lag=0、无输入丢失和允许的 Sink 重复。
7. 容器、网络和命名卷全部清零。

详细命令和证据解释见 [部署与验证](docs/deployment.md)。

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

- Checkpoint 是停流协调：Source 暂停，整图 drain 完成后才写 manifest；其暂停
  时间会直接增加输入处理延迟。
- Checkpoint 位于共享命名卷，允许 Worker 间恢复，但共享卷和单 JobManager
  仍是单故障域。
- File Sink 为普通追加写。恢复会重放最近完整 Checkpoint 之后的数据，因此允许
  重复行，不是 Exactly-once。
- Kafka Source 在多并发时按 `partition % parallelism == subtask` 确定性分配；
  Source parallelism 不能超过 topic partition 能力。
- 不提供认证、TLS、多租户、不可信 UDF 沙箱或 JobManager HA。

中级规格和验收标准位于 `.trae/specs/build-pystream-intermediate/`。初级基线由
Git tag `v0.1.0` 固定，可使用 `git revert` 回退中级提交。
