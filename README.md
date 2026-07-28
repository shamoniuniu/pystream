# PyStream

PyStream 是一个用于课程实践的简易分布式流计算系统。项目使用 Python 3.11，
以 YAML 描述 DAG，以 Python 函数描述 Map、KeyBy 和 Reduce 逻辑，并在独立
JobManager、Worker 和 Kafka 容器之间完成调度、网络 Shuffle 与窗口聚合。

> 当前状态：第一阶段基础功能已实现。事件时间、Watermark、Retract、自动恢复、
> At-least-once 和 Exactly-once 仅有扩展边界，**尚未实现，也不应作为当前能力
> 对外宣称**。

## 已实现能力

| 评分能力 | 实现 |
|---|---|
| 作业 API 与 DAG | 严格 `pystream/v1` YAML、拓扑校验、分支/合流、每算子并发度 |
| 逻辑分发 | CLI 打包带 SHA-256 清单的 ZIP，Worker 下载、安全解压和隔离加载 UDF |
| 调度 | JobManager 将逻辑算子展开为物理任务，进行全量 slot 预检和均衡放置 |
| Shuffle | 跨 Worker TCP 长连接；FORWARD、REBALANCE、稳定 HASH |
| 算子 | Kafka JSON Source、Map、KeyBy、Reduce、处理时间滚动窗口、CSV File Sink |
| 背压 | 有界输入/输出队列与 `writer.drain()` |
| 失败行为 | 任务、连接或 Worker 失败时整作业进入 `FAILED`，不自动恢复 |
| 可观测性 | JSON 日志、健康端点、Worker/任务状态和连接/队列/记录指标 |
| 示例 | 大小写不敏感的分布式 WordCount |

## 目录

```text
src/pystream/          引擎实现
examples/wordcount/    YAML 与 Python UDF 示例
deploy/compose.yaml    Kafka、JobManager、3 Worker 集群
scripts/               演示、验证和清理脚本
tests/                 契约、单元和 loopback 集成测试
docs/                  架构、模块、API、部署、测试与排障文档
```

## 本地开发

要求 Python 3.11。也可以用 Conda 创建 Python 3.11 环境，再由 pip 安装项目。

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev]"
.\.venv\Scripts\python -m pystream --help
.\.venv\Scripts\python -m pytest
.\.venv\Scripts\python -m ruff check src tests
.\.venv\Scripts\python -m ruff format --check src tests
```

当前开发机若只有 Python 3.13，可用于离线测试；正式容器仍固定 Python 3.11.9。

## Docker 快速演示

要求 Docker Desktop 已启动并使用 Linux 容器。完整流程和结果解释见
[部署与验证](docs/deployment.md)。

```powershell
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/produce_wordcount.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/submit_wordcount.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/wait_for_window.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/verify_wordcount.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/cleanup_wordcount.py
docker compose -f deploy/compose.yaml down
```

示例输入为：

```json
{"word":"APPLE","count":1}
{"word":"pie","count":1}
{"word":"apple","count":1}
```

10 秒演示窗口的输出为无表头 CSV，行顺序不固定：

```csv
2026/07/26T12:00:10,apple,2
2026/07/26T12:00:10,pie,1
```

实际窗口结束时间由记录到达时刻决定。

## 文档导航

- [文档索引与维护规则](docs/README.md)
- [架构与数据流](docs/architecture.md)
- [模块说明](docs/modules.md)
- [YAML、UDF 与运行协议参考](docs/api.md)
- [Docker 部署与 WordCount 验证](docs/deployment.md)
- [测试与性能测量](docs/testing.md)
- [性能基线与复现](docs/performance.md)
- [故障排查](docs/troubleshooting.md)
- [阶段语义与后续路线](docs/roadmap.md)

## 当前限制

- 第一阶段没有 Checkpoint、状态恢复、自动重启和端到端事务。
- Kafka 使用手动 offset 模式，但第一阶段没有将 offset 与状态组成一致快照。
- File Sink 为普通追加写；故障重跑可能重复，不能称为 Exactly-once。
- JobManager 是单实例；不提供认证、TLS、多租户或不可信 UDF 沙箱。
- 多容器端到端验证必须在提供 Docker 的机器执行。

项目规格和验收标准位于
`.trae/specs/build-pystream-engine/`，课程原始题目文件保留在仓库根目录。
