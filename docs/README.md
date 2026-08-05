# 文档索引与维护规则

## 文档清单

| Doc | Diátaxis quadrant | Responsibility path | Source of truth | Last verified | Verification cadence | Staleness signal |
|---|---|---|---|---|---|---|
| `README.md` | tutorial | 项目维护者；首次入口 | 包版本、CLI、验收脚本 | 2026-08-05 | 每里程碑 | 能力/命令变化 |
| `docs/architecture.md` | explanation | 架构维护者 | control/runtime/checkpoint | 2026-08-05 | 模块边界变化 | 拓扑/语义变化 |
| `docs/modules.md` | explanation | 各模块修改者 | `src/pystream/` | 2026-08-05 | 顶层模块变化 | 新模块或依赖变化 |
| `docs/api.md` | reference | API 修改者 | models/protocol/client | 2026-08-05 | 公共契约变化 | 字段/端点变化 |
| `docs/deployment.md` | how-to；operational | 部署执行者 | Compose/acceptance scripts | 2026-08-05（Core/HA 通过） | 部署资产变化 | E2E 失效 |
| `docs/testing.md` | how-to | 开发者 | `pyproject.toml`、tests | 2026-08-05 | 质量门变化 | 命令/门槛变化 |
| `docs/performance.md` | reference | 性能验证者 | benchmark/report | 2026-07-27 | 每阶段 | 参数/环境变化 |
| `docs/troubleshooting.md` | how-to；operational | 运行人员 | 状态、日志、evidence | 2026-08-05 | 新故障变化 | 错误/处理变化 |
| `docs/roadmap.md` | explanation | 规格维护者 | 已批准 spec/evidence | 2026-08-05 | 阶段开始/结束 | 声明与实现不一致 |

## 读者与任务

- 验收者：从根 README 进入，运行 Core/HA 并核对结构化 evidence。
- 作业开发者：阅读 API，编写 YAML 和可信 Python UDF。
- 引擎维护者：阅读架构与模块文档，按依赖方向修改代码。
- 运行人员：使用部署、排障、健康端点、指标和 JSON 日志。

## 权威来源

**每类事实只能有一个权威位置。重复内容必须改成链接，或明确标记为摘要；不得维护
两份可独立修改的命令、字段表或运行语义。**

| 信息 | 权威来源 |
|---|---|
| 当前代码行为 | `src/pystream/` 与自动化测试 |
| YAML/UDF 契约 | API models、artifact UDF loader |
| 数据协议 | common records、runtime protocol |
| Checkpoint/事务 schema | `src/pystream/checkpoint/` |
| 部署拓扑 | `deploy/compose.advanced.yaml` 与 Dockerfile |
| 验收流程 | `scripts/run_advanced_acceptance.py` |
| 原始运行证据 | `reports/advanced-*-evidence.json` |
| 当前/未来边界 | 高级 spec 与 `docs/roadmap.md` |
| 测试门槛 | `pyproject.toml` |

## 当前状态核对

| 主题 | 当前事实 | 验证 |
|---|---|---|
| 数据语义 | aligned Barrier + transactional manifest Exactly-once | Core/HA fault diff=0 |
| 控制面 | active/passive JobManager + epoch fencing | 25.828s takeover |
| 存储 | S3 metadata/checkpoint，4 MinIO 可失去 1 节点 | fault checkpoint 通过 |
| 安全 | HTTPS/Bearer、mTLS、Kafka SSL、file Secret | 正/负路径测试 |
| 可观测性 | 5 HA targets、8 SLO rules | Prometheus API evidence |
| 兼容性 | 无 execution 基础路径、显式 At-least-once | Core regression |
| 剩余边界 | 单 Kafka、单 Docker 主机、单 output volume、可信 UDF | roadmap |

部署或恢复逻辑变化后，“Core/HA 已通过”立即失效，必须重跑 E2E 并更新 image ID、
UTC、时延和验收报告。

## 新鲜度规则

- 代码变更必须同步检查权威文档。
- `Last verified` 早于对应验证周期时文档视为 stale。
- 部署失败、故障演练新发现、接口字段、依赖或阶段能力变化触发当次更新。
- 历史设计如仍有价值应移到归档并标注 `ARCHIVED`，不能留在操作入口。

## Docs-as-code

```powershell
.\.venv\Scripts\python -m pytest --no-cov tests/contract/test_documentation.py
.\.venv\Scripts\python -m ruff check src tests scripts
.\.venv\Scripts\python -m ruff format --check src tests scripts
```

`test_documentation.py` 检查入口、模块 docstring、关键语义和相对链接。Docker 文档的
最终可用性由完整 Core/HA 演示验证。

## 可发现性

从根 README 两步内应能找到：

1. 安装与质量门。
2. Core/HA 启动和验收。
3. YAML/UDF 与 Exactly-once 输出读取契约。
4. Worker/JM/MinIO/TLS/Checkpoint 排障。
5. At-least-once 兼容路径与 Exactly-once 已实证边界。
6. Kafka、主机、output volume 和 UDF 的剩余风险。

需要口头说明才能完成任一项，视为文档缺陷。
