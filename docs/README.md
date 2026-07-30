# 文档索引与维护规则

## 文档清单

| Doc | Diátaxis quadrant | Responsibility path | Source of truth | Last verified | Verification cadence | Staleness signal |
|---|---|---|---|---|---|---|
| `README.md` | tutorial | 项目维护者；首次运行入口 | `pyproject.toml`、`deploy/compose.yaml`、CLI | 2026-07-30 | 每次里程碑 | 安装、命令或实现阶段变化 |
| `docs/architecture.md` | explanation；architectural | 架构维护者；理解控制流和数据流 | `src/pystream/control`、`runtime`、`worker` | 2026-07-30 | 每次模块边界变化 | 拓扑、协议、失败语义变化 |
| `docs/modules.md` | explanation | 各模块修改者；定位责任 | `src/pystream/` | 2026-07-30 | 每次顶层模块变化 | 新模块缺失或依赖方向变化 |
| `docs/api.md` | reference | API 修改者；编写作业 | Pydantic 模型、协议模型、CLI parser | 2026-07-30 | 每次公共契约变化 | YAML、UDF、CLI 或帧字段变化 |
| `docs/deployment.md` | how-to；operational | 部署执行者；启动和验收 | Dockerfile、Compose、`scripts/` | 2026-07-30（Docker E2E 通过） | 每次部署资产变化 | 镜像、端口、卷、脚本变化或 E2E 失败 |
| `docs/testing.md` | how-to | 开发者；执行质量门 | `pyproject.toml`、`tests/` | 2026-07-30 | 每次质量命令变化 | CI/本地命令或门槛变化 |
| `docs/performance.md` | reference | 性能验证者；复现离线基线 | `scripts/benchmark.py`、`reports/offline-benchmark.json` | 2026-07-27 UTC | 每次里程碑或性能路径变化 | 参数、测量范围或基线环境变化 |
| `docs/troubleshooting.md` | how-to；operational | 部署执行者；定位失败 | 健康/状态 API、JSON 日志事件 | 2026-07-30 | 每次故障处理变化 | 错误类型、日志字段或端点变化 |
| `docs/roadmap.md` | explanation | 规格维护者；判断当前语义边界 | 已批准规格、状态/Sink 扩展接口 | 2026-07-30 | 每阶段开始/结束 | 实现能力与阶段标签不一致 |

## 读者与任务

- 课程验收者：从根 README 进入，按部署文档运行 WordCount，按测试文档核对证据。
- 作业开发者：阅读 API 参考，编写 YAML 和可信 Python UDF。
- 引擎维护者：阅读架构与模块文档，按依赖方向修改代码。
- 运行人员：使用部署和排障文档、健康端点、状态接口及 JSON 日志。

## 权威来源

**每类事实只能有一个权威位置。重复内容必须改成链接，或明确标记为非权威摘要；
不得维护两份可独立修改的命令、字段表或运行语义。**

| 信息 | 权威来源 |
|---|---|
| 当前代码行为 | `src/pystream/` 与自动化测试 |
| YAML/UDF 契约 | `src/pystream/api/models.py`、`artifact/udf.py` |
| 数据协议 | `common/records.py`、`runtime/protocol.py` |
| 部署拓扑 | `deploy/compose.yaml` 与 `Dockerfile` |
| 可执行演示步骤 | `scripts/`，`docs/deployment.md` 负责串联 |
| 性能测量与原始基线 | `scripts/benchmark.py`、`reports/offline-benchmark.json` |
| 当前/未来语义边界 | 已批准规格与 `docs/roadmap.md` |
| 测试门槛 | `pyproject.toml` |

根 README 只提供入口和短路径。字段级细节只在 API 参考维护；排障文档引用事件名，
不复制协议实现。

## 当前状态核对

| 主题 | 当前事实 | 验证 |
|---|---|---|
| 拓扑 | 单 JobManager、3 Worker、单 Kafka KRaft Broker | Docker E2E 2026-07-30 通过 |
| 依赖 | Python 3.11、aiohttp、aiokafka、Pydantic、PyYAML | `pyproject.toml`、Dockerfile |
| 路由 | FORWARD、REBALANCE、规范 JSON + SHA-256 HASH | 路由单测、loopback 集成测试 |
| 恢复 | Worker 新 incarnation 触发整作业 Checkpoint 恢复 | 单元测试与 SIGKILL E2E |
| 中级能力 | Watermark、Retract、Checkpoint、At-least-once 已实现 | `docs/roadmap.md` 与验收报告 |
| 规划能力 | 事务 Sink、Exactly-once、JobManager HA 尚未实现 | `docs/roadmap.md` |

部署资产变化后，当前“完整通过”状态立即失效，必须重跑 E2E 并更新日期、镜像
身份和验收报告。

## 新鲜度规则

- 每个相关代码变更必须同步检查其权威文档；每个里程碑执行一次全量文档核对。
- 若 `Last verified` 早于对应验证周期，或权威代码/配置已改变但文档未同批更新，
  文档立即视为 `stale`。
- 部署失败、故障演练发现新步骤、接口字段变化、依赖升级、阶段能力上线，均触发
  当次更新，不等待周期检查。
- 与当前实现冲突的文档应直接修正；历史设计如仍有价值，移动到归档目录并在标题
  标注 `ARCHIVED`，不得留在操作入口。

## Docs-as-code

文档与对应代码在同一变更中提交，提交记录提供追踪关系。每次文档变更至少执行：

```powershell
.\.venv\Scripts\python -m pytest --no-cov tests/contract/test_documentation.py
.\.venv\Scripts\python -m ruff check src tests
.\.venv\Scripts\python -m ruff format --check src tests
```

`test_documentation.py` 检查入口文件、模块覆盖、关键命令/限制和相对链接。Markdown
链接由测试解析，不依赖外部网络。Docker 部署文档的可用性最终由完整演示流程验证。

## 可发现性与可用性

从干净克隆开始，读者应在两步内从根 README 找到以下内容：

1. 如何安装与运行测试。
2. 如何启动集群并验证 WordCount。
3. 如何编写 YAML/UDF。
4. 如何定位 Worker、任务、连接和窗口问题。
5. 当前为什么具备 At-least-once、但不具备 Exactly-once。

若新读者只能依靠口头说明完成其中任一任务，应视为文档缺陷并在交付前修复。
