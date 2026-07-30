# 测试与性能测量

测试命令和覆盖率门槛的权威来源是 `pyproject.toml`，行为证据位于 `tests/`。

## 质量门

目标解释器为 Python 3.11：

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev]"
.\.venv\Scripts\ruff check .
.\.venv\Scripts\ruff format --check .
.\.venv\Scripts\python -m pytest
```

pytest 默认启用 branch coverage，并要求 `src/pystream` 总覆盖率至少 80%。

2026-07-30 最终本地门禁：

```text
372 passed
84.40% total coverage
108 files formatted
Ruff check passed
```

完整日志：`reports/intermediate-python311-tests.log`。

## 测试分层

| 层 | 路径 | 证明内容 |
|---|---|---|
| 契约 | `tests/contract/` | YAML/DAG、演示脚本、部署资产、文档和公共约束 |
| 单元 | `tests/unit/` | Watermark、Retract、Store、Coordinator、恢复、fencing |
| 离线集成 | `tests/integration/` | TCP loopback、DATA/CONTROL 保序、背压、多 Runtime |
| Docker E2E | `run_intermediate_acceptance.ps1` | Kafka、3 Worker、Checkpoint、SIGKILL、恢复和 lag |

关键测试：

- `test_job_api.py`：execution/event-time/checkpoint/restart 与 DAG 能力传播。
- `test_connectors.py`：事件时间、Watermark、replay skip、确定性 partition 分配。
- `test_operators.py`：窗口、late record、changelog/retract 和状态 round-trip。
- `test_checkpoint.py` / `test_store.py`：停流顺序、manifest-last、损坏回退。
- `test_task_runtime.py`：CONTROL、多输入 Watermark、snapshot/restore。
- `test_manager.py`：自动恢复、attempt fencing、取消竞态、Source 并发部署。
- `test_demo_scripts.py`：At-least-once 多重集、显式 Checkpoint、Kafka lag 验证。
- `test_deployment_assets.py`：固定镜像、非 root 卷权限、原生 Docker 编排。

## 选择性运行

```powershell
.\.venv\Scripts\python -m pytest -o addopts="" tests/contract -q
.\.venv\Scripts\python -m pytest -o addopts="" tests/unit/operators -q
.\.venv\Scripts\python -m pytest -o addopts="" tests/unit/control -q
```

局部测试关闭默认覆盖率参数，避免未加载模块造成假失败；最终必须运行完整 pytest。

## Python 3.11 容器门禁

宿主机只有其他 Python 版本时，可以在候选镜像中挂载工作树：

```powershell
docker create --name pystream-python311-tests --user 0 `
  -v "${PWD}:/workspace" -w /workspace pystream:0.2.0 `
  /bin/sh -lc "/opt/venv/bin/pip install -e '.[dev]' && /opt/venv/bin/python -m pytest"
docker start pystream-python311-tests
docker inspect --format "{{.State.Status}} {{.State.ExitCode}}" pystream-python311-tests
docker logs pystream-python311-tests
docker rm -f pystream-python311-tests
```

Windows 长路径可能使宿主测试失败；Linux/Python 3.11 容器结果是发布门禁。

## Docker 故障验收

按 [部署文档](deployment.md)运行标准脚本。至少保存：

- 初级 WordCount 正确输出与跨 Worker HASH 证据。
- baseline 与 recovery 输出多重集。
- Checkpoint 1/2 状态。
- RestartCount、StartedAt、incarnation、attempt/recovery attempts。
- 所有 Task 的同一 restored checkpoint。
- Kafka committed/end offset 与 lag=0。
- `intermediate_acceptance=passed` 和 `compose_project_resources=0`。

普通 pytest 不替代 Docker E2E。

## 性能测量

默认离线基准：

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

该基准不包含 Kafka、TCP、Docker、Checkpoint 和恢复，不能替代多容器性能或暂停
时间测量。结果解释见 [性能基线](performance.md)。
