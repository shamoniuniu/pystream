# 测试与性能测量

测试命令和覆盖率门槛的权威来源是 `pyproject.toml`，行为证据位于 `tests/`。

## 质量门

目标解释器为 Python 3.11：

```powershell
.\.venv\Scripts\python -m ruff check .
.\.venv\Scripts\python -m ruff format --check .
.\.venv\Scripts\python -m pytest
git diff --check
```

pytest 默认启用 branch coverage，并要求 `src/pystream` 总覆盖率至少 80%。最终门禁
必须无 skip，并在 Linux/Python 3.11 中执行。

2026-08-05 最终结果：

```text
487 passed, 0 skipped
83.01% branch coverage
Ruff check passed
147 files formatted
```

## 测试分层

| 层 | 路径 | 证明内容 |
|---|---|---|
| 契约 | `tests/contract/` | YAML/DAG、示例、部署资产、文档、manifest reader |
| 单元 | `tests/unit/` | Barrier、事务、store、coordinator、HA、fencing |
| 安全 | `tests/security/` | TLS、Token、身份、Secret redaction |
| 集成 | `tests/integration/` | TCP/TLS、DATA/CONTROL、背压、多 Runtime、MinIO |
| Docker E2E | advanced acceptance scripts | 真实 Kafka/MinIO/JM/Worker 故障和 Exactly-once |

关键覆盖：

- API：默认 Exactly-once、显式 At-least-once、Sink capability。
- Runtime：per-input Barrier gate、快慢输入、timeout/abort/断连。
- Source：frozen offset、complete/abort、restore seek。
- Sink：pre-commit、decision、幂等 finalize、manifest-last。
- Checkpoint：task set/SHA/size、DECIDED/FINALIZED、损坏回退。
- Control：lease、双 contender、protective step-down、takeover、旧 epoch 拒绝。
- Manager：旧 takeover cache 单调恢复、同 operator 并发部署。
- Security：错 Token/CA/证书身份/过期证书和 Secret 不泄露。
- Scripts：只读取 verified manifest fragments、Kafka lag 和故障注入契约。

## 选择性运行

```powershell
.\.venv\Scripts\python -m pytest --no-cov tests/contract -q
.\.venv\Scripts\python -m pytest --no-cov tests/unit/control -q
.\.venv\Scripts\python -m pytest --no-cov tests/security -q
```

局部测试关闭 coverage 只用于快速定位；最终必须运行完整 pytest。

Windows 长路径可能导致 pytest 临时目录失败，可显式缩短：

```powershell
.\.venv\Scripts\python -m pytest --basetemp C:\t\pystream
```

## Python 3.11 Linux 门禁

候选镜像可挂载工作树执行：

```powershell
docker create --name pystream-python311-tests --user 0 `
  -v "${PWD}:/workspace" -w /workspace pystream:0.3.0 `
  /bin/sh -lc "/opt/venv/bin/pip install -e '.[dev]' && /opt/venv/bin/python -m pytest"
docker start -a pystream-python311-tests
docker rm -f pystream-python311-tests
```

Ruff、format 与 full pytest 的最终结果记录在
`reports/advanced-m7-build-receipt.json` 和开发日志。

## Docker 故障验收

普通 pytest 不替代 Docker E2E。最终候选必须运行：

```powershell
.\scripts\run_advanced_core_acceptance.ps1 -PythonCommand .\.venv\Scripts\python.exe
.\scripts\run_advanced_ha_acceptance.ps1 -PythonCommand .\.venv\Scripts\python.exe
```

必须保存：

- 三个确定性提交窗口的 checkpoint phase/attempt/epoch。
- JobManager 接管和 Worker 恢复耗时。
- 单 MinIO 节点退出后的新 checkpoint。
- manifest-visible rows 与 baseline diff=0。
- Kafka committed/end/lag=0。
- 5 个 Prometheus JobManager/Worker targets 和 8 条 rules。
- 最终 Docker 资源与临时 Secret 为 0。

2026-08-05 Docker 最终结果：

```text
Core: 13.870s / 15.598s Worker recovery, diff=0, lag=0
HA: 25.828s JM takeover, 19.755s Worker recovery, diff=0, lag=0
```

## 性能测量

离线算子基准：

```powershell
.\.venv\Scripts\python scripts\benchmark.py `
  --records 50000 --partitions 2 `
  --map-parallelism 2 --key-parallelism 2 `
  --reduce-parallelism 3 --sink-parallelism 1 `
  --window-seconds 300 --word-cardinality 100 `
  --report reports\offline-benchmark.json
```

该基准不包含 Kafka、TLS、Docker、S3、Checkpoint 或恢复，不能替代多容器 SLO。
结果解释见 [性能基线](performance.md)。
