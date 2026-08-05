# PyStream v0.3.0 最终验收

## 结论

Milestone 7 于 2026-08-05 在 `feature/advanced-v0.3` 完成。Core 与 HA Docker
E2E、Linux/Python 3.11 全量质量门、Exactly-once output diff、Kafka lag、安全
负面路径、Prometheus 和资源清理均通过。

候选镜像：

```text
pystream:0.3.0
sha256:8c9d3581807f4419cf5776cbc3590e61452afd1d4b38720bd68016609f29ca8c
Python 3.11.9
```

## Core

Core 先通过基础 WordCount 和显式 At-least-once 中级回归，再对同一固定 Kafka
输入执行三个 Exactly-once 场景。

| 场景 | Attempt | 恢复 | Committed diff | Kafka lag |
|---|---:|---:|---:|---:|
| baseline | 0 | 不适用 | 0 | 0 |
| `before_barrier` Worker SIGKILL | 1 | 13.870s | 0 | 0 |
| `before_decision` Worker SIGKILL | 1 | 15.598s | 0 | 0 |

可见行只来自 output manifest 引用并通过 identity/SHA/size 校验的 fragments：

```text
2026/07/29T00:00:05,1,1
2026/07/29T00:00:05,2,1
```

最终容器、网络、卷均为 0，临时 PKI/Secret 目录已删除。

## HA

HA 拓扑包含 4 个双盘 MinIO、S3 HAProxy、2 JobManagers、3 Workers、Kafka、
JobManager HAProxy 和 Prometheus。

1. 初始 ACTIVE=1、STANDBY=1。
2. 无/错 Token、错 CA、错误内部证书身份均被拒绝。
3. checkpoint 1 在 `after_decision` gate 停止，active 在 FINALIZED 前退出。
4. standby 25.828 秒接管，coordinator epoch 从 1 增至 2，并完成 finalize。
5. 单 MinIO 节点退出后 28.988 秒完成 checkpoint 2。
6. 降级存储下 Worker SIGKILL 后 19.755 秒恢复到 attempt 2。
7. checkpoint 3 FINALIZED；最终 manifests 为 1、2、3。

最终结果：

```text
JobManager takeover SLO: 25.828s <= 30s
Worker recovery SLO:     19.755s <= 60s
Committed output diff:   0
Kafka lag:               0
Prometheus targets:      5
Prometheus rules:        8
Docker resources:        0
Temporary secrets:       removed
```

## 质量门

```text
Linux Python:      3.11.9
pytest:            487 passed, 0 skipped
branch coverage:   83.01%
Ruff check:        passed
Ruff format:       147 files
git diff --check:  passed
Secret scan:      0 matches
```

真实 MinIO integration 在隔离测试网络中运行，没有使用 skip 豁免。

## 证据

- `reports/advanced-core-evidence.json`
- `reports/advanced-core-acceptance.log`
- `reports/advanced-core-runtime.log`
- `reports/advanced-ha-evidence.json`
- `reports/advanced-ha-acceptance.log`
- `reports/advanced-ha-runtime.log`
- `reports/core-before_barrier-worker-failure.json`
- `reports/core-before_decision-worker-failure.json`
- `reports/ha-active-jobmanager-failure.json`
- `reports/ha-minio-failure.json`
- `reports/ha-worker-failure.json`
- `reports/advanced-python311-tests.log`

## 声明边界

本验收不证明 Kafka broker HA、Docker 主机容灾、File output volume 容灾、跨机房
部署或不可信 UDF 隔离。Python 依赖仍按在线版本范围解析，镜像不是 byte-for-byte
可复现构建。

## Git 治理

- Milestone 7 使用独立 commit。
- `main` 不合并。
- 现有 `v0.3.0` tag 不移动。
- 回退方式：`git revert <milestone-7-sha>`。
