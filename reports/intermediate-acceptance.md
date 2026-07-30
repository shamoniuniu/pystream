# PyStream 中级验收报告

## 结论

2026-07-30，PyStream 0.2.0 中级功能在本机 Linux Docker 环境完整通过。验收覆盖
初级兼容、中级事件时间/Retract、停流 Checkpoint、Worker SIGKILL、自动拉起、
整作业恢复、At-least-once、Kafka lag 和资源清理。

## 环境与制品

- 分支：`feature/intermediate-v0.2`
- 源码起点：`5c01f4e6fec44dc9d802220421cfe8996eb41a88` + content-addressed 工作树
- Docker Engine：29.6.2
- Python：3.11.9
- Kafka：3.9.1 KRaft
- 验收镜像：`sha256:8ba4586871f237379ba40fa1e68b4106816852333e52d5866269902fa9b1e9ae`
- 构建输入 manifest：`0337e7f09fa993ffb18ec5224c18995e89177154`

镜像只作为本地验收候选；Python 依赖仍按版本范围在线解析，不声明字节级可复现
或对外发布。

## 自动化门禁

| 门禁 | 结果 |
|---|---|
| Ruff lint | 通过 |
| Ruff format | 108 files formatted |
| Python 3.11 pytest | 372 passed |
| Branch coverage | 84.40% |
| 初级 WordCount E2E | 通过 |
| 中级 baseline/recovery E2E | 通过 |
| Worker SIGKILL/自动恢复 | 通过 |
| 最终 Docker 资源 | 0 |

## 业务结果

中级作业 ID：

```text
ba4225fb878840a7aacd818d86e85dc0
```

Checkpoint 前 baseline 各出现一次：

```text
2026/07/29T00:00:05,1,1
2026/07/29T00:00:05,2,1
```

故障边界 recovery 窗口各出现两次：

```text
2026/07/29T00:00:10,1,1
2026/07/29T00:00:10,2,2
```

每行额外一次是追加 File Sink 在恢复重放时允许的重复。

## 故障与恢复证据

- 目标 Worker：`worker-1`
- 容器：`pystream-worker-1-1`
- RestartCount：`0 -> 1`
- StartedAt：发生变化
- incarnation：发生变化
- 作业 attempt：`0 -> 1`
- recovery attempts：`0 -> 1`
- 故障前完整 Checkpoint：1
- 全部恢复 Task 的 restored checkpoint：1
- 恢复后完整 Checkpoint：2

故障使用容器内：

```sh
kill -9 $(cat /proc/1/task/1/children)
```

该方式终止 tini 的业务子进程，使 `restart: unless-stopped` 执行自动拉起。

## Kafka 与一致性

| Partition | Committed | End | Lag |
|---:|---:|---:|---:|
| 0 | 6 | 6 | 0 |
| 1 | 4 | 4 | 0 |

- 输入丢失：否
- Sink 重复：是，且只发生在 Checkpoint 后 recovery 窗口
- 当前语义：At-least-once
- Exactly-once：否

## 运行和清理

验收脚本输出：

```text
intermediate_acceptance=passed
compose_project_resources=0
```

完整证据：

- `reports/intermediate-acceptance.log`
- `reports/intermediate-runtime.log`
- `reports/intermediate-failure-evidence.json`
- `reports/intermediate-python311-tests.log`
- `reports/intermediate-build-inputs.txt`
- `reports/intermediate-build-receipt.json`

## 限制

- Checkpoint 暂停 Source 并 drain 全图，暂停时间会增加输入延迟。
- checkpoint 共享卷与单 JobManager 是单故障域。
- File Sink 非事务，恢复允许重复。
- 未实现 Exactly-once、事务 Sink、持续流 barrier 对齐或 JobManager HA。
