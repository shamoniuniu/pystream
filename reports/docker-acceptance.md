# Docker 验收记录（v0.1.0 历史基线）

> 本文件保留初级阶段历史证据。当前 0.2.0 中级验收见
> `reports/intermediate-acceptance.md`，不得用本文件判断当前恢复能力。

## 验收结论

PyStream 基础阶段的 Python 3.11、Docker 多容器、Kafka、跨 Worker
Shuffle、WordCount、取消、Worker 故障和资源清理均已实际验证通过。
第一阶段只提供失败检测，不提供自动恢复或一致性恢复语义。

## 环境

- Docker Desktop 4.83.0
- Docker Engine 29.6.2，Linux/amd64
- Python 3.11.9
- Kafka 3.9.1，KRaft 模式
- 1 个 JobManager、3 个 Worker、12 个 slots

## 质量门槛

基于当前工作树重建 `pystream-test:py311` 后执行：

```powershell
docker run --rm pystream-test:py311 python -m pystream --help
docker run --rm pystream-test:py311 ruff check .
docker run --rm pystream-test:py311 ruff format --check .
docker run --rm pystream-test:py311 python -m pytest
```

结果为 284 passed、0 skipped、行覆盖率 86.42%。Linux 符号链接安全
用例实际 PASS；CLI、Ruff 和格式检查全部通过。

## WordCount E2E

- 作业 ID：`5ef33f9eeb1749509ee52e0656c37753`
- Kafka 分区 offset：分区 0 为 1/1，分区 1 为 2/2，lag 均为 0
- 输出：

```csv
2026/07/27T15:31:20,pie,1
2026/07/27T15:31:20,apple,2
```

任务分布到全部 3 个 Worker。关键映射为：

- `by_word:0` 位于 worker-2
- `totals:0` 位于 worker-1
- `totals:2` 位于 worker-3

运行日志包含 worker-2 到 worker-1、worker-2 到 worker-3 的
`connection_opened` 事件，证明 KeyBy 到 Reduce 的 HASH 通道跨 Worker。

首次 E2E 暴露了 Kafka consumer rebalance 后重复消费：Source 禁用自动提交，
但 TaskRuntime 未在成功发送后手动提交 offset。修复后重新构建镜像并复测，
结果从错误的 `apple=4,pie=2` 恢复为预期的 `apple=2,pie=1`。

## 生命周期与故障

- 取消上述运行作业后状态为 `CANCELLED`，10 个 tasks 停止并释放 10 slots。
- 故障作业 ID 前缀为 `e9c73`；停止 worker-2 后 3 秒内进入 `FAILED`。
- Worker 故障期间 JobManager 健康，最后错误可查询，所有 slots 最终释放。
- worker-2 重启后重新注册，恢复为 0/4 slots 使用。
- `docker compose down` 后项目容器和网络为 0，6 个命名卷按文档保留。
- `docker compose down -v` 后项目容器、网络和命名卷均为 0。

## 运行时姿态

| 工作负载 | CPU 保留/上限 | 内存保留/上限 | 超限行为 |
|---|---:|---:|---|
| JobManager | 0.10 / 0.50 | 128M / 512M | CPU 节流；内存超限终止，作业控制面失败 |
| Worker | 0.10 / 0.50 | 128M / 512M | CPU 节流；内存超限导致作业 FAILED |
| Kafka | 0.25 / 1.00 | 512M / 1G | CPU 节流；内存超限导致 broker 不可用 |

JobManager 和 Worker 使用非 root 用户、只读根文件系统、`cap_drop: ALL`
和 `no-new-privileges`。健康探针区分服务可用性，`depends_on` 约束 Kafka、
Topic 初始化、JobManager 与 Worker 的启动顺序。停止宽限为 20 秒；
取消路径先停止任务和连接，再释放 slots。第一阶段遇到 Worker 丢失时明确
将作业标记为 `FAILED`，不宣称自动恢复、At-least-once 或 Exactly-once。

## 可重复命令

```powershell
docker compose -f deploy/compose.yaml up -d --build
python scripts/produce_wordcount.py
python scripts/submit_wordcount.py
python scripts/wait_for_window.py
python scripts/verify_wordcount.py
docker compose -f deploy/compose.yaml down
docker compose -f deploy/compose.yaml down -v
```

证据刷新规则：修改 Dockerfile、Compose、运行时、Kafka Source、调度或
WordCount 脚本后，应重新执行 Python 3.11 容器测试和完整 E2E，并更新本记录。
