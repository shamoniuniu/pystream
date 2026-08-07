<div align="center">

# PyStream

**使用 Python 构建的教学型分布式流处理引擎**

YAML DAG · Kafka · 事件时间 · Exactly-once · JobManager HA · S3/MinIO · mTLS

[快速开始](#快速开始) · [核心能力](#核心能力) · [系统架构](#系统架构) ·
[运行验收](#运行验收) · [项目文档](#项目文档)

</div>

PyStream 使用 Python 3.11 实现一套可运行、可测试、可注入故障的分布式流计算系统。
作业通过 YAML 描述 DAG，通过 Python UDF 编写业务逻辑，由 JobManager、Worker、
Kafka 和 S3 兼容对象存储共同完成调度、Shuffle、事件时间计算、Checkpoint 和恢复。

当前代码版本为 `0.3.0`。Core/HA Docker E2E 已验证端到端 Exactly-once、
JobManager 自动接管、单 MinIO 节点故障恢复、安全控制面和 Prometheus 监控。

> [!NOTE]
> 这是面向教学、实验和架构验证的项目，不是生产级流处理平台。Kafka broker、
> Docker 主机和 File Sink 输出卷仍是单故障域。

## 核心能力

| 领域 | 已实现能力 |
|---|---|
| 作业模型 | 严格 `pystream/v1` YAML、DAG 校验、分支/合流、算子并行度 |
| 算子 | Kafka Source、Map、KeyBy、窗口 Reduce、Changelog/Retract、File Sink |
| 数据交换 | FORWARD、REBALANCE、跨 Worker HASH Shuffle、有界队列与背压 |
| 事件时间 | RFC3339、有限乱序 Watermark、多输入 min/idle、滚动窗口、迟到丢弃 |
| Checkpoint | 持续流 aligned Barrier、per-input gate、Kafka frozen offset、状态快照 |
| Exactly-once | 事务 File Sink、PREPARED/DECIDED/FINALIZED、manifest-last 可见性 |
| 持久化 | S3 artifact、Checkpoint、Job metadata revision、ETag CAS |
| 高可用 | 双 JobManager active/passive、lease、epoch fencing、接管后 finalize |
| 安全 | 外部 HTTPS + Bearer Token、内部/数据面 mTLS、Kafka SSL、文件 Secret |
| 可观测性 | JSON 日志、Prometheus 指标、8 条 SLO 告警规则 |

## 系统架构

```text
CLI / Acceptance Tools
        |
        | HTTPS + Bearer Token
        v
  HAProxy :8080  -------- leader-only routing
        |
        +--------+----------------+
        v                         v
  JobManager A              JobManager B
  ACTIVE / STANDBY          STANDBY / ACTIVE
        |
        | lease, metadata, artifacts, checkpoints
        v
  S3 HAProxy ------> MinIO 1..4
        |
        | mTLS control + coordinator epoch
        v
  Worker 1 <==== TLS data plane ====> Worker 2 <====> Worker 3
        |
        +------ Kafka SSL Source
        +------ Transactional File Sink

  Prometheus ---- mTLS scrape ----> JobManagers / Workers
```

Exactly-once Checkpoint 沿数据通道注入有序 Barrier。所有输入对齐后，算子快照状态，
Sink 预提交事务；Coordinator 持久化不可逆的 `DECIDED` 决定后，再幂等发布
committed fragments 和 output manifest。接管后的 JobManager 可以继续完成
`DECIDED` 但尚未 `FINALIZED` 的 Checkpoint。

更完整的设计说明见 [架构与数据流](docs/architecture.md)。

## 快速开始

### 环境要求

- Python `3.11`
- Docker Desktop，使用 Linux containers
- PowerShell 7+（运行自动化验收）
- Git

### 本地安装

```powershell
git clone https://github.com/shamoniuniu/pystream.git
cd pystream

python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

检查 CLI 和高级作业：

```powershell
.\.venv\Scripts\python.exe -m pystream --version
.\.venv\Scripts\python.exe -m pystream validate examples\advanced\job.yaml
```

验证成功后会输出 8 个逻辑算子、12 个物理任务以及 FORWARD/HASH 边。

### 运行测试

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pytest
```

pytest 默认启用 branch coverage，最低门槛为 80%。最终验收环境为
Linux/Python 3.11。

## 定义作业

作业包包含 `job.yaml` 和同步 Python UDF。下面是高级示例的核心配置：

```yaml
api_version: pystream/v1

job:
  name: advanced-exactly-once

execution:
  delivery_guarantee: exactly_once
  event_time:
    max_out_of_orderness: 2s
    idle_timeout: 30s
  checkpoint:
    interval: 10s
    timeout: 30s
  restart:
    max_attempts: 6
    delay: 1s

operators:
  - id: words
    type: source
    parallelism: 2
    config:
      connector: kafka
      topic: advanced-words
      event_time:
        pointer: /event_time
        format: rfc3339

  - id: output
    type: sink
    parallelism: 1
    config:
      connector: file
      format: csv
      output_path: /data/output
```

完整示例位于 [`examples/advanced`](examples/advanced)，字段、UDF 和协议说明见
[API 参考](docs/api.md)。

### 一致性模式

| 配置 | 语义 |
|---|---|
| 无 `execution` | 基础兼容模式：fail-fast、逐条 offset commit |
| `delivery_guarantee: at_least_once` | DRAIN Checkpoint、append Sink，恢复边界允许重复 |
| `delivery_guarantee: exactly_once` | aligned Barrier、事务 Sink、decision/finalize |
| 有 `execution` 但未声明 guarantee | 默认 `exactly_once` |

> [!IMPORTANT]
> Exactly-once 的可见结果只能来自 output manifest 引用且通过 identity、SHA-256
> 和 size 校验的 committed fragments。扫描 `pending/` 或 `committed/` 目录不能
> 作为结果真值。

## 运行验收

验收脚本会生成临时 PKI/Secret、构建镜像、启动完整集群、注入故障、验证输出，
最后删除容器、网络、卷和临时 Secret。

### Core

Core profile 包含单 JobManager、单 MinIO、3 Workers、Kafka 和 Prometheus：

```powershell
.\scripts\run_advanced_core_acceptance.ps1 `
  -PythonCommand .\.venv\Scripts\python.exe
```

它验证基础 WordCount、显式 At-least-once 回归、Exactly-once baseline，以及
Barrier 前和 decision 前的 Worker SIGKILL。

### HA

HA profile 包含 2 JobManagers、4 个双盘 MinIO 节点、3 Workers、两个 HAProxy、
Kafka 和 Prometheus：

```powershell
.\scripts\run_advanced_ha_acceptance.ps1 `
  -PythonCommand .\.venv\Scripts\python.exe
```

它验证单 active、安全正反路径、`DECIDED -> FINALIZED` 窗口接管、单 MinIO 节点
退出，以及降级存储下的 Worker 恢复。

### 已验证结果

| 场景 | 结果 |
|---|---|
| Core `before_barrier` Worker 故障 | 13.870s 恢复，output diff=0 |
| Core `before_decision` Worker 故障 | 15.598s 恢复，output diff=0 |
| HA active JobManager 退出 | 25.828s 接管，epoch 1 → 2 |
| 单 MinIO 节点退出后 Checkpoint | 28.988s 完成 |
| 降级存储下 Worker 故障 | 19.755s 恢复，output diff=0 |
| Kafka / Prometheus / 清理 | lag=0，5 targets，8 rules，资源=0 |
| Python 3.11 质量门 | 487 passed，0 skipped，83.01% branch coverage |

原始证据和完整结论见
[`reports/advanced-acceptance.md`](reports/advanced-acceptance.md)。

## 项目结构

```text
src/pystream/
  api/              YAML 模型、解析和 DAG 校验
  artifact/         作业包构建、摘要和安全解压
  checkpoint/       快照、decision、finalized 和 output manifest
  control/          JobManager、调度、Checkpoint、HA lease
  operators/        Source、Map、KeyBy、Reduce、事务 File Sink
  runtime/          数据协议、通道、Shuffle、Barrier gate
  security/         TLS、证书身份、Bearer 和 Secret
  storage/          S3 端口、条件写和错误映射
  worker/           Worker 注册、部署和 Task 生命周期

examples/           基础、中级和高级作业
deploy/             Core/HA Compose、HAProxy、Prometheus
scripts/            生产、提交、故障注入、验证和清理
tests/              契约、单元、集成和安全测试
reports/            Docker E2E、开发和质量门证据
docs/               架构、API、部署、测试和排障文档
```

## 项目文档

- [文档索引与维护规则](docs/README.md)
- [架构与数据流](docs/architecture.md)
- [模块说明](docs/modules.md)
- [YAML、UDF 与运行协议参考](docs/api.md)
- [Docker 部署与验收](docs/deployment.md)
- [测试与性能测量](docs/testing.md)
- [性能基线与复现](docs/performance.md)
- [故障排查](docs/troubleshooting.md)
- [阶段语义与后续路线](docs/roadmap.md)

## 当前边界

PyStream 已在单机 Docker 环境中证明 Worker、active JobManager 和单 MinIO 节点
故障下的 Exactly-once，但不声明以下能力：

- Kafka broker HA
- Docker 主机或跨可用区容灾
- File Sink 输出卷丢失后的恢复
- Active-active JobManager
- 动态扩缩容、Savepoint 或状态重分片
- 多租户和不可信 Python UDF 沙箱
- byte-for-byte 可复现镜像构建

中级历史验收仍保留 `compose_project_resources=0` 成功标记；高级验收使用结构化
`resource_cleanup` 证据。当前开发位于 `feature/advanced-v0.3`，正式合并和发布
不属于自动验收流程。
