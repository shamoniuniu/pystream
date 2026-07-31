# PyStream 高级功能 v0.3 规格

## Metadata

- Change ID: `build-pystream-advanced`
- Baseline: `v0.2.0^{}` / `64b875bbdd45f9d4cd4718422adacbbfcc1e4001`
- Branch: `feature/advanced-v0.3`
- Target package version: `0.3.0`
- Status: Accepted for implementation

## Goals

1. 实现持续流 aligned Checkpoint Barrier。
2. 实现 Kafka offset、算子状态和事务 File Sink 的端到端 Exactly-once。
3. 实现 Checkpoint `DECIDED -> FINALIZED` 不可逆两阶段提交。
4. 使用 S3 兼容对象存储持久化 artifact、Checkpoint 和 JobManager 元数据。
5. 使用 active/passive JobManager、ETag CAS lease 和 coordinator epoch 提供自动接管。
6. 使用内部 mTLS、外部 HTTPS + Bearer Token 和文件 Secret 明确信任边界。
7. 导出 Prometheus 指标和告警规则。
8. 用 core/ha 两套 Docker E2E 证明 Worker、JobManager 和单存储节点故障语义。

## Non-Goals

- Active-active JobManager。
- Kafka broker 高可用。
- Docker 主机、File Sink 输出卷整体丢失后的容灾。
- 跨主机/可用区/地域部署。
- 动态扩缩容、Savepoint、状态重分片和多租户。
- 不可信 UDF 沙箱。
- 合并 `main`、发布镜像或创建 `v0.3.0` tag。

## Compatibility

- 保持 `api_version: pystream/v1`。
- 缺少 `execution` 时保持 v0.1 fail-fast 和逐条 offset commit。
- `execution` 存在时 `delivery_guarantee` 默认 `exactly_once`。
- 显式 `delivery_guarantee: at_least_once` 保留 v0.2 DRAIN、append Sink 和允许重复语义。
- `examples/intermediate/job.yaml` 必须显式配置 `at_least_once`。
- 旧 snapshot/manifest schema 不得被新版本误读；不兼容版本必须明确拒绝。

## Public Configuration

```yaml
api_version: pystream/v1

execution:
  delivery_guarantee: exactly_once
  event_time:
    max_out_of_orderness: 2s
    idle_timeout: 30s
  checkpoint:
    interval: 10s
    timeout: 30s
    max_consecutive_failures: 3
  restart:
    max_attempts: 3
    delay: 2s
```

`delivery_guarantee` 只能为：

- `at_least_once`
- `exactly_once`

Exactly-once 作业的所有 Sink 必须声明事务能力，否则 Graph 校验失败。

## Record And Control Contract

- DATA 继续通过 `DATA_BATCH`。
- WATERMARK、CHECKPOINT_DRAIN、BARRIER、CHECKPOINT_COMPLETE 通过有序 `CONTROL` frame。
- BARRIER 必须包含非负 `checkpoint_id`。
- 通道内 DATA 与 CONTROL 严格保序。
- attempt_id 和 coordinator_epoch 同时参与 fencing。
- 旧 attempt/epoch 的连接、部署、状态上报、快照和提交请求必须拒绝。

## Aligned Barrier Semantics

1. JobManager 对全部当前 epoch/attempt Task 执行 arm。
2. Source 短暂停止产生新记录，冻结 partition next offsets 和时间状态。
3. Source 将 BARRIER 排在全部前序 DATA 后广播，再立即恢复消费。
4. 下游一条输入收到 BARRIER 后，该连接暂停读取 post-barrier 帧。
5. Task 继续处理未对齐输入的 pre-barrier DATA。
6. 全部物理输入收到相同 Barrier 后，Task 快照状态。
7. Sink 在快照前 pre-commit 当前事务。
8. Task 转发 Barrier，然后解除所有输入 gate。
9. abort、cancel、断连和 stop 必须解除 gate。

禁止：

- 把 Barrier 传给 UDF。
- 在内存中无界缓存 post-barrier DATA。
- 在全部输入对齐前快照多输入 Task。
- Barrier ID 回退、重复输入 Barrier 或跨 attempt/epoch 混合。

## Source Offset Contract

- 每个活动 Checkpoint 持有独立 frozen offset mapping。
- Source 在 Barrier 后继续消费时不得修改 frozen mapping。
- DECIDED/complete 后只提交该 Checkpoint 的 frozen mapping。
- abort 删除 frozen mapping，不提交 offset。
- 恢复使用最近合法 DECIDED Checkpoint 的 mapping 显式 seek。

## Transaction File Sink

目录：

```text
/data/output/<job_id>/<sink_operator>/
  pending/attempt-<attempt>/tx-<uuid>/part-<subtask>.csv
  committed/checkpoint-<checkpoint>/part-<subtask>.csv
  manifests/checkpoint-<checkpoint>.json
```

生命周期：

```text
ACTIVE -> PREPARED -> COMMITTED
ACTIVE/PREPARED -> ABORTED
```

规则：

- open 时创建 ACTIVE transaction。
- aligned Barrier 时 flush、fsync、close 并 PREPARED，然后创建下一 ACTIVE transaction。
- descriptor 记录 checkpoint/attempt/task、pending path、size 和 SHA-256。
- `decision.json` 之前的失败可以 abort。
- `decision.json` 之后只能幂等 finalize，不得 abort。
- finalize 使用确定性目标路径和同文件系统 `os.replace`。
- 目标已存在时必须复验 size/SHA，匹配才算幂等成功。
- 所有 committed fragment 完成后才原子写 output manifest。
- 读取方只读取 output manifest 引用的 fragment。
- 未被 DECIDED Checkpoint 引用的 pending transaction 在恢复时清理。

## Checkpoint State Machine

```text
IDLE
  -> ARMED
  -> ALIGNING
  -> PREPARED
  -> DECIDED
  -> FINALIZING
  -> FINALIZED

ARMED/ALIGNING/PREPARED -> ABORTED -> RECOVERING
DECIDED/FINALIZING      -> FINALIZING
```

不变量：

- checkpoint_id、attempt_id、coordinator_epoch 单调且不复用。
- `decision.json` 只在任务全集、快照摘要和事务 descriptor 全部验证后写入。
- decision 是不可逆提交决定。
- finalize 可被当前 leader 或接管 leader 重复执行。
- Source offset commit、Sink finalize 和 output manifest 必须属于同一个 checkpoint。

## Object Storage Contract

对象键：

```text
pystream/
  artifacts/<sha256>.zip
  control/leader.json
  jobs/<job_id>/current.json
  jobs/<job_id>/revisions/<revision:020d>.json
  checkpoints/<job_id>/<checkpoint:020d>/
    attempts/<attempt:08d>/tasks/<task_sha256>.json
    decision.json
    finalized.json
```

规则：

- immutable 对象使用 `If-None-Match: *`。
- leader/current 指针使用 ETag + `If-Match` CAS。
- HTTP 409/412 映射为明确并发冲突。
- 对象内容使用规范 JSON、schema version、size 和 SHA 校验。
- Local 与 S3 实现共享 repository ports。
- HA profile 使用 4 节点、每节点 2 drive 的分布式对象存储。

## JobManager HA

- 两个实例只能有一个 active。
- lease 默认 TTL 10 秒、renew 3 秒、standby poll 1 秒。
- lease acquire/renew/takeover 使用对象存储条件写。
- 新 leader CAS 成功后 coordinator_epoch +1。
- active 失去续租能力时必须在 TTL 内停止写入并进入 standby/protective mode。
- Worker 保存最高 epoch，拒绝更低 epoch。
- HAProxy 只向 `/health/leader` 返回 200 的实例转发写请求。

接管：

1. 获取 lease 和新 epoch。
2. 加载 current pointers 和 metadata revisions。
3. 完成所有 DECIDED 未 FINALIZED Checkpoint。
4. 等待 Worker 重新注册。
5. 对活动作业 attempt +1，统一进入 RECOVERING。
6. 从最近 DECIDED Checkpoint 恢复。
7. 全 Task RUNNING 后才开放 leader health。

## Persistent Job Metadata

revision 至少包含：

- canonical job definition。
- artifact SHA。
- delivery guarantee。
- Job status 和 attempt。
- next checkpoint、last decided、last finalized。
- recovery counters、last failure。
- coordinator epoch。
- finalize backlog。

revision 不可变；`current.json` 仅通过 ETag CAS 指向新 revision。

## Security

- 外部管理入口：HTTPS + Bearer Token。
- 内部 JobManager/Worker HTTP：mTLS。
- Worker 数据面：asyncio TLS + 双向证书。
- Kafka demo：SSL 客户端证书。
- 对象存储：TLS + access key Secret。
- Prometheus scrape：内部证书或 scrape token。
- Bearer 比较使用 constant-time 比较。
- Secret 只从 `--*-secret-file`、Docker Secret 或测试注入读取。
- 禁止把 Token、私钥、access key、业务 payload 写入日志/报告/Git。
- 证书身份必须与 service/worker identity 匹配。

## Observability

至少导出：

- leader role/epoch/lease renew failures。
- checkpoint phase/duration/decision/finalize retry。
- barrier alignment duration/blocked inputs。
- transaction active/prepared/committed/aborted。
- recovery duration。
- object-store operation/status。
- TLS handshake failures 和 auth rejections。

告警：

- active JobManager 数量持续 15 秒不等于 1。
- lease 剩余时间不足 2 个 renew interval。
- DECIDED 未 FINALIZED 超过 30 秒。
- Checkpoint 连续失败达到配置上限。
- 对象存储不可读写。
- JM 接管超过 30 秒，Worker 恢复超过 60 秒。
- 证书剩余有效期低于 7 天。

## Docker Acceptance

### Core

- 单对象存储、单 JobManager、3 Workers、Kafka、Prometheus。
- 基础 WordCount 与显式 At-least-once 中级回归。
- Exactly-once 无故障基准。
- Barrier 前和 PREPARED/DECIDED 前 Worker SIGKILL。
- Worker 60 秒内恢复。
- committed output 与基准逐记录一致，Kafka lag=0。

### HA

- 4 对象存储节点、对象存储 LB、HAProxy、2 JobManagers、3 Workers、Kafka、Prometheus。
- active count=1。
- Token/mTLS 正反路径。
- DECIDED/FINALIZED 前终止 active JobManager。
- standby 30 秒内接管，epoch 增加并完成 finalize。
- 终止一个对象存储节点后仍能读写 artifact/metadata/checkpoint。
- 再注入 Worker 故障并保持 Exactly-once。
- 最终资源和 Secret 清零。

## Quality Gates

- Python 3.11。
- Ruff check。
- Ruff format check。
- 全量 pytest 无 skip。
- branch coverage `>=80%`。
- `git diff --check`。
- Secret scan 0 命中。
- core/ha Docker E2E。
- staged review、receipt、独立 commit、Git note、远端分支。

## Residual Failure Domains

- 单 Kafka broker。
- 单 Docker 主机。
- 单 File output volume。
- 可信 UDF 可阻塞或终止 Worker。

以上边界必须写入 README、架构、部署、测试和排障文档，不得误宣称跨主机生产级 HA。
