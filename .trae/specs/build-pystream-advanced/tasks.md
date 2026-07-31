# PyStream 高级功能 v0.3 任务

## Git Rules

- 基线：`v0.2.0^{}` = `64b875bbdd45f9d4cd4718422adacbbfcc1e4001`。
- 分支：`feature/advanced-v0.3`。
- 每个里程碑独立 commit、Git note、push。
- 回退只使用 `git revert <sha>`；不使用 `git reset --hard`。
- 用户批准前不合并 `main`、不发布镜像、不创建 `v0.3.0` tag。

## Tasks

- [x] Task 0: 建立高级治理和回退基线
  - [x] 中级 Python 3.11：373 tests、84.40% coverage、Ruff/format
  - [x] 创建并推送 annotated `v0.2.0`
  - [x] 创建并推送 `feature/advanced-v0.3`
  - [x] 固化 plan/spec/tasks/checklist 和开发日志
  - [x] 提交并推送高级治理回退点

- [x] Task 1: 公共契约与高级状态机
  - [x] 包版本升级到 0.3.0
  - [x] 新增 DeliveryGuarantee/CheckpointPhase/CoordinatorRole
  - [x] execution 默认 exactly_once，可显式 at_least_once
  - [x] Graph 校验 Sink Exactly-once capability
  - [x] snapshot/decision/finalized schema
  - [x] coordinator_epoch 贯穿部署、状态和控制请求
  - [x] 状态机和兼容契约测试

- [x] Task 2: 持续流 aligned Barrier
  - [x] Source 短暂停顿注入 Barrier 后立即恢复
  - [x] checkpoint-specific frozen offsets
  - [x] per-input Barrier gate
  - [x] all-input alignment 后 snapshot/forward/unblock
  - [x] abort/cancel/failure 无 gate 泄漏
  - [x] 对齐时间和 blocked input 指标
  - [x] 双输入、乱序、超时、断连和背压测试

- [ ] Task 3: 事务 File Sink 与两阶段提交
  - [ ] ACTIVE/PREPARED/COMMITTED/ABORTED 生命周期
  - [ ] flush/fsync/hash transaction descriptor
  - [ ] DECIDED/FINALIZED Checkpoint
  - [ ] 幂等 fragment finalize
  - [ ] output manifest-last 可见性
  - [ ] pending transaction 恢复清理
  - [ ] 三个提交窗口 test hooks 与状态机测试

- [ ] Task 4: S3 对象存储与持久元数据
  - [ ] Local/S3 repository ports
  - [ ] immutable put 与 ETag CAS
  - [ ] S3 ArtifactRepository
  - [ ] S3 CheckpointStore
  - [ ] Job metadata revisions/current pointer
  - [ ] 单节点 core 与 4 节点 HA 存储拓扑
  - [ ] fake S3、条件冲突和 MinIO integration tests

- [ ] Task 5: 双 JobManager 主备
  - [ ] 10s lease / 3s renew / 1s poll
  - [ ] active/standby role 与 protective step-down
  - [ ] coordinator epoch fencing
  - [ ] JobRun metadata 持久化
  - [ ] DECIDED 未 FINALIZED 接管恢复
  - [ ] Worker leader 变化后重注册
  - [ ] HAProxy leader-only routing
  - [ ] 双 contender、lease loss、stale epoch tests

- [ ] Task 6: mTLS、Token、Secret 与 Prometheus
  - [ ] 外部 HTTPS/Bearer Token
  - [ ] 内部 HTTP mTLS
  - [ ] Worker 数据面 mTLS
  - [ ] Kafka SSL client
  - [ ] 对象存储 TLS/access key Secret
  - [ ] PKI/Secret 生成与轮换脚本
  - [ ] Prometheus metrics 与 rules
  - [ ] 认证、证书、redaction、metrics tests

- [ ] Task 7: Core Docker Exactly-once 验收
  - [ ] 基础与显式 At-least-once 回归
  - [ ] Exactly-once 无故障基准
  - [ ] Barrier 前 Worker SIGKILL
  - [ ] PREPARED/DECIDED 前 Worker SIGKILL
  - [ ] Worker 60 秒恢复
  - [ ] committed output diff=0、lag=0
  - [ ] 资源和 Secret 清零

- [ ] Task 8: HA Docker 验收
  - [ ] 双 JobManager active count=1
  - [ ] 外部 Token/内部 mTLS 正反路径
  - [ ] DECIDED/FINALIZED 前 active JobManager 退出
  - [ ] standby 30 秒接管并 epoch 增加
  - [ ] 单对象存储节点退出后继续读写
  - [ ] 接管后 Worker 故障仍 Exactly-once
  - [ ] Prometheus/SLO/lease/finalize 证据
  - [ ] 资源和 Secret 清零

- [ ] Task 9: 文档、审查和最终推送
  - [ ] README/API/架构/模块/部署/测试/排障/roadmap
  - [ ] 高级开发日志和验收报告
  - [ ] Python 3.11、Ruff、format、coverage
  - [ ] Secret scan
  - [ ] staged diff review 与 receipts
  - [ ] Git notes 与功能分支推送
  - [ ] main/v0.1.0/v0.2.0 不变
