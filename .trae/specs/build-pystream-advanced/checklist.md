# PyStream 高级功能 v0.3 验收清单

## Git 与治理

- [x] `v0.2.0^{}` 指向 `64b875b`
- [x] 高级开发位于 `feature/advanced-v0.3`
- [ ] 每个里程碑有独立 commit、Git note、远端分支和 revert 说明
- [ ] `main`、`v0.1.0`、`v0.2.0` 未被改写
- [ ] 日志和报告不含 Token、私钥、access key 或业务 payload

## 兼容性

- [x] `pystream/v1` 保持可解析
- [x] 无 execution 基础作业行为不变
- [x] execution 作业默认 exactly_once
- [x] 显式 at_least_once 继续通过中级回归
- [x] 不支持事务的 Sink 在 exactly_once 作业中被拒绝

## Barrier

- [ ] Source 只在注入边界短暂停顿
- [ ] Barrier 与 DATA 同通道严格保序
- [ ] 单输入 Barrier 后 post-barrier 帧被 gate
- [ ] 未对齐输入继续处理 pre-barrier DATA
- [ ] 全输入对齐后才 snapshot
- [ ] Barrier 不进入 UDF
- [ ] Barrier 转发后 gate 解除
- [ ] abort/cancel/failure/timeout 无 gate 泄漏
- [ ] 对齐不会产生无界内存缓存

## Source 与状态一致性

- [ ] frozen offset 不受 post-barrier 消费推进
- [ ] complete 只提交对应 frozen offset
- [ ] abort 不提交 offset
- [ ] restore 从同一 decision checkpoint seek
- [ ] Watermark/window/retract 状态与 offset 同 checkpoint

## 事务 Sink

- [ ] open 创建 ACTIVE transaction
- [ ] Barrier pre-commit 执行 flush/fsync/hash
- [ ] snapshot 包含 transaction descriptor
- [ ] decision 前可 abort
- [ ] decision 后拒绝 abort
- [ ] finalize 幂等
- [ ] output manifest 前 fragment 不可见
- [ ] output manifest 覆盖全部 sink subtasks
- [ ] orphan pending 在恢复时清理

## Checkpoint 决定

- [x] 状态机转换完整且非法转换拒绝
- [ ] decision 只在任务全集和摘要合法后写入
- [x] DECIDED 不可逆
- [ ] FINALIZED 可重试
- [ ] 接管 leader 完成 DECIDED 未 FINALIZED
- [ ] pre-decision failure 触发整作业恢复

## 对象存储

- [ ] artifact/checkpoint/metadata 使用 S3 ports
- [ ] immutable key 使用 If-None-Match
- [ ] mutable pointer 使用 ETag If-Match
- [ ] 409/412 不被吞掉
- [ ] schema/SHA/size/identity 严格验证
- [ ] 单 MinIO 节点退出后保持读写

## JobManager HA

- [ ] 任意时刻 active count=1
- [ ] lease TTL/renew/poll 符合规格
- [ ] lease 丢失使旧 active step down
- [ ] takeover epoch 严格增加
- [ ] Worker 拒绝旧 epoch
- [ ] metadata revision 可恢复活动作业
- [ ] Worker 自动重新注册
- [ ] active 退出后 30 秒内恢复服务

## 安全

- [ ] 外部管理 API 要求 HTTPS + Bearer
- [ ] 内部 HTTP 使用 mTLS
- [ ] 数据面使用 mTLS
- [ ] Kafka demo 使用 SSL client
- [ ] 对象存储使用 TLS 和 Secret
- [ ] 错 Token/CA/CN/过期证书被拒绝
- [ ] Secret 只从文件读取
- [ ] 日志和错误响应不泄露 Secret

## 可观测性

- [ ] leader/lease 指标
- [ ] checkpoint/barrier 指标
- [ ] transaction/finalize 指标
- [ ] recovery duration 指标
- [ ] object store 指标
- [ ] TLS/auth rejection 指标
- [ ] Prometheus rules 语法和阈值通过

## Core E2E

- [ ] 无故障 baseline
- [ ] Barrier 前故障
- [ ] PREPARED/DECIDED 前故障
- [ ] Worker 60 秒恢复
- [ ] committed output 无丢失无重复
- [ ] Kafka lag=0
- [ ] 项目资源=0

## HA E2E

- [ ] active/standby 正确
- [ ] DECIDED/FINALIZED 前 active 退出
- [ ] 30 秒内接管和 finalize
- [ ] 单存储节点退出
- [ ] 接管后 Worker 故障
- [ ] committed output diff=0
- [ ] 指标/SLO/安全证据完整
- [ ] 项目资源=0

## 质量门

- [ ] Python 3.11 全量测试无 skip
- [ ] branch coverage >= 80%
- [ ] Ruff check
- [ ] Ruff format check
- [ ] git diff --check
- [ ] Secret scan 0 命中
- [ ] staged review 无未解决 blocker

## 声明边界

- [ ] 不宣称 Kafka broker HA
- [ ] 不宣称 Docker 主机容灾
- [ ] 不宣称 File output volume 容灾
- [ ] 不宣称不可信 UDF 隔离
