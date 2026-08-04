# PyStream 高级功能 v0.3 开发日志

> 本文件记录高级阶段的决策、变更、验证、提交和回退路径。禁止记录业务 payload、
> Bearer Token、私钥、对象存储 access key 或其他 Secret。

## 2026-07-31T02:38:17Z - Task 0 高级治理与回退基线

- 状态：完成；本日志由 Task 0 基线提交承载
- 中级基线：`64b875bbdd45f9d4cd4718422adacbbfcc1e4001`
- 中级标签：创建并推送 annotated `v0.2.0`
- 高级分支：创建并推送 `feature/advanced-v0.3`
- Python 3.11 基线：
  - Ruff：通过
  - format：109 files
  - pytest：373 passed
  - branch coverage：84.40%
- 镜像 smoke：Python 3.11.9 / PyStream 0.2.0 / exit 0
- 发布边界：
  - `v0.2.0` 仅为 source tag
  - 不发布本地镜像
  - Python 依赖仍按版本范围解析，不声明字节级可复现
- 用户确认：
  - 分阶段实现完整 v0.3
  - 默认 Exactly-once，可显式回退 At-least-once
  - 4 节点分布式对象存储
  - 双 JobManager active/passive
  - core/ha 双 profile
  - 内部 mTLS + 外部 Token
  - JobManager 30 秒、Worker 60 秒恢复目标
  - 三个事务崩溃窗口故障证明
- 计划：`.trae/documents/pystream-advanced-v0.3-development-plan.md`
- 规格：`.trae/specs/build-pystream-advanced/`
- 上一回退点：`v0.2.0`
- 结果提交：本里程碑提交 SHA 通过 Git note 关联
- 回退：`git revert <Task 0 SHA>` 或从 `v0.2.0` 新建分支

## 2026-07-31T03:19:57Z - Task 1 公共契约与高级状态机

- 状态：实现和本地质量门完成，等待 staged review、提交和远端备份
- 版本：包、Compose 和中级验收镜像标识升级到 `0.3.0`
- 兼容：
  - 缺少 `execution` 时继续使用基础 fail-fast 行为
  - 存在 `execution` 时默认 `exactly_once`
  - 中级示例显式固定 `at_least_once`
  - Exactly-once Graph 拒绝未声明事务能力的 Sink
- Checkpoint schema：
  - schema version 从 1 升级到 2，旧文档明确拒绝
  - 新增 `CheckpointPhase`、`CheckpointDecision`、`CheckpointFinalization`
  - 新增严格 `TransactionDescriptor`，校验 identity、SHA、大小和相对路径
  - `DECIDED` 禁止回退；`FINALIZING` 允许幂等自重试
- Epoch fencing：
  - `coordinator_epoch` 贯穿部署 DTO、快照、manifest、Worker 控制请求和失败上报
  - Worker 保存全局最高 epoch，拒绝旧 leader 对任意 Task 的后续控制
  - 单 JobManager 兼容 epoch 为 0；选举和租约行为留到 Task 5
- 反例修复：
  - 首轮 149/150 定向测试发现非法 job ID 在 schema 扩大后先触发大小限制；
    调整为身份校验先于序列化和大小检查
  - 不变量复核发现更高 epoch 只 fence 同 Task 的缺口；改为 Worker 全局最高 epoch
- Python 3.11 质量门：
  - pytest：386 passed
  - branch coverage：84.48%
  - Ruff check：通过
  - Ruff format：80 files already formatted
  - `git diff --check`：通过
- 环境说明：
  - 宿主 Python 3.13 的 checkpoint 临时路径受 Windows `MAX_PATH` 影响，不作证据
  - Docker `run` 偶发退出后 CLI 不返回；改用 `create/start/inspect/rm`，测试容器
    最终 `exit=0`
- 上一回退点：`320eb52`
- 结果提交：本里程碑提交 SHA 通过 Git note 关联
- 回退：`git revert <Task 1 SHA>`

## 2026-07-31T03:52:55Z - Task 2 持续流 aligned Barrier

- 状态：实现和本地质量门完成，等待 staged review、提交和远端备份
- 模式分派：
  - `exactly_once` 使用 aligned BARRIER
  - 显式 `at_least_once` 保留原 DRAIN 停流路径
- 数据面：
  - 协议升级到 v3，HELLO 使用 `attempt_id + coordinator_epoch` 双重 fencing
  - BARRIER 与 DATA 复用同一有序输出队列
  - 每条 TCP handler 在 BARRIER 入队后等待独立 gate，不读取 post-barrier frame
  - 未对齐输入继续处理 pre-barrier DATA，无应用层 post-barrier 缓存
- Source：
  - pause 只覆盖冻结状态和 BARRIER 排队，随后立即 resume
  - 每个 checkpoint 保存独立 frozen snapshot/offset mapping
  - complete 按 checkpoint ID 提交 frozen offset；abort 只丢弃 mapping
  - DATA 排入全部下游后才 acknowledge offset/event-time，避免 freeze 早于实际输出
- Runtime：
  - 全输入 Barrier 到齐后才 snapshot、forward、ready、unblock
  - abort、stop、断连失败和 cleanup 均幂等释放全部 gate
  - 记录 source pause、alignment duration、blocked inputs 和 data-plane gate 指标
- 故障/反例：
  - 双输入快慢 Barrier 证明已对齐通道 post-data 不被读取
  - 覆盖 abort、stop、未对齐通道断连、重复/错序 Barrier、旧 epoch HELLO
  - 覆盖 Source 恢复消费后 offset 推进但旧 frozen checkpoint 不漂移
  - 交错审查修复 offset 在 DATA 入队前推进的 race
- Python 3.11 质量门：
  - pytest：396 passed
  - branch coverage：84.86%
  - Ruff check：通过
  - Ruff format：80 files already formatted
  - `git diff --check`：通过
- 环境：测试容器 `pystream-m2-py311` 最终 `exit=0`
- 上一回退点：`ea64976`
- 结果提交：本里程碑提交 SHA 通过 Git note 关联
- 回退：`git revert <Task 2 SHA>`

## 2026-07-31T04:47:06Z - Task 3 事务 File Sink 与两阶段提交

- 状态：实现和本地质量门完成，等待 staged review、提交和远端备份
- Transaction Sink：
  - Exactly-once open 创建 ACTIVE pending fragment；At-least-once 继续 append
  - Barrier 对齐后执行 flush、fsync、close、size/SHA-256 descriptor，再启动下一事务
  - PREPARED fragment 使用确定性 checkpoint 目标和 `os.replace` 幂等提交
  - 已提交目标必须复验 size/SHA；COMMITTED 状态拒绝 abort
- Checkpoint Store：
  - Task snapshot 内容和摘要覆盖 transaction descriptors
  - 验证全 Task/Sink transaction 集合后写不可变 `decision.json`
  - `decision.json` 出现后 Store 和 Coordinator 均禁止回到 abort
  - output manifest 发布完成后写 `finalized.json`，并验证 decision SHA
- Manifest-last：
  - JobManager 挂载共享 output volume
  - 所有 committed fragments 先完成完整性校验，再按 Sink 原子发布 output manifest
  - pending/committed fragment 本身不作为读取可见性真值
- 故障恢复：
  - pre-decision 失败立即 abort 并触发整作业恢复
  - post-decision 未知结果只重放 finalize，不发送 abort
  - Worker 丢失后新 attempt 从 decision snapshot 恢复 transaction 与 frozen offsets
  - 新 Source 提交恢复 offset 后才发布 output manifest/finalized
  - Task 停止期间清理所有未被 durable decision 保护的 pending 目录
- 反例验证：
  - 覆盖 Barrier/PREPARED 前失败、PREPARED 后 decision 前失败
  - 覆盖 DECIDED 后 complete 响应丢失和 Source/Sink 进程重启
  - 覆盖重复 commit/finalize、目标篡改、descriptor 篡改和 orphan 保护
- Python 3.11 质量门：
  - pytest：413 passed
  - branch coverage：84.28%
  - Ruff check：通过
  - Ruff format：113 files already formatted
  - `git diff --check`：通过
- 未在本里程碑宣称：
  - JobManager 接管、对象存储持久化、mTLS、Prometheus 和 Docker 故障验收
  - Kafka broker、Docker 主机或 File output volume HA
- 上一回退点：`cdfbcd7`
- 结果提交：本里程碑提交 SHA 通过 Git note 关联
- 回退：`git revert <Task 3 SHA>`

## 2026-08-04T10:35:06Z - Task 4 对象存储与持久元数据

- 状态：实现、真实 MinIO 验收、Python 3.11 质量门和 staged review 完成
- 对象存储：
  - 新增供应商无关 `ObjectStore` 与 `CheckpointStore` ports
  - S3 adapter 使用 SigV4、path-style、`If-None-Match` 和 ETag `If-Match`
  - 404、409、412 同时按服务错误码和 HTTP 状态严格映射
  - 启动 capability probe 验证创建、CAS、stale CAS、读取和列表语义
  - boto3 最低版本固定为已验证包含两种条件参数的 `1.35.70`
- 持久仓库：
  - artifact 使用 SHA-256 内容寻址不可变对象
  - checkpoint 使用不可变 task snapshot、manifest、decision 和 finalized 对象
  - Job metadata 使用不可变 revision 与 `current.json` ETag CAS
  - immutable/CAS 写响应丢失时重读对象并按内容、identity 和 SHA 对账
  - 每个 JobRun 串行发布 metadata，避免并发状态转换复用 revision
- 部署：
  - `core` profile：单 MinIO、单 JobManager、3 Workers
  - `ha` profile：4 MinIO 节点，每节点 2 个独立 drive，经 HAProxy 暴露 S3
  - MinIO、MinIO Client、HAProxy 和 Kafka 均固定 tag + manifest digest
  - 对象存储凭据只通过 Docker Secret 文件路径注入
- 动态验收：
  - core 与 ha Compose 配置解析通过
  - 单节点 MinIO capability/artifact/checkpoint/metadata integration 通过
  - 四节点正常态经 HAProxy 的相同 integration 通过
  - 停止 `minio-1` 及其两个 drive 后，完整读写和 CAS integration 继续通过
  - `minio-1` 重新启动并恢复 healthy
  - 项目容器、网络、卷和临时 Secret 最终均为 0
  - 修复只读 init 容器缺少 `MC_CONFIG_DIR`、HAProxy 无 Host 健康检查、
    412 后连接复用和裸 ETag 被代理拒绝四个真实部署问题
- Python 3.11 Linux 质量门：
  - pytest：447 passed，无 skip
  - branch coverage：83.60%
  - Ruff check：通过
  - Ruff format：94 files already formatted
  - `git diff --check`：通过
- staged review：
  - 31 个文件均属于 Task 4 范围，无未暂存代码
  - 未解决 blocker：0
  - Secret scan：0 命中
- 上一回退点：`fb5d5fb`
- 提交与远端备份：本日志随 Task 4 commit 推送
