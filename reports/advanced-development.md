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

## 2026-08-04T12:48:32Z - Task 5 双 JobManager 主备与接管

- 状态：实现、Docker HA 故障验收、Python 3.11 质量门完成
- Leader lease 与 fencing：
  - 对象键 `pystream/control/leader.json`，默认 TTL/renew/poll 为 `10s/3s/1s`
  - acquire、renew、release 和过期 takeover 均使用 ETag CAS，epoch 严格递增
  - ACTIVE、STANDBY、PROTECTIVE 三角色；续租失败或超时立即关闭写入和 leader health
  - renew 和 metadata 激活均受剩余 lease 时间约束，禁止过期 lease 继续开放 ACTIVE
- 接管与恢复：
  - 新 leader 清空进程内 Worker 视图，从 current revision 重建 JobRun/ExecutionGraph
  - 活动作业统一进入 RECOVERING，等待足够 Worker 重注册后 attempt +1
  - 兼容读取 Task 4 展开默认字段的旧 metadata，并用规范定义重新发布
  - DECIDED backlog 直接作为恢复 manifest；store 保留原 decision epoch，
    Worker complete 使用新 leader epoch，最终幂等写 finalized
- HTTP 与 Worker：
  - `/health/active` 供 Worker，`/health/leader` 供外部管理流量
  - standby/protective 写请求返回 `503` 和 `Retry-After: 1`
  - Worker 首次注册失败不退出，404/503 或更高 epoch 后自动重新注册
  - Worker 保存最高 epoch，stale stop/deploy/checkpoint 请求被拒绝
- HA 部署：
  - ha profile 新增 2 JobManager、3 HA Worker 和双 frontend HAProxy
  - 外部 `:8080` 仅路由 ready leader，内部 `:8082` 路由 active leader
  - HAProxy 使用 Docker DNS 动态解析；单个 JobManager 缺失时仍可启动和恢复 healthy
- Docker 动态证据：
  - 初始角色：ACTIVE=1、STANDBY=1、Worker epoch=1
  - 无作业 active SIGKILL：`11.148s` 接管到 epoch 2
  - 真实运行作业 active SIGKILL：`12.452s`，epoch 3、attempt 1、10 tasks RUNNING
  - 最终镜像双 JobManager 重建：`16.813s`，epoch 4、12 tasks RUNNING
  - checkpoint-enabled Worker 重建：`8.284s`，12 tasks RUNNING，满足 `<=60s`
  - stale epoch 2 控制请求返回 `409`，epoch 3 任务保持 RUNNING
  - 对象存储入口中断：旧 active 进入 PROTECTIVE，leader health=false
  - 存储恢复：`9.762s` 重新选主至 epoch 6，attempt 4、12 tasks RUNNING
  - 最终项目资源：containers=0、networks=0、volumes=0
- 故障/修复记录：
  - 修复持久 Job 读取误生成 async generator
  - 修复旧 metadata 中非 reduce `emit_mode` 默认字段无法往返
  - 修复 DECIDED 接管使用旧 checkpoint/旧命令 epoch 导致 Worker fencing 拒绝
  - 修复 HAProxy 单后端 DNS 消失后无法重启及 healthcheck 误报
  - 基础 WordCount 无 `execution` 的 Worker 故障按兼容契约进入 FAILED；
    Worker 恢复 SLO 使用启用 checkpoint/restart 的高级作业验收
- Python 3.11 Linux 质量门：
  - pytest：465 passed，无 skip
  - branch coverage：83.58%
  - Ruff check：通过
  - Ruff format：127 files already formatted
  - `git diff --check`：通过
- staged review：
  - 22 个文件均属于 Task 5 范围，未暂存差异为 0
  - receipt JSON 校验通过
  - 未解决 blocker：0
  - Secret scan：0 命中
- 未在本里程碑宣称：
  - mTLS、Bearer Token、Prometheus
  - DECIDED/FINALIZED Docker 精确窗口与 committed output diff
  - Kafka broker、Docker 主机或 File output volume HA
- 上一回退点：`8882d9e`
- 提交与远端备份：本日志随 Task 5 commit 推送

## 2026-08-05T03:09:40Z - Task 6 mTLS、Token、Secret 与 Prometheus

- 状态：实现、core/HA 安全烟测、Python 3.11 质量门完成
- TLS、认证与 Secret：
  - 新增统一 SSL context、SAN/CN 身份校验、恒定时间 Bearer 比较和 file-only
    Secret loader
  - 外部 `:8080` 终止 HTTPS，并以 `haproxy` 服务证书连接 ready leader
  - 内部 `:8082` TCP TLS 透传，保留 Worker 客户端证书身份并选择 active leader
  - JobManager/Worker HTTP、Worker 数据面均强制 mTLS；Worker 注册身份绑定
    certificate SAN 与 `worker_id`
  - CLI、演示 producer/consumer、故障注入和等待脚本均从文件加载 CA、Token 和证书
  - Kafka broker 要求 SSL client auth；私钥使用 PKCS#12，CA truststore 使用 JKS
  - MinIO core/HA、S3 HAProxy 与 PyStream S3 client 全链路 TLS
  - `scripts/generate_dev_pki.py` 支持生成/强制轮换 CA、服务证书、Kafka stores、
    Token 和对象存储凭据；manifest 仅包含证书元数据
- Prometheus：
  - JobManager/Worker 提供受客户端证书或 scrape token 保护的 `/metrics`
  - 导出 leader/lease、checkpoint/barrier、transaction/finalize、recovery、
    object store、TLS/auth 和证书到期指标
  - Prometheus `v3.5.0` 镜像固定 digest；配置与 8 条 SLO rules 经 `promtool` 通过
- 安全负面测试：
  - 无 Token/错 Token 返回 401，正确 Token 可访问；管理 Token 与 metrics Token 隔离
  - 错 CA、过期证书在 TLS 握手阶段被拒绝
  - 错服务身份、Worker certificate/worker_id 不匹配返回 403
  - 数据面拒绝非 Worker 证书；日志、错误、metrics 和 PKI manifest 不包含 Secret
- core Docker 动态证据：
  - TLS Kafka、TLS MinIO、HTTPS JobManager/HAProxy、3 mTLS Workers 和 Prometheus 健康
  - 正确 Token 观察到 `3/3` healthy Workers；无 Token/错 Token 均为 401
  - 明文 HTTP 到 HTTPS 入口被断开
  - Prometheus mTLS targets：1 JobManager + 3 Workers 全部 up
  - 真实 WordCount 经 Kafka SSL、控制面 mTLS 和数据面 mTLS 完成，
    committed 兼容输出为 `pie=1, apple=2`
  - 指标证据：auth rejection=2、object store requests=53、证书剩余约 30 天
- HA Docker 动态证据：
  - 4 个双盘 MinIO 节点、TLS S3 代理、2 JobManagers、3 Workers、Kafka、
    HAProxy 和 Prometheus 全部 healthy
  - Prometheus 角色为 `jobmanager-2=ACTIVE`、`jobmanager-1=STANDBY`
  - 两个 JobManager 和三个 HA Worker metrics targets 全部 up
  - Prometheus 证书经内部 TLS 透传后访问 Worker API，被服务身份策略拒绝为 403
- 故障/修复记录：
  - 为 Python 3.13/OpenSSL 严格链验证补充 certificate SKI/AKI
  - 按 Apache Kafka 镜像原生 filename/credentials file 契约挂载 SSL Secret
  - cert-only PKCS#12 不被 Java 识别为 trust anchor，改为标准库生成 JKS v2
  - MinIO 本地 health/init 命令显式处理自签 CA，实际 S3 数据路径仍严格校验
  - Compose 宿主 Secret source 变量改为 `*_SOURCE`，避免 M5 `.env` 的容器
    `*_FILE` 路径覆盖新凭据
- Python 3.11 Linux 质量门：
  - pytest：478 passed，无 skip
  - branch coverage：82.92%
  - Ruff check：通过
  - Ruff format：通过
  - `git diff --check`：通过
- staged review：
  - 41 个文件均属于 Task 6 范围，未暂存差异为 0
  - receipt JSON 与 core/ha Compose 解析通过
  - 私钥、Bearer、AWS key 模式 Secret scan：0 命中
  - 未解决 blocker：0
- 资源清理：core/HA containers=0、networks=0、volumes=0
- 未在本里程碑宣称：
  - DECIDED/FINALIZED Docker 精确崩溃窗口和 committed output fault diff
  - Kafka broker、Docker 主机或 File output volume HA
- 上一回退点：`c39edd8`
- 提交与远端备份：本日志随 Task 6 commit 推送

## 2026-08-05T05:33:02Z - Task 7/8/9 最终 Exactly-once 与 HA 验收

- 状态：Core/HA Docker E2E、文档和 Linux/Python 3.11 最终质量门完成
- 候选镜像：
  - tag：`pystream:0.3.0`
  - ID：`sha256:8c9d3581807f4419cf5776cbc3590e61452afd1d4b38720bd68016609f29ca8c`
  - created：`2026-08-05T04:47:29.968777384Z`
- 确定性故障窗口：
  - `before_barrier`：全 Task arm，Source 尚未注入 Barrier
  - `before_decision`：snapshot/PREPARED 完成，durable decision 尚未写入
  - `after_decision`：decision 已写入，finalize 尚未开始
  - 仅 `PYSTREAM_ENABLE_TEST_HOOKS=true` 或 `--enable-test-hooks` 注册测试路由
- Core Docker：
  - 基础 WordCount 与显式 At-least-once 中级回归通过
  - Exactly-once baseline 通过
  - `before_barrier` Worker SIGKILL：`13.870s` 恢复，attempt 1
  - `before_decision` Worker SIGKILL：`15.598s` 恢复，attempt 1
  - 三组 manifest-visible committed rows diff=0
  - Kafka 两 partitions lag=0
  - containers=0、networks=0、volumes=0、临时 Secret 已删除
- HA Docker：
  - 初始角色 ACTIVE=1、STANDBY=1
  - 无/错 Token、错 CA、错误内部证书身份均被拒绝
  - checkpoint 1 在 DECIDED 后、FINALIZED 前终止 active JobManager
  - standby `25.828s` 接管，epoch 1 -> 2，完成 checkpoint 1 finalize
  - 单 MinIO 节点退出后 `28.988s` 完成 checkpoint 2
  - 降级存储下 Worker SIGKILL：`19.755s` 恢复到 attempt 2
  - checkpoint 3 FINALIZED；manifest IDs=1,2,3
  - 最终 committed rows diff=0、Kafka lag=0
  - Prometheus：5 个 JobManager/Worker targets、8 条 rules
  - containers=0、networks=0、volumes=0、临时 Secret 已删除
- 实现修复：
  - Worker 恢复每次比较 durable latest manifest，防止 takeover cache 使
    `last_decided` 落后于 `last_finalized`
  - 算子之间保持下游优先，同一算子 subtasks 并发部署，降低恢复耗时
  - MinIO 故障删除节点并以无 9000 监听的 holder 保留旧 IP/alias，避免 Docker
    把 MinIO peer 地址重分配给 Worker
  - cleanup 在 Compose down 前显式删除 holder/解除 pause
  - Prometheus evidence 使用实际复数 job labels
- 失败与取证：
  - 早期 HA 504/503：修复旧 active 重启、leader-ready 重试和存储稳定判断
  - 旧 restore cache 触发 `last_finalized > last_decided`：实现单调恢复并补回归
  - `docker stop` 导致 MinIO IP 被 Worker 复用：改为固定 IP failure holder
  - `docker pause` 模拟节点卡死导致 lease 反复失稳：恢复为真实节点退出
  - Docker 29.6 禁止 `--network none` 容器再连接普通网络：改为释放旧节点后
    直接按原 IP 创建 holder
  - 第一次最终 HA 功能链通过但 Prometheus target filter 使用单数 label：
    修正后完整重跑通过
- Python 3.11 Linux 最终质量门：
  - pytest：487 passed、0 skipped
  - 真实 MinIO conditional write/repository integration：通过
  - branch coverage：83.01%
  - Ruff check：通过
  - Ruff format：147 files
  - `git diff --check`：通过
  - 文档/部署/演示契约短门：33 passed
- Secret 与声明边界：
  - 私钥、长 Bearer 值、AWS access key 模式：0 命中
  - 不宣称 Kafka broker HA、Docker 主机容灾、File output volume 容灾或
    不可信 UDF 隔离
  - Python 在线范围依赖仍使镜像不是 byte-for-byte 可复现
- 最终证据：
  - `reports/advanced-acceptance.md`
  - `reports/advanced-core-evidence.json`
  - `reports/advanced-ha-evidence.json`
  - `reports/advanced-python311-tests.log`
  - `reports/advanced-m7-build-receipt.json`
- 上一回退点：`8b2c84a`
- Git 策略：独立 Milestone 7 commit；不合并 main；不移动现有 `v0.3.0` tag
