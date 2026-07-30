# PyStream 中级开发日志

> 本文件记录中级阶段每个任务的变更、验证、提交和回退路径。运行时业务 payload、
> 凭据和 token 不得写入本文件。

## 2026-07-29T06:21:19Z - Task 0 治理与回退基线

- 状态：完成并推送
- 起点：`main/origin/main = 24384c14a7c7f95db350016e623421d5d37af674`
- 标签：创建并推送 annotated tag `v0.1.0`
- 审查回执：已写入并推送 `refs/notes/commits`
- 分支：创建并推送 `feature/intermediate-v0.2`
- 计划：`.trae/documents/pystream-intermediate-development-plan.md`
- 规格：`.trae/specs/build-pystream-intermediate/`
- 验证：
  - `git rev-parse HEAD` 与 `git rev-parse origin/main` 均为初级 SHA
  - `git push origin v0.1.0` 成功
  - `git push -u origin feature/intermediate-v0.2` 成功
- 测试：本任务只增加治理文档，未修改运行代码；沿用初级验收
  `284 tests / 86.42% coverage / Ruff / format`
- 回退：
  - 查看初级基线：`git switch main`
  - 从初级标签重建分支：`git switch -c <new-branch> v0.1.0`
  - 后续提交仅使用 `git revert <sha>`，不使用 `git reset --hard`
- 结果提交：`a3ffe22` (`chore: establish intermediate development baseline`)
- 回退提交：`git revert a3ffe22`

## 2026-07-29T06:54:23Z - Task 1-3 API、事件时间与控制通道

- 状态：完成并推送
- 变更：
  - 新增 execution/event-time/checkpoint/restart 严格 YAML 模型
  - 新增 RFC 6901 JSON Pointer、事件时间窗口和 changelog 能力传播
  - Source 按 Kafka partition 生成有限乱序 Watermark
  - Runtime 合并活跃输入最小 Watermark并处理 idle/active
  - 数据面升级协议 v2，DATA_BATCH 与 CONTROL 分离
  - 控制消息广播全部物理出通道，HELLO 增加 attempt_id
- 兼容：
  - 未配置 execution 的旧作业仍逐条 commit/fail-fast
  - 处理时间窗口、Source metrics 和 File Sink 默认列保持原行为
- 验证：
  - 专项：`162 passed`
  - 全仓：`315 passed`
  - Ruff lint：通过
  - Ruff format：通过
- 环境：Python 3.11 目标容器；宿主机 Python 3.13 无 pytest/Ruff，未作为证据
- 问题与处理：
  - 首轮 6 个配置测试失败：改为字段级 validator，恢复精确错误路径
  - 首轮 17 个运行测试失败：兼容模式隐藏新增 metrics，并更新协议 v2 断言
  - 测试容器登录 shell 重置 PATH：后续固定使用 `/opt/venv/bin/python`
- 上一回退点：`a3ffe22`
- 结果提交：`7ae0cae` (`feat: add event-time windows and watermarks`)
- 回退提交：`git revert 7ae0cae`

## 2026-07-29T07:07:29Z - Task 4 Changelog/Retract

- 状态：完成并推送
- 变更：
  - Reduce `emit_mode=changelog` 产生 INSERT/UPDATE_BEFORE/UPDATE_AFTER
  - 消费 Changelog 的 Reduce 使用 retract_udf 撤回旧 key/window 贡献
  - retract 返回 null 删除空状态，不存在状态的撤回显式失败
  - Map/KeyBy/Shuffle 保留 change_kind
  - File Sink columns 使用 JSON Pointer 输出通用 CSV
  - 新增 12-task 事件时间 + count distribution 示例 YAML/UDF
- 验证：
  - 算子/API 专项：`137 passed`
  - 首次全仓：`321 passed`
  - 示例与 UDF 专项：`63 passed`
  - Ruff lint：通过
  - Ruff format：通过
- 语义边界：
  - Changelog 模式窗口关闭时只清理已持续下发的状态，不额外 DELETE
  - UPDATE_BEFORE/UPDATE_AFTER 可按新旧 key 路由到不同下游 task
  - File Sink 仍为非事务追加写
- 上一回退点：`7ae0cae`
- 结果提交：`0ca56cf` (`feat: add retract changelog processing`)
- 回退提交：`git revert 0ca56cf`

## 2026-07-29T08:08:39Z - Task 5-6 Checkpoint Store 与停流协调基础

- 状态：完成并推送
- 变更：
  - 新增版本化 JSON TaskSnapshot/Manifest、SHA-256、64 MiB 上限和原子写
  - manifest-last 校验执行图任务全集，损坏高版本自动回退到前一合法版本
  - Source pause 后保存 partition next offset、事件时间基线和 Watermark
  - TaskRuntime 收齐全部物理输入 DRAIN 后写状态并广播全部输出通道
  - Worker 增加 arm/trigger/wait/complete/abort HTTP API
  - JobManager 增加单作业串行协调、超时 abort、连续失败阈值和周期触发
  - JobManager/Worker 共享 `/data/checkpoints` 命名卷
- 正确性规则：
  - 未收齐全部输入 DRAIN 不得写 Task snapshot
  - 未收齐执行图全部当前 attempt descriptor 不得写 manifest
  - manifest 写入前不得提交 Source offset 或恢复消费
  - 超时和响应丢失视为未知结果；已落 manifest 保留为合法恢复点
  - Checkpoint ID 严格递增，失败编号不复用，延迟旧 DRAIN 不污染下一轮
- 验证：
  - Source/Store/Operator 专项：`66 passed`
  - Runtime/Worker/Coordinator 专项：`76 + 5 + 2 + 4 passed`
  - 控制面集成专项：`16 passed`
  - 部署参数与 Compose 契约：`9 passed`，`docker compose config --quiet` 通过
  - 全仓：`341 passed`，总覆盖率 `84.89%`
  - Ruff lint：通过
  - Ruff format：79 个文件通过
  - `git diff --check`：通过
- 问题与处理：
  - 局部 pytest 首次导入镜像内旧包：后续固定设置 `PYTHONPATH=/workspace/src`
  - 空 assignment 不调用 consumer pause：删除错误 FakeConsumer 断言，保留内部暂停和
    complete 前后精确 commit 断言
  - 识别到 to_thread 取消竞态：取消时等待原子写线程结束，再执行 abort 清理
  - staged review 发现 manifest 重试时间戳、arm 未知结果和 HTTP 超时边界：
    改为 manifest 幂等返回、请求前登记可能已 arm Task、由 Coordinator 统一控制超时
  - Windows 绑定卷下个别 Ruff 容器输出后未退出：终止会话并用 `timeout 30s` 独立复跑
- 未完成边界：
  - 恢复时跨旧 Source subtask 合并 partition 状态留到 Task 7
  - attempt 递增、整作业重调度和 Worker 重注册恢复尚未实现
  - 当前追加 File Sink 仍允许故障边界重复，不宣称 Exactly-once
- 上一回退点：`0ca56cf`
- 结果提交：`0c2acb6` (`feat: add coordinated checkpoints`)
- 审查回执：Git note 已关联提交并推送 `refs/notes/commits`
- 回退提交：`git revert 0c2acb6`

## 2026-07-29T09:02:53Z - Task 6-7 Source 状态合并与整作业自动恢复

- 状态：完成并推送
- 变更：
  - Task/部署/状态上报/stop/Checkpoint API 全链路增加 attempt fencing
  - Worker 对低 attempt 拒绝、同 attempt 幂等、高 attempt 停旧换新
  - 部署 DTO 携带已验证的 restore descriptors
  - TaskRuntime 在建立输出连接前恢复 Source、Operator 和每输入 Watermark
  - Source 按 operator 合并旧 subtasks 的 partition offset/event-time 状态
  - JobManager 增加 `RECOVERING -> DEPLOYING -> RUNNING` 整作业恢复循环
  - 恢复执行停止旧任务、释放 slot、延迟、扫描最高合法 manifest 和下游优先重部署
  - 重试耗尽进入 FAILED，RECOVERING 中 cancel 取消 sleep/部署并阻止后续 attempt
- 正确性规则：
  - 旧 attempt HELLO、状态上报、stop 和 Checkpoint 请求不得影响新 attempt
  - 同一逻辑 Source task 使用自身旧 Watermark 基线，同时可恢复其他 subtask 的 partition
  - assignment 到首条消息才稳定时先 seek 并跳过预取消息，避免 Watermark 提前推进
  - 每个 recovery 只允许一个 leader；并发失败更新原因但不创建第二个循环
  - 无完整 manifest 从初始状态恢复；有 manifest 时全图使用同一 checkpoint_id
- 验证：
  - 恢复载荷、Worker fencing、Runtime restore 专项：`91 passed`
  - 控制面完整专项（含恢复、耗尽、取消和初始部署失败）：`52 passed`
  - 扩展恢复专项：`112 passed`
  - Source 延迟 assignment/offset 恢复专项：`34 passed`
  - 全仓：`354 passed`，总覆盖率 `84.50%`
  - Ruff lint：通过
  - Ruff format：79 个文件通过
  - `git diff --check`：通过
- 故障路径覆盖：
  - 旧 attempt 状态上报被忽略
  - 旧 stop 不停止高 attempt Runtime
  - null offset 且延迟 assignment 时不丢首条消息
  - 当前 Task 失败后恢复到最近完整 Checkpoint
  - 初次部署失败进入 Recovery 后成功
  - 连续恢复部署失败耗尽重试
  - Recovery delay 期间 cancel
- 未完成边界：
  - Compose SIGKILL、Worker 容器自动拉起和 restart count 证据留到 Task 8
  - Kafka/文件 Sink 实际 E2E 的无丢失与允许重复证明留到 Task 8
  - JobManager 重启恢复仍不在中级范围
- 上一回退点：`0c2acb6`
- 结果提交：`5ee80b3` (`feat: add automatic job recovery`)
- 审查回执：Git note 已关联提交并推送 `refs/notes/commits`
- 回退提交：`git revert 5ee80b3`

## 2026-07-29T10:22:35Z - Task 8 预检：Worker 进程 incarnation

- 状态：实现完成，待提交
- 预检发现：
  - SIGKILL 后 Compose 会以相同 `worker_id` 拉起新进程
  - 原注册逻辑会刷新旧 Worker 心跳并保留旧 slot，导致 JobManager 无法通过心跳超时
    识别旧 Runtime 已消失
  - 该缺口会使作业保持虚假 RUNNING，阻断实际自动恢复验收
- 变更：
  - Worker 每次进程启动生成唯一 `incarnation_id`，注册和健康接口均公开该身份
  - 相同 incarnation 重复注册保持幂等，不触发无意义恢复
  - 不同 incarnation 重注册时识别旧 Worker 承载的作业
  - 中级作业触发单 leader 整作业恢复；基础作业保持失败而不自动恢复
  - Worker 资源接口暴露 incarnation，供 SIGKILL 验收比较新旧进程
- 验证：
  - 控制面、调度和 Worker HTTP 专项：`31 passed`
  - 全仓：`357 passed`，总覆盖率 `84.45%`
  - Ruff lint：通过
  - Ruff format：79 个文件通过
  - `git diff --check`：通过
- 故障路径覆盖：
  - 同 incarnation 注册重试不触发恢复
  - 新 incarnation 在旧 slot 仍占用时触发最近 Checkpoint 恢复
  - 恢复完成后 attempt 递增且全部 Task 使用同一 restored checkpoint
- 未完成边界：
  - 容器 restart count、新 incarnation 实际重注册和恢复时间仍需 Docker SIGKILL 实证
  - At-least-once 输入无丢失及追加 Sink 重复边界仍需中级 E2E 实证
- 上一回退点：`5ee80b3`
- 结果提交：本条随本里程碑提交，SHA 将在提交后回填并写入 Git note
- 回退提交：提交后使用 `git revert <本里程碑 SHA>`

## 2026-07-30T15:00:00Z - Task 8 Docker E2E 与故障恢复验收

- 状态：完成，待最终提交
- 关键修复：
  - Source 多并发改为确定性 manual partition assignment，消除 group rebalance
    造成的跨 Runtime 重放
  - 同 Runtime 重放 offset 直接跳过并记录 `records_replayed`
  - 同一 Source subtasks 并发部署，恢复 attempt 保持相同 partition 身份
  - 镜像预创建并授权 `/data/checkpoints`，修复非 root named volume 写入
  - 原生 Docker 编排增加 resource ledger、命令硬超时、create/start 有界重试
  - SIGKILL 改为终止 tini 的业务子进程，使 `unless-stopped` 自动拉起
  - 验收使用显式 Checkpoint API 固定故障边界，不依赖墙钟周期
  - Kafka lag 验证分离 metadata consumer 与 group offset consumer，避免 rebalance
- 实证：
  - 初级 WordCount：`apple=2, pie=1`
  - 中级 baseline：`(1,1)`、`(2,1)` 各一次
  - 故障前完整 Checkpoint：1
  - Worker RestartCount：`0 -> 1`，StartedAt/incarnation 改变
  - 作业恢复 attempt：`0 -> 1`；全任务 restored checkpoint 为 1
  - 恢复后完整 Checkpoint：2
  - recovery 两行各出现一次额外重复
  - Kafka partition 0：`6/6/lag=0`
  - Kafka partition 1：`4/4/lag=0`
  - 最终：`intermediate_acceptance=passed`、`compose_project_resources=0`
- 问题与处理：
  - `docker kill` 被视为人工停止，不触发 `unless-stopped`；改杀业务子进程
  - Docker CLI 可能超时但请求已生效；start 超时后 inspect，仍为 created 才重试
  - 10s/60s 周期无法稳定形成重放窗口；增加显式完整 Checkpoint 端点
  - consumer 未跟踪 topic 时 metadata 缓存为空；使用无 group metadata consumer
  - 恢复可能快速跳过 RECOVERING；验收改查持久化 `recovery.attempts`
- 证据：
  - `reports/intermediate-acceptance.md`
  - `reports/intermediate-acceptance.log`
  - `reports/intermediate-runtime.log`
  - `reports/intermediate-failure-evidence.json`
- 上一回退点：`5c01f4e`
- 结果提交：随最终中级里程碑提交，SHA 在提交后通过 Git note 关联
- 回退提交：`git revert <最终中级里程碑 SHA>`

## 2026-07-30T15:10:00Z - Task 9 最终质量门与文档

- 状态：质量门完成，待 staged review/提交/推送
- 自动化：
  - Python 3.11：`372 passed`
  - branch coverage：`84.40%`
  - Ruff lint：通过
  - Ruff format：`109 files already formatted`
  - `git diff --check`：通过
- 文档：
  - README、API、架构、模块、部署、测试、排障和 roadmap 已更新到 0.2.0
  - 明确停流 Checkpoint 暂停成本、共享卷/单 JobManager 故障域
  - 明确 File Sink 追加重复和 Exactly-once 非目标
- 制品边界：
  - 基础 Python/Kafka 镜像使用 digest
  - Python 包依赖仍为范围解析；本地镜像仅作验收候选，不对外发布
- staged review 修复：
  - 发现显式 Checkpoint 最长可运行 30 秒，但等待脚本沿用 10 秒 HTTP timeout
  - `wait_for_checkpoint.py` 现将 CLI `--timeout` 同时传给 `JobManagerClient`
  - 新增旧实现会失败的请求超时契约测试
  - Python 3.11 最终镜像回归：`27 passed`
- 最终本地候选制品：
  - 输入 manifest：`7a444a3d8fafb30cf9e15b81771717809687d8f9`
  - 镜像：`pystream:0.2.0`
  - image ID：`sha256:6607f69ea1b6f4344eeb9a687931994d9e2794da84d5e911311fa711b3c88ad6`
  - OCI revision 与输入 manifest 标签核对通过
  - 镜像 smoke：`python=3.11.9; pystream=0.2.0; checkpoint_timeout_fix=present`
- 环境恢复记录：
  - Docker Desktop 重启后 Linux Engine 管道未恢复，日志报告 `WSL update required`
  - 本机 WSL `2.7.11.0` 已是 Winget 当前版本，在线更新源返回 HTTP 403
  - 未修改 WSL/Docker 配置或数据盘；停止残留 Desktop 进程、轮换宿主日志并干净启动
  - Engine `29.6.2` 恢复后最终镜像构建成功
  - 二次构建前 `com.docker.build` 历史端点锁死；仅重启 Build 子进程后增量构建完成
- 待完成：
  - staged diff 审查、独立提交、Git note 和功能分支推送
