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
