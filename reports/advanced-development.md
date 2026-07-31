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
