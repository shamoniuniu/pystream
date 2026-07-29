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

- 状态：实现完成，待提交
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
  - Ruff format：9 个文件机械格式化后通过待复验
- 环境：Python 3.11 目标容器；宿主机 Python 3.13 无 pytest/Ruff，未作为证据
- 问题与处理：
  - 首轮 6 个配置测试失败：改为字段级 validator，恢复精确错误路径
  - 首轮 17 个运行测试失败：兼容模式隐藏新增 metrics，并更新协议 v2 断言
  - 测试容器登录 shell 重置 PATH：后续固定使用 `/opt/venv/bin/python`
- 上一回退点：`a3ffe22`
- 结果提交：本条随本里程碑提交，SHA 记录在下一条日志和 Git note。
