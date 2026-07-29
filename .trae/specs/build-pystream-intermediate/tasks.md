# PyStream 中级功能任务

## 执行规则

- 基线标签：`v0.1.0`
- 开发分支：`feature/intermediate-v0.2`
- 每个 Task 完成后更新 `reports/intermediate-development.md`、执行专项测试、审查、
  独立 commit 并推送。
- 用户确认前不合并 `main`。

## Tasks

- [x] Task 0: 建立中级治理与回退基线
  - [x] 创建并推送 `v0.1.0` annotated tag
  - [x] 创建并推送 `feature/intermediate-v0.2`
  - [x] 固化 spec、tasks、checklist 和开发计划
  - [x] 创建开发日志并提交首个远端回退点

- [x] Task 1: 扩展 YAML/API 与公共契约
  - [x] 增加 execution/event-time/checkpoint/restart 配置
  - [x] 增加 event-time window、emit_mode、retract_udf、Sink columns
  - [x] DataStream 传播 changelog 属性并校验消费能力
  - [x] 增加 RFC 6901 JSON Pointer
  - [x] 增加 UDFKind.RETRACT
  - [x] 完成配置、DAG、UDF 与兼容性契约测试

- [x] Task 2: 实现事件时间和 Watermark
  - [x] Source 提取/校验 RFC3339 时间
  - [x] Source 按 partition 生成有限乱序 Watermark
  - [x] Clock 增加单调时间
  - [x] 事件时间窗口按 Watermark 触发
  - [x] 迟到记录丢弃、指标和日志
  - [x] 多输入 Watermark min、idle/active 切换测试

- [x] Task 3: 实现控制消息数据面
  - [x] 协议升级 v2 和 CONTROL frame
  - [x] DATA/CONTROL 顺序与背压
  - [x] 控制消息广播全部物理通道
  - [x] DataPlaneServer 分离 DATA/CONTROL
  - [x] attempt_id HELLO fencing
  - [x] 协议、Channel、Runtime 集成测试

- [ ] Task 4: 实现 Changelog/Retract
  - [ ] changelog Reduce 产生 INSERT/UPDATE_BEFORE/UPDATE_AFTER
  - [ ] 下游 Reduce add/retract/delete state
  - [ ] Map/KeyBy/Shuffle 保留 change_kind
  - [ ] 通用 File Sink columns
  - [ ] 中级二级聚合 UDF 与算子测试

- [ ] Task 5: 实现版本化 Checkpoint Store
  - [ ] TaskSnapshot/Manifest 模型
  - [ ] 规范 JSON、SHA、大小上限、原子写
  - [ ] 完整 manifest 扫描与损坏回退
  - [ ] attempt 目录隔离和旧写入拒绝
  - [ ] Store 单元测试

- [ ] Task 6: 实现停流协调 Checkpoint
  - [ ] Worker arm/trigger/status/complete/abort API
  - [ ] Source pause、partition snapshot、精确 commit/resume
  - [ ] CHECKPOINT_DRAIN 多输入收齐和状态写入
  - [ ] Source operator partition 状态合并
  - [ ] timeout/abort/连续失败
  - [ ] Coordinator 和多 Runtime 集成测试

- [ ] Task 7: 实现整作业自动恢复
  - [ ] Job/Task RECOVERING 与 attempt 状态机
  - [ ] 故障去重、停止、释放、延迟、重新调度
  - [ ] Worker 重注册和高 attempt 部署
  - [ ] 从最高完整 Checkpoint restore/seek
  - [ ] max attempts、cancel 优先和状态接口
  - [ ] 控制面、Worker、Runtime 恢复测试

- [ ] Task 8: 完成部署和中级 demo
  - [ ] Compose 共享 checkpoint 卷与 0.2.0 镜像
  - [ ] event-time/retract 作业与 UDF
  - [ ] 生产、提交、等待 Checkpoint、故障注入、验证、清理脚本
  - [ ] 初级 WordCount 兼容回归
  - [ ] Worker SIGKILL、重启、恢复与无丢失验证

- [ ] Task 9: 文档、日志和最终验收
  - [ ] 更新 README/API/架构/模块/部署/测试/排障/roadmap
  - [ ] 开发日志逐提交完整
  - [ ] 中级验收报告完整
  - [ ] Ruff、格式、全量 pytest、覆盖率通过
  - [ ] Docker E2E、故障注入、清理通过
  - [ ] 功能分支推送，main 和 v0.1.0 未变化
