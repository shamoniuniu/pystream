# PyStream 中级功能验收清单

## Git 与日志

- [x] `v0.1.0` 指向初级提交 `24384c1`
- [x] 中级开发位于 `feature/intermediate-v0.2`
- [ ] 每个里程碑有独立 commit、远端分支和 `git revert` 记录
- [ ] `main` 未被中级开发直接修改
- [ ] 开发日志、运行日志、验收报告齐全且不含敏感信息

## API 与兼容性

- [ ] 原 `pystream/v1` WordCount 无修改解析和运行
- [ ] 未知字段继续拒绝并给出路径
- [ ] 事件时间配置、持续时间、JSON Pointer 严格校验
- [ ] changelog/retract DAG 能力传播和错误配置拒绝
- [ ] File Sink 未配置 columns 时输出格式不变

## 事件时间

- [ ] RFC3339 offset 时间统一转 UTC，naive/leap second 拒绝
- [ ] 每 partition Watermark 使用有限乱序策略
- [ ] 多输入只推进活跃输入 Watermark 最小值
- [ ] 全 idle 不推进，恢复 active 不回退
- [ ] `event_time <= watermark` 丢弃且有 metric/log
- [ ] 窗口只在 Watermark 到达 window end 时触发并清理

## Retract

- [ ] 首次状态产生 INSERT
- [ ] 更新严格产生 UPDATE_BEFORE 后 UPDATE_AFTER
- [ ] Map/KeyBy/Shuffle 保留 change_kind
- [ ] 下游 Reduce 正确 add/retract
- [ ] retract 返回 null 删除状态
- [ ] 二级 count distribution demo 结果正确

## Checkpoint

- [ ] CONTROL 不进入 UDF且不越过前序 DATA
- [ ] WATERMARK/DRAIN 广播全部物理通道
- [ ] Source pause 后 snapshot 精确 partition next offsets
- [ ] 全部入通道 DRAIN 到达后才写 Task snapshot
- [ ] manifest 覆盖执行图全部当前 attempt Task
- [ ] 无 manifest、损坏、超限、旧 attempt 快照不恢复
- [ ] manifest 完成后才 commit offsets/resume
- [ ] timeout abort 后 Source 可继续运行

## 自动恢复

- [ ] Worker 进程 SIGKILL 后 Compose 自动拉起
- [ ] Worker 重注册并重新参与调度
- [ ] 作业进入 RECOVERING，attempt 递增
- [ ] 全图停止、释放 slot、下游优先重新部署
- [ ] Source partition 换 subtask 后仍按 manifest seek
- [ ] Reduce/Watermark 状态恢复
- [ ] 恢复后无输入丢失，允许 Sink 重复
- [ ] 旧 attempt 连接/状态/快照被 fencing
- [ ] 重试耗尽进入 FAILED
- [ ] RECOVERING 中 cancel 停止后续重试

## 质量门

- [ ] Python 3.11 Ruff check 通过
- [ ] Python 3.11 Ruff format check 通过
- [ ] 全量 pytest 通过且无 skip
- [ ] 分支覆盖率 >= 80%
- [ ] 初级 WordCount Docker E2E 通过
- [ ] 中级 event-time/retract Docker E2E 通过
- [ ] Worker 故障恢复 Docker E2E 通过
- [ ] `docker compose down -v` 无项目资源残留

## 声明边界

- [ ] 文档明确停流 Checkpoint 的暂停成本
- [ ] 文档明确共享卷和单 JobManager 故障域
- [ ] 文档明确追加 File Sink 可能重复
- [ ] 文档未宣称 Exactly-once
