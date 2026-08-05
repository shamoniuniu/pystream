# 故障排查

先保留状态、evidence 和运行日志，再重启。高级证据位于
`reports/advanced-{core,ha}-*.{log,json}`。

## 最短诊断路径

管理入口要求 CA、客户端证书和 Bearer Token。优先使用 tools 容器：

```powershell
docker compose -f deploy\compose.advanced.yaml --profile ha ps
docker compose -f deploy\compose.advanced.yaml --profile ha logs --no-color jobmanager-1
docker compose -f deploy\compose.advanced.yaml --profile ha logs --no-color worker-ha-1
```

状态重点：

```text
role leader_ready coordinator_epoch attempt checkpoint.phase
last_decided_id last_finalized_id healthy_workers
```

## TLS、Token 或身份拒绝

- 外部 401：检查 external token file 是否挂载，不能改用明文 CLI 参数。
- TLS handshake 失败：检查 CA、证书有效期、SAN 和 client certificate。
- 内部 403：证书可建立 TLS，但 SAN/CN 不符合 endpoint 身份策略。
- Worker 注册 403：证书身份必须与 `worker_id` 绑定。
- `/metrics` 401/403：metrics token/Prometheus certificate 与管理 Token 分离。

禁止把 Token、私钥或 access key 打印出来排障。检查 PKI manifest 的证书元数据和
Prometheus auth rejection 指标。

## Leader 或路由异常

`/health/live` 只证明进程存活；HAProxy 使用 `/health/leader`，只有 ready active
返回 200。

- 两个 standby：检查 S3 leader object、lease renew 错误和 object store 可用性。
- active 短暂返回 503：可能正在 protective step-down 或 takeover。
- epoch 不增加：检查 ETag CAS 冲突和 standby 日志。
- 旧 active 恢复后必须成为 standby，不能手工绕过 leader routing。
- takeover 只在完成 DECIDED finalize、元数据加载和 Worker 重注册后 leader-ready。

## Checkpoint 或 Exactly-once 失败

检查顺序：

1. Task 是否全部 arm。
2. Source 是否冻结 next offsets 并注入同 checkpoint/attempt/epoch 的 BARRIER。
3. 输入 gate 是否只阻塞 post-barrier 帧。
4. 全 task snapshot 和 PREPARED transaction descriptor 是否齐全。
5. `decision.json` 是否写入。
6. DECIDED 后是否仅重试 finalize，未执行 abort。
7. `finalized.json` 和 output manifest 是否完成。
8. `last_decided_id >= last_finalized_id` 且不会因旧 takeover cache 回退。

`pending_transactions=1` 可以是正在处理下一 checkpoint 的 ACTIVE transaction，
不代表历史 PREPARED 泄漏。可见结果必须由 manifest reader 校验，不能直接 glob。

## Worker 故障后恢复慢

标准故障注入杀容器内业务子进程：

```powershell
docker exec <worker-container> /bin/sh -c `
  "kill -9 `$(cat /proc/1/task/1/children)"
```

不要使用 `docker kill` 代替该测试；`restart: unless-stopped` 会把人工容器停止视为
不自动恢复。

检查：

- RestartCount、StartedAt、incarnation 是否改变。
- 作业 attempt 是否增加、所有 Task attempt 是否一致。
- restored checkpoint 是否为 durable latest manifest。
- 同一算子 subtasks 是否并发进入 DEPLOYING。
- Worker 恢复 SLO 是 60 秒；最终 HA 实测 19.755 秒。

## 单 MinIO 节点故障

直接 `docker stop` 后，Docker 可能把旧 `minio-1` IP 分给新 Worker，存活 MinIO
会把 peer 连接发到错误 TLS 身份。验收脚本采用：

1. 记录节点 network/IP。
2. 删除故障 MinIO 容器。
3. 在同 IP/alias 创建无 9000 监听的 failure holder。
4. 等待 S3 HAProxy 摘除 backend。

这样 peer 得到连接拒绝且网络身份不被复用。holder 只用于故障模型，清理时先删除。
若 leader 在存储收敛期间进入 protective，等待同一 ready epoch 稳定后再触发
checkpoint。

## Kafka lag 异常

lag verifier 分离 metadata consumer 和 group offset reader，避免加入业务 consumer
group 触发 rebalance。逐 partition 检查：

```text
committed_offset end_offset lag
```

最终必须两 partitions lag=0。Topic metadata 刚创建时允许有限重试，超时后仍缺失
才判失败。

## Prometheus 证据异常

配置 job label 是复数：

```text
pystream-jobmanagers
pystream-workers
```

HA 最终要求两个 JobManager 与三个 Workers 共 5 个 healthy targets，rules=8。
target up 只证明 scrape 成功，active count/lease/finalize 仍需 rules 和状态证据。

## Docker 清理失败

重新运行验收脚本会先删除 failure holder、解除 paused 容器，再执行 Compose down。
最终结构化证据必须满足：

```text
containers=0 networks=0 volumes=0
secret_directory_removed=true
```

不要删除不带 `com.docker.compose.project=pystream-advanced` 标签的资源。

## 剩余故障域

Kafka broker、Docker 主机和 File output volume 不具备本项目 HA。UDF 是可信代码，
阻塞或终止 Worker 时只能依靠 Worker/作业恢复，不能隔离恶意行为。
