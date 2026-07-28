# Docker 部署与 WordCount 验证

本文面向课程演示和验收执行者。部署拓扑的权威来源是
`deploy/compose.yaml`；脚本行为的权威来源是 `scripts/`。

> 验证状态：Compose、镜像和脚本已通过静态契约测试；2026-07-26 当前开发机
> 没有 `docker` 命令，因此多容器运行结果仍待在安装 Docker 的机器验证。

## 前置条件

- Docker Desktop，启用 Linux containers。
- Docker Compose v2，即 `docker compose`。
- 建议至少 4 CPU、4 GiB 可用内存。
- 主机端口 8080 未被占用。

确认：

```powershell
docker version
docker compose version
```

## 启动集群

从仓库根目录执行：

```powershell
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml ps
```

预期服务：

- `kafka`：单 Broker KRaft，内部端口 9092。
- `kafka-init`：创建 2 分区 `words` topic 后退出 0。
- `jobmanager`：主机 `http://localhost:8080`。
- `worker-1/2/3`：每个 4 slots，内部控制端口 8081、数据端口 9000。

等待所有长期服务显示 healthy：

```powershell
docker compose -f deploy/compose.yaml ps
```

检查控制面：

```powershell
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/v1/workers
```

应有 3 个 healthy Worker 和 12 个总 slots。

## 执行 WordCount

`tools` 使用同一镜像和输出卷。按顺序执行：

```powershell
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/produce_wordcount.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/submit_wordcount.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/wait_for_window.py
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/verify_wordcount.py
```

步骤含义：

1. `produce` 删除并重建 `words` topic，写入 APPLE、pie、apple 三条 JSON。
2. `submit` 等待 3 Worker，打包 `examples/wordcount` 并提交。
3. `wait` 等待 10 秒处理时间窗口产生 CSV。
4. `verify` 检查作业仍为 RUNNING、任务至少跨 2 Worker、CSV 汇总为
   `apple=2, pie=1`，并打印 JSON 证据。

输出时间由实际到达窗口决定，行顺序不固定。文件位于命名卷中的：

```text
/data/output/<job_id>/output/part-00000.csv
```

## 查看状态与 Shuffle 证据

最近 job_id 写入输出卷 `.last_wordcount_job_id`。提交脚本也会打印 job_id。

```powershell
docker compose -f deploy/compose.yaml --profile tools run --rm tools `
  -m pystream status <job_id> --jobmanager-url http://jobmanager:8080

docker compose -f deploy/compose.yaml logs jobmanager worker-1 worker-2 worker-3
```

验收证据：

- 状态输出中 Source、Map、KeyBy、Reduce、Sink 物理任务分布到至少 2 Worker。
- `by_word -> totals` 的逻辑边在 `pystream validate` 中显示 `hash`。
- Worker 日志存在 `connection_opened`，其 upstream/downstream task 属于不同
  Worker 时证明跨节点通道。
- Worker `/tasks` 可查看记录数、批次数和队列指标。

## 清理作业

取消最近作业并删除其输出：

```powershell
docker compose -f deploy/compose.yaml --profile tools run --rm tools scripts/cleanup_wordcount.py
```

停止容器，保留卷：

```powershell
docker compose -f deploy/compose.yaml down
```

停止并删除 Kafka、制品、输出和 Worker 工作卷：

```powershell
docker compose -f deploy/compose.yaml down -v
```

`down -v` 会删除演示数据，不可恢复。

## 重复演示

每轮都先运行 `produce_wordcount.py`。它重建 topic，避免旧消息污染结果。若希望
保留 topic，使用 `--keep-topic`，但此时验证结果会包含历史输入，不适合作为
标准验收。

## 故障行为验证

第一阶段只验证 fail-fast，不验证恢复：

```powershell
docker compose -f deploy/compose.yaml stop worker-2
Start-Sleep -Seconds 20
Invoke-RestMethod http://localhost:8080/v1/jobs/<job_id>
```

预期：

- JobManager 健康端点仍可访问。
- 使用 worker-2 的运行作业进入 `FAILED`。
- 其余任务被停止，slot 被释放。
- 作业不会自动重启，CSV 也不具备去重保证。

恢复演示环境：

```powershell
docker compose -f deploy/compose.yaml start worker-2
```

重新提交前先执行清理和数据重置。

## 验收记录

完成实机验证后记录以下内容：

```text
日期:
操作系统:
Docker Client/Server:
Docker Compose:
镜像摘要:
健康服务:
job_id:
参与 Worker:
输出行:
故障测试结果:
```

若任何命令与实际资产不一致，应先修正文档或脚本，不得只在验收现场口头补充。
