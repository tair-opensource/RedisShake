# RedisShake 增强功能：支持自定义清空命令异步执行及校验

## 背景

在 Redis 集群环境中，目标端可能通过 `rename-command` 禁用了 `FLUSHALL`，但希望启动 RedisShake 时采用异步清空（`FLUSHALL ASYNC`）以减少对目标集群的影响。原版 RedisShake 仅支持同步发送 `FLUSHALL`，无法满足这些定制需求。

本次改造为 RedisShake 增加了以下能力：
- 支持自定义清空命令名（如重命名后的命令）；
- 支持同步/异步清空模式，异步模式下自动等待所有主节点完成内存释放（轮询 lazyfree_pending_objects，并发等待所有主节点）；
- 提供可配置的超时时间，避免无限等待，超时后 panic 退出；
- 在等待过程中输出详细的进度日志，便于监控；

本次改造在不破坏原有功能的前提下，为 RedisShake 增加了灵活的清空控制能力，尤其适用于需要重命名危险命令或采用异步清空的生产环境。通过清晰的日志输出和超时保护，运维人员可以直观监控清空进度，确保同步开始前目标端处于干净状态。

---

## 新增配置项

在配置文件的 `[advanced]` 段中，增加以下三个配置项：

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `flushall_command` | string | `"FLUSHALL"` | 新增自定义清空命令，如 "sf7xncm5frvddpc3flushall" |
| `flushall_mode` | string | `"sync"` | 新增自定义清空命令执行执行模式， 同步"sync" 或 异步"async"（异步模式仅在目标为 Redis Cluster 且 Redis 版本 ≥ 4.0 时生效） |
| `flushall_async_timeout` | int | `10` | 新增自定义清空命令执行执行超时时间，单位：分钟|

---

## 具体改动说明
```text
- internal/config/config.go
  ├─ 新增字段 FlushAllCommand, FlushAllMode, FlushAllAsyncTimeout (支持自定义清空命令、模式和超时)

- internal/writer/redis_cluster_writer.go
  ├─ 新增方法 GetAddresses() (返回所有主节点地址列表)

- cmd/redis-shake/main.go
  ├─ 新增函数 parseLazyfreePending() (解析 INFO memory 中的 lazyfree_pending_objects)
  ├─ 新增函数 waitAsyncFlushForNode() (单节点轮询等待，2秒间隔，支持超时)
  ├─ 新增函数 waitAsyncFlushForCluster() (并发等待所有主节点)
  ├─ 主流程调整: 提前调用 theWriter.StartWrite() 启动后台协程
  ├─ 主流程调整: 新增 redisWriterOpts 变量保存目标端配置
  ├─ 主流程调整: 重写 empty_db_before_sync 逻辑，根据配置发送自定义清空命令，异步模式下调用等待函数
  └─ 主流程调整: 删除原重复的 theWriter.StartWrite() 调用
```

---

## 功能说明

### 1. 自定义清空命令
- 当 `empty_db_before_sync = true` 时，RedisShake 将使用 `flushall_command` 指定的命令清空目标库；
- 如果 `flushall_mode = "async"` 不区分大小写，程序会自动在命令末尾追加 `ASYNC` 参数；

### 2. 异步清空等待（仅集群模式）
- 异步模式下，RedisShake 将 `FLUSHALL ASYNC` 广播到集群所有主节点；
- 随后，为每个主节点创建一个临时连接，每 2 秒轮询 `INFO memory` 中的 `lazyfree_pending_objects` 指标；
- 当该指标变为 0 时，表示该节点已完成内存释放；
- 等待所有主节点完成后，再开始正式的数据同步；

### 3. 超时保护
- 若在 `flushall_async_timeout` 分钟内仍有节点未完成，程序会 panic 并输出超时日志，避免进程永久阻塞；

### 4. 日志输出
- 每次轮询输出 `[节点地址] lazyfree_pending_objects = 数值`（INFO 级别），方便监控清空进度；
- 节点完成后输出 `Node completed async flush`，最终输出 `All cluster nodes have completed async flush`；

### 5. 注意事项
- 异步模式依赖 FLUSHALL ASYNC 命令和 lazyfree_pending_objects 指标，需要目标 Redis 版本 ≥ 4.0；
- 如果目标端通过 rename-command 禁用了 FLUSHALL，必须将 flushall_command 设置为重命名后的命令名，所有节点必须统一配置；
- 异步等待功能仅支持目标端为 Redis Cluster（redis_writer.cluster = true），若配置为异步模式但 writer 不是集群模式，程序会 panic；
- 等待期间会为每个主节点创建一个临时 Redis 连接，完成后自动释放，不影响后续同步；
- 若清空命令执行失败（如权限不足、命令不存在），RedisShake 会在日志中报错并 panic，阻止同步继续，保证数据安全；

---

## 自行构建
```shell script
git clone -b feature/flushall-async-custom https://github.com/SHOWufei/RedisShake.git
cd RedisShake
sh build.sh
```

---

## 使用示例

### 场景一：自定义命令 + 同步清空（默认行为）
```toml
[advanced]
empty_db_before_sync = true
flushall_command = "MYFLUSHALL"
flushall_mode = "sync"
```
效果：发送 MYFLUSHALL 命令到目标库，同步执行，不等待

### 场景二：自定义命令 + 异步清空（推荐）
```toml
[advanced]
empty_db_before_sync = true
flushall_command = "sf7xncm5frvddpc3flushall"
flushall_mode = "async"
flushall_async_timeout = 10 
```
效果：发送 MYFLUSHALL ASYNC 到所有主节点，轮询等待各节点 lazyfree_pending_objects 归零，超时后 panic。

### 场景二：预期日志
```text
2026-04-01 20:17:02 INF redisClusterWriter connected to redis cluster successful. addresses=[10.10.10.03:7992 10.10.10.01:7990 10.10.10.02:7991]
2026-04-01 20:17:02 INF Sending flush command: sf7xncm5frvddpc3flushall ASYNC
2026-04-01 20:17:02 INF Async flush mode enabled, waiting for all cluster nodes to complete...
2026-04-01 20:17:02 INF set target redis max qps to 300000
2026-04-01 20:17:02 INF set target redis max qps to 300000
2026-04-01 20:17:02 INF set target redis max qps to 300000
2026-04-01 20:17:04 INF [10.10.10.02:7991] lazyfree_pending_objects = 11126268
2026-04-01 20:17:04 INF [10.10.10.03:7992] lazyfree_pending_objects = 11125512
2026-04-01 20:17:04 INF [10.10.10.01:7990] lazyfree_pending_objects = 11125534
2026-04-01 20:17:06 INF [10.10.10.01:7990] lazyfree_pending_objects = 5562767
2026-04-01 20:17:06 INF [10.10.10.03:7992] lazyfree_pending_objects = 5562756
2026-04-01 20:17:06 INF [10.10.10.02:7991] lazyfree_pending_objects = 5563134
2026-04-01 20:17:08 INF [10.10.10.02:7991] lazyfree_pending_objects = 0
2026-04-01 20:17:08 INF [10.10.10.01:7990] lazyfree_pending_objects = 5562767
2026-04-01 20:17:08 INF [10.10.10.02:7991] Node completed async flush (lazyfree_pending_objects=0)
2026-04-01 20:17:08 INF [10.10.10.03:7992] lazyfree_pending_objects = 5562756
2026-04-01 20:17:10 INF [10.10.10.03:7992] lazyfree_pending_objects = 0
2026-04-01 20:17:10 INF [10.10.10.03:7992] Node completed async flush (lazyfree_pending_objects=0)
2026-04-01 20:17:10 INF [10.10.10.01:7990] lazyfree_pending_objects = 0
2026-04-01 20:17:10 INF [10.10.10.01:7990] Node completed async flush (lazyfree_pending_objects=0)
2026-04-01 20:17:10 INF All cluster nodes have completed async flush
2026-04-01 20:17:10 INF status information: http://localhost:8999
2026-04-01 20:17:10 INF status information: watch -n 0.3 'curl -s http://localhost:8999 | python -m json.tool'
2026-04-01 20:17:10 INF start syncing...
2026-04-01 20:17:10 INF [reader_11.11.11.01_7990] source db is not doing bgsave! continue.
2026-04-01 20:17:10 INF [reader_11.11.11.03_7992] source db is not doing bgsave! continue.
2026-04-01 20:17:10 INF [reader_11.11.11.02_7991] source db is not doing bgsave! continue.
2026-04-01 20:17:15 INF read_count=[1707282], read_ops=[0.00], write_count=[1707279], write_ops=[0.00], src-1, syncing rdb, size=[9.8 MiB/117 MiB]
2026-04-01 20:17:20 INF read_count=[5029274], read_ops=[1005896.24], write_count=[5029271], write_ops=[1005895.64], src-2, syncing rdb, size=[33 MiB/117 MiB]
2026-04-01 20:17:25 INF read_count=[8216680], read_ops=[637475.67], write_count=[8216678], write_ops=[637475.87], src-0, syncing rdb, size=[53 MiB/117 MiB]
2026-04-01 20:17:30 INF read_count=[11646753], read_ops=[686010.97], write_count=[11646753], write_ops=[686011.37], src-1, syncing rdb, size=[79 MiB/117 MiB]
2026-04-01 20:17:35 INF read_count=[14698402], read_ops=[610337.51], write_count=[14698399], write_ops=[610336.91], src-2, syncing rdb, size=[103 MiB/117 MiB]
2026-04-01 20:17:40 INF read_count=[16688657], read_ops=[397998.26], write_count=[16688657], write_ops=[397998.86], src-0, syncing aof, diff=[0]
2026-04-01 20:17:45 INF read_count=[16688657], read_ops=[0.00], write_count=[16688657], write_ops=[0.00], src-1, syncing aof, diff=[0]
2026-04-01 20:17:50 INF read_count=[16688657], read_ops=[0.00], write_count=[16688657], write_ops=[0.00], src-2, syncing aof, diff=[0]
2026-04-01 20:17:55 INF read_count=[16688657], read_ops=[0.00], write_count=[16688657], write_ops=[0.00], src-0, syncing aof, diff=[0]
```

