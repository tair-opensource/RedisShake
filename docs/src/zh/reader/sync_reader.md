---
outline: deep
---

# Sync Reader

## 介绍

当源端数据库兼容 PSync 协议时，推荐使用 `sync_reader`。兼容 PSync 协议的数据库有：

* Redis
* Tair
* Valkey
* ElastiCache 需要提供 aws_psync 配置

优势：数据一致性最佳，对源库影响小，可以实现不停机的切换

原理：RedisShake 模拟 Slave 连接到 Master 节点，Master 会向 RedisShake 发送数据，数据包含全量与增量两部分。全量是一个 RDB 文件，增量是 AOF 数据流，RedisShake 会接受全量与增量将其暂存到硬盘上。全量同步阶段：RedisShake 首先会将 RDB 文件解析为一条条的 Redis 命令，然后将这些命令发送至目的端。增量同步阶段：RedisShake 会持续将 AOF 数据流同步至目的端。

### 常见问题

**连接断开**：如果遇到 RedisShake 至源端节点的连接被断开，可通过源端对应节点的运行日志确认，多是因为源端 `client-output-buffer-limit replica` 参数设置过小，建议适当调高。

## 配置

```toml
[sync_reader]
cluster = false            # set to true if source is a redis cluster
address = "127.0.0.1:6379" # when cluster is true, set address to one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
sync_rdb = true # set to false if you don't want to sync rdb
sync_aof = true # set to false if you don't want to sync aof
```

* `cluster`：源端是否为集群
* `address`：源端地址, 当源端为集群时，`address` 为集群中的任意一个节点即可
* 鉴权：
    * 当源端使用 ACL 账号时，配置 `username` 和 `password`
    * 当源端使用传统账号时，仅配置 `password`
    * 当源端无鉴权时，不配置 `username` 和 `password`
* `tls`：源端是否开启 TLS/SSL，不需要配置证书因为 RedisShake 没有校验服务器证书
* `sync_rdb`：是否同步 RDB，设置为 false 时，RedisShake 会跳过全量同步阶段
* `sync_aof`：是否同步 AOF，设置为 false 时，RedisShake 会跳过增量同步阶段，此时 RedisShake 会在全量同步阶段结束后退出