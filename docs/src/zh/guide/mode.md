---
outline: deep
---

# 迁移模式选择

## 概述

目前 RedisShake 有三种迁移模式：`PSync`、`RDB` 和
`SCAN`，分别对应 [`sync_reader`](../reader/sync_reader.md)、[`rdb_reader`](../reader/rdb_reader.md)
和 [`scan_reader`](../reader/scan_reader.md)。

* 对于从备份中恢复数据的场景，可以使用 `rdb_reader`。
* 对于数据迁移场景，优先选择 `sync_reader`。一些云厂商没有提供 PSync 协议支持，可以选择`scan_reader`。
* 对于长期的数据同步场景，RedisShake 目前没有能力承接，因为 PSync 协议并不可靠，当复制连接断开时，RedisShake 将无法重新连接至源端数据库。如果对于可用性要求不高，可以使用 `scan_reader`。如果写入量不大，且不存在大 key，也可以考虑 `scan_reader`。

不同模式各有优缺点，需要查看各 Reader 章节了解更多信息。

## Redis Cluster 架构

当源端 Redis 以 cluster 架构部署时，可以使用 `sync_reader` 或者 `scan_reader`。两者配置项中均有开关支持开启 cluster 模式，会通过 `cluster nodes` 命令自动获取集群中的所有节点，并建立连接。

## Redis Sentinel 架构

1. 通常情况下，忽略 Sentinel 组件，直接将 Redis 的连接信息写入 RedisShake 配置文件即可。
::: warning
需要注意的是使用 `sync_reader` 连接被 Sentinel 接管的 Redis Master 节点时，RedisShake 会被 Sentinel 当做 Slave 节点，从而引发非预期内问题。
所以此类场景应尽量选择备库作为源端。
:::
2. 如果不方便直接获取 Redis 的连接信息([#888](https://github.com/tair-opensource/RedisShake/pull/888#issuecomment-2513984861))，可以将 Sentinel 的信息配置在 RedisShake 配置文件中，RedisShake 会自动从 Sentinel 中获取主节点地址。配置参考：
```toml
[sync_reader]
cluster = false
address = "" # 源端 Redis 的地址会从 Sentinel 中获取
username = ""
password = "redis6380password"
tls = false
[sync_reader.sentinel]
master_name = "mymaster"
address = "127.0.0.1:26380"
username = ""
password = ""
tls = false

[redis_writer]
cluster = false
address = "" # 目标端 Redis 的地址会从 Sentinel 中获取
username = ""
password = "redis6381password"
tls = false
[redis_writer.sentinel]
master_name = "mymaster1"
address = "127.0.0.1:26380"
username = ""
password = ""
tls = false

```

## 云 Redis 服务

主流云厂商都提供了 Redis 服务，不过有几个原因导致在这些服务上使用 RedisShake 较为复杂：
1. 引擎限制。存在一些自研的 Redis-like 数据库没有兼容 PSync 协议。
2. 架构限制。较多云厂商支持代理模式，即在用户与 Redis 服务之间增加 Proxy 组件。因为 Proxy 组件的存在，所以 PSync 协议无法支持。
3. 安全限制。在原生 Redis 中 PSync 协议基本会触发 fork(2)，会导致内存膨胀与用户请求延迟增加，较坏情况下甚至会发生 out of memory。尽管这些都有方案缓解，但并不是所有云厂商都有这方面的投入。
4. 商业策略。较多用户使用 RedisShake 是为了下云或者换云，所以部分云厂商并不希望用户使用 RedisShake，从而屏蔽了 PSync 协议。

下文会结合实践经验，介绍一些特殊场景下的 RedisShake 使用方案。

### 阿里云「云数据库 Redis」与「云原生内存数据库Tair」

「云数据库 Redis」与「云原生内存数据库Tair」都支持 PSync 协议，推荐使用 `sync_reader`。用户需要创建一个具有复制权限的账号（可以执行 PSync 命令），RedisShake 使用该账号进行数据同步，具体创建步骤见 [创建与管理账号](https://help.aliyun.com/zh/redis/user-guide/create-and-manage-database-accounts)。

例外情况：
1. 2.8 版本的 Redis 实例不支持创建复制权限的账号，需要 [升级大版本](https://help.aliyun.com/zh/redis/user-guide/upgrade-the-major-version-1)。
2. 集群架构的 Reids 与 Tair 实例在 [代理模式](https://help.aliyun.com/zh/redis/product-overview/cluster-master-replica-instances#section-h69-izd-531) 下不支持 PSync 协议。
3. 读写分离架构不支持 PSync 协议。

在不支持 PSync 协议的场景下，可以使用 `scan_reader`。需要注意的是，`scan_reader` 会对源库造成较大的压力。

### AWS ElastiCache 

优选 `sync_reader`, AWS ElastiCache 默认情况下没有开启 PSync 协议，但是可以通过提交工单的方式请求开启 PSync 协议。AWS 会在工单中给出一份重命名的 PSync 命令，比如 `xhma21yfkssync` 和 `nmfu2bl5osync`。此命令效果等同于 `psync` 命令，只是名字不一样。
用户修改 RedisShake 配置文件中的 `aws_psync` 配置项即可。对于单实例只写一对 `ip:port@cmd` 即可，对于集群实例，需要写上所有的 `ip:port@cmd`，以逗号分隔。

不方便提交工单时，可以使用 `scan_reader`。需要注意的是，`scan_reader` 会对源库造成较大的压力。

### AWS MemoryDB

AWS MemoryDB 不提供 PSync 权限，可使用 `scan_reader` 与 `rdb_reader`。




