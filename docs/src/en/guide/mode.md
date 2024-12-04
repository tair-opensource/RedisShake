---
outline: deep
---

# Migration Mode Selection

## Overview

Currently, RedisShake has three migration modes: `PSync`, `RDB`, and `SCAN`, corresponding to [`sync_reader`](../reader/sync_reader.md), [`rdb_reader`](../reader/rdb_reader.md), and [`scan_reader`](../reader/scan_reader.md) respectively.

* For scenarios of recovering data from backups, use `rdb_reader`.
* For data migration scenarios, prefer `sync_reader`. Some cloud vendors don't provide PSync protocol support, in which case `scan_reader` can be chosen.
* For long-term data synchronization scenarios, RedisShake currently can't handle them because the PSync protocol is not reliable. When the replication connection is disconnected, RedisShake will not be able to reconnect to the source database. If availability requirements are not high, you can use `scan_reader`. If the write volume is not large and there are no large keys, `scan_reader` can also be considered.

Different modes have their pros and cons. Refer to each Reader section for more information.

## Redis Cluster Architecture

When the source Redis is deployed in a cluster architecture, you can use `sync_reader` or `scan_reader`. Both have switches in their configuration items to enable cluster mode, which will automatically obtain all nodes in the cluster through the `cluster nodes` command and establish connections.

## Redis Sentinel Architecture

1. Typically, you can ignore the Sentinel component and directly write the Redis connection information into the RedisShake configuration file.
::: warning
Note that when using `sync_reader` to connect to a Redis Master node managed by Sentinel, RedisShake will be treated as a Slave node by Sentinel, which may cause unexpected issues. Therefore, in such scenarios, it is recommended to choose a replica as the source.
:::
2. If it is not convenient to directly obtain the Redis connection information, you can configure the Sentinel information in the RedisShake configuration file. RedisShake will automatically obtain the master node address from Sentinel. Configuration reference:
```toml
[sync_reader]
cluster = false
address = "" # The source Redis address will be obtained from Sentinel
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
address = "" # The target Redis address will be obtained from Sentinel
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


## Cloud Redis Services

Mainstream cloud vendors all provide Redis services, but there are several reasons that make using RedisShake on these services more complex:
1. Engine limitations. Some self-developed Redis-like databases don't support the PSync protocol.
2. Architecture limitations. Many cloud vendors support proxy mode, i.e., adding a Proxy component between the user and the Redis service. Due to the Proxy component, the PSync protocol can't be supported.
3. Security limitations. In native Redis, the PSync protocol typically triggers fork(2), leading to memory bloat and increased user request latency. In worse cases, it may even cause out of memory. Although there are solutions to mitigate these issues, not all cloud vendors have invested in this area.
4. Business strategies. Many users use RedisShake to migrate off the cloud or switch clouds, so some cloud vendors don't want users to use RedisShake, thus blocking the PSync protocol.

The following introduces some RedisShake usage schemes in special scenarios based on practical experience.

### Alibaba Cloud Redis & Tair

Alibaba Cloud Redis and Tair both support the PSync protocol, and `sync_reader` is recommended. Users need to create an account with replication permissions (able to execute PSync commands). RedisShake uses this account for data synchronization. See [Create and manage accounts](https://help.aliyun.com/zh/redis/user-guide/create-and-manage-database-accounts) for specific creation steps.

Exceptions:
1. Version 2.8 Redis instances don't support creating accounts with replication permissions. You need to [upgrade to a major version](https://help.aliyun.com/zh/redis/user-guide/upgrade-the-major-version-1).
2. Cluster architecture Redis and Tair instances don't support the PSync protocol under [proxy mode](https://help.aliyun.com/zh/redis/product-overview/cluster-master-replica-instances#section-h69-izd-531).
3. Read-write separation architecture doesn't support the PSync protocol.

In scenarios where the PSync protocol is not supported, `scan_reader` can be used. Note that `scan_reader` will put significant pressure on the source database.

### AWS ElastiCache 

Prefer `sync_reader`. AWS ElastiCache doesn't enable the PSync protocol by default, but you can request to enable it by submitting a ticket. AWS will provide renamed PSync commands in the ticket, such as `xhma21yfkssync` and `nmfu2bl5osync`. These commands have the same effect as the `psync` command, just with different names.
Users only need to modify the `aws_psync` configuration item in the RedisShake configuration file. For a single instance, write one pair of `ip:port@cmd`. For cluster instances, write all `ip:port@cmd` pairs, separated by commas.

When it's inconvenient to submit a ticket, you can use `scan_reader`. Note that `scan_reader` will put significant pressure on the source database.

### AWS MemoryDB

AWS MemoryDB doesn't provide PSync permissions. You can use `scan_reader` and `rdb_reader`.
