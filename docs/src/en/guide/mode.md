---
outline: deep
---

# Migration Mode Selection

## Overview

Currently, RedisShake has three migration modes: `PSync`, `RDB`, and `SCAN`, corresponding to [`sync_reader`](../reader/sync_reader.md), [`rdb_reader`](../reader/rdb_reader.md), and [`scan_reader`](../reader/scan_reader.md) respectively.

* For scenarios of recovering data from backups, you can use `rdb_reader`.
* For data migration scenarios, `sync_reader` should be the preferred choice. Some cloud vendors do not provide support for the PSync protocol, in which case `scan_reader` can be chosen.
* For long-term data synchronization scenarios, RedisShake currently cannot handle them because the PSync protocol is not reliable. When the replication connection is disconnected, RedisShake will not be able to reconnect to the source database. If the demand for availability is not high, you can use `scan_reader`. If the write volume is not large and there are no large keys, `scan_reader` can also be considered.

Different modes have their pros and cons, and you need to check each Reader section for more information.

## Redis Cluster Architecture

When the source Redis is deployed in a cluster architecture, you can use `sync_reader` or `scan_reader`. Both have switches in their configuration items to enable cluster mode, which will automatically obtain all nodes in the cluster through the `cluster nodes` command and establish connections.

## Redis Sentinel Architecture

When the source Redis is deployed in a sentinel architecture and RedisShake uses `sync_reader` to connect to the master, it will be treated as a slave by the master and may be elected as the new master by the sentinel.

To avoid this, you should choose a replica as the source.

## Cloud Redis Service

Mainstream cloud vendors all provide Redis services, but there are several reasons that make using RedisShake on these services more complex:
1. Engine restrictions. Some self-developed Redis-like databases do not support the PSync protocol.
2. Architecture restrictions. Many cloud vendors support proxy mode, i.e., adding a Proxy component between the user and the Redis service. Because of the existence of the Proxy component, the PSync protocol cannot be supported.
3. Security restrictions. In native Redis, the PSync protocol will basically trigger fork(2), leading to memory bloat and increased user request latency. In worse cases, it may even lead to out of memory. Although there are solutions to alleviate these issues, not all cloud vendors have invested in this area.
4. Business strategies. Many users use RedisShake to migrate off the cloud or switch clouds, so some cloud vendors do not want users to use RedisShake, thus blocking the PSync protocol.

The following will introduce some RedisShake usage schemes in special scenarios based on practical experience.

### Alibaba Cloud Redis & Tair

Alibaba Cloud Redis and Tair both support the PSync protocol, and `sync_reader` is recommended. Users need to create an account with replication permissions. RedisShake can use this account for data synchronization. The specific creation steps can be found in [Create and manage database accounts](https://help.aliyun.com/zh/redis/user-guide/create-and-manage-database-accounts).

Exceptions:
1. Version 2.8 Redis instances do not support the creation of accounts with replication permissions. You need to [upgrade to a major version](https://help.aliyun.com/zh/redis/user-guide/upgrade-the-major-version-1).
2. Cluster architecture Redis and Tair instances do not support the PSync protocol under [proxy mode](https://help.aliyun.com/zh/redis/product-overview/cluster-master-replica-instances#section-h69-izd-531).
3. Read-write separation architecture does not support the PSync protocol.

In scenarios where the PSync protocol is not supported, `scan_reader` can be used. It should be noted that `scan_reader` will put significant pressure on the source database.

### AWS ElastiCache and MemoryDB

`sync_reader` is preferred. AWS ElastiCache and MemoryDB do not enable the PSync protocol by default, but you can request to enable the PSync protocol by submitting a ticket. AWS will provide a renamed PSync command in the ticket, such as `xhma21yfkssync` and `nmfu2bl5osync`. This command has the same effect as the `psync` command, just with a different name.
Users only need to modify the `aws_psync` configuration item in the RedisShake configuration file. For a single instance, write one pair of `ip:port@cmd`. For cluster instances, write all `ip:port@cmd`, separated by commas.

When it is inconvenient to submit a ticket, you can use `scan_reader`. It should be noted that `scan_reader` will put significant pressure on the source database.
