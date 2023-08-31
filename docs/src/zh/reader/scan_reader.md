# Scan Reader

## 介绍

::: tip
本方案为次选方案，当可以使用 [`sync_reader`](sync_reader.md) 时，请优选 [`sync_reader`](sync_reader.md)。
:::

`scan_reader` 通过 `SCAN` 命令遍历源端数据库中的所有 Key，并使用 `DUMP` 与 `RESTORE` 命令来读取与写入 Key 的内容。

注意：
1. Redis 的 `SCAN` 命令只保证 `SCAN` 的开始与结束之前均存在的 Key 一定会被返回，但是新写入的 Key 有可能会被遗漏，期间删除的 Key 也可能已经被写入目的端。可以通过 `ksn` 配置解决
2. `SCAN` 命令与 `DUMP` 命令会占用源端数据库较多的 CPU 资源。



## 配置

```toml
[scan_reader]
cluster = false            # set to true if source is a redis cluster
address = "127.0.0.1:6379" # when cluster is true, set address to one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
ksn = false                # set to true to enabled Redis keyspace notifications (KSN) subscription
```

* `cluster`：源端是否为集群
* `address`：源端地址, 当源端为集群时，`address` 为集群中的任意一个节点即可
* 鉴权：
    * 当源端使用 ACL 账号时，配置 `username` 和 `password`
    * 当源端使用传统账号时，仅配置 `password`
    * 当源端无鉴权时，不配置 `username` 和 `password`
* `tls`：源端是否开启 TLS/SSL，不需要配置证书因为 RedisShake 没有校验服务器证书
* `ksn`：开启 `ksn` 参数后 RedisShake 会在 `SCAN` 之前使用 [Redis keyspace notifications](https://redis.io/docs/manual/keyspace-notifications/)
能力来订阅 Key 的变化。当 Key 发生变化时，RedisShake 会使用 `DUMP` 与 `RESTORE` 命令来从源端读取 Key 的内容，并写入目标端。

::: warning
Redis keyspace notifications 不会感知到 `FLUSHALL` 与 `FLUSHDB` 命令，因此在使用 `ksn` 参数时，需要确保源端数据库不会执行这两个命令。
:::
