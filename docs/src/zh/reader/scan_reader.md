# Scan Reader

## 介绍

当源端数据库不兼容 PSync 协议时，推荐使用 `scan_reader`。

优势：兼容性好，只要源端数据库支持 `SCAN` 与 `DUMP` 命令，就可以使用 `scan_reader`。

劣势：

1. 数据一致性不如 [`sync_reader`](./sync_reader.md)。
2. `SCAN` 命令与 `DUMP` 命令会占用源端数据库较多的 CPU 资源

## 配置

```toml
[scan_reader]
cluster = false
address = "127.0.0.1:6379" # when cluster is true, address is one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
ksn = false
```

默认情况下，RedisShake 会使用 `SCAN` 命令来遍历一遍所有的 Key，分别使用 `DUMP` 与 `RESTORE` 命令来从源端读取 Key
的内容，并写入目标端。Redis 的 SCAN 命令只保证 SCAN 的开始与结束之前均存在的 Key 一定会被返回，但是新写入的 Key 有可能会被遗漏。

如果需要提高数据一致性，可以开启 `ksn` 参数，这样 RedisShake 会在 `SCAN`
之前使用 [Redis keyspace notifications](https://redis.io/docs/manual/keyspace-notifications/)
能力来订阅 Key 的变化。当 Key 发生变化时，RedisShake 会使用 `DUMP` 与 `RESTORE` 命令来从源端读取 Key 的内容，并写入目标端。
::: warning
Redis keyspace notifications 不会感知到 `FLUSHALL` 与 `FLUSHDB` 命令，因此在使用 `ksn` 参数时，需要确保源端数据库不会执行这两个命令。
:::
