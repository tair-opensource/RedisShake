---
outline: deep
---
# 配置文件

RedisShake 使用 [TOML](https://toml.io/cn/) 语言书写，所有的配置参数在 all.toml 中均有说明。

配置文件的组成如下：

```toml
transform = "..."

[xxx_reader]
...

[xxx_writer]
...

[advanced]
...
```

一般用法下，只需要书写 `xxx_reader`、`xxx_writer` 两个部分即可，`transform` 和 `advanced` 部分为进阶用法，用户可以根据自己的需求进行配置。

## reader 配置

根据源端的类型，RedisShake 提供了不同的 Reader 配置，用来对接不同的源端。

* 对于支持 [Redis Sync/Psync 协议](https://redis.io/docs/management/replication/)的源端，推荐使用 `sync_xxx_reader`
* 对于不支持 [Redis Sync/Psync 协议](https://redis.io/docs/management/replication/)的源端，可以使用 `scan_xxx_reader`
* 对于使用 dump.rdb 文件恢复数据场景，可以使用 `rdb_reader`

### sync_xxx_reader

对于源端为单机 Redis-like 数据库时，使用 `sync_standalone_reader`；对于源端为 Redis Cluster 时，使用 `sync_cluster_reader`。

#### sync_standlone_reader

```toml
[sync_standlone_reader]
address = "127.0.0.1:6379"
username = "" # keep empty if not using ACL
password = "" # keep empty if no authentication is required
tls = false
```

#### sync_cluster_reader

```toml
[sync_cluster_reader]
address = "127.0.0.1:6379"
username = "" # keep empty if not using ACL
password = "" # keep empty if no authentication is required
tls = false
```

### scan_xxx_reader

对于源端为单机 Redis-like 数据库时，使用 `scan_standalone_reader`；对于源端为 Redis Cluster 时，使用 `scan_cluster_reader`。

#### scan_standlone_reader

```toml
[scan_standlone_reader]
address = "127.0.0.1:6379"
username = "" # keep empty if not using ACL
password = "" # keep empty if no authentication is required
tls = false
```

#### scan_cluster_reader

```toml
[scan_cluster_reader]
address = "127.0.0.1:6379"
username = "" # keep empty if not using ACL
password = "" # keep empty if no authentication is required
tls = false
```

### rdb_reader

```toml
[rdb_reader]
filepath = "/path/to/dump.rdb"
```

filepath 为 dump.rdb 文件的路径，最好使用绝对路径。

## writer 配置

根据目标端的类型，RedisShake 提供了不同的 Writer 配置，用来对接不同的目标端。
目前 RedisShake 支持的目标端有：
* 单机 Redis-like 数据库：redis_standalone_writer
* Redis Cluster：redis_cluster_writer

### redis_standalone_writer

```toml
[redis_standalone_writer]
address = "127.0.0.1:6380"
username = "" # keep empty if not using ACL
password = "" # keep empty if no authentication is required
tls = false
```

### redis_cluster_writer

```toml
[redis_cluster_writer]
address = "127.0.0.1:6380"
username = "" # keep empty if not using ACL
password = "" # keep empty if no authentication is required
tls = false
```

## advanced 配置

```toml
[advanced]
dir = "data"
ncpu = 3 # runtime.GOMAXPROCS, 0 means use runtime.NumCPU() cpu cores

pprof_port = 0 # pprof port, 0 means disable
status_port = 0 # status port, 0 means disable

# log
log_file = "shake.log"
log_level = "info" # debug, info or warn
log_interval = 5 # in seconds

# redis-shake gets key and value from rdb file, and uses RESTORE command to
# create the key in target redis. Redis RESTORE will return a "Target key name
# is busy" error when key already exists. You can use this configuration item
# to change the default behavior of restore:
# panic:   redis-shake will stop when meet "Target key name is busy" error.
# rewrite: redis-shake will replace the key with new value.
# ignore:  redis-shake will skip restore the key when meet "Target key name is busy" error.
rdb_restore_command_behavior = "rewrite" # panic, rewrite or skip

# redis-shake uses pipeline to improve sending performance.
# This item limits the maximum number of commands in a pipeline.
pipeline_count_limit = 1024

# Client query buffers accumulate new commands. They are limited to a fixed
# amount by default. This amount is normally 1gb.
target_redis_client_max_querybuf_len = 1024_000_000

# In the Redis protocol, bulk requests, that are, elements representing single
# strings, are normally limited to 512 mb.
target_redis_proto_max_bulk_len = 512_000_000

# If the source is Elasticache or MemoryDB, you can set this item.
aws_psync = ""
```