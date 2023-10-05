---
outline: deep
---

# 配置文件

RedisShake 使用 [TOML](https://toml.io/cn/) 语言书写，所有的配置参数在 all.toml 中均有说明。

配置文件的组成如下：

```toml
function = "..."

[xxx_reader]
...

[xxx_writer]
...

[advanced]
...
```

一般用法下，只需要书写 `xxx_reader`、`xxx_writer` 两个部分即可，`function` 和 `advanced` 部分为进阶用法，用户可以根据自己的需求进行配置。

## function 配置

参考 [什么是 function](../function/introduction.md)。

## reader 配置

RedisShake 提供了不同的 Reader 用来对接不同的源端，配置详见 Reader 章节：

* [Sync Reader](../reader/sync_reader.md)
* [Scan Reader](../reader/scan_reader.md)
* [RDB Reader](../reader/rdb_reader.md)
* [AOF Reader](../reader/aof_reader.md)

## writer 配置

RedisShake 提供了不同的 Writer 用来对接不同的目标端，配置详见 Writer 章节：

* [Redis Writer](../writer/redis_writer.md)

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