---
outline: deep
---

# Configuration File

RedisShake uses the [TOML](https://toml.io/cn/) language for writing, and all configuration parameters are explained in all.toml.

The configuration file is composed as follows:

```toml
function = "..."

[xxx_reader]
...

[xxx_writer]
...

[advanced]
...
```

Under normal usage, you only need to write the `xxx_reader` and `xxx_writer` parts. The `function` and `advanced` parts are for advanced usage, and users can configure them according to their needs.

## function Configuration

Refer to [What is function](../function/introduction.md).

## reader Configuration

RedisShake provides different Readers to interface with different sources, see the Reader section for configuration details:

* [Sync Reader](../reader/sync_reader.md)
* [Scan Reader](../reader/scan_reader.md)
* [RDB Reader](../reader/rdb_reader.md)

## writer Configuration

RedisShake provides different Writers to interface with different targets, see the Writer section for configuration details:

* [Redis Writer](../writer/redis_writer.md)

## advanced Configuration

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