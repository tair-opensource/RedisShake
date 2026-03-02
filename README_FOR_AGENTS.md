# RedisShake Installation & Usage Guide

> This document helps LLM agents quickly install and configure RedisShake for Redis data migration tasks.
>
> - Official Documentation: [Chinese](https://tair-opensource.github.io/RedisShake/) | [English](https://tair-opensource.github.io/RedisShake/en/)
> - Releases: https://github.com/tair-opensource/RedisShake/releases
> - Docker Image: `ghcr.io/tair-opensource/redisshake`

## What is RedisShake

RedisShake is a Redis data transformation and migration tool. It moves data between Redis instances with zero downtime, supporting Redis 2.8–8.x and Valkey 8.x–9.x across standalone, master-slave, sentinel, and cluster deployments. It also works with cloud services like Alibaba Cloud Tair, AWS ElastiCache, and AWS MemoryDB.

## Install

### Option 1: Download Binary

Download the latest release from https://github.com/tair-opensource/RedisShake/releases

### Option 2: Docker

```shell
docker run --network host \
    -e SYNC=true \
    -e SHAKE_SRC_ADDRESS=127.0.0.1:6379 \
    -e SHAKE_DST_ADDRESS=127.0.0.1:6380 \
    ghcr.io/tair-opensource/redisshake:latest
```

### Option 3: Build from Source

```shell
git clone https://github.com/tair-opensource/RedisShake
cd RedisShake
sh build.sh
```

## Usage

Create a `shake.toml` config file, then run:

```shell
./redis-shake shake.toml
```

## Configuration

A config file has one **reader** section and one **writer** section. Choose one reader and one writer based on your scenario.

### Readers (choose one)

**sync_reader** — Real-time sync via PSync protocol. Best for migration with minimal downtime. See [sync_reader docs](https://tair-opensource.github.io/RedisShake/en/reader/sync_reader.html).

```toml
[sync_reader]
cluster = false            # set to true if source is a Redis cluster
address = "127.0.0.1:6379" # for cluster, any node address works
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication
tls = false
sync_rdb = true            # set to false to skip RDB phase
sync_aof = true            # set to false to skip AOF phase
prefer_replica = false     # set to true to sync from replica
try_diskless = false       # set to true for diskless sync
```

**scan_reader** — Scan-based one-time migration. Use when PSync is unavailable. See [scan_reader docs](https://tair-opensource.github.io/RedisShake/en/reader/scan_reader.html).

```toml
[scan_reader]
cluster = false
address = "127.0.0.1:6379"
username = ""
password = ""
tls = false
dbs = []                   # e.g. [0, 1, 2] to scan specific DBs; empty = all
scan = true
ksn = false                # set to true to enable keyspace notifications
count = 1                  # keys per scan iteration
```

**rdb_reader** — Import from an RDB file. See [rdb_reader docs](https://tair-opensource.github.io/RedisShake/en/reader/rdb_reader.html).

```toml
[rdb_reader]
filepath = "/path/to/dump.rdb"
```

**aof_reader** — Import from an AOF file.

```toml
[aof_reader]
filepath = "/path/to/appendonly.aof"
timestamp = 0
```

### Writers (choose one)

**redis_writer** — Write to a Redis instance.

```toml
[redis_writer]
cluster = false
address = "127.0.0.1:6380"
username = ""
password = ""
tls = false
off_reply = false          # turn off server reply for performance
```

**file_writer** — Export to a file.

```toml
[file_writer]
filepath = "/path/to/output.aof"
type = "aof"  # "cmd", "aof", or "json"
```

### Filter

Filter keys, databases, or commands during migration. See [filter docs](https://tair-opensource.github.io/RedisShake/en/filter/filter.html).

```toml
[filter]
allow_keys = []            # e.g. ["user:1001", "product:2001"]
allow_key_prefix = []      # e.g. ["user:", "product:"]
allow_key_suffix = []
allow_key_regex = []       # e.g. [":\\d{11}:"]
block_keys = []
block_key_prefix = []      # e.g. ["temp:", "cache:"]
block_key_suffix = []
block_key_regex = []

allow_db = []              # e.g. [0, 1, 2]
block_db = []              # e.g. [3, 4, 5]

allow_command = []
block_command = []         # e.g. ["FLUSHALL", "FLUSHDB"]
allow_command_group = []
block_command_group = []

# Lua function for custom data processing
# See: https://tair-opensource.github.io/RedisShake/en/filter/function.html
function = ""
```

### Advanced

```toml
[advanced]
dir = "data"
ncpu = 0                   # 0 = use all CPU cores
pprof_port = 0             # 0 = disable
status_port = 0            # set a port to enable HTTP status monitoring

log_file = "shake.log"
log_level = "info"         # debug, info, warn
log_interval = 5           # seconds

# How to handle "Target key name is busy" during RESTORE
# "panic" = stop, "rewrite" = replace, "skip" = skip the key
rdb_restore_command_behavior = "panic"

pipeline_count_limit = 1024
target_redis_max_qps = 300000
target_redis_proto_max_bulk_len = 512_000_000

empty_db_before_sync = false
```

## Example: Migrate between Two Redis Instances

```toml
[sync_reader]
address = "10.0.0.1:6379"
password = "source_password"

[redis_writer]
address = "10.0.0.2:6380"
password = "target_password"

[filter]
block_key_prefix = ["temp:", "cache:"]
```

```shell
./redis-shake shake.toml
```

## Example: Docker with Environment Variables

For the simplest sync scenario, no config file is needed:

```shell
docker run --network host \
    -e SYNC=true \
    -e SHAKE_SRC_ADDRESS=10.0.0.1:6379 \
    -e SHAKE_DST_ADDRESS=10.0.0.2:6380 \
    ghcr.io/tair-opensource/redisshake:latest
```

## Limitations

- **No checkpoint/resume**: RedisShake 4.x does not support resumable transfer. Restart means full resync from scratch.
- **Static cluster topology**: Cluster topology changes (failover, scaling, slot migration) will cause panic. RedisShake is best suited for one-time migration, not long-term synchronization.
