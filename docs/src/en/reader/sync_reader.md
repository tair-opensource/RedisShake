# Sync Reader

## Introduction

When the source database is compatible with the PSync protocol, `sync_reader` is recommended. Databases compatible with the PSync protocol include:

* Redis
* Tair
* Valkey
* ElastiCache (requires aws_psync configuration)

Advantages: Best data consistency, minimal impact on the source database, and allows for seamless switching.

Principle: RedisShake simulates a Slave connecting to the Master node, and the Master will send data to RedisShake, which includes both full and incremental parts. The full data is an RDB file, and the incremental data is an AOF data stream. RedisShake will accept both full and incremental data and temporarily store them on the hard disk. During the full synchronization phase, RedisShake first parses the RDB file into individual Redis commands, then sends these commands to the destination. During the incremental synchronization phase, RedisShake continues to synchronize the AOF data stream to the destination.

## Configuration

```toml
[sync_reader]
cluster = false            # set to true if source is a redis cluster
address = "127.0.0.1:6379" # when cluster is true, set address to one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
sync_rdb = true # set to false if you don't want to sync rdb
sync_aof = true # set to false if you don't want to sync aof
partial_sync = false # set to true to skip the full sync and stream from the source's current offset
rdb_file_path = ""   # with partial_sync: snapshot taken after connecting, loaded before the stream is replayed
```

* `cluster`: Whether the source is a cluster
* `address`: Source address, when the source is a cluster, `address` can be set to any node in the cluster
* Authentication:
    * When the source uses ACL accounts, configure `username` and `password`
    * When the source uses traditional accounts, only configure `password`
    * When the source does not require authentication, do not configure `username` and `password`
* `tls`: Whether the source has enabled TLS/SSL, no need to configure a certificate because RedisShake does not verify the server certificate
* `sync_rdb`: Whether to synchronize RDB, when set to false, RedisShake will skip the full synchronization phase
* `sync_aof`: Whether to synchronize AOF, when set to false, RedisShake will skip the incremental synchronization phase, at which point RedisShake will exit after the full synchronization phase is complete.
* `partial_sync`: Instead of `PSYNC ? -1`, RedisShake reads `master_replid` and `master_repl_offset` from `INFO replication` and asks for a partial resync from that offset. The source answers `+CONTINUE` and never forks, dumps an RDB or buffers the full-sync delta, which matters on busy sources whose replica output buffer limit cannot be raised (for example ElastiCache under heavy write load). The offset must still be inside the source's `repl-backlog`, so raise `repl-backlog-size` if the source refuses. Requires Redis >= 4.0 / Valkey.
* `rdb_file_path`: Used with `partial_sync`. RedisShake spools the stream and waits for an RDB file to appear at this path (a `BGSAVE` on a replica, or a managed-service backup, taken **after** RedisShake connected). It loads the file, reads its `repl-id` and `repl-offset` aux fields, and replays the spooled stream from `repl-offset + 1`, so the result equals a normal full sync. Without `rdb_file_path` only the stream from the connect offset is replayed, for targets already seeded to exactly that offset.
