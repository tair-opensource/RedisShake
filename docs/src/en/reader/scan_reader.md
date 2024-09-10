---
outline: deep
---

# Scan Reader

::: tip
The performance and data consistency of `scan_reader` are not as good as [`sync_reader`](sync_reader.md). You should choose `sync_reader` whenever possible.
:::

## Principle Introduction

Scan Reader has two stages: SCAN and KSN. The SCAN stage is for full synchronization, while the KSN stage is for incremental synchronization.

### Full Data

**SCAN stage**: Enabled by default, can be disabled through the `scan` configuration. `scan_reader` uses the `SCAN` command to traverse all Keys in the source database, then uses `DUMP` to get the Value corresponding to the Key, and writes to the destination through the `RESTORE` command, completing full data synchronization.

1. Redis's `SCAN` command only guarantees that Keys that <u>exist throughout the SCAN operation</u> will definitely be returned, but newly written Keys may be missed, and Keys deleted during this period may have already been written to the destination.
2. During the SCAN stage, RedisShake will calculate the current synchronization progress through the cursor returned by the `SCAN command`. This progress has a large error and is for reference only. For non-Redis databases, the cursor calculation method is different from Redis, so you may see incorrect progress displays, which can be ignored.

### Incremental Data

**KSN stage**: Disabled by default, can be enabled through `ksn`, which can solve the problem of missing Keys during the SCAN stage. Incremental data synchronization does not start after the SCAN stage ends, but proceeds simultaneously with it, and continues after the SCAN stage ends until RedisShake exits.

`ksn` uses [Redis keyspace notifications](https://redis.io/docs/manual/keyspace-notifications/) capability to subscribe to Key changes. Specifically, RedisShake will use the `psubscribe` command to subscribe to `__keyevent@*__:*`. When a Key changes, RedisShake will receive the modified Key, then use the `DUMP` and `RESTORE` commands to read the content of the Key from the source and write it to the destination.
1. Redis does not enable the `notify-keyspace-events` configuration by default. It needs to be manually enabled, ensuring the value contains `AE`.
2. If the source disconnects during the KSN stage, consider appropriately increasing the value of `client-output-buffer-limit pubsub`. [802](https://github.com/tair-opensource/RedisShake/issues/802)
3. `Redis keyspace notifications` will not detect `FLUSHALL` and `FLUSHDB` commands, so when using the `ksn` parameter, ensure that the source database does not execute these two commands.

### Performance Impact

Both SCAN and KSN stages use the DUMP command to obtain data. The DUMP command is CPU-intensive and will cause high pressure on the source. It needs to be used carefully to avoid affecting the availability of the source instance.
* For the SCAN stage, you can adjust the `count` parameter to reduce the pressure on the source. It's recommended to start from 1 and gradually increase.
* For the KSN stage, there are currently no adjustable parameters. The decision to enable it should be based on an assessment of the write request volume at the source.

Reference data for performance impact: When the source instance's write QPS is about 150,000, the source CPU usage is 47%. After enabling RedisShake, the source CPU usage becomes 91%.

## Configuration

```
cluster = false            # set to true if source is a redis cluster
address = "127.0.0.1:6379" # when cluster is true, set address to one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
dbs = []                   # set you want to scan dbs such as [1,5,7], if you don't want to scan all
scan = true                # set to false if you don't want to scan keys
ksn = false                # set to true to enabled Redis keyspace notifications (KSN) subscription
count = 1                  # number of keys to scan per iteration
```

* `cluster`: Whether the source is a cluster
* `address`: Source address. When the source is a cluster, `address` can be any node in the cluster
* Authentication:
    * When the source uses ACL accounts, configure `username` and `password`
    * When the source uses traditional accounts, only configure `password`
    * When the source has no authentication, do not configure `username` and `password`
* `tls`: Whether the source has enabled TLS/SSL. No need to configure a certificate because RedisShake does not verify the server certificate
* `dbs`: For non-cluster mode sources, supports synchronizing only specified DB libraries.
* `scan`: Whether to enable the SCAN stage. When set to false, RedisShake will skip the full synchronization stage
* `ksn`: After enabling the `ksn` parameter, RedisShake will subscribe to Key changes at the source to achieve incremental synchronization
* `count`: The number of keys fetched from the source each time during full synchronization. The default is 1. Changing to a larger value can significantly improve synchronization efficiency, but will also increase pressure on the source.
