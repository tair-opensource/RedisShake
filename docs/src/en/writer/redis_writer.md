# Redis Writer

## Introduction

`redis_writer` is used to write data to Redis-like databases.

## Configuration

```toml
[redis_writer]
cluster = false
address = "127.0.0.1:6379" # when cluster is true, address is one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
```

* `cluster`: Whether it's a cluster or not.
* `address`: Connection address. When the destination is a cluster, `address` can be any node in the cluster.
* Authentication:
    * When using the ACL account system, configure both `username` and `password`
    * When using the traditional account system, only configure `password`
    * When no authentication is required, leave both `username` and `password` empty
* `tls`: Whether to connect with TLS/SSL. By default the server certificate is not verified.
* `tls_config` (optional, used when `tls = true`):
    * `ca_cert`: CA certificate file (PEM). When set, RedisShake verifies the server certificate against it.
    * `server_name`: The name the server certificate must contain, by default the host of `address`. Cluster nodes are reached by the addresses `CLUSTER NODES` reports, usually IPs, so for a cluster set a name every node's certificate contains.
    * `cert`, `key`: Client certificate and key files (PEM) for servers that require client certificates. Independent of `ca_cert`.

```toml
tls = true
tls_config = { ca_cert = "/path/ca.crt", server_name = "redis.example.com", cert = "/path/client.crt", key = "/path/client.key" }
```

Important notes:
1. When the destination is a cluster, ensure that the commands from the source satisfy the [requirement that keys' hash values belong to the same slot](https://redis.io/docs/reference/cluster-spec/#implemented-subset).
2. It's recommended to ensure that the destination version is greater than or equal to the source version, otherwise unsupported commands may occur. If a lower version is necessary, you can set `target_redis_proto_max_bulk_len` to 0 to avoid using the `restore` command for data recovery.

## Duplicate Key Handling

When a key already exists in the target Redis, you can control the behavior using the `rdb_restore_command_behavior` configuration.

### Configuration

```toml
[advanced]
rdb_restore_command_behavior = "panic"  # panic, rewrite, or skip
```

### Options

| Value | Description |
|-------|-------------|
| `panic` | Stop when encountering duplicate keys (default) |
| `rewrite` | Overwrite the key in the target |
| `skip` | Skip duplicate keys |

### Applicability

| Reader | Phase | Uses RESTORE | Config Applies |
|--------|-------|--------------|----------------|
| `rdb_reader` | RDB | ✅ Yes | ✅ Yes |
| `sync_reader` | RDB phase | ✅ Yes | ✅ Yes |
| `sync_reader` | AOF phase | ❌ No (forwards commands directly) | ❌ No |
| `scan_reader` | Full/KSN | ✅ Yes | ✅ Yes |

### Limitation

When data size exceeds `target_redis_proto_max_bulk_len`, individual commands (SET, HSET, etc.) are used instead of RESTORE, and this configuration may not be respected.



