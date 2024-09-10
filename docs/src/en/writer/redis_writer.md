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
* `tls`: Whether to enable TLS/SSL. No need to configure certificates as RedisShake doesn't verify server certificates.

Important notes:
1. When the destination is a cluster, ensure that the commands from the source satisfy the [requirement that keys' hash values belong to the same slot](https://redis.io/docs/reference/cluster-spec/#implemented-subset).
2. It's recommended to ensure that the destination version is greater than or equal to the source version, otherwise unsupported commands may occur. If a lower version is necessary, you can set `target_redis_proto_max_bulk_len` to 0 to avoid using the `restore` command for data recovery.



