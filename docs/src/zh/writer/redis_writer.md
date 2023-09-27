# Redis Writer

## 介绍

`redis_writer` 用于将数据写入 Redis-like 数据库。

## 配置

```toml
[redis_writer]
cluster = false
address = "127.0.0.1:6379" # when cluster is true, address is one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
```

* `cluster`：是否为集群。
* `address`：连接地址。当目的端为集群时，`address` 填写集群中的任意一个节点即可
* 鉴权：
    * 当使用 ACL 账号体系时，配置 `username` 和 `password`
    * 当使用传统账号体系时，仅配置 `password`
    * 当无鉴权时，不配置 `username` 和 `password`
* `tls`：是否开启 TLS/SSL，不需要配置证书因为 RedisShake 没有校验服务器证书

注意事项：
1. 当目的端为集群时，应保证源端发过来的命令满足 [Key 的哈希值属于同一个 slot](https://redis.io/docs/reference/cluster-spec/#implemented-subset)。
2. 应尽量保证目的端版本大于等于源端版本，否则可能会出现不支持的命令。如确实需要降低版本，可以设置 `target_redis_proto_max_bulk_len` 为 0，来避免使用 `restore` 命令恢复数据。
