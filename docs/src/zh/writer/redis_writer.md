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

* 当目的端为集群时，配置 cluster 为 true，address 为集群中的任意一个节点即可。`redis_writer` 会通过 `cluster nodes` 命令获取集群中的所有节点，并建立连接。
* 当目的端为集群时，应保证源端发过来的命令满足 [Key 的哈希值属于同一个 slot](https://redis.io/docs/reference/cluster-spec/#implemented-subset)。