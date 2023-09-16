# Sync Reader

## 介绍

当源端数据库兼容 PSync 协议时，推荐使用 `sync_reader`。兼容 PSync 协议的数据库有：

* Redis
* Tair
* ElastiCache 部分兼容
* MemoryDB 部分兼容

优势：数据一致性最佳，对源库影响小，可以实现不停机的切换

## 配置

```toml
[sync_reader]
cluster = false            # set to true if source is a redis cluster
address = "127.0.0.1:6379" # when cluster is true, set address to one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
```

* `cluster`：源端是否为集群
* `address`：源端地址, 当源端为集群时，`address` 为集群中的任意一个节点即可
* 鉴权：
    * 当源端使用 ACL 账号时，配置 `username` 和 `password`
    * 当源端使用传统账号时，仅配置 `password`
    * 当源端无鉴权时，不配置 `username` 和 `password`
* `tls`：源端是否开启 TLS/SSL，不需要配置证书因为 RedisShake 没有校验服务器证书