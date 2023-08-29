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

* 当源端为集群时，配置 `cluster` 为 true，`address` 为集群中的任意一个节点即可。`sync_reader` 会通过 `cluster nodes` 命令获取集群中的所有节点信息，并建立连接获取数据。

