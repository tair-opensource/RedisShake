# Sync Reader

## 介绍

当源端数据库兼容 PSync 协议时，推荐使用 `sync_reader`。兼容 PSync 协议的数据库有：

* Redis
* Tair
* ElastiCache 部分兼容
* MemoryDB 部分兼容

优势：数据一致性最佳，可以实现不停机的切换

## 配置

```toml
[sync_reader]
cluster = false
address = "127.0.0.1:6379" # when cluster is true, address is one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
```
