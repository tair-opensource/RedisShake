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
