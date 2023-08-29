# rdb_reader

## 介绍

可以使用 `rdb_reader` 来从 RDB 文件中读取数据，然后写入目标端。常见于从备份文件中恢复数据。

## 配置

```toml
[rdb_reader]
filepath = "/tmp/dump.rdb"
```

* 应传入绝对路径。
