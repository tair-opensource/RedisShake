# aof_reader

## 介绍

可以使用 `aof_reader` 从 AOF 文件中读取数据并写入目标端，常用于从备份文件恢复数据或按时间戳进行数据闪回。

## 配置

```toml
[aof_reader]
filepath = "/tmp/appendonly.aof.manifest" # 或单 AOF 文件 "/tmp/appendonly.aof"
timestamp = 0                             # 闪回截止时间戳（秒），0 表示不限制
```

- `filepath` 应传入绝对路径，支持 Redis 7+ Multi-Part AOF manifest 文件或单个 AOF 文件。
- `timestamp` 为 Unix 秒级时间戳，设为 `0` 时读取全部命令。

## 主要流程

![aof_reader](/aof_reader.jpg)
