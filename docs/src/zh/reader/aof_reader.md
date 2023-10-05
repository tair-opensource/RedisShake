# aof_reader

## 介绍

可以使用 `aof_reader` 来从 AOF 文件中读取数据，然后写入目标端。常见于从备份文件中恢复数据，还支持数据闪回。

## 配置

```toml
[aof_reader]
aoffilepath="/tmp/appendonly.aof.manifest" #或者单aof文件 "/tmp/appendonly.aof"
aoftimestamp="0"
```

* 应传入绝对路径。

## 主要流程如下：
![aof_reader.jpg](/public/aof_reader.jpg)