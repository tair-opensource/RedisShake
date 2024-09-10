---
outline: deep
---

# 配置文件

RedisShake 使用 [TOML](https://toml.io/cn/) 语言书写，所有的配置参数在 all.toml 中均有说明。

配置文件的组成如下：

```toml

[xxx_reader]
...

[xxx_writer]
...

[filter]
...

[advanced]
...
```


## reader 配置

RedisShake 提供了不同的 Reader 用来对接不同的源端，配置详见 Reader 章节：

* [Sync Reader](../reader/sync_reader.md)
* [Scan Reader](../reader/scan_reader.md)
* [RDB Reader](../reader/rdb_reader.md)
* [AOF Reader](../reader/aof_reader.md)

## writer 配置

RedisShake 提供了不同的 Writer 用来对接不同的目标端，配置详见 Writer 章节：

* [Redis Writer](../writer/redis_writer.md)

## filter 配置

允许通过配置文件设置过滤规则，参考 [过滤与加工](../filter/filter.md) 与 [function](../filter/function.md)。

## advanced 配置

参考 [shake.toml 配置文件](https://github.com/tair-opensource/RedisShake/blob/v4/shake.toml)。