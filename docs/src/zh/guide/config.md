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

`[filter]` 配置段包含两层能力：

* **规则过滤器：** 通过 `allow_*`、`block_*` 列表控制同步哪些 Key、数据库、命令或命令组。详细语义与示例见 [过滤与加工](../filter/filter.md)。
* **Lua function 钩子：** 在 `function` 选项中编写内联 Lua 代码，对通过规则过滤的命令进行改写或拆分。更多 API 与最佳实践见 [function](../filter/function.md)。

过滤器总是先于 Lua 执行。被规则拦截的命令既不会进入脚本，也不会写入目标端，从而把 Lua 的处理范围限定在已经允许的少量流量上。

## advanced 配置

参考 [shake.toml 配置文件](https://github.com/tair-opensource/RedisShake/blob/v4/shake.toml)。