---
outline: deep
---

# Configuration File

RedisShake uses the [TOML](https://toml.io/) language for writing, and all configuration parameters are explained in all.toml.

The configuration file is composed as follows:

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

## reader Configuration

RedisShake provides different Readers to interface with different sources, see the Reader section for configuration details:

* [Sync Reader](../reader/sync_reader.md)
* [Scan Reader](../reader/scan_reader.md)
* [RDB Reader](../reader/rdb_reader.md)
* [AOF Reader](../reader/aof_reader.md)

## writer Configuration

RedisShake provides different Writers to interface with different targets, see the Writer section for configuration details:

* [Redis Writer](../writer/redis_writer.md)

## filter Configuration

The `[filter]` section contains two layers:

* **Rule engine:** Configure `allow_*` and `block_*` lists to keep or drop keys, databases, commands, and command groups. See [Filter and Processing](../filter/filter.md) for detailed semantics and examples.
* **Lua function hook:** Provide inline Lua code via the `function` option to rewrite commands after they pass the rule engine. See [function](../filter/function.md) for API details and best practices.

Filters always run before the Lua hook. Commands blocked by the rule engine never enter the script or reach the writer, so you can reserve the Lua layer for the smaller, approved subset of traffic.

## advanced Configuration

Refer to the [shake.toml configuration file](https://github.com/tair-opensource/RedisShake/blob/v4/shake.toml).
