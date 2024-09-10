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

You can set filter rules through the configuration file. Refer to [Filter and Processing](../filter/filter.md) and [function](../filter/function.md).

## advanced Configuration

Refer to the [shake.toml configuration file](https://github.com/tair-opensource/RedisShake/blob/v4/shake.toml).
