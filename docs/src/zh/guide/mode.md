---
outline: deep
---

# 迁移模式选择

目前 RedisShake 有三种迁移模式：`PSync`、`RDB` 和
`SCAN`，分别对应 [`psync_reader`](../reader/psync_reader.md)、[`rdb_reader`](../reader/rdb_reader.md)
和 [`scan_reader`](../reader/scan_reader.md)。这三种模式各有适合的场景，本文根据场景特点介绍如何选择。

TODO
