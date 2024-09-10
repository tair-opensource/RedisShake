---
outline: deep
---

# 什么是 RedisShake

RedisShake 是一个用于处理和迁移 Redis 数据的工具，它提供以下特性：

1. **Redis 兼容性**：RedisShake 兼容从 2.8 到 7.2 的 Redis 版本，并支持各种部署方式，包括单机，主从，哨兵和集群。
2. **云服务兼容性**：RedisShake 与主流云服务提供商提供的流行 Redis-like 数据库无缝工作，包括但不限于：
    - [阿里云-云数据库 Redis 版](https://www.aliyun.com/product/redis)
    - [阿里云-云原生内存数据库Tair](https://www.aliyun.com/product/apsaradb/kvstore/tair)
    - [AWS - ElastiCache](https://aws.amazon.com/elasticache/)
    - [AWS - MemoryDB](https://aws.amazon.com/memorydb/)
3. **Module 兼容**：RedisShake
   与 [TairString](https://github.com/tair-opensource/TairString)，[TairZSet](https://github.com/tair-opensource/TairZset)
   和 [TairHash](https://github.com/tair-opensource/TairHash) 模块兼容。
4. **多种导出模式**：RedisShake 支持 PSync，RDB 和 Scan 导出模式。
5. **数据处理**：RedisShake 通过自定义脚本实现数据过滤和转换。

## 贡献

我们欢迎社区的贡献。对于重大变更，请先开一个 issue 来讨论你想要改变的内容。我们特别感兴趣的是：

1. 添加对更多模块的支持
2. 提高对 Readers 和 Writers 的支持
3. 分享你的 Lua 脚本和最佳实践

## 历史

RedisShake 是阿里云 [Tair 团队](https://github.com/tair-opensource)
积极维护的一个项目。它的演变可以追溯到其初始版本，该版本是从 [redis-port](https://github.com/CodisLabs/redis-port) 分支出来的。

版本（不同版本间配置不通用）：

- [RedisShake 2.x](https://github.com/tair-opensource/RedisShake/tree/v2) 版本带来了一系列的改进和更新，提高了其整体稳定性和性能。
- [RedisShake 3.x](https://github.com/tair-opensource/RedisShake/tree/v3) 版本是一个重要的里程碑，整个代码库被完全重写和优化，具有更好的效率和可用性。
- [RedisShake 4.x](https://github.com/tair-opensource/RedisShake/tree/v4) 版本
  ，进一步增强了 [Reader](../reader/scan_reader.md)、配置、可观察性和 function 相关的特性。

## 许可证

RedisShake 在 [MIT 许可证](https://github.com/tair-opensource/RedisShake/blob/v2/license.txt) 下开源。