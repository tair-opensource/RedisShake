---
outline: deep
---

# What is RedisShake

RedisShake is a tool for processing and migrating Redis data, offering the following features:

1. **Redis Compatibility**: RedisShake is compatible with Redis versions from 2.8 to 7.2 and supports various deployment methods, including standalone, master-slave, sentinel, and cluster.
2. **Cloud Service Compatibility**: RedisShake seamlessly works with popular Redis-like databases provided by mainstream cloud service providers, including but not limited to:
    - [Alibaba Cloud - ApsaraDB for Redis](https://www.alibabacloud.com/product/apsaradb-for-redis)
    - [Alibaba Cloud - Tair](https://www.alibabacloud.com/product/tair)
    - [AWS - ElastiCache](https://aws.amazon.com/elasticache/)
    - [AWS - MemoryDB](https://aws.amazon.com/memorydb/)
3. **Module Compatibility**: RedisShake is compatible with [TairString](https://github.com/tair-opensource/TairString), [TairZSet](https://github.com/tair-opensource/TairZset), and [TairHash](https://github.com/tair-opensource/TairHash) modules.
4. **Various Export Modes**: RedisShake supports PSync, RDB, and Scan export modes.
5. **Data Processing**: RedisShake implements data filtering and transformation through custom scripts.

## Contributions

We welcome contributions from the community. For significant changes, please open an issue first to discuss what you would like to change. We are particularly interested in:

1. Adding support for more modules
2. Enhancing the support for Readers and Writers
3. Sharing your Lua scripts and best practices

## History

RedisShake is a project actively maintained by the Alibaba Cloud [Tair Team](https://github.com/tair-opensource). Its evolution can be traced back to its initial version, which was branched out from [redis-port](https://github.com/CodisLabs/redis-port).

Versions (configurations are not interchangeable between different versions):

- The [RedisShake 2.x](https://github.com/tair-opensource/RedisShake/tree/v2) version brought a series of improvements and updates, enhancing its overall stability and performance.
- The [RedisShake 3.x](https://github.com/tair-opensource/RedisShake/tree/v3) version was a significant milestone, with the entire codebase being completely rewritten and optimized for better efficiency and availability.
- The [RedisShake 4.x](https://github.com/tair-opensource/RedisShake/tree/v4) version further enhanced features related to the [Reader](../reader/scan_reader.md), configuration, observability, and [function](../function/introduction.md).

## License

RedisShake is open-source under the [MIT License](https://github.com/tair-opensource/RedisShake/blob/v2/license.txt).