# RedisShake 4.x: Redis Data Processing & Migration Tool

[![CI](https://github.com/tair-opensource/RedisShake/actions/workflows/ci.yml/badge.svg?event=push&branch=v4)](https://github.com/tair-opensource/RedisShake/actions/workflows/ci.yml)
[![CI](https://github.com/tair-opensource/RedisShake/actions/workflows/pages.yml/badge.svg?branch=v4)](https://github.com/tair-opensource/RedisShake/actions/workflows/pages.yml)
[![CI](https://github.com/tair-opensource/RedisShake/actions/workflows/release.yml/badge.svg?branch=v4)](https://github.com/tair-opensource/RedisShake/actions/workflows/release.yml)

- [中文文档](https://tair-opensource.github.io/RedisShake/)
- [English Documentation](https://tair-opensource.github.io/RedisShake/en/)

## Overview

RedisShake is a tool designed for processing and migrating Redis data. It offers the following features:

1. **Redis Compatibility**: RedisShake is compatible with Redis versions ranging from 2.8 to 7.2, and supports various
   deployment methods including standalone, master-slave, sentinel, and cluster.

2. **Cloud Service Compatibility**: RedisShake works seamlessly with popular Redis-like databases provided by leading
   cloud service providers, including but not limited to:
    - [Alibaba Cloud - ApsaraDB for Redis](https://www.alibabacloud.com/product/apsaradb-for-redis)
    - [Alibaba Cloud - Tair](https://www.alibabacloud.com/product/tair)
    - [AWS - ElastiCache](https://aws.amazon.com/elasticache/)
    - [AWS - MemoryDB](https://aws.amazon.com/memorydb/)

3. **Module Compatibility**: RedisShake is compatible
   with [TairString](https://github.com/tair-opensource/TairString), [TairZSet](https://github.com/tair-opensource/TairZset),
   and [TairHash](https://github.com/tair-opensource/TairHash) modules.

4. **Multiple Export Modes**: RedisShake supports PSync, RDB, and Scan export modes.

5. **Data Processing**: RedisShake enables data filtering and transformation through custom scripts.

## Getting Started

### Installation

#### Download the Binary Package

Download the binary package directly from the [Releases](https://github.com/tair-opensource/RedisShake/releases) page.

#### Compile from Source

To compile from source, ensure that you have a Golang environment set up on your local machine:

```shell
git clone https://github.com/alibaba/RedisShake
cd RedisShake
sh build.sh
```

### Usage

Assume you have two Redis instances:

* Instance A: 127.0.0.1:6379
* Instance B: 127.0.0.1:6380

Create a new configuration file `shake.toml`:

```toml
[sync_reader]
address = "127.0.0.1:6379"

[redis_writer]
address = "127.0.0.1:6380"
```

To start RedisShake, run the following command:

```shell
./redis-shake shake.toml
```

For more detailed information, please refer to the documentation:

- [中文文档](https://tair-opensource.github.io/RedisShake/)
- [English Documentation](https://tair-opensource.github.io/RedisShake/en/)

## Contributing

We welcome contributions from the community. For significant changes, please open an issue first to discuss what you
would like to change. We are particularly interested in:

1. Adding support for more modules
2. Enhancing support for Readers and Writers
3. Sharing your Lua scripts and best practices

## History

RedisShake is a project actively maintained by the [Tair team](https://github.com/tair-opensource) at Alibaba Cloud. Its
evolution can be traced back to its initial version, which was forked
from [redis-port](https://github.com/CodisLabs/redis-port).

During its evolution:

- The [RedisShake 2.x](https://github.com/tair-opensource/RedisShake/tree/v2) version brought a series of improvements
  and updates, enhancing its overall stability and performance.
- The [RedisShake 3.x](https://github.com/tair-opensource/RedisShake/tree/v3) version represented a significant
  milestone where the entire codebase was completely rewritten and optimized, leading to better efficiency and
  usability.
- The current version, [RedisShake 4.x](https://github.com/tair-opensource/RedisShake/tree/v4), has further enhanced
  features related to readers, configuration, observability, and functions.

## License

RedisShake is open-sourced under the [MIT license](https://github.com/tair-opensource/RedisShake/blob/v2/license.txt).
