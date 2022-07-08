# redis-shake

[![CI](https://github.com/alibaba/RedisShake/actions/workflows/ci.yml/badge.svg?branch=v3)](https://github.com/alibaba/RedisShake/actions/workflows/ci.yml)

- [中文文档](./README_zh.md)

redis-shake is a tool for Redis data migration and provides a certain degree of data cleaning capabilities.

## Feature

* ⚡ High efficiency
* 🌲 Native Redis data structure support
* 🌐 Support single instance and cluster
* ✅ Tested on Redis 5.0, Redis 6.0 and Redis 7.0
* 🤗 Supports custom filtering rules using lua
* 💪 Supports large instance migration
* 💖 Supports restore mode and sync mode
* ☁️ Supports ElastiCache and Aliyun Redis

![image.png](https://s2.loli.net/2022/06/30/vU346lVBrNofKzu.png)

# Document

## Install

### Binary package

Release: [https://github.com/alibaba/RedisShake/releases](https://github.com/alibaba/RedisShake/releases)

### Compile from source

After downloading the source code, run the `sh build.sh` command to compile.

```shell
sh build.sh
```

## Usage

1. Edit `redis-shake.toml` or `restore.toml` and modify the source and target configuration items in it.
2. Start redis-shake.

```shell
./bin/redis-shake redis-shake.toml
# or
./bin/redis-shake restore.toml
```

3. Check data synchronization status.

## Configure

The redis-shake configuration file refers to `redis-shake.toml`. In order to avoid ambiguity, it is mandatory that each
configuration in the configuration file needs to be assigned a value, otherwise an error will be reported.

## Data filtering

redis-shake supports custom filtering rules using lua scripts. redis-shake can be started with
the following command:

```shell
./bin/redis-shake redis-shake.toml filter/xxx.lua
```

Some following filter templates are provided in `filter` directory:

1. `filter/print.lua`：print all commands
2. `filter/swap_db.lua`：swap the data of db0 and db1

### Custom filter rules

Refer to `filter/print.lua` to create a new lua script, and implement the filter function in the lua script. The
arguments of the function are:

- id: command id
- is_base: is the command read from the dump.rdb file
- group: command group, see the description file
  under [redis/src/commands](https://github.com/redis/redis/tree/unstable/src/commands)
- cmd_name: command name
- keys: keys in command
- slots: slots in command
- db_id: database id
- timestamp_ms: timestamp of the command in milliseconds. The current version does not support it.

The return value is:

- code
    - 0: allow this command to pass
    - 1: this command is not allowed to pass
    - 2: this command should not appear, and let redis-shake exit with an error
- db_id: redirected db_id

# Contribution

## Lua script

Welcome to share more creative lua scripts.

1. Add lua scripts under `filters/`.
2. Add description to `README.md`.
3. Submit a pull request.

## Redis Module support

1. Add code under `internal/rdb/types`.
2. Add a command file under `scripts/commands`, and use the script to generate a `table.go` file and move it to
   the `internal/commands` directory.
3. Add test cases under `test/cases`.
4. Submit a pull request.

# 感谢

redis-shake 旧版是阿里云基于豌豆荚开源的 redis-port 进行二次开发的一个支持 Redis 异构集群实时同步的工具。
redis-shake v3 在 redis-shake 旧版的基础上重新组织代码结构，使其更具可维护性的版本。

redis-shake v3 参考借鉴了以下项目：

- https://github.com/HDT3213/rdb
- https://github.com/sripathikrishnan/redis-rdb-tools