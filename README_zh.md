# redis-shake

[![CI](https://github.com/alibaba/RedisShake/actions/workflows/ci.yml/badge.svg?branch=v3)](https://github.com/alibaba/RedisShake/actions/workflows/ci.yml)

redis-shake 是一个用来做 Redis 数据迁移的工具，并提供一定程度的数据清洗能力。

## 特性

* ⚡ 高效地数据迁移
* 🌲 支持 Redis 原生数据结构
* 🌐 支持源端为单机实例，目的端为单机或集群实例
* ✅ 测试在 Redis 5.0、Redis 6.0 和 Redis 7.0
* 🤗 支持使用 lua 自定义过滤规则
* 💪 支持大实例迁移

![image.png](https://s2.loli.net/2022/06/30/vU346lVBrNofKzu.png)

# 文档

## 安装

### 从 Release 下载安装

Release: [https://github.com/alibaba/RedisShake/releases](https://github.com/alibaba/RedisShake/releases)

### 从源码编译

下载源码后，运行 `sh build.sh` 命令编译。

```shell
sh build.sh
```

## 运行

1. 编辑 redis-shake.toml，修改其中的 source 与 target 配置项
2. 启动 redis-shake：

```shell
./bin/redis-shake redis-shake.toml
```

3. 观察数据同步情况

## 配置

redis-shake 配置文件参考 `redis-shake.toml`。 为避免歧义强制要求配置文件中的每一项配置均需要赋值，否则会报错。

## 数据过滤

redis-shake 支持使用 lua 脚本自定义过滤规则，可以实现对数据进行过滤。 搭配 lua 脚本时，redis-shake 启动命令：

```shell
./bin/redis-shake redis-shake.toml filter/xxx.lua
```

lua 的书写参照 `filter/*.lua` 文件，目前提供以下过滤模板供参考：

1. `filter/print.lua`：打印收到的所有命令
2. `filter/swap_db.lua`：交换 db0 和 db1 的数据

### 自定义过滤规则

参照 `filter/print.lua` 新建一个 lua 脚本，并在 lua 脚本中实现 filter 函数，该函数的参数为：

- id：命令序列号
- is_base：是否是从 dump.rdb 文件中读取的命令
- group：命令组，不同命令所归属的 Group 见 [redis/src/commands](https://github.com/redis/redis/tree/unstable/src/commands) 下的描述文件
- cmd_name：命令名称
- keys：命令的 keys
- slots：keys 的 slots
- db_id：数据库 id
- timestamp_ms：命令的时间戳，单位为毫秒。目前版本不支持。

返回值为：

- code
  - 0：表示不过滤该命令
  - 1：表示过滤该命令
  - 2：表示不应该出现该命令，并让 redis-shake 报错退出
- db_id：重定向的 db_id

# 贡献

## lua 脚本

欢迎分享更有创意的 lua 脚本。

1. 在 `filters/` 下添加相关脚本。
2. 在 `README.md` 中添加相关描述。
3. 提交一份 pull request。

## Redis Module 支持

1. 在 `internal/rdb/types` 下添加相关类型。
2. 在 `scripts/commands` 下添加相关命令，并使用脚本生成 `table.go` 文件，移动至 `internal/commands` 目录。
3. 在 `test/cases` 下添加相关测试用例。
4. 提交一份 pull request。

# 感谢

redis-shake 旧版是阿里云基于豌豆荚开源的 redis-port 进行二次开发的一个支持 Redis 异构集群实时同步的工具。
redis-shake v3 在 redis-shake 旧版的基础上重新组织代码结构，使其更具可维护性的版本。

redis-shake v3 参考借鉴了以下项目：

- https://github.com/HDT3213/rdb
- https://github.com/sripathikrishnan/redis-rdb-tools