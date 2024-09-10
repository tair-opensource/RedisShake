---
outline: deep
---

# 什么是 function

RedisShake 通过提供 function 功能，实现了的 [ETL(提取-转换-加载)](https://en.wikipedia.org/wiki/Extract,_transform,_load) 中的 `transform` 能力。通过利用 function 可以实现类似功能：
* 更改数据所属的 `db`，比如将源端的 `db 0` 写入到目的端的 `db 1`。
* 对数据进行筛选，例如，只将 key 以 `user:` 开头的源数据写入到目标端。
* 改变 Key 的前缀，例如，将源端的 key `prefix_old_key` 写入到目标端的 key `prefix_new_key`。
* ...

要使用 function 功能，只需编写一份 lua 脚本。RedisShake 在从源端获取数据后，会将数据转换为 Redis 命令。然后，它会处理这些命令，从中解析出 `KEYS`、`ARGV`、`SLOTS`、`GROUP` 等信息，并将这些信息传递给 lua 脚本。lua 脚本会处理这些数据，并返回处理后的命令。最后，RedisShake 会将处理后的数据写入到目标端。

以下是一个具体的例子：
```toml
function = """
shake.log(DB)
if DB == 0
then
    return
end
shake.call(DB, ARGV)
"""

[sync_reader]
address = "127.0.0.1:6379"

[redis_writer]
address = "127.0.0.1:6380"
```
`DB` 是 RedisShake 提供的信息，表示当前数据所属的 db。`shake.log` 用于打印日志，`shake.call` 用于调用 Redis 命令。上述脚本的目的是丢弃源端 `db` 0 的数据，将其他 `db` 的数据写入到目标端。

除了 `DB`，还有其他信息如 `KEYS`、`ARGV`、`SLOTS`、`GROUP` 等，可供调用的函数有 `shake.log` 和 `shake.call`，具体请参考 [function API](#function-api)。

## function API

### 变量

因为有些命令中含有多个 key，比如 `mset` 等命令。所以，`KEYS`、`KEY_INDEXES`、`SLOTS` 这三个变量都是数组类型。如果确认命令只有一个 key，可以直接使用 `KEYS[1]`、`KEY_INDEXES[1]`、`SLOTS[1]`。

| 变量 | 类型 | 示例 | 描述 |
|-|-|-|-----|
| DB | number | 1 | 命令所属的 `db` |
| GROUP | string | "LIST" | 命令所属的 `group`，符合 [Command key specifications](https://redis.io/docs/reference/key-specs/)，可以在 [commands](https://github.com/tair-opensource/RedisShake/tree/v4/scripts/commands) 中查询每个命令的 `group` 字段 |
| CMD | string | "XGROUP-DELCONSUMER" | 命令的名称 |
| KEYS | table | \{"key1", "key2"\} | 命令的所有 Key |
| KEY_INDEXES | table | \{2, 4\} | 命令的所有 Key 在 `ARGV` 中的索引 |
| SLOTS | table | \{9189, 4998\} | 当前命令的所有 Key 所属的 [slot](https://redis.io/docs/reference/cluster-spec/#key-distribution-model) |
| ARGV | table | \{"mset", "key1", "value1", "key2", "value2"\} | 命令的所有参数 |

### 函数
* `shake.call(DB, ARGV)`：返回一个 Redis 命令，RedisShake 会将该命令写入目标端。
* `shake.log(msg)`：打印日志。

## 最佳实践


### 过滤 Key

```lua
local prefix = "user:"
local prefix_len = #prefix

if string.sub(KEYS[1], 1, prefix_len) ~= prefix then
  return
end

shake.call(DB, ARGV)
```

效果是只将 key 以 `user:` 开头的源数据写入到目标端。没有考虑 `mset` 等多 key 命令的情况。

### 过滤 DB

```lua
shake.log(DB)
if DB == 0
then
    return
end
shake.call(DB, ARGV)
```

效果是丢弃源端 `db` 0 的数据，将其他 `db` 的数据写入到目标端。


### 过滤某类数据结构

可以通过 `GROUP` 变量来判断数据结构类型，支持的数据结构类型有：`STRING`、`LIST`、`SET`、`ZSET`、`HASH`、`SCRIPTING` 等。

#### 过滤 Hash 类型数据
```lua
if GROUP == "HASH" then
  return
end
shake.call(DB, ARGV)
```

效果是丢弃源端的 `hash` 类型数据，将其他数据写入到目标端。

#### 过滤 [LUA 脚本](https://redis.io/docs/interact/programmability/eval-intro/)

```lua
if GROUP == "SCRIPTING" then
  return
end
shake.call(DB, ARGV)
```

效果是丢弃源端的 `lua` 脚本，将其他数据写入到目标端。常见于主从同步至集群时，存在集群不支持的 LUA 脚本。

### 修改 Key 的前缀

```lua
local prefix_old = "prefix_old_"
local prefix_new = "prefix_new_"

shake.log("old=" .. table.concat(ARGV, " "))

for i, index in ipairs(KEY_INDEXES) do
  local key = ARGV[index]
  if string.sub(key, 1, #prefix_old) == prefix_old then
    ARGV[index] = prefix_new .. string.sub(key, #prefix_old + 1)
  end
end

shake.log("new=" .. table.concat(ARGV, " "))
shake.call(DB, ARGV)
```
效果是将源端的 key `prefix_old_key` 写入到目标端的 key `prefix_new_key`。

### 交换 DB
    
```lua
local db1 = 1
local db2 = 2

if DB == db1 then
  DB = db2
elseif DB == db2 then
  DB = db1
end
shake.call(DB, ARGV)
```

效果是将源端的 `db 1` 写入到目标端的 `db 2`，将源端的 `db 2` 写入到目标端的 `db 1`, 其他 `db` 不变。