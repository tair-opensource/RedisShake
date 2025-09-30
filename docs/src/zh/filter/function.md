---
outline: deep
---

# 什么是 function

**function** 选项是 `[filter]` 配置段的 Lua 钩子。内置过滤规则会先判定命令是否允许离开 RedisShake，只有通过过滤的命令才会进入 Lua 脚本，在写入目标端之前完成重写、拆分或补充信息。该钩子适用于静态 allow/block 规则难以覆盖的轻量级转换需求。

通过 function 可以：

* 更改数据所属的 `db`，例如将源端 `db 0` 写入到目标端 `db 1`。
* 根据业务规则筛选或丢弃部分数据。
* 重写命令，例如将 `MSET` 拆分成多个 `SET`，或追加新的 key 前缀。
* 基于输入数据额外产生命令（如写入监控或预热缓存）。

## 执行流程

1. RedisShake 从 Reader 获取命令，并解析出命令名称、Key、slot、命令组等信息。
2. 内置过滤规则会先判定命令，未通过的命令不会交给 Lua 或 writer。
3. 对剩余数据，RedisShake 创建 Lua 虚拟机并注入只读上下文变量（如 `DB`、`CMD`、`KEYS`）以及 `shake` 辅助函数。
4. Lua 脚本可多次调用 `shake.call` 决定向下游输出哪些命令。

如果脚本未调用 `shake.call`，原始命令将不会写入目标端。这方便实现“丢弃并替换”的逻辑，但也意味着忘记调用 `shake.call` 会导致数据悄然丢失，测试时请务必配合日志。

## 快速上手

在配置文件的 `[filter]` 节写入 Lua 脚本：

```toml
[filter]
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

`DB` 表示当前命令所属的数据库；`shake.log` 用于打印日志，`shake.call` 则将命令写入目标端。上述脚本会丢弃源端 `db 0` 的数据，并同步其他数据库的数据。

## function API

### 变量

由于 `MSET` 等命令包含多个 Key，`KEYS`、`KEY_INDEXES`、`SLOTS` 都是数组类型。如果可以确定命令只有一个 Key，可直接使用 `KEYS[1]`、`KEY_INDEXES[1]`、`SLOTS[1]`。

| 变量 | 类型 | 示例 | 描述 |
| --- | --- | --- | --- |
| `DB` | number | `1` | 命令所属的数据库 |
| `CMD` | string | `"XGROUP-DELCONSUMER"` | 命令名称 |
| `GROUP` | string | `"LIST"` | 命令所属的组，符合 [Command key specifications](https://redis.io/docs/reference/key-specs/)，可在 [commands](https://github.com/tair-opensource/RedisShake/tree/v4/scripts/commands) 中查看 |
| `KEYS` | table | `{"key1", "key2"}` | 命令包含的所有 Key |
| `KEY_INDEXES` | table | `{2, 4}` | 所有 Key 在 `ARGV` 中的索引 |
| `SLOTS` | table | `{9189, 4998}` | 当前命令所有 Key 的 slot（集群模式） |
| `ARGV` | table | `{"mset", "key1", "value1", "key2", "value2"}` | 命令的所有参数，索引 `1` 为命令名称 |

### 函数

* `shake.call(db, argv_table)`：向写入端输出命令。`argv_table` 的第一个元素必须是命令名称；可多次调用以拆分命令，例如将 `MSET` 拆分为多个 `SET`。 
* `shake.log(msg)`：在 `shake.log` 中输出带有 `lua log:` 前缀的日志，可用于调试脚本。

## 最佳实践

### 通用建议

* **保持幂等。** RedisShake 可能会重试命令，脚本应避免依赖不可重复的副作用。
* **注意空 Key。** 某些命令（如 `PING`）没有 Key，访问 `KEYS[1]` 前需要判空，避免脚本运行异常。
* **尽量保持脚本简单。** 复杂循环会增加 Lua VM 的执行时间，可考虑通过 filter 缩小处理范围或在链路外完成重度计算。

### 过滤 Key

```lua
local prefix = "user:"
local prefix_len = #prefix

if not KEYS[1] or string.sub(KEYS[1], 1, prefix_len) ~= prefix then
  return
end

shake.call(DB, ARGV)
```

效果是只将 key 以 `user:` 开头的源数据写入目标端；未考虑 `MSET` 等多 key 场景。

### 过滤 DB

```lua
shake.log(DB)
if DB == 0
then
    return
end
shake.call(DB, ARGV)
```

效果是丢弃源端 `db 0` 的数据，将其他 `db` 的数据写入目标端。

### 过滤某类数据结构

可以通过 `GROUP` 变量来判断数据结构类型，支持 `STRING`、`LIST`、`SET`、`ZSET`、`HASH`、`SCRIPTING` 等。

#### 过滤 Hash 类型数据

```lua
if GROUP == "HASH" then
  return
end
shake.call(DB, ARGV)
```

效果是丢弃源端的 `hash` 类型数据，将其他数据写入到目标端。

#### 过滤 [Lua 脚本](https://redis.io/docs/interact/programmability/eval-intro/)

```lua
if GROUP == "SCRIPTING" then
  return
end
shake.call(DB, ARGV)
```

效果是丢弃源端的 Lua 脚本，将其他数据写入到目标端。常见于主从同步至集群时，目标不支持部分脚本。

### 拆分命令

```lua
if CMD == "MSET" then
  for i = 2, #ARGV, 2 do
    shake.call(DB, {"SET", ARGV[i], ARGV[i + 1]})
  end
  return
end

shake.call(DB, ARGV)
```

该模式将一条 `MSET` 拆分为多条 `SET`，适合目标端只能处理单 Key 写入的场景。

### 修改 Key 前缀

```lua
local prefix_old = "prefix_old_"
local prefix_new = "prefix_new_"

shake.log("old=" .. table.concat(ARGV, " "))

for _, index in ipairs(KEY_INDEXES) do
  local key = ARGV[index]
  if key and string.sub(key, 1, #prefix_old) == prefix_old then
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

效果是将源端的 `db 1` 写入到目标端的 `db 2`，将源端的 `db 2` 写入到目标端的 `db 1`，其他 `db` 不变。

## 排障建议

* **脚本无法编译：** 启动时 RedisShake 会提前编译脚本并在语法错误时退出，检查日志中给出的行号。
* **目标端没有数据：** 确保所有分支都调用了 `shake.call`，并使用 `shake.log` 输出关键信息进行验证。
* **性能下降：** 脚本过重可能成为瓶颈，可通过 filter 缩小输入或将复杂计算移出 RedisShake。
