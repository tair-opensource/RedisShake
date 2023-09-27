---
outline: deep
---

# 最佳实践

## 过滤

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

## 修改

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