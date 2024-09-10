---
outline: deep
---

# What is function

RedisShake provides a function feature that implements the `transform` capability in [ETL (Extract-Transform-Load)](https://en.wikipedia.org/wiki/Extract,_transform,_load). By utilizing functions, you can achieve similar functionalities:
* Change the `db` to which data belongs, for example, writing data from source `db 0` to destination `db 1`.
* Filter data, for instance, only writing source data with keys starting with `user:` to the destination.
* Modify key prefixes, such as writing a source key `prefix_old_key` to a destination key `prefix_new_key`.
* ...

To use the function feature, you only need to write a Lua script. After RedisShake retrieves data from the source, it converts the data into Redis commands. Then, it processes these commands, parsing information such as `KEYS`, `ARGV`, `SLOTS`, `GROUP`, and passes this information to the Lua script. The Lua script processes this data and returns the processed commands. Finally, RedisShake writes the processed data to the destination.

Here's a specific example:
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
`DB` is information provided by RedisShake, indicating the db to which the current data belongs. `shake.log` is used for logging, and `shake.call` is used to call Redis commands. The purpose of the above script is to discard data from source `db 0` and write data from other `db`s to the destination.

In addition to `DB`, there is other information such as `KEYS`, `ARGV`, `SLOTS`, `GROUP`, and available functions include `shake.log` and `shake.call`. For details, please refer to [function API](#function-api).

## function API

### Variables

Because some commands contain multiple keys, such as the `mset` command, the variables `KEYS`, `KEY_INDEXES`, and `SLOTS` are all array types. If you are certain that a command has only one key, you can directly use `KEYS[1]`, `KEY_INDEXES[1]`, `SLOTS[1]`.

| Variable | Type | Example | Description |
|-|-|-|-----|
| DB | number | 1 | The `db` to which the command belongs |
| GROUP | string | "LIST" | The `group` to which the command belongs, conforming to [Command key specifications](https://redis.io/docs/reference/key-specs/). You can check the `group` field for each command in [commands](https://github.com/tair-opensource/RedisShake/tree/v4/scripts/commands) |
| CMD | string | "XGROUP-DELCONSUMER" | The name of the command |
| KEYS | table | {"key1", "key2"} | All keys of the command |
| KEY_INDEXES | table | {2, 4} | The indexes of all keys in `ARGV` |
| SLOTS | table | {9189, 4998} | The [slots](https://redis.io/docs/reference/cluster-spec/#key-distribution-model) to which all keys of the current command belong |
| ARGV | table | {"mset", "key1", "value1", "key2", "value2"} | All parameters of the command |

### Functions
* `shake.call(DB, ARGV)`: Returns a Redis command that RedisShake will write to the destination.
* `shake.log(msg)`: Prints logs.

## Best Practices


### Filtering Keys

```lua
local prefix = "user:"
local prefix_len = #prefix

if string.sub(KEYS[1], 1, prefix_len) ~= prefix then
  return
end

shake.call(DB, ARGV)
```

The effect is to only write source data with keys starting with `user:` to the destination. This doesn't consider cases of multi-key commands like `mset`.

### Filtering DB

```lua
shake.log(DB)
if DB == 0
then
    return
end
shake.call(DB, ARGV)
```

The effect is to discard data from source `db 0` and write data from other `db`s to the destination.


### Filtering Certain Data Structures

You can use the `GROUP` variable to determine the data structure type. Supported data structure types include: `STRING`, `LIST`, `SET`, `ZSET`, `HASH`, `SCRIPTING`, etc.

#### Filtering Hash Type Data
```lua
if GROUP == "HASH" then
  return
end
shake.call(DB, ARGV)
```

The effect is to discard `hash` type data from the source and write other data to the destination.

#### Filtering [LUA Scripts](https://redis.io/docs/interact/programmability/eval-intro/)

```lua
if GROUP == "SCRIPTING" then
  return
end
shake.call(DB, ARGV)
```

The effect is to discard `lua` scripts from the source and write other data to the destination. This is common when synchronizing from master-slave to cluster, where there are LUA scripts not supported by the cluster.

### Modifying Key Prefixes

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
The effect is to write the source key `prefix_old_key` to the destination key `prefix_new_key`.

### Swapping DBs
    
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

The effect is to write source `db 1` to destination `db 2`, write source `db 2` to destination `db 1`, and leave other `db`s unchanged.