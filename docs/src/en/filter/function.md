---
outline: deep
---

# What is function

The **function** option extends the `[filter]` section with a Lua hook. Built-in filter rules run first to decide whether a command should leave RedisShake; only the surviving commands enter the Lua function, where you can reshape, split, or enrich them before they reach the destination. This hook is intended for lightweight adjustments that are difficult to express with static allow/block lists.

With the function feature you can:

* Change the database (`db`) to which data belongs (for example, write source `db 0` into destination `db 1`).
* Filter or drop specific data, keeping only keys that match custom business rules.
* Rewrite commands, such as expanding `MSET` into multiple `SET` commands or adding new key prefixes.
* Emit additional commands (for metrics or cache warming) derived from the incoming data stream.

## Execution Flow

1. RedisShake retrieves commands from the reader and parses metadata such as command name, keys, key slots, and group.
2. Built-in filter rules evaluate the command. Anything blocked here never reaches Lua or the writer.
3. For the remaining entries, RedisShake creates a Lua state and exposes read-only context variables (`DB`, `CMD`, `KEYS`, and so on) plus helper functions under the `shake` table.
4. Your Lua code decides which commands to send downstream by calling `shake.call` zero or more times.

If your script does not invoke `shake.call`, the original command is suppressed. This makes it easy to implement drop-and-replace logic, but also means forgetting a `shake.call` will silently discard data. Always add logging while testing.

## Quick Start

Place the Lua script inline in the `[filter]` section of the configuration file:

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

`DB` is information provided by RedisShake, indicating the database to which the current data belongs. `shake.log` is used for logging, and `shake.call` emits a Redis command to the destination. The above script discards data from source `db 0` and forwards data from the other databases.

## function API

### Variables

Because some commands contain multiple keys, such as `MSET`, the variables `KEYS`, `KEY_INDEXES`, and `SLOTS` are all array types. If you are certain that a command has only one key, you can directly use `KEYS[1]`, `KEY_INDEXES[1]`, and `SLOTS[1]`.

| Variable | Type | Example | Description |
| --- | --- | --- | --- |
| `DB` | number | `1` | The database to which the command belongs. |
| `CMD` | string | `"XGROUP-DELCONSUMER"` | The name of the command. |
| `GROUP` | string | `"LIST"` | The command group, conforming to [Command key specifications](https://redis.io/docs/reference/key-specs/). You can check the `group` field for each command in [commands](https://github.com/tair-opensource/RedisShake/tree/v4/scripts/commands). |
| `KEYS` | table | `{"key1", "key2"}` | All keys of the command. |
| `KEY_INDEXES` | table | `{2, 4}` | Indexes of all keys inside `ARGV`. |
| `SLOTS` | table | `{9189, 4998}` | Hash slots of the keys (cluster mode). |
| `ARGV` | table | `{"mset", "key1", "value1", "key2", "value2"}` | All command arguments, including the command name at index `1`. |

### Functions

* `shake.call(db, argv_table)`: Emits a command to the writer. The first element of `argv_table` must be the command name. You can call `shake.call` multiple times to split one input into several outputs (for example, expand `MSET` into multiple `SET`).
* `shake.log(msg)`: Prints logs prefixed with `lua log:` in `shake.log`. Use this to verify script behaviour during testing.

## Best Practices

### General Recommendations

* **Keep scripts idempotent.** RedisShake may retry commands, so ensure the emitted commands do not rely on side effects.
* **Guard against missing keys.** Always check whether `KEYS[1]` exists before slicing to avoid runtime errors with keyless commands such as `PING`.
* **Prefer simple logic.** Complex loops increase Lua VM time and can slow down synchronization. Offload heavy transformations to upstream processes when possible.

### Filtering Keys

```lua
local prefix = "user:"
local prefix_len = #prefix

if not KEYS[1] or string.sub(KEYS[1], 1, prefix_len) ~= prefix then
  return
end

shake.call(DB, ARGV)
```

The effect is to only write source data with keys starting with `user:` to the destination. This does not consider cases of multi-key commands like `MSET`.

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

You can use the `GROUP` variable to determine the data structure type. Supported data structure types include `STRING`, `LIST`, `SET`, `ZSET`, `HASH`, `SCRIPTING`, and more.

#### Filtering Hash Type Data

```lua
if GROUP == "HASH" then
  return
end
shake.call(DB, ARGV)
```

The effect is to discard `hash` type data from the source and write other data to the destination.

#### Filtering [Lua Scripts](https://redis.io/docs/interact/programmability/eval-intro/)

```lua
if GROUP == "SCRIPTING" then
  return
end
shake.call(DB, ARGV)
```

The effect is to discard Lua scripts from the source and write other data to the destination. This is common when synchronizing from master-slave to cluster, where there are Lua scripts not supported by the cluster.

### Splitting Commands

```lua
if CMD == "MSET" then
  for i = 2, #ARGV, 2 do
    shake.call(DB, {"SET", ARGV[i], ARGV[i + 1]})
  end
  return
end

shake.call(DB, ARGV)
```

This pattern expands one `MSET` into several `SET` commands to improve compatibility with destinations that prefer single-key writes.

### Modifying Key Prefixes

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

## Troubleshooting

* **Script fails to compile:** RedisShake validates the Lua code during startup and panics on syntax errors. Check the configuration logs for the exact line number.
* **No data reaches the destination:** Ensure that `shake.call` is invoked for every branch. Adding `shake.log` statements helps confirm which code path runs.
* **Performance drops:** Heavy scripts may become CPU-bound. Consider narrowing the scope with filters or moving expensive operations out of RedisShake.
