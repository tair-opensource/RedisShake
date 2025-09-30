---
outline: deep
---
# Built-in Filter Rules

RedisShake evaluates filter rules after commands are parsed but before anything is sent to the destination. The filter therefore controls which commands ever leave RedisShake, and only the commands that pass this stage are eligible for further processing by the optional [function](./function.md) hook.

## Where filtering happens

```
source reader  -->  filter rules  -->  (optional Lua function)  -->  writer / target
```

* Commands enter the filter after RedisShake has parsed the RESP payload from the reader. At this point the request is already considered valid and would be forwarded if no filters were configured.
* Filtering happens before any other transformation stage, so blocked commands never reach the optional Lua function or the writer.
* The stage operates on the same command representation that writers use, which keeps behaviour consistent for all readers.

## How Filter Evaluation Works

1. **Block rules run first.** If a key, database, command, or command group matches a `block_*` rule, the entire entry is dropped immediately.
2. **Allow lists are optional.** When no `allow_*` rule is configured for a category, everything is permitted by default. As soon as you define an allow list, only the explicitly listed items will pass.
3. **Multi-key consistency.** Commands with multiple keys (for example, `MSET`) must either pass for all keys or the entry is discarded. RedisShake also emits logs when a mixed result is detected to help you troubleshoot your patterns.

Combining allow and block lists lets you quickly express exceptions such as “allow user keys except temporary cache variants.” Block rules take precedence, so avoid listing the same pattern in both allow and block lists.

## Key Filtering

RedisShake supports filtering by key names, prefixes, suffixes, and regular expressions. For example:

```toml
[filter]
allow_keys = ["user:1001", "product:2001"]          # allow-listed key names
allow_key_prefix = ["user:", "product:"]             # allow-listed key prefixes
allow_key_suffix = [":active", ":valid"]             # allow-listed key suffixes
allow_key_regex = [":\\d{11}:"]                     # allow-listed key regex (11-digit phone numbers)
block_keys = ["temp:1001", "cache:2001"]              # block-listed key names
block_key_prefix = ["temp:", "cache:"]                # block-listed key prefixes
block_key_suffix = [":tmp", ":old"]                  # block-listed key suffixes
block_key_regex = [":test:\\d{11}:"]                # block-listed key regex with "test" prefix
```

Regular expressions follow Go’s syntax. Escape backslashes carefully when writing inline TOML strings. Regex support allows complex tenant-isolation scenarios, such as filtering phone numbers or shard identifiers.

## Database Filtering

Limit synchronization to specific logical databases or skip known noisy ones:

```toml
[filter]
allow_db = [0, 1, 2]
block_db = [3, 4, 5]
```

If neither `allow_db` nor `block_db` is set, all databases are synchronized.

## Command and Command-Group Filtering

Restrict the traffic by command name or by the Redis command group. This is useful when the destination lacks support for scripting or cluster administration commands.

```toml
[filter]
allow_command = ["GET", "SET"]
block_command = ["DEL", "FLUSHDB"]

allow_command_group = ["STRING", "HASH"]
block_command_group = ["SCRIPTING", "PUBSUB"]
```

Command groups follow the [Redis command key specifications](https://redis.io/docs/reference/key-specs/). Use groups to efficiently exclude entire data structures (for example, block `SCRIPTING` to avoid unsupported Lua scripts when synchronizing to a cluster).

## Configuration Reference

| Option | Type | Description |
| --- | --- | --- |
| `allow_keys` / `block_keys` | `[]string` | Exact key names to allow or block. |
| `allow_key_prefix` / `block_key_prefix` | `[]string` | Filter keys by prefix. |
| `allow_key_suffix` / `block_key_suffix` | `[]string` | Filter keys by suffix. |
| `allow_key_regex` / `block_key_regex` | `[]string` | Regular expressions evaluated against the full key. |
| `allow_db` / `block_db` | `[]int` | Logical database numbers to include or exclude. |
| `allow_command` / `block_command` | `[]string` | Redis command names. |
| `allow_command_group` / `block_command_group` | `[]string` | Redis command groups such as `STRING`, `HASH`, `SCRIPTING`. |

All options are optional. When both an allow and block rule apply to the same category, block rules win. Keep configurations symmetrical across active/standby clusters to avoid asymmetric data drops during failover.
