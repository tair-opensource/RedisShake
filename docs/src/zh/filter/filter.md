---
outline: deep
---
# 内置过滤规则

RedisShake 在命令完成解析后、写入目标端之前应用过滤规则。过滤器决定哪些命令能够离开 RedisShake，只有通过该阶段的命令才会进入可选的 [function](./function.md) 钩子继续处理。

## 过滤所在位置

```
源端 reader  -->  过滤规则  -->  （可选 Lua function）  -->  writer / 目标端
```

* 命令在 reader 解析 RESP 之后进入过滤阶段，此时已经确认请求合法，如未配置过滤器就会被直接转发。
* 过滤早于其他加工阶段执行，被拦截的命令不会传递给可选的 Lua 脚本或 writer。
* 该阶段使用与 writer 相同的命令表示形式，因此对所有 reader 都保持一致的行为。

## 过滤流程说明

1. **优先执行阻止规则。** 只要命中任意 `block_*` 规则（键、DB、命令或命令组），整个命令会立即被丢弃。
2. **允许列表是可选的。** 未配置某类 `allow_*` 时，该类别默认全部放行。一旦配置允许列表，就只有明确列出的项才能通过。
3. **多 Key 命令需全部通过。** `MSET` 等多 Key 命令需要全部 Key 同时满足过滤条件，否则整条命令会被丢弃，并在日志中提示混合结果，便于排查配置。

通过组合允许与阻止规则，可以快速表达诸如“允许 user 前缀但排除临时缓存”等需求。阻止规则优先生效，请避免同一模式同时出现在允许与阻止列表中。

## 过滤 Key

RedisShake 支持通过键名、前缀、后缀以及正则表达式进行过滤，例如：

```toml
[filter]
allow_keys = ["user:1001", "product:2001"]          # 允许的键名
allow_key_prefix = ["user:", "product:"]             # 允许的键名前缀
allow_key_suffix = [":active", ":valid"]             # 允许的键名后缀
allow_key_regex = [":\\d{11}:"]                     # 允许的键名正则（11 位手机号）
block_keys = ["temp:1001", "cache:2001"]              # 阻止的键名
block_key_prefix = ["temp:", "cache:"]                # 阻止的键名前缀
block_key_suffix = [":tmp", ":old"]                  # 阻止的键名后缀
block_key_regex = [":test:\\d{11}:"]                # 阻止的键名正则，带 test 前缀
```

正则表达式使用 Go 语法，书写内联 TOML 时请注意反斜杠转义。借助正则可以灵活实现租户隔离或按编号过滤等场景。

## 过滤数据库

可限制同步的逻辑库，或跳过已知的噪声库：

```toml
[filter]
allow_db = [0, 1, 2]
block_db = [3, 4, 5]
```

如果未同时配置 `allow_db` 和 `block_db`，默认同步全部数据库。

## 过滤命令与命令组

可以按命令名称或 Redis 命令组进行限制，常用于目标端不支持某些脚本或管理命令的场景。

```toml
[filter]
allow_command = ["GET", "SET"]
block_command = ["DEL", "FLUSHDB"]

allow_command_group = ["STRING", "HASH"]
block_command_group = ["SCRIPTING", "PUBSUB"]
```

命令组遵循 [Redis command key specifications](https://redis.io/docs/reference/key-specs/)。通过命令组可以快速过滤整类数据结构，例如在向集群迁移时阻止 `SCRIPTING`，避免目标不支持的 Lua 脚本。

## 配置项速查

| 配置项 | 类型 | 说明 |
| --- | --- | --- |
| `allow_keys` / `block_keys` | `[]string` | 精确匹配的键名白名单 / 黑名单。 |
| `allow_key_prefix` / `block_key_prefix` | `[]string` | 按键名前缀过滤。 |
| `allow_key_suffix` / `block_key_suffix` | `[]string` | 按键名后缀过滤。 |
| `allow_key_regex` / `block_key_regex` | `[]string` | 使用正则表达式匹配完整键名。 |
| `allow_db` / `block_db` | `[]int` | 包含或排除的逻辑库编号。 |
| `allow_command` / `block_command` | `[]string` | 指定允许或阻止的命令名称。 |
| `allow_command_group` / `block_command_group` | `[]string` | 指定允许或阻止的命令组，如 `STRING`、`HASH`、`SCRIPTING`。 |

上述配置均为可选项；当允许与阻止规则同时命中时，以阻止为准。建议在主备实例之间保持一致的过滤配置，以避免切换后出现数据差异。
