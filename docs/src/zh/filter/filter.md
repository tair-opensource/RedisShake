---
outline: deep
---
# 内置过滤规则
RedisShake 提供了多种内置的过滤规则，用户可以根据需要选择合适的规则。

## 过滤 Key
RedisShake 支持通过键名、键名前缀和后缀进行过滤。您可以在配置文件中设置以下选项，例如：
```toml
allow_keys = ["user:1001", "product:2001"] # 允许的键名
allow_key_prefix = ["user:", "product:"] # 允许的键名前缀
allow_key_suffix = [":active", ":valid"] # 允许的键名后缀
block_keys = ["temp:1001", "cache:2001"] # 阻止的键名
block_key_prefix = ["temp:", "cache:"] # 阻止的键名前缀
block_key_suffix = [":tmp", ":old"] # 阻止的键名后缀
```
如果不设置这些选项，默认允许所有键。

## 过滤数据库
您可以指定允许或阻止的数据库编号，例如：
```toml
allow_db = [0, 1, 2]
block_db = [3, 4, 5]
```
如果不设置这些选项，默认允许所有数据库。

## 过滤命令
RedisShake 允许您过滤特定的 Redis 命令，例如：
```toml
allow_command = ["GET", "SET"]
block_command = ["DEL", "FLUSHDB"]
``` 

## 过滤命令组

您还可以按命令组进行过滤，可用的命令组包括：
SERVER, STRING, CLUSTER, CONNECTION, BITMAP, LIST, SORTED_SET, GENERIC, TRANSACTIONS, SCRIPTING, TAIRHASH, TAIRSTRING, TAIRZSET, GEO, HASH, HYPERLOGLOG, PUBSUB, SET, SENTINEL, STREAM
例如：
```toml
allow_command_group = ["STRING", "HASH"]
block_command_group = ["SCRIPTING", "PUBSUB"]
```
