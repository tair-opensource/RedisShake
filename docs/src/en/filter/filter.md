---
outline: deep
---
# Built-in Filter Rules
RedisShake provides various built-in filter rules that users can choose from according to their needs.

## Filtering Keys
RedisShake supports filtering by key name, key name prefixes, and suffixes. You can set the following options in the configuration file, for example:
```toml
allow_keys = ["user:1001", "product:2001"] # allowed key names  
allow_key_prefix = ["user:", "product:"] # allowed key name prefixes
allow_key_suffix = [":active", ":valid"] # allowed key name suffixes
block_keys = ["temp:1001", "cache:2001"] # blocked key names
block_key_prefix = ["temp:", "cache:"] # blocked key name prefixes
block_key_suffix = [":tmp", ":old"] # blocked key name suffixes
```
If these options are not set, all keys are allowed by default.

## Filtering Databases
You can specify allowed or blocked database numbers, for example:
```toml
allow_db = [0, 1, 2]
block_db = [3, 4, 5]
```
If these options are not set, all databases are allowed by default.

## Filtering Commands
RedisShake allows you to filter specific Redis commands, for example:
```toml
allow_command = ["GET", "SET"]
block_command = ["DEL", "FLUSHDB"]
``` 

## Filtering Command Groups

You can also filter by command groups. Available command groups include:
SERVER, STRING, CLUSTER, CONNECTION, BITMAP, LIST, SORTED_SET, GENERIC, TRANSACTIONS, SCRIPTING, TAIRHASH, TAIRSTRING, TAIRZSET, GEO, HASH, HYPERLOGLOG, PUBSUB, SET, SENTINEL, STREAM
For example:
```toml
allow_command_group = ["STRING", "HASH"]
block_command_group = ["SCRIPTING", "PUBSUB"]
```
