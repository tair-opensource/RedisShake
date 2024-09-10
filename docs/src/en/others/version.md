# Cross-version Migration

Common Redis-like databases are generally backward compatible, meaning there are no compatibility issues when migrating from a lower version instance to a higher version.

However, when migrating from a higher version instance to a lower version, compatibility issues may arise. For example, importing data from Redis 7.0 into Redis 4.0.

It's recommended to avoid such scenarios as much as possible due to various incompatibility issues [[794]](https://github.com/tair-opensource/RedisShake/issues/794) [[699]](https://github.com/tair-opensource/RedisShake/issues/699):
1. Binary data encoding incompatibility
2. Unsupported commands, seen in the incremental phase of SYNC

If migrating to a lower version is unavoidable, you can resolve it through the following methods:
1. For binary data encoding incompatibility, consider modifying the `target_redis_proto_max_bulk_len` parameter by setting it to 0.
The original purpose of `target_redis_proto_max_bulk_len` is to handle extremely large keys. RedisShake will convert binary data larger than `target_redis_proto_max_bulk_len` into RESP commands during synchronization. For example, a list structure with many elements will be converted into multiple `RPUSH` commands. In version downgrade scenarios, you can set it to 0, prompting RedisShake to convert all binary data into RESP commands, thus avoiding binary data encoding incompatibility issues.
2. For unsupported command issues, it's recommended to directly filter out the unsupported commands.
