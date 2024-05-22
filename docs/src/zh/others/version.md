# 跨版本迁移

常见 Redis-like 数据库都是向后兼容的，低版本实例迁移至高版本时，不会存在兼容问题。
但是，高版本实例迁移至低版本时，可能会存在不兼容问题，例如：Redis 7.0 的数据导入至 Redis 4.0。
建议尽量避免此类场景，因为会存在多种不兼容场景 [[794]](https://github.com/tair-opensource/RedisShake/issues/794) [[699]](https://github.com/tair-opensource/RedisShake/issues/699)：
1. 二进制数据编码不兼容
2. 命令不支持，见于 SYNC 的增量阶段


如果无法避免迁移至低版本，可以通过以下方式解决：
1. 对于二进制数据编码不兼容，可以考虑修改 `target_redis_proto_max_bulk_len` 参数，将其设置为 0。
`target_redis_proto_max_bulk_len` 本意是为了处理超大 Key，RedisShake 会在同步过程中将大于 `target_redis_proto_max_bulk_len` 的二进制数据转换为 RESP 命令。比如元素非常多的 list 结构，RedisShake 会将其转换为多个 `RPUSH` 命令。在降低版本场景中，可以将其设置为 0，让 RedisShake 将所有二进制数据转换为 RESP 命令，以此避免二进制数据编码不兼容问题。
2. 对于命令不支持问题，建议直接过滤掉不支持的命令。