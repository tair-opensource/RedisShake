# Redis Writer

## 介绍

`redis_writer` 用于将数据写入 Redis-like 数据库。

## 配置

```toml
[redis_writer]
cluster = false
address = "127.0.0.1:6379" # when cluster is true, address is one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
```

* `cluster`：是否为集群。
* `address`：连接地址。当目的端为集群时，`address` 填写集群中的任意一个节点即可
* 鉴权：
    * 当使用 ACL 账号体系时，配置 `username` 和 `password`
    * 当使用传统账号体系时，仅配置 `password`
    * 当无鉴权时，不配置 `username` 和 `password`
* `tls`：是否开启 TLS/SSL，不需要配置证书因为 RedisShake 没有校验服务器证书

注意事项：
1. 当目的端为集群时，应保证源端发过来的命令满足 [Key 的哈希值属于同一个 slot](https://redis.io/docs/reference/cluster-spec/#implemented-subset)。
2. 应尽量保证目的端版本大于等于源端版本，否则可能会出现不支持的命令。如确实需要降低版本，可以设置 `target_redis_proto_max_bulk_len` 为 0，来避免使用 `restore` 命令恢复数据。

## 重复 Key 处理

当目标 Redis 中已存在相同的 Key 时，可以通过 `rdb_restore_command_behavior` 配置项控制行为。

### 配置

```toml
[advanced]
rdb_restore_command_behavior = "panic"  # panic, rewrite 或 skip
```

### 选项说明

| 值 | 说明 |
|----|------|
| `panic` | 遇到重复 Key 时停止运行（默认） |
| `rewrite` | 覆盖目标端的 Key |
| `skip` | 跳过重复 Key |

### 适用范围

| Reader | 阶段 | 使用 RESTORE | 配置生效 |
|--------|------|-------------|---------|
| `rdb_reader` | RDB | ✅ 是 | ✅ 是 |
| `sync_reader` | RDB 阶段 | ✅ 是 | ✅ 是 |
| `sync_reader` | AOF 阶段 | ❌ 否（直接转发命令） | ❌ 否 |
| `scan_reader` | 全量/KSN | ✅ 是 | ✅ 是 |

### 限制

当数据大小超过 `target_redis_proto_max_bulk_len` 阈值时，会回退使用普通命令（如 SET、HSET 等），此时 `rdb_restore_command_behavior` 配置可能不生效。
