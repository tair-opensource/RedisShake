---
outline: deep
---

# 版本兼容性

RedisShake 支持多种 Redis 和 Valkey 版本，本文档详细说明各版本的特性支持情况，以及 Redis 与 Valkey 互相迁移的支持边界。

## 版本支持概览

| 数据库 | 支持版本 |
| --- | --- |
| Redis | 2.8 - 8.12.x |
| Valkey | 8.x - 9.2.x |

> **注意：** 命令规范基于 Redis 8.12 和 Valkey 9.2，共计支持 526 个命令。

## Redis 版本支持详情

### Redis 2.8 - 3.x

- 基础数据类型：String、List、Set、Sorted Set、Hash
- 基础命令支持

### Redis 4.x

- 所有 2.8 - 3.x 特性
- Module 支持（TairString、TairHash、TairZset，以及未知 `MODULE_2` 通过 `RESTORE` 透传）
- Stream 数据类型（4.0 不支持，5.0 引入）

### Redis 5.x - 6.x

- 所有 4.x 特性
- Stream 数据类型
- Module 支持

### Redis 7.x

- 所有 6.x 特性
- Function 支持

### Redis 8.0 - 8.4.x

**新增支持：**
- Hash 字段过期命令：`HSETEX`、`HGETEX`、`HGETDEL`、`HTTL`、`HPTTL`、`HPERSIST`、`HEXPIRE`、`HEXPIREAT`、`HPEXPIRE`、`HPEXPIREAT`、`HEXPIRETIME`、`HPEXPIRETIME`
- Hash 字段过期 RDB 格式（RDB type 22 - 25）
- Stream 命令：`XACKDEL`、`XDELEX`（8.2+）
- 8.4.x 命令：`CLIENT NO-TOUCH`、`CLIENT SETINFO`、`CLUSTER MIGRATION`、`CLUSTER MYSHARDID`、`CLUSTER SLOT-STATS`、`CLUSTER SYNCSLOTS`、`DELEX`、`DIGEST`、`MSETEX`、`SFLUSH`、`TRIMSLOTS`、`WAITAOF`

**不支持：**
- Vector Sets（向量数据类型）
- Redis Stack 模块的命令重写（未知 `MODULE_2` 在不超过 `target_redis_proto_max_bulk_len` 时可通过 `RESTORE` 同引擎透传）

### Redis 8.6 - 8.12.x

**新增支持（RDB v17）：**
- Stream 新格式：`RDB_TYPE_STREAM_LISTPACKS_4`（26，IDMP）与 `RDB_TYPE_STREAM_LISTPACKS_5`（27，XNACK）；正常大小走 `RESTORE` 透传完整保留 IDMP/NACK 状态，命令重写模式还原条目、消费组与 PEL（不还原 IDMP/NACK）
- Set 成员过期：`RDB_TYPE_SET_LISTPACK_EX`（34）与 `RDB_TYPE_SET_METADATA`（35），以及 `SADDEX`、`SEXPIRE`、`SPEXPIRE`、`SPEXPIREAT`、`SPERSIST`、`STTL`、`SPTTL` 等命令
- Hash 模板：`RDB_OPCODE_HASH_TEMPLATE`（242）与 `RDB_TYPE_HASH_TMPL_LP`/`ARRAY` 及 `_REF`（29 - 32）；RDB 专用的 `_REF` 类型自动改写为 `HSET` 写入
- Sparse Array：`RDB_TYPE_ARRAY`（28）与 `AR*` 命令
- Roaring Bitmap 与 GCRA：`RDB_TYPE_BITMAP`（33）与 `RDB_TYPE_GCRA`（36）支持 `RESTORE` 透传（不支持拆分为普通命令）
- 新 Opcode：`RDB_OPCODE_KEY_NOEVICT`（241）、`RDB_OPCODE_HASH_TEMPLATE`（242）、`RDB_OPCODE_KEY_META`（243）
- 其余新增命令：`INCREX`、`LMOVEM`、`BLMOVEM`、`ZMOVE`、`BZMOVE`、`ZMOVEMEMBER`、`SDIFFCARD`、`SUNIONCARD`、`XCFGSET`、`XIDMPRECORD`、`XNACK`、`BITCONVERT`、`BITROAROP`、`BITRANGEINTARRAY`、`BLESS`、`HIMPORT`、`HOTKEYS`、`BACKUP` 等

## Valkey 版本支持详情

### Valkey 8.x

- 基础数据类型和命令
- 与 Redis 7.x 功能基本一致

### Valkey 9.0 - 9.2.x

**新增支持（RDB v80 / v81）：**
- Hash 字段过期：`RDB_TYPE_HASH_2`（22）与全套 Hash 字段过期命令
- Path Hash（9.2）：`RDB_TYPE_PATH_HASH`（23，按 `VALKEY` 文件头与 Redis 的 type 23 消歧）与 `PHSET`/`PHGET`/`PHGETALL`/`PHDEL`/`PHMGET`/`PHMSET` 等全套 `PH*` 命令
- 新 Opcode：`RDB_OPCODE_UPDATE`（242，forkless sync 内联复制命令）与 `RDB_OPCODE_SLOT_IMPORT`（243，槽导入元数据）
- 其余新增命令：`ACL SETROLE`/`GETROLE`/`DELROLE`/`ROLES`/`DIGEST`、`CLUSTERSCAN`、`CONFIG INFO`、`HOTKEYS`、`INCREX`

**Valkey 独有命令：**
- 连接类：`CLIENT CAPA`、`CLIENT IMPORT-SOURCE`
- 集群类：`CLUSTER CANCELSLOTMIGRATIONS`、`CLUSTER FLUSHSLOT`、`CLUSTER GETSLOTMIGRATIONS`、`CLUSTER MIGRATESLOTS`、`CLUSTERSCAN`
- 服务端类：`COMMANDLOG`、`CONFIG INFO`
- 字符串类：`DELIFEQ`
- 脚本类：`SCRIPT SHOW`
- 哨兵类：`SENTINEL GET-PRIMARY-ADDR-BY-NAME`、`SENTINEL IS-PRIMARY-DOWN-BY-ADDR`、`SENTINEL PRIMARIES`、`SENTINEL PRIMARY`
- 数据类型：`PH*`（Path Hash）

> **注意：** Valkey 在哨兵命令中使用 `PRIMARY` 术语替代 `MASTER`。

## 特性支持矩阵

| 特性 | Redis 2.8 - 7.x | Redis 8.0 - 8.4 | Redis 8.6 - 8.12 | Valkey 8.x | Valkey 9.0 | Valkey 9.2 |
| --- | --- | --- | --- | --- | --- | --- |
| 基础数据类型 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Stream | ✓ (5.0+) | ✓ | ✓ (含 IDMP/XNACK) | ✓ | ✓ | ✓ |
| Module | ✓ (4.0+) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Function | ✓ (7.0+) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Hash 字段过期 | ✗ | ✓ | ✓ | ✗ | ✓ | ✓ |
| Set 成员过期 | ✗ | ✗ | ✓ (8.12) | ✗ | ✗ | ✗ |
| Hash 模板 | ✗ | ✗ | ✓ (8.12) | ✗ | ✗ | ✗ |
| Sparse Array | ✗ | ✗ | ✓ (8.8+) | ✗ | ✗ | ✗ |
| Roaring Bitmap / GCRA | ✗ | ✗ | ✓ (RESTORE) | ✗ | ✗ | ✗ |
| Path Hash | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ |
| Vector Sets | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |

## Redis 与 Valkey 互相迁移支持情况

Redis 和 Valkey 从 8.x / 9.x 开始在 RDB 编码上出现分歧（例如 type 22 在 Redis 8.x 是 `HASH_METADATA_PRE_GA`，在 Valkey 9.x 是 `HASH_2`；type 23 在 Redis 8.x 是 `HASH_LISTPACK_EX_PRE_GA`，在 Valkey 9.2 是 `PATH_HASH`；Redis 8.12 的 Stream 统一使用 type 27，Valkey 9.2 仍使用 type 21；此外 `scan_reader` 的 `DUMP` 尾部版本号在两引擎间也不兼容）。

因此，**直接用 `RESTORE` 跨引擎恢复会报 `ERR Bad data format`**。在 Redis 与 Valkey 之间互相迁移时，**必须在配置中设置 `target_redis_proto_max_bulk_len = 0`**，强制所有 key 改用普通命令（`SET`、`HSET`、`HPEXPIREAT`、`XADD` 等）重写恢复。

### 跨引擎配置注意事项

- `target_redis_proto_max_bulk_len = 0` 会先发送 `DEL` 再写入命令，`rdb_restore_command_behavior`（`panic` / `skip` / `rewrite`）对这些 key 不生效，目标端同名 key 会被直接覆盖。
- 拆分命令后发送的命令数增多，全量同步速度会低于 `RESTORE` 模式。
- 仅支持 `RESTORE` 透传的对象（未知 `MODULE_2`、Redis 8.12 Roaring Bitmap、GCRA）无法通过命令重写跨引擎迁移。
- 增量同步阶段（`sync_reader` AOF 流）会原样转发写命令；若源端产生了目标引擎不支持的独有命令，需在 `filter` 中过滤。

### 双向迁移特性支持矩阵

以下矩阵假定已配置 `target_redis_proto_max_bulk_len = 0`：

| 数据类型 / 特性 | Redis → Valkey | Valkey → Redis | 说明 |
| --- | --- | --- | --- |
| 基础数据类型（String/List/Set/ZSet/Hash） | ✓ | ✓ | 全量命令重写 + 增量转发 |
| Stream（消息、消费组、PEL） | ✓ | ✓ | Redis 8.12 的 IDMP/NACK 状态在命令重写后不保留；增量需过滤 `XACKDEL`/`XDELEX`/`XNACK` |
| Hash 字段过期（HFE） | ✓ (目标 ≥ Valkey 9.0) | ✓ (目标 ≥ Redis 8.0) | 全量重写为 `HSET` + `HPEXPIREAT`，增量命令兼容 |
| Lua 脚本 / Function | ✓ (视命令兼容) | ✓ (视命令兼容) | 目标引擎需支持脚本中调用的命令 |
| Redis 独有类型（Set 成员过期、Sparse Array、Roaring Bitmap、GCRA） | ✗ | — | Valkey 不支持对应类型与命令，需过滤相关 key/命令 |
| Valkey 独有类型（Path Hash） | — | ✗ | Redis 不支持 `PH*` 类型与命令，需过滤相关 key/命令 |
| 未知 Module（`MODULE_2`） | ✗ | ✗ | 依赖 `RESTORE` 透传，跨引擎且二进制不兼容时不适用 |
| 引擎独有增量命令 | 需过滤 | 需过滤 | 如 Redis 的 `XACKDEL`/`XDELEX`/`SADDEX`/`AR*`，Valkey 的 `DELIFEQ`/`PH*`/`ACL SETROLE` |

## 跨版本迁移

关于同引擎跨版本降级迁移的注意事项，请参考[跨版本迁移](./version.md)文档。
