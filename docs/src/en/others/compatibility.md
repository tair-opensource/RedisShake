---
outline: deep
---

# Version Compatibility

RedisShake supports multiple Redis and Valkey versions. This document details feature support for each version as well as cross-engine migration support between Redis and Valkey.

## Version Support Overview

| Database | Supported Versions |
| --- | --- |
| Redis | 2.8 - 8.12.x |
| Valkey | 8.x - 9.2.x |

> **Note:** Command specifications are based on Redis 8.12 and Valkey 9.2, with a total of 526 commands supported.

## Redis Version Support Details

### Redis 2.8 - 3.x

- Basic data types: String, List, Set, Sorted Set, Hash
- Basic command support

### Redis 4.x

- All 2.8 - 3.x features
- Module support (TairString, TairHash, TairZset, plus unknown `MODULE_2` passthrough via `RESTORE`)
- Stream data type (not in 4.0, introduced in 5.0)

### Redis 5.x - 6.x

- All 4.x features
- Stream data type
- Module support

### Redis 7.x

- All 6.x features
- Function support

### Redis 8.0 - 8.4.x

**Newly Supported:**
- Hash field expiration commands: `HSETEX`, `HGETEX`, `HGETDEL`, `HTTL`, `HPTTL`, `HPERSIST`, `HEXPIRE`, `HEXPIREAT`, `HPEXPIRE`, `HPEXPIREAT`, `HEXPIRETIME`, `HPEXPIRETIME`
- Hash field expiration RDB format (RDB types 22 - 25)
- Stream commands: `XACKDEL`, `XDELEX` (8.2+)
- 8.4.x commands: `CLIENT NO-TOUCH`, `CLIENT SETINFO`, `CLUSTER MIGRATION`, `CLUSTER MYSHARDID`, `CLUSTER SLOT-STATS`, `CLUSTER SYNCSLOTS`, `DELEX`, `DIGEST`, `MSETEX`, `SFLUSH`, `TRIMSLOTS`, `WAITAOF`

**Not Supported:**
- Vector Sets
- Command-level rewrite for Redis Stack modules (unknown `MODULE_2` values within `target_redis_proto_max_bulk_len` can be migrated on the same engine via `RESTORE` passthrough)

### Redis 8.6 - 8.12.x

**Newly Supported (RDB v17):**
- Stream formats: `RDB_TYPE_STREAM_LISTPACKS_4` (26, IDMP) and `RDB_TYPE_STREAM_LISTPACKS_5` (27, XNACK); normal-size keys use `RESTORE` passthrough to preserve full IDMP/NACK state, while command rewrite reconstructs entries, consumer groups, and PEL (without IDMP/NACK)
- Set member expiration: `RDB_TYPE_SET_LISTPACK_EX` (34) and `RDB_TYPE_SET_METADATA` (35), plus `SADDEX`, `SEXPIRE`, `SPEXPIRE`, `SPEXPIREAT`, `SPERSIST`, `STTL`, `SPTTL`, etc.
- Hash templates: `RDB_OPCODE_HASH_TEMPLATE` (242) and `RDB_TYPE_HASH_TMPL_LP`/`ARRAY` and `_REF` (29 - 32); RDB-only `_REF` types are automatically rewritten as `HSET` commands
- Sparse Array: `RDB_TYPE_ARRAY` (28) and `AR*` commands
- Roaring Bitmap & GCRA: `RDB_TYPE_BITMAP` (33) and `RDB_TYPE_GCRA` (36) via `RESTORE` passthrough (command rewrite not supported)
- New opcodes: `RDB_OPCODE_KEY_NOEVICT` (241), `RDB_OPCODE_HASH_TEMPLATE` (242), `RDB_OPCODE_KEY_META` (243)
- Additional commands: `INCREX`, `LMOVEM`, `BLMOVEM`, `ZMOVE`, `BZMOVE`, `ZMOVEMEMBER`, `SDIFFCARD`, `SUNIONCARD`, `XCFGSET`, `XIDMPRECORD`, `XNACK`, `BITCONVERT`, `BITROAROP`, `BITRANGEINTARRAY`, `BLESS`, `HIMPORT`, `HOTKEYS`, `BACKUP`, etc.

## Valkey Version Support Details

### Valkey 8.x

- Basic data types and commands
- Functionally equivalent to Redis 7.x

### Valkey 9.0 - 9.2.x

**Newly Supported (RDB v80 / v81):**
- Hash field expiration: `RDB_TYPE_HASH_2` (22) and full hash field expiration commands
- Path Hash (9.2): `RDB_TYPE_PATH_HASH` (23, disambiguated from Redis type 23 via the `VALKEY` RDB header) and full `PH*` commands (`PHSET`, `PHGET`, `PHGETALL`, `PHDEL`, `PHMGET`, `PHMSET`, etc.)
- New opcodes: `RDB_OPCODE_UPDATE` (242, inline replication commands during forkless sync) and `RDB_OPCODE_SLOT_IMPORT` (243, slot import metadata)
- Additional commands: `ACL SETROLE`/`GETROLE`/`DELROLE`/`ROLES`/`DIGEST`, `CLUSTERSCAN`, `CONFIG INFO`, `HOTKEYS`, `INCREX`

**Valkey-specific Commands:**
- Connection: `CLIENT CAPA`, `CLIENT IMPORT-SOURCE`
- Cluster: `CLUSTER CANCELSLOTMIGRATIONS`, `CLUSTER FLUSHSLOT`, `CLUSTER GETSLOTMIGRATIONS`, `CLUSTER MIGRATESLOTS`, `CLUSTERSCAN`
- Server: `COMMANDLOG`, `CONFIG INFO`
- String: `DELIFEQ`
- Scripting: `SCRIPT SHOW`
- Sentinel: `SENTINEL GET-PRIMARY-ADDR-BY-NAME`, `SENTINEL IS-PRIMARY-DOWN-BY-ADDR`, `SENTINEL PRIMARIES`, `SENTINEL PRIMARY`
- Data types: `PH*` (Path Hash)

> **Note:** Valkey uses `PRIMARY` terminology instead of `MASTER` for Sentinel commands.

## Feature Support Matrix

| Feature | Redis 2.8 - 7.x | Redis 8.0 - 8.4 | Redis 8.6 - 8.12 | Valkey 8.x | Valkey 9.0 | Valkey 9.2 |
| --- | --- | --- | --- | --- | --- | --- |
| Basic Data Types | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Stream | ✓ (5.0+) | ✓ | ✓ (incl. IDMP/XNACK) | ✓ | ✓ | ✓ |
| Module | ✓ (4.0+) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Function | ✓ (7.0+) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Hash Field Expiration | ✗ | ✓ | ✓ | ✗ | ✓ | ✓ |
| Set Member Expiration | ✗ | ✗ | ✓ (8.12) | ✗ | ✗ | ✗ |
| Hash Templates | ✗ | ✗ | ✓ (8.12) | ✗ | ✗ | ✗ |
| Sparse Array | ✗ | ✗ | ✓ (8.8+) | ✗ | ✗ | ✗ |
| Roaring Bitmap / GCRA | ✗ | ✗ | ✓ (RESTORE) | ✗ | ✗ | ✗ |
| Path Hash | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ |
| Vector Sets | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |

## Redis ↔ Valkey Cross-Engine Migration

Starting from Redis 8.x and Valkey 9.x, the two engines have diverged in RDB binary encodings (for example, type 22 is `HASH_METADATA_PRE_GA` in Redis vs `HASH_2` in Valkey; type 23 is `HASH_LISTPACK_EX_PRE_GA` in Redis vs `PATH_HASH` in Valkey 9.2; Redis 8.12 saves streams as type 27 while Valkey 9.2 uses type 21; `scan_reader` `DUMP` footer versions also differ across engines).

Because of these encoding differences, **direct `RESTORE` across engines fails with `ERR Bad data format`**. When migrating between Redis and Valkey in either direction, **you must set `target_redis_proto_max_bulk_len = 0`** so that all keys are rewritten as standard commands (`SET`, `HSET`, `HPEXPIREAT`, `XADD`, etc.).

### Cross-Engine Configuration Caveats

- With `target_redis_proto_max_bulk_len = 0`, RedisShake emits `DEL` followed by write commands; `rdb_restore_command_behavior` (`panic` / `skip` / `rewrite`) does not apply and existing target keys are overwritten.
- Command rewriting sends more commands and is slower than `RESTORE` during full sync.
- Types that only support `RESTORE` passthrough (unknown `MODULE_2`, Redis 8.12 Roaring Bitmap, GCRA) cannot be migrated across engines via command rewriting.
- Incremental replication (`sync_reader` AOF stream) forwards commands as-is; filter out engine-specific commands unsupported by the target using `filter`.

### Bidirectional Migration Support Matrix

The matrix below assumes `target_redis_proto_max_bulk_len = 0`:

| Data Type / Feature | Redis → Valkey | Valkey → Redis | Notes |
| --- | --- | --- | --- |
| Basic Data Types (String/List/Set/ZSet/Hash) | ✓ | ✓ | Full sync command rewrite + incremental forwarding |
| Stream (entries, consumer groups, PEL) | ✓ | ✓ | Redis 8.12 IDMP/NACK state is not preserved under command rewrite; filter `XACKDEL`/`XDELEX`/`XNACK` in incremental sync |
| Hash Field Expiration (HFE) | ✓ (target ≥ Valkey 9.0) | ✓ (target ≥ Redis 8.0) | Rewritten as `HSET` + `HPEXPIREAT`; incremental commands are compatible |
| Lua Scripts / Functions | ✓ (command-dependent) | ✓ (command-dependent) | Target engine must support all commands invoked inside scripts |
| Redis-only Types (Set member expiry, Sparse Array, Roaring Bitmap, GCRA) | ✗ | — | Unsupported on Valkey; filter out those keys/commands |
| Valkey-only Types (Path Hash) | — | ✗ | Unsupported on Redis; filter out `PH*` keys/commands |
| Unknown Modules (`MODULE_2`) | ✗ | ✗ | Requires `RESTORE` passthrough; incompatible across engines |
| Engine-specific Incremental Commands | Filter required | Filter required | e.g., Redis `XACKDEL`/`XDELEX`/`SADDEX`/`AR*`; Valkey `DELIFEQ`/`PH*`/`ACL SETROLE` |

## Cross-Version Migration

For notes on same-engine version downgrade migration, please refer to the [Cross-version Migration](./version.md) document.
