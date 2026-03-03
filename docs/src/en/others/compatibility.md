---
outline: deep
---

# Version Compatibility

RedisShake supports multiple Redis and Valkey versions. This document details the feature support for each version.

## Version Support Overview

| Database | Supported Versions |
|----------|-------------------|
| Redis | 2.8 - 8.4.x |
| Valkey | 8.x - 9.x |

> **Note:** Command specifications are based on Redis 8.4 and Valkey 9.x (unstable), with a total of 434 commands supported.

## Redis Version Support Details

### Redis 2.8 - 3.x

- Basic data types: String, List, Set, Sorted Set, Hash
- Basic command support

### Redis 4.x

- All 2.8-3.x features
- Module support (TairString, TairHash, TairZset)
- Stream data type (not in 4.0, introduced in 5.0)

### Redis 5.x - 6.x

- All 4.x features
- Stream data type
- Module support

### Redis 7.x

- All 6.x features
- Function support

### Redis 8.x

**Newly Supported:**
- Hash field expiration commands: HSETEX, HGETEX, HGETDEL, HTTL, HPTTL, HPERSIST, HEXPIRE, HEXPIREAT, HPEXPIRE, HPEXPIREAT, HEXPIRETIME, HPEXPIRETIME
- Hash field expiration RDB format (RDB type 22-25)
- XACKDEL/XDELEX commands (8.2+)

**Not Supported:**
- Vector Sets
- Redis Stack modules (RedisJSON, RediSearch, RedisTimeSeries, RedisBloom)

### Redis 8.4.x

**Newly Supported Commands:**
- Connection: CLIENT NO-TOUCH, CLIENT SETINFO
- Cluster: CLUSTER MIGRATION, CLUSTER MYSHARDID, CLUSTER SLOT-STATS, CLUSTER SYNCSLOTS
- String: DELEX, DIGEST, MSETEX
- Server: SFLUSH, TRIMSLOTS
- Generic: WAITAOF

## Valkey Version Support Details

### Valkey 8.x

- Basic data types and commands
- Functionally equivalent to Redis 7.x

### Valkey 9.x

**Newly Supported:**
- Hash field expiration commands (same as Redis 8.x)
- Hash field expiration RDB format

**Valkey-specific Commands:**
- Connection: CLIENT CAPA, CLIENT IMPORT-SOURCE
- Cluster: CLUSTER CANCELSLOTMIGRATIONS, CLUSTER FLUSHSLOT, CLUSTER GETSLOTMIGRATIONS, CLUSTER MIGRATESLOTS
- Server: COMMANDLOG (with subcommands: GET, HELP, LEN, RESET)
- String: DELIFEQ
- Scripting: SCRIPT SHOW
- Sentinel: SENTINEL GET-PRIMARY-ADDR-BY-NAME, SENTINEL IS-PRIMARY-DOWN-BY-ADDR, SENTINEL PRIMARIES, SENTINEL PRIMARY

> **Note:** Valkey uses "PRIMARY" terminology instead of "MASTER" for Sentinel commands.

## Feature Support Matrix

| Feature | Redis 2.8-7.x | Redis 8.x | Redis 8.4.x | Valkey 8.x | Valkey 9.x |
|---------|---------------|-----------|-------------|------------|------------|
| Basic Data Types | ✓ | ✓ | ✓ | ✓ | ✓ |
| Stream | ✓ (5.0+) | ✓ | ✓ | ✓ | ✓ |
| Module | ✓ (4.0+) | ✓ | ✓ | ✓ | ✓ |
| Function | ✓ (7.0+) | ✓ | ✓ | ✓ | ✓ |
| Hash Field Expiration | ✗ | ✓ | ✓ | ✗ | ✓ |
| XACKDEL/XDELEX | ✗ | ✓ (8.2+) | ✓ | ✗ | ✗ |
| Vector Sets | ✗ | ✗ | ✗ | ✗ | ✗ |
| WAITAOF | ✗ | ✗ | ✓ | ✗ | ✓ |
| COMMANDLOG | ✗ | ✗ | ✗ | ✗ | ✓ |

## Cross-Version Migration

For notes and recommendations on cross-version migration, please refer to the [Cross-version Migration](./version.md) document.
