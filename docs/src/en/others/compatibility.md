---
outline: deep
---

# Version Compatibility

RedisShake supports multiple Redis and Valkey versions. This document details the feature support for each version.

## Version Support Overview

| Database | Supported Versions |
|----------|-------------------|
| Redis | 2.8 - 8.x |
| Valkey | 8.x - 9.x |

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

**Not Supported:**
- Vector Sets
- Redis Stack modules (RedisJSON, RediSearch, RedisTimeSeries, RedisBloom)

## Valkey Version Support Details

### Valkey 8.x

- Basic data types and commands
- Functionally equivalent to Redis 7.x

### Valkey 9.x

**Newly Supported:**
- Hash field expiration commands (same as Redis 8.x)
- Hash field expiration RDB format

## Feature Support Matrix

| Feature | Redis 2.8-7.x | Redis 8.x | Valkey 8.x | Valkey 9.x |
|---------|---------------|-----------|------------|------------|
| Basic Data Types | ✓ | ✓ | ✓ | ✓ |
| Stream | ✓ (5.0+) | ✓ | ✓ | ✓ |
| Module | ✓ (4.0+) | ✓ | ✓ | ✓ |
| Function | ✓ (7.0+) | ✓ | ✓ | ✓ |
| Hash Field Expiration | ✗ | ✓ | ✗ | ✓ |
| XACKDEL/XDELEX | ✗ | ✓ (8.2+) | ✗ | ✗ |
| Vector Sets | ✗ | ✗ | ✗ | ✗ |

## Cross-Version Migration

For notes and recommendations on cross-version migration, please refer to the [Cross-version Migration](./version.md) document.
