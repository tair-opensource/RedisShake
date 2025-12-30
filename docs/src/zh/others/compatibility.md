---
outline: deep
---

# 版本兼容性

RedisShake 支持多种 Redis 和 Valkey 版本，本文档详细说明各版本的特性支持情况。

## 版本支持概览

| 数据库 | 支持版本 |
|--------|----------|
| Redis | 2.8 - 8.x |
| Valkey | 8.x - 9.x |

## Redis 版本支持详情

### Redis 2.8 - 3.x

- 基础数据类型：String、List、Set、Sorted Set、Hash
- 基础命令支持

### Redis 4.x

- 所有 2.8-3.x 特性
- Module 支持（TairString、TairHash、TairZset）
- Stream 数据类型（4.0 不支持，5.0 引入）

### Redis 5.x - 6.x

- 所有 4.x 特性
- Stream 数据类型
- Module 支持

### Redis 7.x

- 所有 6.x 特性
- Function 支持

### Redis 8.x

**新增支持：**
- Hash 字段过期命令：HSETEX、HGETEX、HGETDEL、HTTL、HPTTL、HPERSIST、HEXPIRE、HEXPIREAT、HPEXPIRE、HPEXPIREAT、HEXPIRETIME、HPEXPIRETIME
- Hash 字段过期 RDB 格式（RDB type 22-25）

**不支持：**
- Vector Sets（向量数据类型）
- Redis Stack 模块（RedisJSON、RediSearch、RedisTimeSeries、RedisBloom）

## Valkey 版本支持详情

### Valkey 8.x

- 基础数据类型和命令
- 与 Redis 7.x 功能基本一致

### Valkey 9.x

**新增支持：**
- Hash 字段过期命令（与 Redis 8.x 相同）
- Hash 字段过期 RDB 格式

## 特性支持矩阵

| 特性 | Redis 2.8-7.x | Redis 8.x | Valkey 8.x | Valkey 9.x |
|------|---------------|-----------|------------|------------|
| 基础数据类型 | ✓ | ✓ | ✓ | ✓ |
| Stream | ✓ (5.0+) | ✓ | ✓ | ✓ |
| Module | ✓ (4.0+) | ✓ | ✓ | ✓ |
| Function | ✓ (7.0+) | ✓ | ✓ | ✓ |
| Hash 字段过期 | ✗ | ✓ | ✗ | ✓ |
| XACKDEL/XDELEX | ✗ | ✓ (8.2+) | ✗ | ✗ |
| Vector Sets | ✗ | ✗ | ✗ | ✗ |

## 跨版本迁移

关于跨版本迁移的注意事项和建议，请参考[跨版本迁移](./version.md)文档。
