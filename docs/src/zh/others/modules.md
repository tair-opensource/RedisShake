---
outline: deep
---

# Redis Modules

Redis Modules 是 Redis 4.0 版本引入的一个新特性，它允许开发者扩展 Redis 的功能。通过创建模块，开发者可以定义新的命令，数据类型，甚至改变 Redis 的行为。因此，Redis Modules 可以极大地增强 Redis 的灵活性和可扩展性。

由于 Redis Modules 可以定义新的数据类型和命令，RedisShake 需要对这些新的数据类型和命令进行专门的处理，才能正确地迁移或同步这些数据。否则，如果 RedisShake 不理解这些新的数据类型和命令，它可能会无法正确地处理这些数据，或者在处理过程中出错。因此，对于使用了 Redis Modules 的 Redis 实例，一般需要 RedisShake 为其使用的 Module 提供相应的适配器，以便正确地处理这些自定义的数据类型和命令。

## 已支持的 Redis Modules 列表

- [TairHash](https://github.com/tair-opensource/TairHash)：支持 field 级别设置过期和版本的 Hash 数据结构。
- [TairString](https://github.com/tair-opensource/TairString)：支持版本的 String 结构，可以实现分布式锁/乐观锁。
- [TairZset](https://github.com/tair-opensource/TairZset)：支持最多 256 维的 double 排序，可以实现多维排行榜。

## 如何支持新的 Redis Modules

### 核心流程

相关代码在`internal\rdb`目录下，如需要支持其它 Redis Modules 类型，可分解为以下三个步骤：
- 从rdb文件中正确读入
   - RedisShake 中已经 对 redis module 自定的几种类型进行了封装，从 rdb 文件进行读取时，可直接借助于已经封装好的函数进行读取（`internal\rdb\structure\module2_struct.go`）
- 构建一个合适的中间数据结构类型，用于存储相应数据（key + value）
- 大小key 的处理
   - 小key
      - 在实际工作中，执行`LoadFromBuffer`函数从rdb读入数据时，其对应的 value 值会流动到两个地方，一个是直接存储在缓存区中一份，用于小 key 发送时直接读取（与`restore`命令有关），一个流动到上述的中间数据结构中，被下述的 `rewrite`函数使用
   - 大key
      - 借助于` rewrite` 函数，从上述的中间数据结构中读取，并拆分为对应的命令进行发送

![module-supported.jpg](/public/module-supported.jpg)

### 补充命令测试
为了确保正常，需要在` tests\helpers\commands` 里面添加对应 module 的命令，来测试相关命令可以在 rdb、sync、scan 三个模式下工作正常。测试框架具体见[pybbt](https://pypi.org/project/pybbt/)，具体思想——借助于redis-py 包，对其进行封装，模拟客户端发送命令，然后比对实际的返回值与已有的返回值。

### 补充命令列表
RedisShake 在针对大 key 进行传输时，会查命令表格`RedisShake\internal\commands\table.go`，检查命令的合规性，因此在添加新 module 时，需要将对应的命令加入表格，具体可参照`RedisShake\scripts`部分代码

### 补充 ci 
在 ci 测试中，需要添加对自定义 module 的编译，具体可见` ci.yml` 内容



