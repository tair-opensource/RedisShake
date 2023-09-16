---
outline: deep
---


# 介绍
可以为 RedisShake 贡献代码支持其它自定义 module 类型


# 核心流程
相关代码在`internal\rdb`目录下，如需要支持其它 redis module 类型，可分解为以下三个步骤——

- 从rdb文件中正确读入
   - RedisShake 中已经 对 redis module 自定的几种类型进行了封装，从 rdb 文件进行读取时，可直接借助于已经封装好的函数进行读取（`internal\rdb\structure\module2_struct.go`）
- 构建一个合适的中间数据结构类型，用于存储相应数据（key + value）
- 大小key 的处理
   - 小key
      - 在实际工作中，执行`LoadFromBuffer`函数从rdb读入数据时，其对应的 value 值会流动到两个地方，一个是直接存储在缓存区中一份，用于小 key 发送时直接读取（与`restore`命令有关），一个流动到上述的中间数据结构中，被下述的 `rewrite`函数使用
   - 大key
      - 借助于` rewrite` 函数，从上述的中间数据结构中读取，并拆分为对应的命令进行发送

![module-supported.jpg](/public/module-supported.jpg)


# 其它
## 补充命令测试
为了确保正常，需要在` tests\helpers\commands` 里面添加对应 module 的命令，来测试相关命令可以在 rdb、sync、scan 三个模式下工作正常。测试框架具体见[pybbt](https://pypi.org/project/pybbt/)，具体思想——借助于redis-py 包，对其进行封装，模拟客户端发送命令，然后比对实际的返回值与已有的返回值。

## 补充命令列表
RedisShake 在针对大 key 进行传输时，会查命令表格`RedisShake\internal\commands\table.go`，检查命令的合规性，因此在添加新 module 时，需要将对应的命令加入表格，具体可参照`RedisShake\scripts`部分代码
## 补充 ci 
在 ci 测试中，需要添加对自定义 module 的编译，具体可见` ci.yml` 内容



# 已支持的 redis module 列表

- [TairHash](https://github.com/tair-opensource/TairHash)
- [TairString](https://github.com/tair-opensource/TairString)
- [TairZset](https://github.com/tair-opensource/TairZset)
