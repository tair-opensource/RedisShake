## redis-shake

**这是私有化维护的开源项目redis-shake，用于从redis中迁出数据，目前常用于导入SOS。**

**SOS是高性能、低成本、高可用的自研分布式KV存储系统，可以支持Redis中KV型的数据，性能与redis无异，而成本可以降低到1/5或以下。**

**有需要的使用的小伙伴欢迎到 [SOS介绍主页](https://git.woa.com/groups/sos/-/homepage) 围观，或直接联系我们。本库新开发的特征也会提交到RedisShake的GitHub原库中。**

### 1. 原文链接

- [英文文档](./README_en.md)
- [中文文档](./README_zh.md)
- [快速开始](./docs/quick_start.md)

### 2. 基本用法

使用三步，即可实现从redis迁移到sos集群（或者任何redis协议的proxy）。

1⃣️ 编译
~~~sh
sh build.sh
~~~

2⃣️ 改配置

打开`redis-shake.toml`，我们把源和目标填上，其他配置用默认的就行。

~~~sh
[source]
type = "sync" # 支持两种模式：sync或者restore。sync适用于支持主从命令的redis server；restore模式适用于直接操作rdb文件。
address = "127.0.0.1:6379" # 如果是sync模式，填要迁出的redis server地址。
#rdb_file_path = "/data/dump.rdb" # 如果是restore模式，填要导入的rdb文件路径。

[target]
address = "127.0.0.1:8379" # SOS的redis proxy地址
password = "" # SOS的service_name
~~~

3⃣️ 运行

~~~sh
./bin/redis-shake ./redis-shake.toml
~~~

然后屏幕就会出现类似这样的信息：

<img width="" src="/uploads/4E968647D4CA478988232F385F89B1C5/image.png" alt="image.png" />

### 3. 解锁更多

💭    
问题1.    
`sync`和`restore`模式分别怎么用？   
答：   
根据初步调研，istore-redis支持主从同步命令，因此开发者可以很方便地用这个工具的`sync`模式进行数据迁出或者其他同步操作。   
sync模式本质上是通过redis原生的sync/psync命令让master进行主从同步，首先会把现有的存量数据导出成为rdb文件，把rdb文件逐条记录进行发送。期间心跳会一直通过PING保持，在存量数据同步完毕之后，会把增量数据根据实际请求逐条同步到目标机器上。   
所以开发者在使用sync模式时，只需要关注存量数据迁移完之后，就可以开始进行到sos集群的双写了，这样也方便业务进行观察。   

对于腾讯云redis，目前不支持直接主从拉取数据，经调研之后比较合适的方式是导出成rdb文件（如上配置的dump.rdb之类），我们再通过`restore`模式进行导入。   
存量的导入与sync模式一样，但对于增量需要开发者手动保持追加到sos集群中。  

以上为目前调研的方案，如果有更好的建议，欢迎提供~~~

💭    
问题2.    
能不能提高并发？   
答：   
可以通过更改配置文件中的并发数来提高：   
```
rdb_restore_parallel_num = 1
```
目前并发数只会影响rdb文件的并发发送，不会影响增量部分，以此保证增量数据的有序性。   

经简单测试，同机房100w条200字节的redis数据在并发数为1时倒入SOS需要9分钟，而并发16时只需要半分钟。   

💭    
问题3.    
能不能使用配置中的pipeline_count_limit ？    
答：   
不能！SOS的redis proxy不支持pipeline模式，所以这个配置项在这个私有化项目中配为1。当然这个工具可以进行任意redis到redis的迁移，其他接入层支持pipeline的话是可以用的。   
