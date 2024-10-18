# 架构与性能说明

## 架构图

当源端和目的端都为集群（Cluster）时，同步架构图如下。

![集群同步架构图](/architecture-c2c.svg)

当源端和目的端为单实例节点（Standalone）时，同步架构图如下。

![单实例节点同步架构图](/architecture-s2s.svg)

## 架构说明

从架构图可以看到，数据从源端（Source）同步到目的端（Destination），主要经过 Cluster Reader、Main、Cluster Writer 三部分处理。

### Cluster Reader

Cluster Reader 即集群读入类，其根据源端分片数量创建同等数量的 Standalone Reader，每个 Standalone Reader 开启一个协程（Goroutinue）并行的从每个源端分片进行读入，并将数据存入相应的管道（Reader Channel）交付给下一环节处理。

### Main

Main 即主函数，其根据 Reader Channel 数量开启多个协程，并行的对管道中数据分别执行 Parse、Filter、Function 操作，再调用 Cluster Writer 的 Write 方法，将数据分发给写入端。

- Parse：数据包解析

- Filter：filter 功能，过滤操作

- Function：funtion 功能，执行 lua 函数

### Cluster Writer

Cluster Writer 即集群写入类，根据目的端分片数量创建同等数量的 Standalone Writer，Cluster Writer 的 Write 方法可以将数据分发到对应 slot 的 Standalone Writer 的管道中（Writer Channel），Standalone Writer 再将数据写入到目的端。

## 性能和资源占用

### 测试环境

- 服务器：ecs.i4g.8xlarge 32核，磁盘读入速度 2.4 GB/s，写入速度 1.5 GB/s

- 源端和目的端 Redis 集群：1 GB 12 分片

### 测试工具

- redis-benchmark：redis 的压力测试工具，为源端创造持续的写入流量

分别对 redisshake 两种模式 sync 和 scan，数据同步全量同步和增量同步两个阶段设计了测试案例。对于全量同步阶段，需要提前写入数据到源端，再开启 redisshake 同步；对于增量同步阶段，先开启 redisshake 开始同步，再利用 redis-benchmark 持续产生写入流量。

其中，sync 模式下，全量同步阶段同步一个 rdb 文件，增量同步阶段则是 aof 数据流；在 scan 模式下，全量同步采用 scan 遍历源端数据库，增量同步阶段则是开启 ksn 进行键值同步。

对于增量同步阶段，redis-benchmark 脚本设置如下，产生的写请求大概为 1500k/s，可以占满 ECS 服务器的前 16 个 cpu 内核。

```bash
taskset -c 0-15 redis-benchmark \
  --threads 16 -r 10000000 -n 1000000000 -t set \
  -h host -a 'username:password' \
  --cluster -c 256 -d 8 -P 2
```

测试结果可见 [RedisShake 云端性能测试结果](https://github.com/OxalisCu/RedisShake/tree/benchmark-backup-cloud/demo)

### 性能数据

对于源端集群和目的端集群同步两种方式的同步速率也进行了对比。

- 12c-12c：一个 redisshake 采用 Cluster 模式同步

- 12(s-s)：每个分片启动一个 redisshake 分别采用 Standalone 模式同步

测试得到同步速率如下，bench 代表 redis-benchmark 产生的写入流量速率，scan 模式下设置 count = 10。

|                 | bench | 12c-12c | 12(s-s)         | 12c-12c/12(s-s) |
| --------------- | ----- | ------- | --------------- | --------------- |
| **sync + aof**  | 1599k | 1520k   | 12*(130k)=1560k | 0.97            |
| **sync + rdb**  |       | 1498k   | 12*(220k)=2640k | 0.57            |
| **scan + ksn**  | 1084k | 1081k   | 12*(95k)=1140k  | 0.95            |
| **scan + scan** |       | 665k    | 12*(58k)=696k   | 0.95            |

### 资源消耗

cpu 占用和 disk 读写速率采用 htop 工具监测，network 收发速率采用 iftop 工具监测，得到结果如下。

|                 | cpu                                 | network                              | disk       |
| --------------- | ----------------------------------- | ------------------------------------ | ---------- |
| **sync + aof**  | 16 核占用 70%-90%，总使用率 1276.9%  | 发送速率 1340Mb/s，接收速率 998Mb/s   | 155.91MB/s |
| **sync + rdb**  | 32 核占用 50%-60%，总使用率 1605.0%  | 发送速率 435Mb/s，接收速率 82.1 Mb/s  | 113.53KB/s |
| **scan + ksn**  | 16 核占用 90%-100%，总使用率 1911.4% | 发送速率 2100Mb/s，接收速率 1330 Mb/s | 172.07KB/s |
| **scan + scan** | 32 核占用 40%-60%，总使用率 1297.2%  | 发送速率 1130Mb/s，接收速率 533Mb/s   | 155.78KB/s |