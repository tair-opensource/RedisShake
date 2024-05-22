# 如何判断数据一致

在增量同步阶段经常需要判断源端与目标端数据是否一致，介绍两种经验方法：
1. 通过日志或者监控，观察到 RedisShake 的同步流量为 0，认为同步结束，两端一致。
2. 对比源端与目的端 Key 数量是否相等，如果相等，认为一致。

如果 Key 带有过期时间，会导致 Key 数量不一致。原因：
1. 因为过期算法限制，源端中可能存在一些 Key 已经过期但实际上没有被删除，这批 Key 在目的端可能会被删除，导致目的端 Key 数量少于源端。
2. 源端和目的端独立运行，各自的过期算法独立运行，过期算法具有随机性，会导致源端和目的端删除的 Key 不一致，导致 Key 数量不一致。

在实践中，带有过期时间的 Key 一般认为是允许不一致的，不会影响业务，所以可以仅校验不带有过期时间的 Key 数量是否一致。如下所示，应当分别计算源端和目的端的 `$keys-$expires` 的值是否一样。[[795]](https://github.com/tair-opensource/RedisShake/issues/795) [[791]](https://github.com/tair-opensource/RedisShake/issues/791)
```
127.0.0.1:6379> info keyspace
# Keyspace
db0:keys=4463175,expires=2,avg_ttl=333486
```