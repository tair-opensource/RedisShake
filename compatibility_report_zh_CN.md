# Redis 大版本兼容性报告

本文档根据 Redis 社区的 Release Note 以及阿里云 Tair 团队阅读 Redis 代码和日常运营遇到的问题撰写，供 Redis 用户升级大版本前评估和参考。如有遗漏，欢迎提 issue 补充，采纳后会同时添加您到贡献者列表。

# 用户视角兼容性差异：

## 4.0 版本升级到 5.0 版本

1.  `KEYS` 命令遍历过程中只跳过已过期的 key，不再删除。不能再使用 `KEYS` 命令来主动清理已过期的 key，`SCAN` 命令仍会删除扫描到的已过期数据。
    -   社区 PR：[https://github.com/redis/redis/commit/edc47a3a](https://github.com/redis/redis/commit/edc47a3a)

2.  Lua 脚本默认使用效果复制，而不再复制脚本本身，防止脚本在主备上执行结果不一致带来的数据不一致，可以通过配置项 `lua-replicate-commands` 更改（在 7.0 版本移除），或是 `DEBUG lua-always-replicate-commands` 命令更改。
    -   社区 PR：[https://github.com/redis/redis/commit/3e1fb5ff](https://github.com/redis/redis/commit/3e1fb5ff)
    
    ```plaintext
    1. 使用随机性命令，复制脚本会导致在主备上结果不同
    local key = KEYS[1]
    local member = redis.call('SPOP', key)
    redis.call('SET', 'last_removed', member)
    return member

    2. 由于过期/逐出行为，导致 get 得到的结果在主备上不同，导致脚本在主备执行不同分支 
    local key = KEYS[1]
    local value = redis.call('GET', key)
    if value then
        redis.call('SET', key, tonumber(value) + 1)
    else
        redis.call('SET', key, 1)
    end
    ```

3.  只读脚本会复制为 `SCRIPT LOAD` 命令，不再复制 `EVAL`。
    -   社区 PR：[https://github.com/redis/redis/commit/dfbce91a](https://github.com/redis/redis/commit/dfbce91a)

4.  备库默认忽略 `maxmemory` 限制，不再尝试逐出数据，可以通过配置项 `slave-ignore-maxmemory` 更改。
    -   社区 PR：[https://github.com/redis/redis/commit/447da44d](https://github.com/redis/redis/commit/447da44d)

5.  Redis 在开启 `volatile-lru` 内存逐出策略的时候，会禁止使用 `shared obj` 来表示诸如 1,2,3 等类似的数字（避免更新 LRU 信息的时候互相影响），副作用是浪费了内存。但是在主备复制时，如果走了全量复制，那么备库在加载 RDB 时没有复用那个策略，导致备库加载起来之后内存中的 key 使用了 `shared obj` 对象。因此这就会导致主备虽然数据一致，但是内存中的数据编码不一样。从而导致备库使用的内存比主库的小。该问题在 5.0 版本修复。升级后可能会遇到内存容量不一样的情况，社区 MR：
    -   [https://github.com/redis/redis/commit/bd92389c2dc1afcf53218880fc31ba45e66f4ded](https://github.com/redis/redis/commit/bd92389c2dc1afcf53218880fc31ba45e66f4ded)
    -   [https://github.com/redis/redis/commit/0ed0dc3c02dfffdf6bfb9e32f1335ddc34f37723](https://github.com/redis/redis/commit/0ed0dc3c02dfffdf6bfb9e32f1335ddc34f37723)

## 5.0 版本升级到 6.0 版本

1.  `OBJECT ENCODING` 命令支持识别并返回 stream 类型，原来对 stream 类型的数据使用 `OBJECT ENCODING` 命令会错误返回 "unknown"。
    -   社区 PR：[https://github.com/redis/redis/pull/7797](https://github.com/redis/redis/pull/7797)

2.  集群模式下，只读事务允许在只读节点上执行，而不是重定向到主节点。
    -   社区 PR：[https://github.com/redis/redis/pull/7766](https://github.com/redis/redis/pull/7766)
    
    ```plaintext
    原来的行为：
    connect to a replica in cluster
    > readonly
    > get k
    "v"
    > multi
    > get k
    > exec
    (error) MOVED

    改动后：
    connect to a replica in cluster
    > readonly
    > get k
    "v"
    > multi
    > get k
    > exec
    "v"
    ```

3.  `BRPOP`/`BLPOP`/`BRPOPLPUSH` timeout 参数由整数改为浮点数。引入了当 timeout 小于等于 0.001 秒时，会被解析为 0 导致永久阻塞的 bug，在 7.0 版本才修复。
    -   命令文档：[https://redis.io/docs/latest/commands/brpop/](https://redis.io/docs/latest/commands/brpop/), [https://redis.io/docs/latest/commands/blpop/](https://redis.io/docs/latest/commands/blpop/), [https://redis.io/docs/latest/commands/brpoplpush/](https://redis.io/docs/latest/commands/brpoplpush/)

4.  `BZPOPMIN`/`BZPOPMAX` timeout 参数由整数改为浮点数。引入了当 timeout 小于等于 0.001 秒时，会被解析为 0 导致永久阻塞的 bug，在 7.0 版本才修复。
    -   命令文档：[https://redis.io/docs/latest/commands/bzpopmin/](https://redis.io/docs/latest/commands/bzpopmin/), [https://redis.io/docs/latest/commands/bzpopmax/](https://redis.io/docs/latest/commands/bzpopmax/)

5.  `SETRANGE`, `APPEND` 命令所能生成的最大 string 类型长度由确定的 512MB 修改为使用配置 `proto_max_bulk_len` 控制（`proto_max_bulk_len` 默认值仍为 512MB）。
    -   社区 PR：[https://github.com/redis/redis/pull/4633](https://github.com/redis/redis/pull/4633)

## 6.0 版本升级到 6.2 版本

1.  当 AOF fsync 的策略是 'always' 时，fsync 失败会直接让进程退出。
    -   社区 PR：[https://github.com/redis/redis/pull/8347](https://github.com/redis/redis/pull/8347)

2.  命令的执行时间统计包含 Block 的时间，Block 时间过长的命令也会被统计到 `INFO commandstats` 和 `LATENCY` 族命令的结果中。
    -   社区 PR：[https://github.com/redis/redis/pull/7491](https://github.com/redis/redis/pull/7491)

3.  将 `SRANDMEMBER` 命令在 RESP 3 协议下的返回值类型从 Set 改为 Array，因为当使用负数的 `COUNT` 参数时，可以允许执行结果包含重复元素。
    -   社区 PR：[https://github.com/redis/redis/pull/8504](https://github.com/redis/redis/pull/8504)

4.  `PUBSUB NUMPAT` 命令返回值含义改变，6.0 及以前版本返回的是所有 pattern 的订阅总数（客户端数量），6.2 版本开始返回 pattern 总数（pattern 数量）。
    -   社区 PR：[https://github.com/redis/redis/pull/8472](https://github.com/redis/redis/pull/8472)
    -   命令文档：[https://redis.io/docs/latest/commands/pubsub-numpat/](https://redis.io/docs/latest/commands/pubsub-numpat/)

5.  `CLIENT TRACKING` 如果订阅了相互包含的前缀会返回错误，用于防止客户端收到重复的消息。
    -   社区 PR：[https://github.com/redis/redis/pull/8176](https://github.com/redis/redis/pull/8176)

6.  `SWAPDB` 命令会 touch 两个 DB 中的所有 watch keys，让事务失败。
    -   社区 PR：[https://github.com/redis/redis/pull/8239](https://github.com/redis/redis/pull/8239)
    
    ```plaintext
    原来的行为：
    client A> select 0
    client A> set k 0
    client A> select 1
    client A[1]> set k 1
    client A[1]> watch k
    client A[1]> multi
    client A[1]> get k

    client B> swapdb 0 1

    client A[1]> exec
    "0"

    改动后：
    client A> select 0
    client A> set k 0
    client A> select 1
    client A[1]> set k 1
    client A[1]> watch k
    client A[1]> multi
    client A[1]> get k

    client B> swapdb 0 1

    client A[1]> exec
    (nil)
    ```

7.  `FLUSHDB` 会清除 server 上所有客户端的所有 tracking key 记录。
    -   社区 PR：[https://github.com/redis/redis/pull/8039](https://github.com/redis/redis/pull/8039)

8.  `BITOPS` 最大限制长度由确定的 512MB 修改为使用配置 `proto_max_bulk_len` 控制（`proto_max_bulk_len` 默认值仍为 512MB）。
    -   社区 PR：[https://github.com/redis/redis/pull/8096](https://github.com/redis/redis/pull/8096)

9.  `bind` 配置在启动 server 时需要指明 `-` 前缀，有 `-` 前缀的地址会自动忽略 `EADDRNOTAVAIL` 错误，否则不忽略；之前的版本默认会忽略 `EADDRNOTAVAIL` 错误。同时支持使用 `bind *` 来监听所有地址，之前的版本只能通过省略 `bind` 配置来实现。
    -   社区 PR：[https://github.com/redis/redis/pull/7936](https://github.com/redis/redis/pull/7936)
    
    ```plaintext
    原来的行为：
    bind 任何地址，只要有一个地址成功，其它失败不会影响 Redis 启动

    改动后：
    bind 默认地址(127.0.0.1/::1)，只要有一个地址成功，其它失败不会影响 Redis 启动
    bind 指定'-' + 非默认地址(如 -192.168.1.100)，该地址绑定失败不会影响 Redis 启动，不加'-'的非默认地址失败会导致 Redis 启动失败
    ```

10. 使用参数启动 `redis-server` 不再重置 `save` 配置，之前的版本只要使用了额外的参数，无论是否包括 `save`，就会导致 `save` 配置先被重置为 `""`，然后再加载其他额外参数。
    -   社区 PR：[https://github.com/redis/redis/pull/7092](https://github.com/redis/redis/pull/7092)
    
    ```plaintext
    原来的行为：
    redis-server --other-args xx                   --> save ""
    redis-server [--other-args xx] --save 3600 1   --> save "3600 1"
    redis-server                                   --> save "3600 1 300 100 60 10000"

    改动后：
    redis-server --other-args xx                   --> save "3600 1 300 100 60 10000"
    redis-server [--other-args xx] --save 3600 1   --> save "3600 1"
    redis-server                                   --> save "3600 1 300 100 60 10000"
    ```

11. `SLOWLOG` 中记录原始命令而不是 rewrite 之后的命令。
    -   社区 PR：[https://github.com/redis/redis/pull/8006](https://github.com/redis/redis/pull/8006)
    
    ```plaintext
    原来的行为：
    SPOP 等命令导致 key 删除/GEOADD/incrbyfloat 等命令如果被慢日志记录，记录的会是 rewrite 后的命令 DEL/ZADD/SET

    改动后：
    慢日志中始终展示的是真正执行（未经 rewrite 的）命令
    ```

## 6.2 版本升级到 7.0 版本

1.  Lua 脚本不再进行持久化和复制。`SCRIPT LOAD`，`SCRIPT FLUSH` 也不再复制，`lua-replicate-commands` 配置移除，现在 Lua 确定进行效果复制。
    -   社区 PR：[https://github.com/redis/redis/pull/9812](https://github.com/redis/redis/pull/9812)

2.  `SHUTDOWN`/`SIGTERM`/`SIGINT` 触发的退出现在会等待 replica 同步最多 `shutdown-timeout` 秒。
    -   社区 PR：[https://github.com/redis/redis/pull/9872](https://github.com/redis/redis/pull/9872)
    -   命令文档：[https://redis.io/docs/latest/commands/shutdown/](https://redis.io/docs/latest/commands/shutdown/)

3.  ACL 默认的 channel 权限从 `allchannels` 改为 `resetchannels`，默认禁止所有 channel 的 Pub/Sub。
    -   社区 PR：[https://github.com/redis/redis/pull/10181](https://github.com/redis/redis/pull/10181)

4.  ACL load 过程中如果有相同的用户，不再静默地以最后一条为准，现在会抛出错误。
    -   社区 PR：[https://github.com/redis/redis/pull/9330](https://github.com/redis/redis/pull/9330)

5.  过期时间永远以毫秒级绝对时间戳进行复制。
    -   社区 PR：[https://github.com/redis/redis/pull/8474](https://github.com/redis/redis/pull/8474)

6.  移除 `STRALGO` 命令，添加 `LCS` 作为一个独立的命令。
    -   社区 PR：[https://github.com/redis/redis/pull/9799](https://github.com/redis/redis/pull/9799)
    -   命令文档：[https://redis.io/docs/latest/commands/lcs/](https://redis.io/docs/latest/commands/lcs/)

7.  增加了三个 immutable 配置 `enable-protected-configs`, `enable-debug-command`, `enable-module-command`, 默认值均为 no，分别默认禁止修改带有 `PROTECTED_CONFIG` 属性的配置，默认禁止使用 `DEBUG` 命令，默认禁止使用 `MODULE` 命令。
    -   社区 PR：[https://github.com/redis/redis/pull/9920](https://github.com/redis/redis/pull/9920)

8.  禁止 `SAVE`, `PSYNC`, `SYNC`, `SHUTDOWN` 命令在事务中执行；事务中的 `BGSAVE`, `BGREWRITEAOF`, `CONFIG SET appendonly` 命令会延迟到事务结束后进行。
    -   社区 PR：[https://github.com/redis/redis/pull/10015](https://github.com/redis/redis/pull/10015)

9.  `ZPOPMIN`, `ZPOPMAX` 命令当 key 的类型错误或输入的 count 参数为负数时，会返回错误而不是空数组。
    -   社区 PR：[https://github.com/redis/redis/pull/9711](https://github.com/redis/redis/pull/9711)

10. `CONFIG REWRITE` 命令会将已经加载的 module rewrite 为 `loadmodule` 写入文件中。
    -   社区 PR：[https://github.com/redis/redis/pull/4848](https://github.com/redis/redis/pull/4848)

11. `X[AUTO]CLAIM` 当消息已经被删除时，不再返回 nil，`XCLAIM` 会自动将已经删除的消息从 PEL 中移除，`XAUTOCLAIM` 的返回值会新增已删除消息 ID 列表。
    -   社区 PR：[https://github.com/redis/redis/pull/10227](https://github.com/redis/redis/pull/10227)
    -   命令文档：[https://redis.io/docs/latest/commands/xclaim/](https://redis.io/docs/latest/commands/xclaim/), [https://redis.io/docs/latest/commands/xautoclaim/](https://redis.io/docs/latest/commands/xautoclaim/)
    
    ```plaintext
    原来的行为：
    > XADD x 1 f1 v1
    "1-0"
    > XADD x 2 f1 v1
    "2-0"
    > XADD x 3 f1 v1
    "3-0"
    > XGROUP CREATE x grp 0
    OK
    > XREADGROUP GROUP grp Alice COUNT 2 STREAMS x >
    1) 1) "x"
       2) 1) 1) "1-0"
             2) 1) "f1"
                2) "v1"
          2) 1) "2-0"
             2) 1) "f1"
                2) "v1"
    > XDEL x 1 2
    (integer) 2
    > XCLAIM x grp Bob 0 0-99 1-0 1-99 2-0
    1) (nil)
    2) (nil)
    > XPENDING x grp
    1) (integer) 2
    2) "1-0"
    3) "2-0"
    4) 1) 1) "Bob"
          2) "2"

    改动后：
    > 到 XDEL 为止都相同
    > XCLAIM x grp Bob 0 0-99 1-0 1-99 2-0
    (empty array)
    > XPENDING x grp
    1) (integer) 0
    2) (nil)
    3) (nil)
    4) (nil)
    ```

12. `XREADGROUP` 命令使用 block 参数，导致客户端因此阻塞时，如果 Stream key 被删除，会唤醒 block 的客户端。
    -   社区 PR：[https://github.com/redis/redis/pull/10306](https://github.com/redis/redis/pull/10306)

13. `SORT`/`SORT_RO` 命令在用户没有所有 key 的读权限时，使用 `BY`/`GET` 参数会报错。
    -   社区 PR：[https://github.com/redis/redis/pull/10340](https://github.com/redis/redis/pull/10340)

14. 当 replica 持久化失败时，会 panic 退出而不是继续执行主节点发来的命令。添加了配置项 `replica-ignore-disk-write-errors`，默认值为 0。设置为 1 会忽略持久化失败，和低版本行为一致。
    -   社区 PR：[https://github.com/redis/redis/pull/10504](https://github.com/redis/redis/pull/10504)

15. 移除了 Lua 中的 `print()` 函数，需要使用 `redis.log` 代替。
    -   社区 PR：[https://github.com/redis/redis/pull/10651](https://github.com/redis/redis/pull/10651)

16. `PFCOUNT` 和 `PUBLISH` 命令禁止在只读脚本中调用，因为它们可能产生复制流量。
    -   社区 PR：[https://github.com/redis/redis/pull/10744](https://github.com/redis/redis/pull/10744)

17. 命令的统计以子命令为单位，`INFO commandstats` 命令返回具体每个子命令的信息，比如现在会具体展示 `CLIENT LIST`、`CLIENT TRACKING` 命令的统计信息而不是笼统的展示 `CLIENT` 命令。
    -   社区 PR：[https://github.com/redis/redis/pull/9504](https://github.com/redis/redis/pull/9504)

18. 引入了 Multi Part AOF 机制，现在 AOF 文件夹中将会有 Meta、BASE AOF 和 INCR AOF 三种文件，文件夹路径由配置 `appenddirname` 决定。
    -   社区 PR：[https://github.com/redis/redis/pull/9788](https://github.com/redis/redis/pull/9788)

## 7.0 版本升级到 7.2 版本

1.  开启了 Client Tracking 的客户端执行 Lua 脚本时，跟踪的 key 由 Lua 脚本声明的 key 变为脚本中的具体命令实际读取的 key。
    -   社区 PR：[https://github.com/redis/redis/pull/11770](https://github.com/redis/redis/pull/11770)
    
    ```plaintext
    原来的行为：
    client A> CLIENT TRACKING on
    client A> EVAL "redis.call('get', 'key2')" 2 key1 key2

    client B> MSET key1 1 key2 2

    client A> -> invalidate: 'key1'
    client A> -> invalidate: 'key2'

    改动后：
    client A> CLIENT TRACKING on
    client A> EVAL "redis.call('get', 'key2')" 2 key1 key2

    client B> MSET key1 1 key2 2

    client A> -> invalidate: 'key2'
    ```

2.  所有命令（包括一个 Lua 脚本内部）在执行过程中使用快照时间，所看到的时间不再变化，典型的例子是不能在一个 Lua 脚本中等待一个 key 过期。
    -   社区 PR：[https://github.com/redis/redis/pull/10300](https://github.com/redis/redis/pull/10300)

3.  ACL 不再移除冗余的权限变更记录，所有的操作记录都将被记录，影响 `ACL SAVE`, `ACL GETUSER`, `ACL LIST` 命令的输出。
    -   社区 PR：[https://github.com/redis/redis/pull/11224](https://github.com/redis/redis/pull/11224)

4.  `XREADGROUP` 和 `X[AUTO]CLAIM` 命令无论是否读取/声明成功，都会创建消费者。
    -   社区 PR：[https://github.com/redis/redis/pull/11099](https://github.com/redis/redis/pull/11099)

5.  `XREADGROUP` 命令在没有读取到消息时，会 reset 该 consumer 的 idle 字段，同时增加 active-time 字段记录 consumer 上次成功读取到消息的时间。
    -   社区 PR：[https://github.com/redis/redis/pull/11099](https://github.com/redis/redis/pull/11099)

6.  当阻塞命令解除阻塞时，会重新检查进行 ACL 权限、内存限制等检查，并根据解除阻塞后，重新执行命令时的情况区分错误码。
    -   社区 PR：[https://github.com/redis/redis/pull/11012](https://github.com/redis/redis/pull/11012)
    
    ```plaintext
    原来的行为：
    ACL、OOM check --> block command --> unblock --> execute

    改动后：
    ACL、OOM check --> block command --> unblock --> ACL、OOM recheck --> execute
    ```

# 重要 Bugfix：

## 5.0 版本

1.  修复了判断 key 过期的逻辑，防止命令执行过程中，key 因为过期被删除导致的 UAF crash 问题（4.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/commit/68d71d83](https://github.com/redis/redis/commit/68d71d83)

2.  修复了当 AOF 刷盘策略设置为 everysec 时，有可能出现最后一秒内的数据没有落盘导致数据丢失的 Bug（4.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/commit/c6b1252f](https://github.com/redis/redis/commit/c6b1252f)

3.  修复了事务中的命令错误统计到了 exec 命令上的 Bug（4.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/commit/d6aeca86](https://github.com/redis/redis/commit/d6aeca86)

## 6.0 版本

1.  修复了 `KEYS` 命令使用以 `\*\0` 开头的模式时，会返回所有 key 的 Bug（backport to 5.0，4.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/commit/c7f75266](https://github.com/redis/redis/commit/c7f75266)

2.  当字符串中包含 `\0` 时，尝试将该字符串转换为浮点类型的操作应该报错，影响 `HINCRBYFLOAT` 命令（5.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/commit/6fe55c2f](https://github.com/redis/redis/commit/6fe55c2f)

3.  `-READONLY` 报错应该中断事务（5.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/commit/8783304a](https://github.com/redis/redis/commit/8783304a)

## 6.2 版本

1.  `EXISTS` 命令不再改变 LRU，`OBJECT` 命令不会展示已经过期的 key 的信息。（backport to 6.0，5.0 及以下仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/8016](https://github.com/redis/redis/pull/8016)

2.  修复当从哈希表中随机挑选一个元素的操作只能支持 0 ~ 2^31 - 1 的范围，当哈希表元素超过 2^31 时，影响逐出、`RANDOMKEY`、`SRANDMEMBER` 等行为的公平性。（backport to 6.0，5.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/8133](https://github.com/redis/redis/pull/8133)

3.  `SMOVE` 命令没有改变目标 key 时，不通知 `WATCH` 和 `CLIENT TRACKING`。（backport to 6.0，5.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9244](https://github.com/redis/redis/pull/9244)

4.  修复了在 pipeline 中使用超时的 Lua 脚本时，有可能导致服务端不再处理 pipeline 中后续的其它命令，直到该连接有新的消息。（6.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/8715](https://github.com/redis/redis/pull/8715)

5.  `SCRIPT KILL` 能够终止 Lua 中的 pcall。原来如果 Lua 中一直在执行 pcall，那么 `SCRIPT KILL` 产生的 error 就会被 pcall 捕获，导致脚本不会停止，现在通过在 Lua VM 层面产生 error，来终止 Lua 执行。（6.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/8661](https://github.com/redis/redis/pull/8661)

6.  将 `SRANDMEMBER` 命令在 RESP 3 协议下的返回值类型从 Set 改为 Array，因为当使用负数的 `COUNT` 参数时，可以允许执行结果包含重复元素。（6.0 版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/8504](https://github.com/redis/redis/pull/8504)

## 7.0 版本

1.  修复了用户参数过大导致触发 Lua 栈溢出的 assert。（backport to 6.0，5.0 版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9809](https://github.com/redis/redis/pull/9809)

2.  修复了使用 `list-compress-depth` 配置时可能出现的 crash。（6.2 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9849](https://github.com/redis/redis/pull/9849)

3.  修复了当单个 String/List 超过 4GB 时，开启 `rdbcompression` 配置生产 RDB 时会导致 RDB 损坏的问题。（6.2 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9776](https://github.com/redis/redis/pull/9776)

4.  修复了当向 Set/Hash 添加超过 2GB 大小的元素时，会导致 crash 的 Bug。（6.2 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9916](https://github.com/redis/redis/pull/9916)

5.  当设置极大或极小（很大的负数）的过期时间，导致过期时间溢出时应该报错。（backport to 6.2，6.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/8287](https://github.com/redis/redis/pull/8287)

6.  当 `DECRBY` 命令输入 `LLONG_MIN` 时应该报错而不是导致溢出。（6.2 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9577](https://github.com/redis/redis/pull/9577)

7.  修复了 ZSet 元素数量大于 `UINT32_MAX` 时，可能出现的 rank 计算溢出。（6.2 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9249](https://github.com/redis/redis/pull/9249)

8.  修复了 Lua 中的写命令会无视 `CLIENT TRACKING NOLOOP` 的 Bug。（6.0 版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/11052](https://github.com/redis/redis/pull/11052)

9.  修复了 `CLIENT TRACKING` 开启时，key 失效的 push 包有可能穿插在当前连接执行其它命令的回包之间（backport to 6.2，6.0 版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/11038](https://github.com/redis/redis/pull/11038), [https://github.com/redis/redis/pull/9422](https://github.com/redis/redis/pull/9422)

10. 当 `WATCH` 的 key 在 `EXEC` 执行时已经过期，无论是否被删除，事务都应该失败。另外在事务执行期间，事务中 server 用于判断是否过期的时间戳不应该更新。（backport to 6.0，5.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9194](https://github.com/redis/redis/pull/9194)

11. 修复了 `SINTERSTORE` 命令操作的 key 类型不对时没有返回 `WRONGTYPE` 错误，在特殊情况下有可能导致目标 key 被删除的 Bug（backport to 6.0，5.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9032](https://github.com/redis/redis/pull/9032)

12. 修复了客户端内存超过 `client-output-buffer-limit` 的 soft limit 后，如果一直没有流量，超过了 soft limit timeout 后仍然不会被断连接的 Bug。（backport to 6.2，6.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/8833](https://github.com/redis/redis/pull/8833)

13. 修复了 `XREADGROUP`, `XCLAIM`, `XAUTOCLAIM` 命令创建消费者时，没有发出 keyspace notification；`XGROUP DELCONSUMER` 命令删除不存在的消费者时，不应该发出 keyspace notification。（6.2 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/9263](https://github.com/redis/redis/pull/9263)

14. 修复了 `GEO` 命令在 search 时，有可能遗漏掉比较靠近搜索边界的点的 Bug。（6.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/10018](https://github.com/redis/redis/pull/10018)

15. 修复了在 Lua 的返回值处理过程中，使用 metatable 时如果触发错误会导致 server crash 问题。（6.0 及以下版本仍有影响）
    -   社区 PR：[https://github.com/redis/redis/pull/11032](https://github.com/redis/redis/pull/11032)

16. 修复了以秒为单位的 Blocking 命令（比如 `BLPOP`/`BRPOP`）在 timeout 小于 0.001 秒时有可能被解析为 0 导致永久阻塞的 bug（6.2 及以下版本仍有影响）。
    -   社区 PR：[https://github.com/redis/redis/pull/11688](https://github.com/redis/redis/pull/11688)

17. 修复了恶意的 `KEYS`, `HRANDFIELD`, `SRANDMEMBER`, `ZRANDMEMBER` 命令可能导致 server hang 死，（`KEYS` 仍有可能在 6.2 及以下版本造成 server hang 死）。
    -   社区 PR：[https://github.com/redis/redis/pull/11676](https://github.com/redis/redis/pull/11676)

## 7.2 版本

1.  修复了 `SRANDMEMBER`, `ZRANDMEMBER`, `HRANDFIELD` 命令在使用 `count` 参数时，有小概率死循环的 Bug（backport to 7.0，6.2 及以下版本仍有问题）
    -   社区 PR：[https://github.com/redis/redis/pull/12276](https://github.com/redis/redis/pull/12276)

2.  `HINCRBYFLOAT` 命令在解析 increment 参数失败后不会创建 key（backport to 6.0，5.0 及以下版本仍有问题）
    -   社区 PR：[https://github.com/redis/redis/pull/11149](https://github.com/redis/redis/pull/11149)
    -   命令文档：[https://redis.io/docs/latest/commands/hincrbyfloat/](https://redis.io/docs/latest/commands/hincrbyfloat/)

3.  `LSET` 命令将 List 中大量的小元素换成大元素时，极端情况下可能导致 crash（7.0 版本仍有问题）
    -   社区 PR：[https://github.com/redis/redis/pull/12955](https://github.com/redis/redis/pull/12955)
    -   命令文档：[https://redis.io/docs/latest/commands/lset/](https://redis.io/docs/latest/commands/lset/)
