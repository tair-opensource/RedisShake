RedisShake is mainly used to synchronize data from one redis to another.<br>
Thanks to the Douyu's WSD team for the support. <br>

* [中文文档](https://yq.aliyun.com/articles/691794)
* [English tutorial](https://github.com/alibaba/RedisShake/wiki/tutorial-about-how-to-set-up)
* [中文使用文档](https://github.com/alibaba/RedisShake/wiki/%E7%AC%AC%E4%B8%80%E6%AC%A1%E4%BD%BF%E7%94%A8%EF%BC%8C%E5%A6%82%E4%BD%95%E8%BF%9B%E8%A1%8C%E9%85%8D%E7%BD%AE%EF%BC%9F)
* [Release package](https://github.com/alibaba/RedisShake/releases)

# Redis-Shake
---
Redis-shake is developed and maintained by NoSQL Team in Alibaba-Cloud Database department.<br>
Redis-shake has made some improvements based on [redis-port](https://github.com/CodisLabs/redis-port), including bug fixes, performance improvements and feature enhancements.<br>

# Main Functions
---
The type can be one of the followings:<br>

* **decode**: Decode dumped payload to human readable format (hex-encoding).
* **restore**: Restore RDB file to target redis.
* **dump**: Dump RDB file from source redis.
* **sync**: Sync data from source redis to target redis by `sync` or `psync` command. Including full synchronization and incremental synchronization.
* **rump**: Sync data from source redis to target redis by `scan` command. Only support full synchronization. Plus, RedisShake also supports fetching data from given keys in the input file when `scan` command is not supported on the source side. This mode is usually used when `sync` and `psync` redis commands aren't supported.

Please check out the `conf/redis-shake.conf` to see the detailed parameters description.<br>

# Support
---
Redis version from 2.x to 5.0.
Supports `Standalone`, `Cluster`, and some proxies type like `Codis`, `twemproxy`,  `Aliyun Cluster Proxy`, `Tencent Cloud Proxy` and so on.<br>
For `codis` and `twemproxy`, there maybe some constraints, please checkout this [question](https://github.com/alibaba/RedisShake/wiki/FAQ#q-does-redisshake-supports-codis-and-twemproxy).

# Configuration
Redis-shake has several parameters in the configuration(`conf/redis-shake.conf`) that maybe confusing, if this is your first time using, please visit this [tutorial](https://github.com/alibaba/RedisShake/wiki/tutorial-about-how-to-set-up).

# Verification
---
User can use [RedisFullCheck](https://github.com/alibaba/RedisFullCheck) to verify correctness.<br>

# Metric
---
Redis-shake offers metrics through restful api and log file.<br>

* restful api: `curl 127.0.0.1:9320/metric`.
* log: the metric info will be printed in the log periodically if enable.
* inner routine heap: `curl http://127.0.0.1:9310/debug/pprof/goroutine?debug=2`

# Redis Type
---
Both the source and target type can be standalone, opensource cluster and proxy. Although the architecture patterns of different vendors are different for the proxy architecture, we still support different cloud vendors like alibaba-cloud, tencent-cloud and so on.<br>
If the target is open source redis cluster, redis-shake uses [redis-go-cluster](https://github.com/chasex/redis-go-cluster) driver to write data. When target type is proxy, redis-shakes write data in round-robin way.<br>
If the source is redis cluster, redis-shake launches multiple goroutines for parallel pull. User can use `rdb.parallel` to control the RDB syncing concurrency.<br>
The "move slot" operations must be disabled on the source side.<br>

# Code branch rules
---
Version rules: a.b.c.

*  a: major version
*  b: minor version. even number means stable version.
*  c: bugfix version

| branch name | rules |
| - | :- |
| master | master branch, do not allowed push code. store the latest stable version. develop branch will merge into this branch once new version created.|
| **develop**(main branch) | develop branch. all the bellowing branches fork from this. |
| feature-\* | new feature branch. forked from develop branch and then merge back after finish developing, testing, and code review. |
| bugfix-\* | bugfix branch. forked from develop branch and then merge back after finish developing, testing, and code review. |
| improve-\* | improvement branch. forked from develop branch and then merge back after finish developing, testing, and code review.  |

Tag rules:<br>
Add tag when releasing: "release-v{version}-{date}". for example: "release-v1.0.2-20180628"<br>
User can use `-version` to print the version.

# Usage
---
You can **directly download** the binary in the [release package](https://github.com/alibaba/RedisShake/releases), and use `start.sh` script to start it directly: `./start.sh redis-shake.conf sync`.<br>
You can also build redis-shake yourself according to the following steps, the `go` and `govendor` must be installed before compile:
*  git clone https://github.com/alibaba/RedisShake.git
*  cd RedisShake
*  export GOPATH=\`pwd\`
*  cd src/vendor
*  govendor sync     #please note: must install govendor first and then pull all dependencies: `go get -u github.com/kardianos/govendor`
*  cd ../../ && ./build.sh
*  ./bin/redis-shake -type=$(type_must_be_sync_dump_restore_decode_or_rump) -conf=conf/redis-shake.conf #please note: user must modify collector.conf first to match needs.

# Shake series tool
---
We also provide some tools for synchronization in Shake series.<br>

* [MongoShake](https://github.com/aliyun/MongoShake): mongodb data synchronization tool.
* [RedisShake](https://github.com/aliyun/RedisShake): redis data synchronization tool.
* [RedisFullCheck](https://github.com/aliyun/RedisFullCheck): redis data synchronization verification tool.
* [NimoShake](https://github.com/alibaba/NimoShake): sync dynamodb to mongodb.

Plus, we have a DingDing(钉钉) group so that users can join and discuss, please scan the code.
![DingDing](resources/dingding_group.png)<br>

# Thanks
---
| Username | Mail |
| :------: | :------: |
| ceshihao | davidzheng23@gmail.com |
| wangyiyang | wangyiyang.kk@gmail.com |
| muicoder | muicoder@gmail.com |
| zhklcf | huikangzhu@126.com |
| shuff1e | sfxu@foxmail.com |
| xuhualin | xuhualing8439523@163.com |
