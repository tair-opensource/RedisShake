Redis-shake is mainly used to synchronize data from one redis database to another.<br>
Thanks to the Douyu's WSD team for the support. <br>

* [中文文档](https://yq.aliyun.com/articles/691794)

# Redis-Shake
---
Redis-shake is developed and maintained by NoSQL Team in Alibaba-Cloud Database department.<br>
Redis-shake has made some improvements based on [redis-port](https://github.com/CodisLabs/redis-port), including bug fixes, performance improvements and feature enhancements.<br>

# Main Functions
---
The type can be one of the following:<br>

* **decode**: Decode dumped payload to human readable format (hex-encoding).
* **restore**: Restore RDB file to target redis.
* **dump**: Dump RDB file from souce redis.
* **sync**: Sync data from source redis to target redis.

Please check out the `conf/redis-shake.conf` to see the detailed parameters description.<br>

# Verification
---
User can use [redis-full-check](https://github.com/aliyun/redis-full-check) to verify correctness.<br>

# Metric
---
Redis-shake offers metrics through restful api and log file.<br>

* restful api: `curl 127.0.0.1:9320/metric`.
* log: the metric info will be printed in the log periodically if enable.

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
Add tag when releasing: "release-v{version}-{date}". for example: "release-v1.0.2-20180628"

# Usage
---
*  git clone https://github.com/aliyun/redis-shake.git
*  cd redis-shake/src/vendor
*  GOPATH=\`pwd\`/../..; govendor sync     #please note: must install govendor first and then pull all dependencies
*  cd ../../ && ./build.sh
*  ./bin/collector -type=$(type_must_be_sync_dump_restore_or_decode) -conf=conf/redis-shake.conf #please note: user must modify collector.conf first to match needs.

# Shake series tool
---
We also provide some tools for synchronization in Shake series.<br>

* [mongo-shake](https://github.com/aliyun/mongo-shake): mongodb data synchronization tool.
* [redis-shake](https://github.com/aliyun/redis-shake): redis data synchronization tool.
* [redis-full-check](https://github.com/aliyun/redis-full-check): redis data synchronization verification tool.

Plus, we have a WeChat group so that users can join and discuss, but the group user number is limited. So please add my WeChat number: `vinllen_xingge` first, and I will add you to this group.<br>
