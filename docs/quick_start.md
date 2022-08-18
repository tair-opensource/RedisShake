# 快速开始

## 实例信息

### 实例 A

- 地址：r-aaaaa.redis.zhangbei.rds.aliyuncs.com
- 端口：6379
- 密码：r-aaaaa:xxxxx

### 实例 B

- 地址：r-bbbbb.redis.zhangbei.rds.aliyuncs.com
- 端口：6379
- 密码：r-bbbbb:xxxxx

### 实例 C 集群实例

- 地址：
    - 192.168.0.1:6379
    - 192.168.0.2:6379
    - 192.168.0.3:6379
    - 192.168.0.4:6379
- 密码：r-ccccc:xxxxx

## 工作目录

```
.
├── redis-shake # 二进制程序
└── redis-shake.toml # 配置文件
```

## 开始

## A -> B 同步

修改 `redis-shake.toml`，改为如下配置：

```toml
[source]
type = "sync"
address = "r-aaaaa.redis.zhangbei.rds.aliyuncs.com:6379"
password = "r-aaaaa:xxxxx"

[target]
type = "standalone"
address = "r-bbbbb.redis.zhangbei.rds.aliyuncs.com:6379"
password = "r-bbbbb:xxxxx"
```

启动 redis-shake：

```bash
./redis-shake redis-shake.toml
```

## A -> C 同步

修改 `redis-shake.toml`，改为如下配置：

```toml
[source]
type = "sync"
address = "r-aaaaa.redis.zhangbei.rds.aliyuncs.com:6379"
password = "r-aaaaa:xxxxx"

[target]
type = "cluster"
address = "192.168.0.1:6379" # 这里写集群中的任意一个节点的地址即可
password = "r-ccccc:xxxxx"
```

启动 redis-shake：

```bash
./redis-shake redis-shake.toml
```