# 快速上手

## 安装

### 下载二进制包

直接从 [Release](https://github.com/tair-opensource/RedisShake/releases) 下载二进制包。

### 从源代码编译

要从源代码编译，确保您在本地机器上设置了 Golang 环境：

```shell
git clone https://RedisShake/
cd RedisShake
sh build.sh
```

## 使用

假设你有两个 Redis 实例：

* 实例 A：127.0.0.1:6379
* 实例 B：127.0.0.1:6380

创建一个新的配置文件 `shake.toml`：

```toml
[sync_reader]
address = "127.0.0.1:6379"

[redis_writer]
address = "127.0.0.1:6380"
```

要启动 RedisShake，运行以下命令：

```shell
./redis-shake shake.toml
```
