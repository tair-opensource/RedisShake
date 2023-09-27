# Quick Start

## Installation

### Download Binary Package

Directly download the binary package from [Release](https://github.com/tair-opensource/RedisShake/releases).

### Compile from Source Code

To compile from the source code, make sure you have set up the Golang environment on your local machine:

```shell
git clone https://github.com/alibaba/RedisShake
cd RedisShake
sh build.sh
```

## Usage

Assume you have two Redis instances:

* Instance A: 127.0.0.1:6379
* Instance B: 127.0.0.1:6380

Create a new configuration file `shake.toml`:

```toml
[sync_reader]
address = "127.0.0.1:6379"

[redis_writer]
address = "127.0.0.1:6380"
```

To start RedisShake, run the following command:

```shell
./redis-shake shake.toml
```

## Precautions

1. Do not run two RedisShake processes in the same directory, as the temporary files generated during runtime may be overwritten, leading to abnormal behavior.
2. Do not downgrade the Redis version, such as from 6.0 to 5.0, because each major version of RedisShake introduces some new commands and encoding methods. If the version is lowered, it may lead to incompatibility.