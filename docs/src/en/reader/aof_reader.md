# aof_reader

## Introduction

`aof_reader` reads commands from AOF backup files and replays them to the configured writer. It is commonly used for offline recovery and timestamp-based point-in-time flashback.

## Configuration

```toml
[aof_reader]
filepath = "/tmp/appendonly.aof.manifest" # or a single AOF file "/tmp/appendonly.aof"
timestamp = 0                             # stop replay at Unix timestamp (seconds); 0 = no limit
```

- `filepath` must be an absolute path and supports both Redis 7+ multi-part AOF manifest files and single AOF files.
- `timestamp` specifies the cutoff Unix timestamp in seconds (`0` replays the full file).

## Workflow

![aof_reader](/aof_reader.jpg)
