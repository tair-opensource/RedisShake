# RDB Reader

## Introduction

The `rdb_reader` can be used to read data from an RDB file and then write it to the target destination. This is commonly used for recovering data from backup files.

## Configuration

```toml
[rdb_reader]
filepath = "/tmp/dump.rdb"
```

* An absolute path should be passed in.

