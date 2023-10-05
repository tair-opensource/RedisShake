# aof_reader

## Introduction

Can use ` aof_ Reader 'to read data from the AOF file and then write it to the target end.
It is commonly used to recover data from backup files and also supports data flash back.

## configuration

```toml
[aof_reader]
aoffilepath="/tmp/appendonly.aof.manifest" #or single-aof: /tmp/appendonly.aof"
aoftimestamp="0"
```

*An absolute path should be passed in.

##The main process is as follows:
![aof_reader.jpg](/public/aof_reader.jpg)