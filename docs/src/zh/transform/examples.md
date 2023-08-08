---
outline: deep
---
# transform 样例

## 阿里云兼容

```lua
-- Aliyun Redis 4.0: skip OPINFO command
function transform(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    if cmd_name == "OPINFO" then
        return 1, db_id -- disallow
    else
        return 0, db_id -- allow
    end
end
```

## AWS 兼容

```lua
-- ElastiCache: skip REPLCONF command
function transform(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    if cmd_name == "REPLCONF" then
        return 1, db_id -- disallow
    else
        return 0, db_id -- allow
    end
end
```

## 过滤命令

### 过滤所有 lua 脚本

```
-- skip all scripts included LUA scripts and Redis Functions.
function filter(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    if group == "SCRIPTING" then
        return 1, db_id -- disallow
    else
        return 0, db_id -- allow
    end
end
```

## key 操作

### 按照前缀过滤 key

```
-- skip keys prefixed with ABC
function filter(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    if #keys ~= 1 then
        return 0, db_id -- allow
    end

    if string.sub(keys[1], 0, 3) == "ABC" then
    return 1, db_id -- disallow
    end

    return 0, db_id -- allow
end
```

