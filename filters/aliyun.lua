-- Aliyun Redis 4.0: skip OPINFO command
function filter(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    if cmd_name == "OPINFO" then
        return 1, db_id -- disallow
    else
        return 0, db_id -- allow
    end
end