-- ElastiCache: skip REPLCONF command
function filter(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    if cmd_name == "REPLCONF" then
        return 1, db_id -- disallow
    else
        return 0, db_id -- allow
    end
end