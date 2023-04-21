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