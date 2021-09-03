--- dbid: 0 -> 1
--- dbid: 1 -> 0
--- dbid: others -> drop
function filter(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    if db_id == 0 then
        -- print("db_id is 0, redirect to 1")
        return 0, 1
    elseif db_id == 1 then
        -- print("db_id is 1, redirect to 0")
        return 0, 0
    else
        return 1, db_id
    end
end