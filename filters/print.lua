--- function name must be `filter`
---
--- arguments:
--- @id number: the sequence of the cmd
--- @is_base boolean: whether the command is decoded from dump.rdb file
--- @group string: the group of cmd
--- @cmd_name string: cmd name
--- @keys table: keys of the command
--- @slots table: slots of the command
--- @db_id: database id
--- @timestamp_ms number: timestamp in milliseconds, 0 if not available

--- return:
--- @code number:
--- * 0: allow
--- * 1: disallow
--- * 2: error occurred
--- @db_id number: redirection database id

function filter(id, is_base, group, cmd_name, keys, slots, db_id, timestamp_ms)
    print(string.format("lua filter. id=[%d], is_base=[%s], db_id=[%d], group=[%s], cmd_name=[%s], keys=[%s], slots=[%s], timestamp_ms=[%d]",
            id, tostring(is_base), db_id, group, cmd_name, table.concat(keys, ", "), table.concat(slots, ", "), timestamp_ms))
    return 0, db_id
end