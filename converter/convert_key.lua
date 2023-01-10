-- remove keys prefixed with ABC
function converter(key)
    prefix = "1|default|"
    if key:find(prefix, 1, true) == 1 then
        return string.sub(key, string.len(prefix)+1, string.len(key))
    end
    return key
end