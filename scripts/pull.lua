local topic = KEYS[1]

local offset = tonumber(ARGV[1])

local limit = tonumber(ARGV[2])

-- Find page where the offset is located.
local page = redis.call('ZREVRANGEBYSCORE', topic .. '/pages', offset, '0', 'LIMIT', 0, 1, 'WITHSCORES')
assert(#page > 0, 'invalid offset')
local number = tonumber(page[1])
local start = tonumber(page[2])

local cursor = offset
local results = {}
while limit > #results do
    local fetch = limit - #results
    local items = redis.call('LRANGE', topic .. '/pages/' .. number, cursor - start, cursor - start + fetch - 1)
    if #items == 0 then
        break
    end

    for i=1,#items do
        table.insert(results, {cursor, items[i]})
        cursor = cursor + 1
    end

    if fetch >= #items then
        number = number + 1
        start = cursor
    end
end

return {cursor, results}
