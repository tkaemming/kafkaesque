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

local function fetch_items()
    local fetch = limit - #results
    local items = redis.call('LRANGE', topic .. '/pages/' .. number, cursor - start, cursor - start + fetch - 1)
    if #items == 0 then
        -- This could be because the page doesn't exist -- if that is the case, check the next page.
        -- TODO: In this case, it might just be more appropriate to error rather than hide missing pages?
        local next = tonumber(redis.call('ZSCORE', topic .. '/pages', (number + 1)))
        if next ~= nil then
            number = number + 1
            cursor = next
            start = cursor
            return true
        else
            return false
        end
    end

    for i=1,#items do
        table.insert(results, {cursor, items[i]})
        cursor = cursor + 1
    end

    if fetch >= #items then
        number = number + 1
        start = cursor
    end

    return true
end

while limit > #results do
    if fetch_items() == false then
        break
    end
end

return {cursor, results}
