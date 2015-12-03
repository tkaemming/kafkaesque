local key = KEYS[1]
local size = tonumber(ARGV[1])
local offset = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

-- Find the offset where the page begins.
local number = redis.call('ZRANGEBYSCORE', key .. ':i', '(' .. offset - size, offset, 'LIMIT', 0, 1)[1]
if number == nil then
    error("invalid offset")
end

local results = {}
while limit > #results do
    local chunk = redis.call('ZRANGEBYSCORE', key .. ':' .. number, offset, '+inf', 'LIMIT', 0, limit)
    for i,v in pairs(chunk) do
        table.insert(results, v)
    end

    -- If the page isn't full, abort to avoid an infinite loop.
    if size > #chunk then
        break
    end

    number = number + 1
end

return results
