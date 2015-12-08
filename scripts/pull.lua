local key = KEYS[1]
local size = tonumber(ARGV[1])
local offset = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

-- Find the offset where the page begins.
local number = redis.call('ZRANGEBYSCORE', key .. '/pages', '(' .. offset - size, offset, 'LIMIT', 0, 1)[1]
if number == nil then
    error("The provided offset (" .. offset .. ") does not exist.")
end

local cursor = offset
local results = {}
while limit > #results do
    local page = redis.call('ZRANGEBYSCORE', key .. '/pages/' .. number, offset, '+inf', 'LIMIT', 0, limit)
    if #page == 0 then
        if not redis.call('EXISTS', key .. '/pages/' .. number) then
            error("Tried to fetch a missing page! This page may have been evicted due to retention policies.")
        end
    end

    for i,v in pairs(page) do
        table.insert(results, v)
    end

    cursor = cursor + #page

    -- If the page isn't full, abort to avoid an infinite loop.
    -- XXX: This assumes that deletions don't occur.
    if size > #page then
        break
    end

    number = number + 1
end

return {cursor, results}
