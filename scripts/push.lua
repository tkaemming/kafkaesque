local topic = KEYS[1]

-- TODO: Figure out configuration for topic settings (page size, TTL, etc.)
local size = tonumber(ARGV[1])

-- TODO: Support variadic arguments for publishing multiple items.
local item = ARGV[2]

local offset = tonumber(redis.call('INCR', topic .. '/offset')) - 1

-- TODO: This could be made more efficient by storing the active page and it's
-- beginning offset instead of searching for it every time.
local number = 0
if offset ~= 0 then
    local page = redis.call('ZREVRANGE', topic .. '/pages', '0', '0', 'WITHSCORES')
    if offset - tonumber(page[2]) >= size then
        number = number + 1
    end
end

-- TODO: We should be able to tell when we are creating a new page without
-- having to query for it.
if redis.call('ZSCORE', topic .. '/pages', number) == false then
    redis.log(redis.LOG_DEBUG, string.format('Starting new page (%s) for %q.', number, topic))
    redis.call('ZADD', topic .. '/pages', offset, number)
end

local page = topic .. '/pages/' .. number

redis.call('ZADD', page, offset, item)

return offset
