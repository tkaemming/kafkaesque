local topic = KEYS[1]

-- TODO: Support variadic arguments for publishing multiple items.
local item = ARGV[1]

local configuration = redis.call('GET', topic)
assert(configuration, 'topic does not exist')
configuration = cmsgpack.unpack(configuration)

local offset = tonumber(redis.call('INCR', topic .. '/offset')) - 1

-- TODO: This could be made more efficient by storing the active page and it's
-- beginning offset instead of searching for it every time.
local number = 0
if offset ~= 0 then
    local page = redis.call('ZREVRANGE', topic .. '/pages', '0', '0', 'WITHSCORES')
    number = tonumber(page[1])
    if offset - tonumber(page[2]) >= configuration['size'] then
        if configuration['ttl'] ~= nil then
            redis.call('EXPIRE', topic .. '/pages/' .. number, configuration['ttl'])
            redis.log(redis.LOG_DEBUG, string.format('Set %s#%s to expire in %s seconds.', topic, number, configuration['ttl']))
        end
        number = number + 1
    end
end

-- TODO: We should be able to tell when we are creating a new page without
-- having to query for it.
if redis.call('ZSCORE', topic .. '/pages', number) == false then
    redis.log(redis.LOG_DEBUG, string.format('Starting new page %s#%s.', topic, number))
    redis.call('ZADD', topic .. '/pages', offset, number)
end

local page = topic .. '/pages/' .. number

redis.call('ZADD', page, offset, cmsgpack.pack({offset, item}))

return offset
