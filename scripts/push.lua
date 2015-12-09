local topic = KEYS[1]

-- TODO: Support variadic arguments for publishing multiple items.
local item = ARGV[1]

local configuration = redis.call('GET', topic)
assert(configuration, 'topic does not exist')
configuration = cmsgpack.unpack(configuration)

local number = 0
local offset = 0

local function start_page ()
    redis.log(redis.LOG_DEBUG, string.format('Starting new page %s#%s.', topic, number))
    assert(redis.call('ZADD', topic .. '/pages', offset, number) == 1)
end

local function close_page ()
    if configuration['ttl'] ~= nil then
        redis.call('EXPIRE', topic .. '/pages/' .. number, configuration['ttl'])
        redis.log(redis.LOG_DEBUG, string.format('Set %s#%s to expire in %s seconds.', topic, number, configuration['ttl']))
    end
    number = number + 1
end

local last = redis.call('ZREVRANGE', topic .. '/pages', '0', '0', 'WITHSCORES')
if #last > 0 then
    number = tonumber(last[1])
    local length = redis.call('LLEN', topic .. '/pages/' .. number)
    offset = tonumber(last[2]) + length
    if length >= configuration['size'] then
        close_page()
        start_page()
    end
else
    start_page()
end

local page = topic .. '/pages/' .. number

redis.call('RPUSH', page, item)

return offset
