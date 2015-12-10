local topic = KEYS[1]

local items = ARGV

local function load_configuration()
    local values = redis.call('HMGET', topic, 'size', 'ttl')
    return {
        size=tonumber(values[1]),
        ttl=tonumber(values[2])
    }
end

local configuration = load_configuration()

local number = 0
local offset = 0
local length = 0  -- length of current page

local function start_page()
    redis.log(redis.LOG_DEBUG, string.format('Starting new page %s#%s.', topic, number))
    assert(redis.call('ZADD', topic .. '/pages', offset, number) == 1)
end

local function close_page()
    if configuration['ttl'] ~= nil then
        redis.call('EXPIRE', topic .. '/pages/' .. number, configuration['ttl'])
        redis.log(redis.LOG_DEBUG, string.format('Set %s#%s to expire in %s seconds.', topic, number, configuration['ttl']))
    end
    number = number + 1
    length = 0
end

local function check_page()
    if length >= configuration['size'] then
        close_page()
        start_page()
    end
    return configuration['size'] - length
end

local last = redis.call('ZREVRANGE', topic .. '/pages', '0', '0', 'WITHSCORES')
if #last > 0 then
    number = tonumber(last[1])
    length = redis.call('LLEN', topic .. '/pages/' .. number)
    offset = tonumber(last[2]) + length
else
    start_page()
end

local cursor = 0
while #items > cursor do
    local remaining = math.min(check_page(), #items - cursor)
    local chunk = {}
    for i=1,remaining do
        table.insert(chunk, items[cursor+i])
    end
    redis.call('RPUSH', topic .. '/pages/' .. number, unpack(chunk))
    cursor = cursor + remaining
end

return offset
