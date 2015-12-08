local key = KEYS[1]
local size = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local item = ARGV[3]

-- TODO: Figure out the maximum representable integer value in Lua. (2^52?)
-- NOTE: The sequence value is a 64-bit signed integer, with a maximum value of 2^63 − 1.
-- NOTE: The score value is a double 64-bit floating point number, with a maximum value of 2 ^ 53.
local sequence = tonumber(redis.call('INCR', key .. '/offset')) - 1

local number = redis.call('GET', key .. '/pages/current') or 0
local page = key .. '/pages/' .. number

redis.call('ZADD', page, sequence, sequence .. '\0' .. item)

-- If our write filled the page, close it and roll over to the next page.
local s = redis.call('ZCARD', page)
if s == size then
    redis.call('SET', key .. '/pages/current', number + 1)
    redis.call('EXPIRE', page, ttl)
    -- TODO: If there is a maximum page limit, then truncate the previous page.
elseif s == 1 then
    redis.call('ZADD', key .. '/pages', sequence, number)
end

return sequence
