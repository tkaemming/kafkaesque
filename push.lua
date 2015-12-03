local key = KEYS[1]
local size = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local item = ARGV[3]

-- TODO: Figure out the maximum representable integer value in Lua. (2^52?)
-- NOTE: The sequence value is a 64-bit signed integer, with a maximum value of 2^63 − 1.
-- NOTE: The score value is a double 64-bit floating point number, with a maximum value of 2 ^ 53.
local sequence = tonumber(redis.call('INCR', key .. ':s')) - 1

local index = tonumber(redis.call('GET', key .. ':i') or 0, 16)
local page = string.format('%s:%x', key, index)

redis.call('ZADD', page, sequence, item)

-- If our write filled the page, close it and roll over to the next page.
if redis.call('ZCARD', page) == size then
    redis.call('SET', key .. ':i', string.format('%x', index + 1))
    redis.call('EXPIRE', page, ttl)
    -- TODO: If there is a maximum page limit, then truncate the previous page.
end

return sequence
