local topic = KEYS[1]

local offset = tonumber(ARGV[1])

local limit = tonumber(ARGV[2])

-- Ensure that a valid offset is being requested.
local endpoint = tonumber(redis.call('GET', topic .. '/offset')) or 0
assert(endpoint >= offset)

-- Find page where the offset is located.
local number = redis.call('ZREVRANGEBYSCORE', topic .. '/pages', offset, '0', 'LIMIT', 0, 1)[1]
assert(number ~= nil)

local cursor = offset
local results = {}
while limit > #results do
    local fetch = limit - #results
    local items = redis.call('ZRANGEBYSCORE', topic .. '/pages/' .. number, cursor, '+inf', 'LIMIT', 0, fetch, 'WITHSCORES')

    assert(#items % 2 == 0)
    -- TODO: Is there a more efficient way to do this than iteration?
    for i=1,#items/2 do
        table.insert(
            results, {
                tonumber(items[i*2]),  -- score (offset)
                items[i*2-1]           -- payload
            }
        )
    end

    -- There are two possibilities why the page is smaller than the fetch size:
    -- 1.) we need to continue the read on the next page (in this case we won't
    -- have hit the endpoint), or
    -- 2.) this is the end of the topic and there is nothing left to read (in
    -- this case we will have reached the endpoint)
    cursor = cursor + (#items / 2)
    if fetch > #items and cursor == endpoint then
        break
    end

    number = number + 1
end

return {cursor, results}
