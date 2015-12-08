local topic = KEYS[1]

-- TODO: Support variadic arguments for publishing multiple items.
local size = tonumber(ARGV[1])

local configuration = {
    size=size
}

assert(redis.call('SETNX', topic, cmsgpack.pack(configuration)) == 1, 'topic already exists')
