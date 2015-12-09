local topic = KEYS[1]

local size = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local configuration = {
    size=size,
    ttl=ttl
}

assert(redis.call('SETNX', topic, cmsgpack.pack(configuration)) == 1, 'topic already exists')
