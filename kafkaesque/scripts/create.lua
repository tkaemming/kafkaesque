local topic = KEYS[1]

assert(redis.call('EXISTS', topic) == 0)

local configuration = {
    size=tonumber(ARGV[1]),
    max=tonumber(ARGV[2]),
    ttl=tonumber(ARGV[3]),
}

-- TODO: There has to be a more elegant way to do this.
local options = {}
for k, v in pairs(configuration) do
    if v ~= nil then
        table.insert(options, k)
        table.insert(options, v)
    end
end

redis.call('HMSET', topic, unpack(options))
