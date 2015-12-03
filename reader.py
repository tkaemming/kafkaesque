import sys
from redis.client import (
    Script,
    StrictRedis,
)

client = StrictRedis()

pull = client.register_script(open('pull.lua').read())

n = int(sys.argv[1])
m = int(sys.argv[2])
results = pull(('topic',), (1024, n, m))
print len(results)
