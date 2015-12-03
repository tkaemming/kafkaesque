import itertools
import sys
from redis.client import (
    Script,
    StrictRedis,
)

client = StrictRedis()

push = client.register_script(open('push.lua').read())

n = int(sys.argv[1])
s = itertools.count(int(sys.argv[2]))
for x in xrange(0, n):
    print push(('topic',), (1024, 60 * 60 * 24 * 7, next(s)))
