import itertools
import sys
from redis.client import (
    Script,
    StrictRedis,
)

client = StrictRedis()

pull = client.register_script(open('pull.lua').read())

counter = itertools.count(0)

n = int(sys.argv[1])
m = int(sys.argv[2])
while True:
    n, results = pull(('topic',), (1024, n, m))
    for i in itertools.imap(int, results):
        j = next(counter)
        assert j == i, '{} should be equal to {}'.format(i, j)
        print i

    if len(results) == 0:
        break
