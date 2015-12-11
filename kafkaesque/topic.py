import functools
import posixpath
from pkg_resources import resource_string

from redis.client import Script


resource_string = functools.partial(resource_string,  'kafkaesque')


def read_script(name):
    return resource_string(posixpath.join('scripts', name))


def load_script(name):
    script = Script(None, read_script(name))

    def call_script(client, keys, args):
        return script(keys, args, client)

    return call_script


create = load_script('create.lua')
push = load_script('push.lua')
pull = load_script('pull.lua')


class Topic(object):
    def __init__(self, client, topic):
        self.client = client
        self.topic = topic

    def create(self, size=1024, max=None, ttl=None):
        return create(self.client, (self.topic,), (size, max, ttl))

    def consume(self, offset, limit=1024):
        return pull(self.client, (self.topic,), (offset, limit))

    def produce(self, batch):
        return push(self.client, (self.topic,), batch)
