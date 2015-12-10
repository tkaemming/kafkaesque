import functools
from pkg_resources import resource_string


resource_string = functools.partial(resource_string,  'kafkaesque')


class Topic(object):
    def __init__(self, client, topic):
        self.client = client
        self.topic = topic

        self.__create = client.register_script(resource_string('scripts/create.lua'))
        self.__push = client.register_script(resource_string('scripts/push.lua'))
        self.__pull = client.register_script(resource_string('scripts/pull.lua'))

    def create(self, size=1024, max=None, ttl=None):
        return self.__create((self.topic,), (size, max, ttl))

    def consume(self, offset, limit=1024):
        return self.__pull((self.topic,), (offset, limit))

    def produce(self, batch):
        return self.__push((self.topic,), batch)
