class Topic(object):
    def __init__(self, client, topic):
        self.client = client
        self.topic = topic

        self.__create = client.register_script(open('scripts/create.lua').read())
        self.__push = client.register_script(open('scripts/push.lua').read())
        self.__pull = client.register_script(open('scripts/pull.lua').read())

    def create(self, size=1024, ttl=None):
        return self.__create((self.topic,), (size, ttl))

    def consume(self, offset, limit=1024):
        return self.__pull((self.topic,), (offset, limit))

    def produce(self, batch):
        return self.__push((self.topic,), batch)
