import itertools
import operator

import click
from redis.client import StrictRedis


class Topic(object):
    def __init__(self, client, topic):
        self.client = client
        self.topic = topic

        self.__create = client.register_script(open('scripts/create.lua').read())
        self.__push = client.register_script(open('scripts/push.lua').read())
        self.__pull = client.register_script(open('scripts/pull.lua').read())

    def create(self, size=1024):
        return self.__create((self.topic,), (size,))

    def consume(self, offset, limit=1024):
        return self.__pull((self.topic,), (offset, limit))

    def produce(self, record):
        return self.__push((self.topic,), (record,))


@click.group()
def cli():
    pass


@cli.command(help="Write messages to a topic.")
@click.argument('topic')
@click.argument('input', type=click.File('rb'), default='-')
def produce(topic, input):
    producer = Topic(StrictRedis(), topic)
    for line in itertools.imap(operator.methodcaller('strip'), input):
        producer.produce(line)


@cli.command(help="Read messages from a topic.")
@click.argument('topic')
def consume(topic):
    consumer = Topic(StrictRedis(), topic)
    cursor = 0
    while True:
        cursor, batch = consumer.batch(cursor)
        if not batch:
            return

        for offset, item in batch:
            print offset, item


if __name__ == '__main__':
    cli()
