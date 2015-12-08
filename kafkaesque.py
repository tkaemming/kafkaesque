import itertools
import logging
import operator

import click
from redis.client import StrictRedis


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
)


class Producer(object):
    def __init__(self, client, topic, size=1024):
        self.client = client
        self.topic = topic
        self.size = size
        self.__push = client.register_script(open('scripts/push.lua').read())

    def produce(self, record):
        return self.__push((self.topic,), (self.size, record,))


class Consumer(object):
    def __init__(self, client, topic):
        self.client = client
        self.topic = topic
        self.__pull = client.register_script(open('scripts/pull.lua').read())

    def consume(self):
        raise NotImplementedError


@click.group()
def cli():
    pass


@cli.command(help="Write messages to a topic.")
@click.argument('topic')
@click.argument('input', type=click.File('rb'), default='-')
def produce(topic, input):
    producer = Producer(StrictRedis(), topic)
    for line in itertools.imap(operator.methodcaller('strip'), input):
        producer.produce(line)


@cli.command(help="Read messages from a topic.")
@click.argument('topic')
def consume(topic):
    raise NotImplementedError


if __name__ == '__main__':
    cli()
