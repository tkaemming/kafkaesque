import itertools
import logging
import operator
import time

import click
from redis.client import StrictRedis


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
)


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

    def offset(self, consumer):
        score = self.client.zscore('{}/consumers'.format(self.topic), consumer)
        return int(score) if score is not None else None

    def commit(self, consumer, offset):
        return self.client.zadd('{}/consumers'.format(self.topic), offset, consumer)


@click.group()
def cli():
    pass


@cli.command(help="Create a topic.")
@click.argument('topic')
@click.option('--page-size', type=click.INT, default=2 ** 16)
@click.option('--ttl', type=click.INT, default=None)
def create(topic, page_size, ttl):
    topic = Topic(StrictRedis(), topic)
    topic.create(page_size, ttl)


@cli.command(help="Write messages to a topic.")
@click.argument('topic')
@click.argument('input', type=click.File('rb'), default='-')
@click.option('--batch-size', type=click.INT, default=1)
def produce(topic, input, batch_size):
    topic = Topic(StrictRedis(), topic)
    batch = []

    def flush():
        print topic.produce(batch), batch
        del batch[:]

    for line in itertools.imap(operator.methodcaller('strip'), input):
        batch.append(line)
        if len(batch) == batch_size:
            flush()

    flush()


@cli.command(help="Read messages from a topic.")
@click.argument('topic')
@click.option('--consumer-id', default=None)
@click.option('-f', '--follow', is_flag=True)
def consume(topic, consumer_id, follow):
    topic = Topic(StrictRedis(), topic)

    if consumer_id:
        offset = topic.offset(consumer_id)
        if offset is not None:
            cursor = offset
            logger.debug('Using existing offset #%s for %r.', cursor, consumer_id)
        else:
            cursor = 0
            logger.debug('No existing cursor for %r, starting from the earliest offset.', consumer_id)
    else:
        cursor = 0
        logger.debug('Using ephemeral consumer registration, starting from #%s.', cursor)

    while True:
        cursor, batch = topic.consume(cursor)
        logger.debug('Retrieved %s items from %s to %s.', len(batch), cursor, cursor + len(batch))
        if not batch:
            if not follow:
                logger.debug('Retrieved empty batch (end of stream.)')
                return
            else:
                logger.debug('Retrieved empty batch.')
                time.sleep(0.1)

        for offset, item in batch:
            print offset, item

        if consumer_id:
            topic.commit(consumer_id, cursor)


if __name__ == '__main__':
    cli()
