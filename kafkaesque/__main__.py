import itertools
import logging
import operator
import time

import click
from redis.client import StrictRedis

from kafkaesque.topic import Topic


logger = logging.getLogger(__name__)


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

    start = time.time()

    def flush():
        print topic.produce(batch), batch
        flush.count += len(batch)
        del batch[:]

    flush.count = 0

    try:
        for line in itertools.imap(operator.methodcaller('strip'), input):
            batch.append(line)
            if len(batch) == batch_size:
                flush()

        flush()
    except KeyboardInterrupt:
        pass

    stop = time.time()
    logger.info(
        'Produced %s records in %s seconds (%s records/second.)',
        flush.count,
        stop - start,
        flush.count / (stop - start),
    )



@cli.command(help="Read messages from a topic.")
@click.argument('topic')
@click.option('--fetch-size', type=click.INT, default=1024)
@click.option('-f', '--follow', is_flag=True)
def consume(topic, follow, fetch_size):
    topic = Topic(StrictRedis(), topic)

    start = time.time()
    cursor = 0
    try:
        while True:
            cursor, batch = topic.consume(cursor, fetch_size)
            logger.debug('Retrieved %s items from %s to %s.', len(batch), cursor, cursor + len(batch))
            if not batch:
                if not follow:
                    logger.debug('Retrieved empty batch (end of stream.)')
                    break
                else:
                    logger.debug('Retrieved empty batch.')
                    time.sleep(0.1)

            for offset, item in batch:
                print offset, item
    except KeyboardInterrupt:
        pass

    stop = time.time()
    logger.info(
        'Consumed %s records in %s seconds (%s records/second.)',
        cursor,
        stop - start,
        cursor / (stop - start),
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)-8s %(message)s',
    )

    cli()
