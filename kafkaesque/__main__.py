import itertools
import logging
import operator
import tabulate
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
@click.option('--max', type=click.INT, default=None)
@click.option('--ttl', type=click.INT, default=None)
def create(topic, page_size, ttl, max):
    topic = Topic(StrictRedis(), topic)
    topic.create(page_size, ttl=ttl, max=max)


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
    n = 0
    try:
        while True:
            lower = cursor
            cursor, batch = topic.consume(cursor, fetch_size)
            logger.debug('Retrieved %s items from %s to %s.', len(batch), lower, cursor)
            if not batch:
                if not follow:
                    logger.debug('Retrieved empty batch (end of stream.)')
                    break
                else:
                    logger.debug('Retrieved empty batch.')
                    time.sleep(0.1)

            for n, (offset, item) in enumerate(batch, n + 1):
                print offset, item
    except KeyboardInterrupt:
        pass

    stop = time.time()
    logger.info(
        'Consumed %s records in %s seconds (%s records/second.)',
        n,
        stop - start,
        n / (stop - start),
    )


@cli.command()
@click.argument('topic')
@click.option('-p', '--pages', type=click.INT, default=10)
def details(topic, pages):
    client = StrictRedis()
    with client.pipeline(transaction=False) as pipeline:
        pipeline.hgetall(topic)
        pipeline.zcard('{}/pages'.format(topic))
        pipeline.zrange('{}/pages'.format(topic), pages * -1, -1, withscores=True)
        results = pipeline.execute()

    def header(label):
        return '\n'.join(('-' * 80, label, '-' * 80))

    print header('CONFIGURATION')
    print tabulate.tabulate(results[0].items(), headers=('key', 'value'))

    print ''

    print header('PAGES ({} total)'.format(results[1]))
    print tabulate.tabulate(results[2], headers=('page', 'offset'))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)-8s %(message)s',
    )

    cli()
