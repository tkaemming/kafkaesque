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


client = StrictRedis()
push = client.register_script(open('scripts/push.lua').read())
pull = client.register_script(open('scripts/pull.lua').read())


@click.group()
def cli():
    pass


@cli.command(help="Write messages to a topic.")
@click.argument('topic')
@click.argument('input', type=click.File('rb'), default='-')
@click.option('--page-size', type=click.INT, default=1024)
@click.option('--ttl', type=click.INT, default=1024 * 60 * 60 * 24 * 7)
def produce(topic, input, ttl, page_size):
    for line in itertools.imap(operator.methodcaller('strip'), input):
        print push((topic,), (page_size, ttl, line))


@cli.command(help="Read messages from a topic.")
@click.argument('topic')
@click.option('--fetch-size', type=click.INT, default=1024)
@click.option('--offset', type=click.INT, default=0)
@click.option('--page-size', type=click.INT, default=1024)
def consume(topic, page_size, offset, fetch_size):
    cursor = offset
    while True:
        logger.info('Fetching offsets %s to %s...', cursor, cursor + fetch_size)
        cursor, results = pull((topic,), (page_size, cursor, fetch_size))

        for offset, item in itertools.imap(operator.methodcaller('split', '\x00', 1), results):
            print offset, item

        # TODO: This also needs to handle an invalid offset if we run into a
        # page that doesn't have any contents.
        if len(results) == 0:
            break


if __name__ == '__main__':
    cli()
