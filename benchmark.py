import gc
import itertools
import time

import click
from redis.client import StrictRedis

from kafkaesque.topic import Topic


@click.command()
@click.option('--batch-size', type=click.INT, default=1)
@click.option('--page-size', type=click.INT, default=2 ** 16)
@click.option('--payload-length', type=click.INT, default=1024)
@click.option('-c', '--count', type=click.INT, default=None)
@click.option('-t', '--topic', default='benchmark')
def benchmark(count, topic, batch_size, page_size, payload_length):
    client = StrictRedis()

    batch = ["x" * payload_length] * batch_size

    topic = Topic(client, topic)
    try:
        topic.create(page_size)
    except Exception:
        pass

    gc.disable()
    start = time.time()

    generator = xrange(1, count + 1) if count else itertools.count(1)

    i = 0
    try:
        for i in generator:
            topic.produce(batch)
            if i % 100 == 0:
                print 'Produced', i, 'batches.'
    except KeyboardInterrupt:
        pass

    end = time.time()

    print 'Produced', i * batch_size, payload_length / 1024.0, 'KB records in', end - start, 'seconds'
    print (i * batch_size) / (end - start), 'messages/sec'


if __name__ == '__main__':
    benchmark()
