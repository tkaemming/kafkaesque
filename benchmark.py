import gc
import itertools
import time

import click
from redis.client import StrictRedis

from kafkaesque import Topic


@click.command()
@click.option('-c', '--count', type=click.INT, default=None)
@click.option('-p', '--payload-length', type=click.INT, default=1024)
@click.option('-s', '--size', type=click.INT, default=2 ** 16)
@click.option('-t', '--topic', default='benchmark')
def benchmark(count, topic, size, payload_length):
    client = StrictRedis()

    payload = "x" * payload_length

    topic = Topic(client, topic)
    try:
        topic.create(size)
    except Exception:
        pass

    gc.disable()
    start = time.time()

    generator = xrange(1, count + 1) if count else itertools.count(1)

    i = 0
    try:
        for i in generator:
            topic.produce(payload)
            if i % 10000 == 0:
                print 'Produced', i, 'records.'
    except KeyboardInterrupt:
        pass

    end = time.time()

    print 'Produced', i, payload_length / 1024.0, 'KB records in', end - start, 'seconds'
    print i / (end - start), 'messages/sec'


if __name__ == '__main__':
    benchmark()
