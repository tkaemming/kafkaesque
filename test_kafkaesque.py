import itertools

import pytest
from redis.client import StrictRedis

from kafkaesque import (
    Consumer,
    Producer,
)


@pytest.yield_fixture
def client():
    client = StrictRedis()
    try:
        yield client
    finally:
        client.flushdb()


def test_produce(client):
    topic = 'example'
    size = 10

    items = []
    generator = itertools.imap(
        lambda (i, v): (str(v), float(i)),
        enumerate(itertools.count()),
    )

    producer = Producer(client, topic, size=size)
    assert client.exists('{}/offset'.format(topic)) is False

    payload, offset = generator.next()
    producer.produce(payload)
    items.append((payload, offset))

    assert client.get('{}/offset'.format(topic)) == '1'
    assert client.zrangebyscore('{}/pages'.format(topic), '-inf', 'inf', withscores=True) == [('0', 0.0)]
    assert client.zrangebyscore('{}/pages/{}'.format(topic, 0), '-inf', 'inf', withscores=True) == items

    for payload, offset in itertools.islice(generator, size):
        producer.produce(payload)
        items.append((payload, offset))

    assert client.get('{}/offset'.format(topic)) == str(size + 1)
    assert client.zrangebyscore('{}/pages'.format(topic), '-inf', 'inf', withscores=True) == [('0', 0.0), ('1', float(size))]
    assert client.zrangebyscore('{}/pages/{}'.format(topic, 0), '-inf', 'inf', withscores=True) == items[:size]
    assert client.zrangebyscore('{}/pages/{}'.format(topic, 1), '-inf', 'inf', withscores=True) == items[size:]


def test_consume(client):
    topic = 'example'
    size = 10

    items = []
    generator = itertools.imap(
        lambda (i, v): [i, str(v)],
        enumerate(itertools.count()),
    )

    producer = Producer(client, topic, size=size)
    for payload, offset in itertools.islice(generator, size + 1):
        producer.produce(payload)
        items.append([payload, offset])

    # Check with batches aligned with page sizes, both full and not.
    consumer = Consumer(client, topic)
    offset, batch = list(consumer.batch(0, limit=size))
    assert items[:size] == batch

    offset, batch = list(consumer.batch(offset, limit=size))
    assert items[size:] == batch

    # Check with batches crossing pages.
    consumer = Consumer(client, topic)
    offset, batch = consumer.batch(0)
    assert batch == items
