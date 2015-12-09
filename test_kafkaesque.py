import itertools

import pytest
from redis.client import StrictRedis

from kafkaesque import Topic


@pytest.yield_fixture
def client():
    client = StrictRedis()
    try:
        yield client
    finally:
        client.flushdb()


def test_create(client):
    topic = Topic(client, 'topic')
    topic.create()

    with pytest.raises(Exception):
        topic.create()


def test_produce(client):
    name = 'example'
    size = 10

    items = []
    generator = itertools.imap(
        lambda i: (i, str(i)),
        itertools.count(),
    )

    topic = Topic(client, name)
    topic.create(size)

    payload, offset = generator.next()
    topic.produce((payload,))
    items.append((payload, offset))

    assert client.zrangebyscore('{}/pages'.format(name), '-inf', 'inf', withscores=True) == [('0', 0.0)]
    assert list(enumerate(client.lrange('{}/pages/{}'.format(name, 0), 0, size))) == items

    for payload, offset in itertools.islice(generator, size):
        topic.produce((payload,))
        items.append((payload, offset))

    assert client.zrangebyscore('{}/pages'.format(name), '-inf', 'inf', withscores=True) == [('0', 0.0), ('1', float(size))]
    assert list(enumerate(client.lrange('{}/pages/{}'.format(name, 0), 0, size))) == items[:size]
    assert list(enumerate(client.lrange('{}/pages/{}'.format(name, 1), 0, size), size)) == items[size:]


def test_consume_page_sizes(client):
    name = 'example'
    size = 10

    items = []
    generator = itertools.imap(
        lambda i: [i, str(i)],
        itertools.count(),
    )

    topic = Topic(client, name)
    topic.create(size)

    for offset, payload in itertools.islice(generator, size + 1):
        topic.produce((payload,))
        items.append([offset, payload])

    offset, batch = list(topic.consume(0, limit=size))
    assert items[:size] == batch

    offset, batch = list(topic.consume(offset, limit=size))
    assert items[size:] == batch


def test_consume_across_pages(client):
    name = 'example'
    size = 10

    items = []
    generator = itertools.imap(
        lambda i: [i, str(i)],
        itertools.count(),
    )

    topic = Topic(client, name)
    topic.create(size)

    for offset, payload in itertools.islice(generator, size + 1):
        topic.produce((payload,))
        items.append([offset, payload])

    # Check with batches crossing pages.
    offset, batch = topic.consume(5)
    assert batch == items[5:]


def test_ttl(client):
    name = 'example'
    size = 10
    ttl = 60

    topic = Topic(client, name)
    topic.create(size, ttl=ttl)

    for i in xrange(0, size + 1):
        topic.produce((i,))

    assert ttl - 1 <= client.ttl('{}/pages/{}'.format(name, 0)) <= ttl
    assert client.ttl('{}/pages/{}'.format(name, 1)) == -1
