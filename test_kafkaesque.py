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
        lambda (i, v): (str(v), float(i)),
        enumerate(itertools.count()),
    )

    topic = Topic(client, name)
    topic.create(size)

    assert client.exists('{}/offset'.format(name)) is False

    payload, offset = generator.next()
    topic.produce(payload)
    items.append((payload, offset))

    assert client.get('{}/offset'.format(name)) == '1'
    assert client.zrangebyscore('{}/pages'.format(name), '-inf', 'inf', withscores=True) == [('0', 0.0)]
    assert client.zrangebyscore('{}/pages/{}'.format(name, 0), '-inf', 'inf', withscores=True) == items

    for payload, offset in itertools.islice(generator, size):
        topic.produce(payload)
        items.append((payload, offset))

    assert client.get('{}/offset'.format(name)) == str(size + 1)
    assert client.zrangebyscore('{}/pages'.format(name), '-inf', 'inf', withscores=True) == [('0', 0.0), ('1', float(size))]
    assert client.zrangebyscore('{}/pages/{}'.format(name, 0), '-inf', 'inf', withscores=True) == items[:size]
    assert client.zrangebyscore('{}/pages/{}'.format(name, 1), '-inf', 'inf', withscores=True) == items[size:]


def test_consume(client):
    name = 'example'
    size = 10

    items = []
    generator = itertools.imap(
        lambda (i, v): [i, str(v)],
        enumerate(itertools.count()),
    )

    topic = Topic(client, name)
    topic.create(size)

    for payload, offset in itertools.islice(generator, size + 1):
        topic.produce(payload)
        items.append([payload, offset])

    # Check with batches aligned with page sizes, both full and not.
    offset, batch = list(topic.consume(0, limit=size))
    assert items[:size] == batch

    offset, batch = list(topic.consume(offset, limit=size))
    assert items[size:] == batch

    # Check with batches crossing pages.
    offset, batch = topic.consume(0)
    assert batch == items


def test_ttl(client):
    name = 'example'
    size = 10
    ttl = 60

    topic = Topic(client, name)
    topic.create(size, ttl=ttl)

    for i in xrange(0, size + 1):
        topic.produce(i)

    assert ttl - 1 <= client.ttl('{}/pages/{}'.format(name, 0)) <= ttl
    assert client.ttl('{}/pages/{}'.format(name, 1)) == -1
