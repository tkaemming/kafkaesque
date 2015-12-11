import collections
import hashlib
import itertools
import time

from kafkaesque.topic import (
    create,
    read_script,
)


sequence = itertools.count()

def roundrobin(value):
    return next(sequence)


class Topic(object):
    def __init__(self, cluster, name, size, partition=roundrobin):
        self.cluster = cluster
        self.name = name
        self.size = size
        self.partition = partition

        # XXX: This is unreliable and inefficient.
        self.digests = {}
        names = ('create', 'push', 'pull')
        results = {}
        with self.cluster.fanout('all') as client:
            for name in names:
                source = read_script('{}.lua'.format(name))
                results[name] = client.execute_command('SCRIPT', 'LOAD', source)
                self.digests[name] = hashlib.sha1(source).hexdigest()

        for name, result in results.iteritems():
            assert set(result.value.values()) == set((self.digests[name],))

    def create(self, size=1024):
        results = {}
        with self.cluster.fanout() as client:
            for i in xrange(0, self.size):
                key = '{}/partitions/{}'.format(self.name, i)
                results[i] = client.target_key(key).execute_command(
                    'EVALSHA',
                    self.digests['create'],
                    1, key,
                    1, size,
                )

        return results

    def produce(self, batch):
        batches = collections.defaultdict(list)
        for record in batch:
            batches[self.partition(record) % self.size].append(record)

        results = {}
        with self.cluster.fanout() as client:
            for i, batch in batches.iteritems():
                key = '{}/partitions/{}'.format(self.name, i)
                results[i] = client.target_key(key).execute_command(
                    'EVALSHA',
                    self.digests['push'],
                    1, key,
                    len(batch), *batch
                )

        return results


if __name__ == '__main__':
    import gc
    import operator
    import sys
    from rb import Cluster

    n = 8

    cluster = Cluster({i: {'db': i, 'port': 6379 + (i % n)} for i in xrange(0, 16)})

    topic = Topic(cluster, 'topic', 32)
    try:
        topic.create()
    except Exception:
        pass

    def flush():
        result = topic.produce(batch)
        flush.count += len(batch)
        del batch[:]
        return result

    flush.count = 0

    batch = []

    gc.disable()

    start = time.time()

    results = []

    for line in itertools.imap(operator.methodcaller('strip'), sys.stdin):
        batch.append(line)
        if len(batch) == 1024:
            results.append(flush())

    results.append(flush())

    stop = time.time()

    print >> sys.stderr, stop - start, 'seconds'
    print >> sys.stderr, flush.count / (stop - start), 'records/second'
