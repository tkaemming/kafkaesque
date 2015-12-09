kafkaesque
==========

"Marked by surreal distortion and often a sense of impending danger."

  - https://en.wiktionary.org/wiki/Kafkaesque

Overview
--------

Benefits:

- Operationally straightforward.

Drawbacks:

- Reduced durability (dependent on `fsync` frequency, see `Durability` below.)
- Reduced throughput (*really* dependent on `fsync` frequency.)
- Reduced capacity (all data must be memory resident.)
- No direct support for topic partitioning or consumer group balancing.
- Reduced offset space per topic: the maximum offset is (2 ^ 53) - 1, the
  largest unambiguous representation of an integer using double-precision
  floating point numbers. Writes that are attempted after this value is reached
  will fail. (This shouldn't be a practical concern, however -- at 10 million
  writes/second it would take around is around 10,424 days -- 28.5 years -- to
  hit this limit. Considering a more realistic write throughput -- around
  70,000 writes/second, around as fast as I could publish batches of 100 4 KB
  messages to a single server using a default Redis configuration on a Early
  2015 MacBook Pro -- this would take about 4,080 years.)

Durability
----------

Infrequent `fsync` calls can lead to data loss!

A server may return a successful response to a produce request without having
flushed the data to disk. If the primary server fails before `fsync`ing the
AOF, *that data will be lost.*

Compounding this problem, the now noncanonical data may have been received by
consumers downstream, who marked those offsets as committed. For example, let's
say a consumer received this data:

    offset  value  `fsync`ed by server
    ------  -----  -------------------
    0       alpha  yes
    1       beta   no
    2       gamma  no

If the server `fsync`s after receiving the first value (zeroith offset), but
does not `fsync` after receiving the additional records, those subsequent
non-`fsync`ed records records will be lost by the server if it crashes.

After the server is restarted, it will then recycle those offsets, leading to
the server having an understanding of history that appears like this:

    offset  value
    ------  -------
    0       alpha
    1       delta
    2       epsilon
    3       zeta

Any clients who were able to fetch all of the records before the server failed
may end up with a noncanonical understanding of history that looks like this,
since they only retrieved records after the latest offset they had already
received -- in this case, anything after the offset `2`:

    offset  value
    ------  -----
    0       alpha
    1       beta
    2       gamma
    3       zeta

With a large number of consumers, a wide window for `fsync`s to occur, and a
high frequency of records being published, it's possible that *no consumer
could share a consistent view of the log with any other* due to inconsistent
consumption rates. Note that this isn't even eventually consistent insofar that
eventually all consumers would see the same version of history -- it's just
plainly inconsistent.

If this is a scary concept to you -- if you're dealing with finances, or other
data that requires stronger (read: any) consistency semantics -- it's a good
sign that you should probably bite the bullet and use Kafka.

Data Structures
---------------

{topic}

    A MessagePack-encoded map storing configuration data for this topic,
    including:

    - size (integer): maximum size of a page in the topic
    - ttl (integer or nil): number of seconds to retain pages after they have
        been closed for writing

{topic}/pages

    A sorted set, that acts as an index of the pages in the topic. This set can
    be used to identify what page contains a particular offset, allowing page
    sizes to be changed over time.

    Items in the sorted set are page numbers, scored by the offset of the first
    item in the page.

{topic}/pages/{number}

    A list containing log records.
