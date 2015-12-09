kafkaesque
==========

"Marked by surreal distortion and often a sense of impending danger."

  - https://en.wiktionary.org/wiki/Kafkaesque

Data Structures
---------------

{topic}

    A MessagePack-encoded map storing configuration data for this topic,
    including:

    - size (integer): maximum size of a page in the topic
    - ttl (integer or nil): number of seconds to retain pages after they have
        been closed for writing

{topic}/consumers

    A sorted set containing registered consumers for this topic.

    Items in the sorted set are consumer identifiers, scored by their latest
    committed offset.

{topic}/pages

    A sorted set, that acts as an index of the pages in the topic. This set can
    be used to identify what page contains a particular offset, allowing page
    sizes to be changed over time.

    Items in the sorted set are page numbers, scored by the offset of the first
    item in the page.

{topic}/pages/{number}

    A list containing log records.
