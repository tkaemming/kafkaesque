kafkaesque
==========

"Marked by surreal distortion and often a sense of impending danger."
(Definition from https://en.wiktionary.org/wiki/Kafkaesque)

Data Structures
---------------

{topic}/partitions/{index}/offset

    A number that represents the next value in a sequence used for addressing
    records in the partition.  (If the value doesn't exist, this is implicitly
    a 0 value.)

    The maximum value of this integer is (2 ^ 53) - 1, the largest unambiguous
    representation of an integer using double-precision floating point numbers.
    Writes that are attempted after this value is reached will fail. (For
    context, this is around 10,424 days -- 28.5 years -- at a sustained write
    rate of 10 million entries a second.)

{topic}/partitions/{index}/pages

    A sorted set, that acts as an index of the pages in the partition. This set
    can be used to identify what page contains a particular offset, allowing
    page sizes to be changed over time.

    Items in the sorted set are page numbers, scored by the offset of the first
    item in the page.

{topic}/partitions/{index}/pages/{number}

    A sorted set, containing log records.

    Items in the sorted set are log records, scored by their offset.
