<p align="center">
    <a href="#readme">
        <img alt="logo" width="40%" src="apply-cold-water.jpg">
    </a>
</p>

---

**water-healer**, Extension of Kafka Streamz to update consumer offset for every successful sink.

This library is an extension for Streamz library to provide `healing` offset for kafka topics and additional functions to stream pipeline.

Anyone who never heard about streamz library, a great reactive programming library, can read more at https://streamz.readthedocs.io/en/latest/core.html

## Table of contents

  * [problem statement](#problem-statement)
  * [Installing from the PyPI](#Installing-from-the-PyPI)
  * [how-to](#how-to)
  * [examples](#examples)
  * [usage](#usage)
    * [sources](#sources)
    * [healing](#healing)
    * [extension](#extension)

## problem statement

Common Kafka consumer level developer use to `poll`, once it `poll`, consumer already updated offset for the topics whether the streaming successful or not, and we know, `processed-once` behavior of streaming processing. So if you stream a very important data related to finance or something like that, you wanted to reprocess that failed streaming.

Example,

```python
# assume we have a topic `test`.
# 1 partition, [1, 2, 3, 4]
# with first offset: 0
# with last offset: 4

# and we have a consumer group name, `group`.
# `group` offset: 0

# and we have a producer, initiated by debezium / confluent-kafka / kafka-python / etc.
producer = Producer()

from streamz import Stream
source = Stream.from_kafka(
    [
        'test'
    ],
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'group',
        'auto.offset.reset': 'earliest',
    },
)
```

I just want to do simple pipeline, each element in topic just plus by one.

```python

def plus_one(row):

    # a simple error
    if row > 3:
        raise Exception('row is bigger than 3!')
    return row + 1

source.map(json.loads).map(plus).sink(print)
```

During `poll`, 

```python

# start python script,
# `group` offset: 0

# every successful `poll`, it will automatically update the offset.

# first polling,
# `group` offset: 1
# kafka -> 1 -> plus_one(1) -> 2 -> print(2)

# second polling,
# `group` offset: 2
# kafka -> 2 -> plus_one(2) -> 3 -> print(3).

# third polling,
# `group` offset: 3
# kafka -> 3 -> plus_one(3) -> exception, halt

# fourth polling, restart python script,
# `group` offset: 4
# kafka -> 4 -> plus_one(4) -> exception, halt
```

Consumer already updated the offset even though the streaming is failed.

On fourth polling, we should pull back `offset` 2, not 3.

## Installing from the PyPI

```bash
pip install water-healer
```

## how-to

```python
import waterhealer as wh
import json

# subscribe to `testing` topic
source = wh.from_kafka(
    ['testing'],
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'group',
        'auto.offset.reset': 'latest',
    })

source.sink(wh.healing, stream = source)
```

## examples

For more complicated example, simply check notebooks in [example](example).

## usage

### sources

* [waterhealer.from_kafka](#waterhealerfrom_kafka)
* [waterhealer.from_kafka_batched](#waterhealerfrom_kafka_batched)

#### waterhealer.from_kafka

```python
class from_kafka(Source):
    """

    Parameters
    ----------
    topics: list of str
        Labels of Kafka topics to consume from
    consumer_params: dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers, Connection string(s) (host:port) by which to reach
        Kafka;
        group.id, Identity of the consumer. If multiple sources share the same
        group, each message will be passed to only one of them.
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages
    start: bool, (default=False)
        Whether to start polling upon instantiation
    debug: bool, (default=False)
        If True, will print topic, partition and offset for each polled message.
    """
```

`waterhealer.from_kafka` always returned a tuple,

```python
(uuid, value)
```

If you want to use waterhealer, you need to make sure `uuid` from `from_kafka` succesfully transported until healing, or else it cannot update the offset.

**Output from `waterhealer.from_kafka` is different from any sources object from `streamz`, `streamz` only returned `value`, not tuple as `waterhealer.from_kafka`.**

#### waterhealer.from_kafka_batched

```python
def from_kafka_batched(
    topics,
    consumer_params,
    poll_interval = '1s',
    partitions = 5,
    start = False,
    dask = False,
    maxlen = 1000,
    **kwargs,
):
    """

    Parameters
    ----------
    topics: list of str
        Labels of Kafka topics to consume from
    consumer_params: dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers, Connection string(s) (host:port) by which to reach
        Kafka;
        group.id, Identity of the consumer. If multiple sources share the same
        group, each message will be passed to only one of them.
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages
    partitions: int, (default=5)
        size of partitions to poll from kafka, this can be done parallel if `dask` is True.
    start: bool, (default=False)
        Whether to start polling upon instantiation
    dask: bool, (default=False)
        If True, partitions will poll in parallel manners.
    maxlen: int, (default=1000)
        max size of polling. sometime lag offset is really, so we don't to make any crash.
    """

```

Same as `waterhealer.from_kafka`, but we pulled partitions in parallel manners.

Example, [kafka-batch-dask-simple-plus-batch.ipynb](example/kafka-batch-dask-simple-plus-batch.ipynb)

### healing

* [waterhealer.healing](#waterhealerhealing)
* [waterhealer.healing_batch](#waterhealerhealing_batch)

#### waterhealer.healing

```python
def healing(
    row: Tuple,
    stream: Callable = None,
    ignore: bool = False,
    asynchronous: bool = False,
):
    """

    Parameters
    ----------
    row: tuple
        (uuid, value)
    stream: waterhealer object
        waterhealer object to connect with kafka.
    ignore: bool, (default=False)
        if True, ignore any failed update offset.
    asynchronous: bool, (default=False)
        if True, it will update kafka offset async manner.
    """
```

Partial code can be like this,

```python
.map(function).sink(healing, stream = source)
```

This function will returned,

```python
return {
    'data': row[1],
    'success': success,
    'reason': reason,
    'partition': partition,
    'offset': offset,
    'topic': topic,
}
```

Example, [simple-plus-element.ipynb](example/simple-plus-element.ipynb)

#### waterhealer.healing_batch

Instead we do it one-by-one, we can do concurrent updates async manner.

```python
def healing_batch(
    rows: Tuple[Tuple],
    stream: Callable = None,
    ignore: bool = False,
    asynchronous: bool = False,
):
    """

    Parameters
    ----------
    row: tuple of tuple
        ((uuid, value),)
    stream: waterhealer object
        waterhealer object to connect with kafka
    ignore: bool, (default=False)
        if True, ignore any failed update offset.
    asynchronous: bool, (default=False)
        if True, it will update kafka offset async manner.
    """
```

Partial code can be like this,

```python
.map(function).partition(5).sink(healing_batch, stream = source)
```

This function will returned,

```python
return [{
    'data': row[1],
    'success': success,
    'reason': reason,
    'partition': partition,
    'offset': offset,
    'topic': topic,
},
{
    'data': row[1],
    'success': success,
    'reason': reason,
    'partition': partition,
    'offset': offset,
    'topic': topic,
}]
```

A list of results `waterhealer.healing`.

Example, [simple-plus-batch.ipynb](example/simple-plus-batch.ipynb)

### extension

* [partition_time](#partition_time)
* [foreach_map](#foreach_map)
* [foreach_async](#foreach_async)

#### partition_time

```python
class partition_time(Stream):
    """ Partition stream into tuples if waiting time expired.

    Examples
    --------
    >>> source = Stream()
    >>> source.partition_time(3).sink(print)
    >>> for i in range(10):
    ...     source.emit(i)
    (0, 1, 2)
    (3, 4, 5)
    (6, 7, 8)
    """
```

This is different from [partition](https://streamz.readthedocs.io/en/latest/api.html#streamz.partition).

`partition` only proceed to next flow if size is equal to `n`. But for `partition_time`, if waiting time is expired, it will proceed, does not care about the size, and expired time only calculated when an element came in.

#### foreach_map

```python
class foreach_map(Stream):
    """ Apply a function to every element in a tuple in the stream
    Parameters
    ----------
    func: callable
    *args :
        The arguments to pass to the function.
    **kwargs:
        Keyword arguments to pass to func
    Examples
    --------
    >>> source = Stream()
    >>> source.foreach_map(lambda x: 2*x).sink(print)
    >>> for i in range(3):
    ...     source.emit((i, i))
    (0, 0)
    (2, 2)
    (4, 4)
    """
```

It is like `map`, but do `map` for each elements in a batch in sequential manner.

#### foreach_async

```python
class foreach_async(Stream):
    """ 
    Apply a function to every element in a tuple in the stream, in async manner.

    Parameters
    ----------
    func: callable
    *args :
        The arguments to pass to the function.
    **kwargs:
        Keyword arguments to pass to func

    Examples
    --------
    >>> source = Stream()
    >>> source.foreach_async(lambda x: 2*x).sink(print)
    >>> for i in range(3):
    ...     source.emit((i, i))
    (0, 0)
    (2, 2)
    (4, 4)
    """
```

It is like `map`, but do `map` for each elements in a batch in async manner.

Partial code can be like this,

```python
.map(function).partition(5).partition(5).foreach_async(wh.healing_batch, stream = source)
```

Example, [simple-plus-nested-batch.ipynb](example/simple-plus-nested-batch.ipynb)