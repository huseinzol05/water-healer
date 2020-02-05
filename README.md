<p align="center">
    <a href="#readme">
        <img alt="logo" width="40%" src="apply-cold-water.jpg">
    </a>
</p>

---

**water-healer**, Extension of Kafka Streamz to update consumer offset for every successful sink.

This library is an extension for Streamz library to provide `healing` offset for kafka topics.

Anyone who never heard about streamz library, a great reactive programming library, can read more at https://streamz.readthedocs.io/en/latest/core.html

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
    },
    healer = True)

source.map(wh.healing, stream = source, callback = print)
```

## examples

For more complicated example, simply check notebooks in [example](example).

## usage

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
    maxlen_memory: int, (100000)
        max size of memory (dict). Oldest automatically delete.
    maxage_memory: int, (1800)
        max age for each values in memory (dict). This will auto delete if the value stay too long in the memory.
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages
    start: bool, (False)
        Whether to start polling upon instantiation

    """
```

`waterhealer.from_kafka` always returned a tuple,

```python
(uuid, value)
```

If you want to use waterhealer, you need to make sure `uuid` from `from_kafka` succesfully transported until sinking, or else it cannot update the offset.

**Output from `waterhealer.from_kafka` is different from any sources object from `streamz`, `streamz` only returned `value`, not tuple as `waterhealer.from_kafka`.**

#### waterhealer.healing

```python
def healing(
    row,
    stream = None,
    callback = None,
    ignore = False,
    silent = False,
    **kwargs,
):
    """

    Parameters
    ----------
    row: tuple
        (uuid, value)
    stream: waterhealer object
        waterhealer object to connect with kafka
    callback: function
        callback function after successful update
    ignore: bool, (default=False)
        if True, if uuid not in memory, it will not stop. 
        This is useful when you do batch processing, you might delete some rows after did some unique operations.
    silent: bool, (default=False)
        if True, will not print any log in this function.
    **kwargs:
        Keyword arguments to pass to callback.

    """
```
