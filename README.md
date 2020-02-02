<p align="center">
    <a href="#readme">
        <img alt="logo" width="40%" src="apply-cold-water.jpg">
    </a>
</p>

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