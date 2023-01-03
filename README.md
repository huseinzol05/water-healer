<p align="center">
    <a href="#readme">
        <img alt="logo" width="40%" src="apply-cold-water.jpg">
    </a>
</p>

---

**water-healer**, Forked of Streamz to deliver processed guarantees at least once for Kafka consumers.

This library also added streaming metrics, auto-shutdown, auto-graceful, unique emit ID, checkpointing, remote logging and additional functions to stream pipeline. Only compatible with `streamz==0.5.2`.

<p align="left">
<a href="https://github.com/hhatto/autopep8"><img alt="Code style: autopep8" src="https://img.shields.io/badge/code%20style-autopep8-000000.svg"></a>
</p>

## Problem statement

### update offset during sinking

Common Kafka consumer level developer use to `poll`, once it `poll`, consumer already updated offset for the topics whether the streaming successful or not, and we know, `processed-once` behavior of streaming processing. So if you stream a very important data related to finance or something like that, you wanted to reprocess that failed streaming.

Example,

```python
# assume we have a topic `test`.
# 1 partition, [1, 2, 3, 4, 5, 6]
# with first offset: 0
# with last offset: 3

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

    # a simple error to mimic programmer error
    if row > 3:
        raise Exception('row is bigger than 3!')
    return row + 1

source.map(json.loads).map(plus).sink(print)
```

During `poll`, 

```python

# remember our queue,    [  1,    2,    3,   4,   5,   6  ]
# offsets             -1    0     1     2    3    4    5
# when pulled at -1, we got element 1, pulled at 0 got element 2, and so on.

# start python script,
# `group` offset: -1

# every successful `poll`, it will automatically update the offset.

# first polling,
# `group` offset: 0
# kafka -> 1 -> plus_one(1) -> 2 -> print(2)

# second polling,
# `group` offset: 1
# kafka -> 2 -> plus_one(2) -> 3 -> print(3).

# third polling,
# `group` offset: 2
# kafka -> 3 -> plus_one(3) -> exception, halt

# fourth polling, restart python script,
# `group` offset: 3
# kafka -> 4 -> plus_one(4) -> exception, halt
```

Consumer already updated the offset even though the streaming is failed.

On fourth polling, we should pull back `offset` 2, not proceed 

### update offset for distributed processing

In a real world, some of real time functions might take some time, maybe caused some long polling like merging data from database or waiting some events.

Let say we have a single stream and 3 workers can execute a function in parallel manner, the function simply like,

```python
def foo(x):
    # wait event, sometime x can caused very long waiting, sometime not
    return
```

Our stream with 3 offsets, FIFO,

```python
offsets = [1, 2, 3]
```

So `1` will go to first worker, `2` will go to second worker, and `3` will go to third worker. The execution time as below,

```text
first worker = return error, 5 seconds
second worker = return result, 2 seconds
third worker = return result, 1 second
```

The queue be like, FIFO,

```python
queue = [3, 2, 1]
```

Offset `3` comes first, but the problem here, offset `1` got error and we do not want to simply update offset `3` because it came first, we want to reprocess from offset `1`.

So, water-healer will wait offset `1` first, after that will execute offset `2` and `3`.

Or maybe this google slide can help you to understand more about water healer, [link to slide](https://docs.google.com/presentation/d/1ixiFIfcnVajMK8L6lY2hG_X5vipQ3gRk_ZsFjuXCWEY/edit?usp=sharing).

Or this Medium article about [Processing guarantees in Kafka](https://medium.com/@andy.bryant/processing-guarantees-in-kafka-12dd2e30be0e).

## Installing from the PyPI

```bash
pip install water-healer
```

## How-to

### update offset

To understand technically of update offset, simply can read at [Problem statement](#Problem-statement)

```python
import waterhealer as wh

# subscribe to `testing` topic
source = wh.from_kafka(
    ['testing'],
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'group',
        'auto.offset.reset': 'latest',
    })

source.healing()
```

We need to use `waterhealer.from_kafka` for streaming source, and use `waterhealer.healing` for sinking or mapping process.

Simply can read more about [waterhealer.kafka.from_kafka](#waterhealerkafkafrom_kafka) and [waterhealer.healing.healing](#waterhealerhealinghealing)

### Provide at-most-once

To ensure at-most-once processed for Kafka consumers, we have to introduce distributed messaging among consumers about failed and successed events so new consumers that joined in the same consumer group will not pulled the same successful events but not yet committed in Kafka offsets.

We can use any persistent database to validate the offsets, we provided `waterhealer.db.redis.Database` for faster and easier interface to use redis in water-healer. To provide at-most-once,

```python
import waterhealer as wh
from redis import StrictRedis
from waterhealer.db.redis import Database

consumer = 'consumer'
redis = StrictRedis()
db = Database(redis = redis, consumer = consumer, key = 'water-healer-scatter')


source = wh.from_kafka(
    ['testing'],
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'group',
        'auto.offset.reset': 'latest',
    },
    db=db)

source.healing()
```

Simply pass `db` parameter for `wh.from_kafka`, default is `None`, will use `waterhealer.db.expiringdict.Database`.

For example,

- `waterhealer.kafka.from_kafka`, [simple-plus-element-kafka-redis.ipynb](example/simple-plus-element-kafka-redis.ipynb).
- `waterhealer.kafka.from_kafka_batched_scatter`, [simple-plus-element-kafka-scatter-redis.ipynb](example/simple-plus-element-kafka-scatter-redis.ipynb).

### streaming metrics

Now we have a simple streaming,

```python
from time import sleep, time
from streamz import Stream

def inc(x):
    sleep(1)
    return x + 1

source = Stream()
source.map(inc).sink(print)
```

![alt text](image/simple-metrics.png)

I want to know for each map / sink functions,

1. input rate
2. output rate
3. time execution
4. input data size
5. output data size

To enable this metrics, simply,

```python
import waterhealer as wh
source = wh.metrics(source = source)
```

And we can start simply start `emit` or use any async sources like `from_kafka`. By default, it will create prometheus exporter at port 8000, [localhost:8000](http://localhost:8000)

The definition of prometheus metrics,

```python
f = function_name
Counter(f'total_before_{f}', f'total input {f}')
Counter(f'total_after_{f}', f'total output {f}')
total_error = Counter(f'total_error_{f}', f'total total {f}')
total_time = Summary(
    f'total_time_{f}', f'summary of time execution {f}'
)
total_time_histogram = Histogram(
    f'total_time_histogram_{f}',
    f'histogram of time execution {f}',
)
data_size_before = Summary(
    f'data_size_before_{f}',
    f'summary of input data size {f} (KB)',
)
data_size_after = Summary(
    f'data_size_after_{f}',
    f'summary of output data size {f} (KB)',
)
data_size_before_histogram = Histogram(
    f'data_size_before_histogram_{f}',
    f'histogram of input data size {f} (KB)',
)
data_size_after_histogram = Histogram(
    f'total_time_after_histogram_{f}',
    f'histogram of output data size {f} (KB)',
)
```

If you check http://localhost:8000,

```text
total_time_source__inc_count 1.0
total_time_source__inc__print_count 1.0
```

`source__inc`, as definition source -> inc() .

`source__inc__print`, as definition source -> inc() -> print() .

We use `sys.getsizeof` to calculate data size, so, we might expect some headcost. By default metrics for time execution and data size will enable, to disable it, simply check [waterhealer.source.metrics](#waterhealersourcemetrics).

### auto shutdown

Problem with streamz, it run everything in asynchronous manner, so it is very hard to make our Streamz script auto restart if got unproper exception handling. To make it auto restart if python script shutdown, you can run it in kubernetes or any auto restart program after use this interface.

```python
from time import sleep, time
from streamz import Stream
import waterhealer as wh

def inc(x):
    sleep(1)
    return x + 1

source = Stream()
source.map(inc).sink(print)
wh.auto_shutdown(source, got_error = True)
```

`wh.auto_shutdown` will wait event loop to close, if closed, halt python script.

### auto graceful shutdown

We also want to Streamz script auto shutdown itself if no update offset after N seconds. This only work if we added `healing()` in our streaming. To make it auto restart if python script shutdown, you can run it in kubernetes or any auto restart program after use this interface.

```python
from time import sleep, time
from streamz import Stream
import waterhealer as wh

def inc(x):
    sleep(1)
    return x + 1

source = Stream()
source.map(inc).healing()
wh.auto_shutdown(source, graceful_offset = 5400)
```

`wh.auto_shutdown(source, graceful_offset = 5400)` will shutdown after 5400 seconds if no update offset after 5400 seconds. To disable it, simply `wh.auto_shutdown(source, graceful_offset = 0)`.

### auto shutdown dask

When dask client disconnected with dask cluster, `wh.auto_shutdown` also can helps us to shutdown the script. To make it auto restart if python script shutdown, you can run it in kubernetes or any auto restart program after use this interface.

```python
client = Client()
wh.auto_shutdown(source, got_dask = True)
```

It will check Dask status, will shutdown if client status not in `('running','closing','connecting','newly-created')`.

### JSON logging with unique emit ID

Let say every emit, you want to put a unique record to keep track for logging purpose, to do that, you need to enable,

```bash
export ENABLE_JSON_LOGGING=true
export LOGLEVEL=DEBUG
```

Or pythonic way,

```bash
import os
os.environ['ENABLE_JSON_LOGGING'] = 'true'
os.environ['LOGLEVEL'] = 'DEBUG'

import waterhealer
```

Default is `false`, if you enable it,

```text
{"written_at": "2021-11-11T05:30:39.786Z", "written_ts": 1636608639786755000, "msg": "{'function_name': 'Stream', 'data': '{\"i\": 0, \"data\": 1}'}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": null, "emit_id": "496cc1c7-3e70-4b54-9a12-7b13924f0b4e"}
{"written_at": "2021-11-11T05:30:39.891Z", "written_ts": 1636608639891420000, "msg": "{'function_name': 'Stream', 'data': '{\"i\": 1, \"data\": 2}'}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": null, "emit_id": "4dbc7d17-e6dd-4a5d-9908-fd0362959bf7"}
{"written_at": "2021-11-11T05:30:39.997Z", "written_ts": 1636608639997099000, "msg": "{'function_name': 'Stream', 'data': '{\"i\": 2, \"data\": 3}'}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": null, "emit_id": "2ef29f90-97ff-4495-8513-7730df850758"}
{"written_at": "2021-11-11T05:30:40.104Z", "written_ts": 1636608640104243000, "msg": "{'function_name': 'Stream', 'data': '{\"i\": 3, \"data\": 4}'}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": null, "emit_id": "bbc9d403-e9d4-4255-8ddd-661d8ae6368d"}
{"written_at": "2021-11-11T05:30:40.206Z", "written_ts": 1636608640206672000, "msg": "{'function_name': 'Stream', 'data': '{\"i\": 4, \"data\": 5}'}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": null, "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.207Z", "written_ts": 1636608640207718000, "msg": "{'function_name': 'partition', 'data': '(\\'{\"i\": 0, \"data\": 1}\\', \\'{\"i\": 1, \"data\": 2}\\', \\'{\"i\": 2, \"data\": 3}\\', \\'{\"i\": 3, \"data\": 4}\\', \\'{\"i\": 4, \"data\": 5}\\')'}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": "partition", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.209Z", "written_ts": 1636608640209274000, "msg": "{'function_name': 'map.json_loads', 'data': \"[{'i': 0, 'data': 1}, {'i': 1, 'data': 2}, {'i': 2, 'data': 3}, {'i': 3, 'data': 4}, {'i': 4, 'data': 5}]\"}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": "map.json_loads", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.210Z", "written_ts": 1636608640210403000, "msg": "increment_left halloooo", "type": "log", "logger": "root", "thread": "MainThread", "level": "INFO", "module": "<ipython-input-5-554c07df66f4>", "line_no": 15, "function_name": "map.increment_left", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.211Z", "written_ts": 1636608640211674000, "msg": "{'function_name': 'map.increment_left', 'data': \"[{'i': 0, 'data': 1, 'left': 2}, {'i': 1, 'data': 2, 'left': 3}, {'i': 2, 'data': 3, 'left': 4}, {'i': 3, 'data': 4, 'left': 5}, {'i': 4, 'data': 5, 'left': 6}]\"}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": "map.increment_left", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.212Z", "written_ts": 1636608640212647000, "msg": "increment_right halloooo", "type": "log", "logger": "root", "thread": "MainThread", "level": "INFO", "module": "<ipython-input-5-554c07df66f4>", "line_no": 25, "function_name": "map.increment_right", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.213Z", "written_ts": 1636608640213564000, "msg": "{'function_name': 'map.increment_right', 'data': \"[{'i': 0, 'data': 1, 'right': 2}, {'i': 1, 'data': 2, 'right': 3}, {'i': 2, 'data': 3, 'right': 4}, {'i': 3, 'data': 4, 'right': 5}, {'i': 4, 'data': 5, 'right': 6}]\"}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": "map.increment_right", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.214Z", "written_ts": 1636608640214567000, "msg": "{'function_name': 'zip', 'data': \"([{'i': 0, 'data': 1, 'left': 2}, {'i': 1, 'data': 2, 'left': 3}, {'i': 2, 'data': 3, 'left': 4}, {'i': 3, 'data': 4, 'left': 5}, {'i': 4, 'data': 5, 'left': 6}], [{'i': 0, 'data': 1, 'right': 2}, {'i': 1, 'data': 2, 'right': 3}, {'i': 2, 'data': 3, 'right': 4}, {'i': 3, 'data': 4, 'right': 5}, {'i': 4, 'data': 5, 'right': 6}])\"}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": "zip", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.215Z", "written_ts": 1636608640215408000, "msg": "need to combine", "type": "log", "logger": "root", "thread": "MainThread", "level": "INFO", "module": "<ipython-input-5-554c07df66f4>", "line_no": 35, "function_name": "map.combine", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
{"written_at": "2021-11-11T05:30:40.216Z", "written_ts": 1636608640216577000, "msg": "{'function_name': 'map.combine', 'data': '[4, 6, 8, 10, 12]'}", "type": "log", "logger": "root", "thread": "MainThread", "level": "DEBUG", "module": "core", "line_no": 498, "function_name": "map.combine", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
[4, 6, 8, 10, 12]
```

`emit_id` also can correlate with `logging.info` / `logging.warning` / `logging.debug` / `logging.error`, you can see `emit_id` along with a message `need to combine`,

```text
{"written_at": "2021-11-11T05:30:40.215Z", "written_ts": 1636608640215408000, "msg": "need to combine", "type": "log", "logger": "root", "thread": "MainThread", "level": "INFO", "module": "<ipython-input-5-554c07df66f4>", "line_no": 35, "function_name": "map.combine", "emit_id": "9e2ecc23-6419-4a94-84a5-6b1cb748590e"}
```

For example,

- [json-logging-emit-id.ipynb](example/json-logging-emit-id.ipynb).
- with kafka example, [simple-plus-element-emit-id.ipynb](example/simple-plus-element-emit-id.ipynb).
- with Dask cluster example, [dask-emit-id.ipynb](example/dask-emit-id.ipynb).

### Remote logging

When come to distributed real time processing, it is very hard to store the variables that caused the streaming halt and we wish we can inspect the variables in playground environment. water-healer included simple Dask plugin to do remote logging,

First, you need to create a logging class, simple as,

```python
from datetime import datetime

class Persistent:
    def __init__(self):
        # initiate GCS / S3 client
        pass
    
    def persist(self, data, now):
        """
        data: Dict[function, args, kwargs, exception, error, key]
        timestamp: datetime.datetime
            from `datetime.now()`.
        """
        now = datetime.strftime(now, '%y-%m-%d-%H-%M-%S')
        with open(f"{data['function']}-{now}.json", 'w') as fopen:
            json.dump(data, fopen)

        # also can do webhook for real time alert
```

The logging class must have `persist` method or else water-healer will throw an error. To initiate the logging class as Dask plugin,

```python
from dask.distributed import Client
client = Client()
source = Stream()
wh.dask_plugin.remote_logging(client, Persistent)
```

Example output [__main__.combine-21-11-24-18-56-52.json](example/__main__.combine-21-11-24-18-56-52.json),

```text
{"function": "__main__.combine", "args": [[[{"i": 3, "data": 4, "left": 5}, {"i": 4, "data": 5, "left": 6}], [{"i": 3, "data": 4, "right": 5}, {"i": 4, "data": 5, "right": 6}]]], "kwargs": [], "exception": ["  File \"<ipython-input-7-b696a701b986>\", line 24, in combine\n    raise Exception('error')\n"], "error": "error", "key": "combine--1efd1c43-36c3-4cf4-93f5-3152287c9251--8432e1d8-1488-4986-9f85-d373360fe491", "emit_id": "1efd1c43-36c3-4cf4-93f5-3152287c9251"}
```

For example,

- [dask-plugin-remote-logging.ipynb](example/dask-plugin-remote-logging.ipynb).

### checkpointing

Let say every emit, I want to store value from each nodes returned, example as,

![alt text](image/stream.png)

Checkpointing is really good for debugging purpose especially if we use dask to do distributed processing, so to enable checkpointing, simply,

```python
import waterhealer as wh
from waterhealer import Stream

def increment(x):
    return x + 1

def increment_plus2(x):
    return x + 2

source = Stream()
source.map(increment, checkpoint = True).map(increment_plus2, checkpoint = True).sink(print)
source.emit(1)
print(source.checkpoint)
```

Output is,

```python
{'Stream.map.increment': 2, 'Stream.map.increment.map.increment_plus2': 4}
```

Checkpointing also can put on `zip`, `sink`, and other interfaces.

For example,

- [checkpointing.ipynb](example/checkpointing.ipynb).
- with Dask checkpointing, [dask-checkpointing.ipynb](example/dask-checkpointing.ipynb).

#### disable checkpointing using OS environment

Incase we are so lazy to remove `checkpoint = True`, we can disable checkpointing using OS environment, by simply set,

```bash
export ENABLE_CHECKPOINTING=false
```

Or pythonic way,

```bash
import os
os.environ['ENABLE_CHECKPOINTING'] = 'false'

import waterhealer
```

### check leakage on offsets

Let say you have a function,

```python
def foo(rows):
    # do something, messed up the offsets
```

This can break the water-healer due to missing offsets and to trace is very hard if your pipeline is very long. You can use decorator `wh.checker.check_leakage` to check leakage for your function,

```python
import waterhealer as wh

@wh.checker.check_leakage
def func(rows):
    # do something cause after uuid not same as before uuid
```

`wh.checker.check_leakage` only validate specific structure of data,

1. `List[Tuple[uuid, data]]`.
2. `List[Dict[uuid, **data]]`.

For example,

- [json-logging-emit-id-check-leakage.ipynb](example/json-logging-emit-id-check-leakage).

**`wh.checker.check_leakage` will raised an exception if found a leakage**.

## API

### checker

* [waterhealer.checker.check_leakage](#waterhealercheckercheck_leakage)

#### check_leakage

A decorator to check UUID leakage in before and after UUIDs.

```python
def check_leakage(func):
    """
    Check leakage for kafka offsets, only support below structure of data,
    1. List[Tuple[uuid, data]]
    2. List[Dict[uuid, **data]]
    """
```

### core

* [waterhealer.core.partition_time](#waterhealercorepartition_time)
* [waterhealer.core.foreach_map](#waterhealercoreforeach_map)
* [waterhealer.core.foreach_async](#waterhealercoreforeach_async)

#### waterhealer.core.partition_time

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

#### waterhealer.core.foreach_map

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

#### waterhealer.core.foreach_async

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
source = Stream()
source.partition(5).foreach_async(lambda x: 2*x).sink(print)
```

Example, [foreach-async.ipynb](example/foreach-async.ipynb)

### dask_plugin

* [waterhealer.dask_plugin.remote_logging](waterhealerdask_pluginremote_logging)

#### waterhealer.dask_plugin.remote_logging

```python
def remote_logging(client, persistent_class,
                   status_finish: List[str] = ['error'],
                   plugin_name: str = 'error-logging'):
    """
    Remote logging for dask worker using dask plugin.
    Only support dask>=2021.1.0 and distributed>=2021.1.0

    Parameters
    ----------
    client: distributed.client.Client
    persistent_class: class
        Must have `persist` and `constructor` methods.
    status_finish: List[str], optional (default=['error'])
    plugin_name: str, optional (default='error-logging')
        Name for plugin.
    """
```

### kafka

* [waterhealer.kafka.from_kafka](#waterhealerkafkafrom_kafka)
* [waterhealer.kafka.from_kafka_batched](#waterhealerkafkafrom_kafka_batched)
* [waterhealer.kafla.from_kafka_batched_scatter](#waterhealerkafkafrom_kafka_batched_scatter)

#### waterhealer.kafka.from_kafka

```python
class from_kafka(Source, KafkaOffset):
    """

    Parameters
    ----------
    topics: List[str]
        Labels of Kafka topics to consume from.
    consumer_params: Dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers, Connection string(s) (host:port) by which to reach
        Kafka;
        group.id, Identity of the consumer. If multiple sources share the same
        group, each message will be passed to only one of them.
    poll_interval: float, optional (default=0.1)
        Seconds that elapse between polling Kafka for new messages.
    start: bool, optional (default=False)
        Whether to start polling upon instantiation.
    db: Callable, optional (default=None)
        persistent layer to check kafka offset to provide once-semantics.
        If None, will initiate waterhealer.db.expiringdict.Database.
    """
```

`waterhealer.kafka.from_kafka` always returned a tuple,

```python
(uuid, value)
```

Data structure of uuid,

```python
{
    'partition': partition,
    'offset': offset,
    'topic': topic,
}
```

If you want to use waterhealer, you need to make sure `uuid` from `from_kafka` succesfully transported until healing, or else it cannot update the offset.

Example, [simple-plus-element-kafka.ipynb](example/simple-plus-element-kafka.ipynb).

**Output from `waterhealer.kafka.from_kafka` is different from any sources object from `streamz`, `streamz` only returned `value`, not tuple as `waterhealer.kafka.from_kafka`.**

#### waterhealer.kafka.from_kafka_batched

```python
class from_kafka_batched(Source, KafkaOffset):
    """

    Parameters
    ----------
    topics: List[str]
        Labels of Kafka topics to consume from.
    consumer_params: Dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers, Connection string(s) (host:port) by which to reach
        Kafka;
        group.id, Identity of the consumer. If multiple sources share the same
        group, each message will be passed to only one of them.
    batch_size: int, optional (default=100)
        batch size of polling.
    batch_timeout: float, optional (default=10)
        timeout for batching if not reach size `batch_size`.
    poll_interval: float, optional (default=0.1)
        Seconds that elapse between polling Kafka for new messages.
    start: bool, optional (default=False)
        Whether to start polling upon instantiation.
    db: Callable, optional (default=None)
        persistent layer to check kafka offset to provide once-semantics.
        If None, will initiate waterhealer.db.expiringdict.Database.
    """
```

Same as `waterhealer.kafka.from_kafka`, but we poll events and emit it as a batch of events.

**Output from `waterhealer.kafka.from_kafka_batched` is different from any sources object from `streamz`, `streamz` only returned `value`, not tuple as `waterhealer.kafka.from_kafka_batched`.**

#### waterhealer.kafka.from_kafka_batched_scatter

```python
def from_kafka_batched_scatter(
    topics: List[str],
    consumer_params: Dict,
    poll_interval: int = 5,
    batch_size: int = 1000,
    dask: bool = False,
    db = None,
    **kwargs,
):
    """

    Parameters
    ----------
    topics: List[str]
        Labels of Kafka topics to consume from.
    consumer_params: Dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers, Connection string(s) (host:port) by which to reach
        Kafka;
        group.id, Identity of the consumer. If multiple sources share the same
        group, each message will be passed to only one of them.
    batch_size: int, optional (default=1000)
        batch size of polling.
    poll_interval: float, optional (default=5.0)
        Seconds that elapse between polling Kafka for new messages.
    dask: bool, optional (default=False)
        If True, will poll events from each partitions distributed among Dask workers.
    db: Callable, optional (default=None)
        persistent layer to check kafka offset to provide once-semantics.
        If None, will initiate waterhealer.db.expiringdict.Database.
    """
```

Same as `waterhealer.kafka.from_kafka`, but we distribute available partitions among Dask workers and will poll it and emit as a batch of events. The distributed only useful if `dask` is `True`.

Example, [simple-plus-element-kafka-scatter.ipynb](example/simple-plus-element-kafka-scatter.ipynb).

### healing

* [waterhealer.healing.healing](#waterhealerhealinghealing)
* [waterhealer.healing.auto_shutdown](#waterhealerhealingauto_shutdown)

#### waterhealer.healing.healing

```python
class healing(Stream):

    def __init__(self, upstream,
                 ignore: bool = False,
                 asynchronous: bool = False,
                 interval: int = 10,
                 **kwargs):
        """
        ignore: bool, (default=False)
            if True, ignore any failed update offset.
        asynchronous: bool, (default=False)
            if True, it will update kafka offset async manner.
        interval: int, (default=10)
            Every interval, will update batch of kafka offsets.
        """
```

Partial code can be like this,

```python
.map(function).healing(interval = 2)
```

`healing` will returned,

```python
[{'topic': 'testing', 'partition': 2, 'offset': 171},
  {'topic': 'testing', 'partition': 4, 'offset': 170},
  {'topic': 'testing', 'partition': 5, 'offset': 46},
  {'topic': 'testing', 'partition': 3, 'offset': 49}]
```

Example, [simple-plus-element-kafka.ipynb](example/simple-plus-element-kafka.ipynb)

#### waterhealer.healing.auto_shutdown

```python
def auto_shutdown(
    source,
    got_error: bool = to_bool(os.environ.get('HEALING_GOT_ERROR', 'true')),
    got_dask: bool = to_bool(os.environ.get('HEALING_GOT_DASK', 'true')),
    graceful_offset: int = int(os.environ.get('HEALING_GRACEFUL_OFFSET', 3600)),
    graceful_polling: int = int(os.environ.get('HEALING_GRACEFUL_POLLING', 1800)),
    interval: int = int(os.environ.get('HEALING_INTERVAL', 5)),
    sleep_before_shutdown: int = int(os.environ.get('HEALING_SLEEP_BEFORE_SHUTDOWN', 2)),
    auto_expired: int = int(os.environ.get('HEALING_AUTO_EXPIRED', 10800)),
    max_total_commit: int = int(os.environ.get('HEALING_MAX_TOTAL_COMMIT', 0)),
):
    """

    Parameters
    ----------
    source: waterhealer.core.Stream
        waterhealer.core.Stream object
    got_error: bool, (default=True)
        if dask streaming got an exception, automatically shutdown the script.
        or set using OS env, `HEALING_GOT_ERROR`.
    got_dask: bool, (default=True)
        if True, will check Dask status, will shutdown if client status not in ('running','closing','connecting','newly-created').
        or set using OS env, `HEALING_GOT_DASK`.
    graceful_offset: int, (default=3600)
        automatically shutdown the script if water-healer not updated any offsets after `graceful_offset` period.
        to disable it, set it to 0.
        or set using OS env, `HEALING_GRACEFUL_OFFSET`.
    graceful_polling: int, (default=1800)
        automatically shutdown the script if kafka consumer not polling after `graceful_polling` period.
        to disable it, set it to 0.
        or set using OS env, `HEALING_GRACEFUL_POLLING`.
    interval: int, (default=5)
        check heartbeat every `interval`.
        or set using OS env, `HEALING_INTERVAL`.
    sleep_before_shutdown: int, (defaut=2)
        sleep (second) before shutdown.
        or set using OS env, `HEALING_SLEEP_BEFORE_SHUTDOWN`.
    auto_expired: int, (default=10800)
        auto shutdown after `auto_expired`. Set to `0` to disable it.
        This is to auto restart the python script to flush out memory leaks.
        or set using OS env, `HEALING_AUTO_EXPIRED`.
    max_total_commit: int, (default=0)
        max total kafka commit, set to `0` to disable it.
        if total commit bigger than `max_total_commit`, it will shutdown the script.
        or set using OS env, `HEALING_MAX_TOTAL_COMMIT`.
    """
```

### source

* [waterhealer.source.metrics](#waterhealermetrics)

#### waterhealer.source.metrics

```python
def metrics(
    source,
    time_execution_metrics: bool = True,
    data_size_metrics: bool = True,
    port: int = 8000,
):
    """

    Parameters
    ----------
    source: source object
        streamz source.
    time_execution_metrics: bool, (default=True)
        added time execution metrics.
    data_size_metrics: bool, (default=True)
        data size metrics.
    port: int, (default=8000)
        default port for prometheus exporter.
        
    """
```

## Examples

For more complicated example, simply check notebooks in [example](example).

## What is the pain points?

1. Hard to maintain offset references.

When your streaming pipeline grow larger, to maintain the offset definition until `healing` is very hard, you need to debug a lot.

2. No web UI.

My front-end sucks.
