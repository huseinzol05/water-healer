"""
Copyright (c) 2017, Continuum Analytics, Inc. and contributors
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

Neither the name of Continuum Analytics nor the names of any contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
THE POSSIBILITY OF SUCH DAMAGE.
"""

from tornado import gen
from streamz.core import Stream, convert_interval
from streamz.sources import Source
from datetime import datetime, timedelta
from itertools import cycle
import confluent_kafka as ck
import streamz
import logging
import asyncio
from typing import Tuple, Callable, Dict
import time


logger = logging.getLogger(__name__)


@gen.coroutine
def _healing(row, consumer, ignore, asynchronous):
    if not consumer:
        raise Exception('consumer must not None')
    success = False
    splitted = row[0].split('<!>')
    if len(splitted) != 3:
        reason = 'invalid uuid'
        partition = None
        offset = None
        topic = None
    else:
        partition, offset, topic = splitted
        offset = int(offset)
        partition = int(partition)
        low_offset, high_offset = consumer.get_watermark_offsets(
            ck.TopicPartition(topic, partition)
        )
        current_offset = consumer.committed(
            [ck.TopicPartition(topic, partition)]
        )[0].offset

        reason = (
            f'committed topic: {topic} partition: {partition} offset: {offset}'
        )

        if current_offset >= high_offset:
            reason = 'current offset already same as high offset, skip'
        elif offset < current_offset:
            reason = 'current offset higher than message offset, skip'
        else:
            try:
                consumer.commit(
                    offsets = [ck.TopicPartition(topic, partition, offset + 1)],
                    asynchronous = asynchronous,
                )
                success = True
            except Exception as e:
                if ignore:
                    logging.warning(str(e))
                    reason = str(e)
                else:
                    logger.exception(e)
                    raise

    return {
        'data': row[1],
        'success': success,
        'reason': reason,
        'partition': partition,
        'offset': offset,
        'topic': topic,
    }


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
    if type(stream) == streamz.dask.starmap:
        consumer = stream.upstreams[0].upstreams[0].consumer
    else:
        consumer = stream.consumer
    result = _healing(
        row = row,
        consumer = consumer,
        ignore = ignore,
        asynchronous = asynchronous,
    )
    return result.result()


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
    if type(stream) == streamz.dask.starmap:
        consumer = stream.upstreams[0].upstreams[0].consumer
    else:
        consumer = stream.consumer

    @gen.coroutine
    def loop():
        r = yield [
            _healing(
                row = row,
                consumer = consumer,
                ignore = ignore,
                asynchronous = asynchronous,
            )
            for row in rows
        ]
        return r

    result = loop()

    return result.result()


@Stream.register_api(staticmethod)
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

    def __init__(
        self,
        topics,
        consumer_params,
        poll_interval = 0.1,
        start = False,
        debug = False,
        **kwargs,
    ):
        self.cpars = consumer_params
        self.cpars['enable.auto.commit'] = False
        self.consumer = None
        self.topics = topics
        self.poll_interval = poll_interval
        super(from_kafka, self).__init__(ensure_io_loop = True, **kwargs)
        self.stopped = True
        self.debug = debug

        if start:
            self.start()

    def do_poll(self):
        if self.consumer is not None:
            msg = self.consumer.poll(0)
            if msg and msg.value() and msg.error() is None:
                return msg

    @gen.coroutine
    def poll_kafka(self):
        while True:
            val = self.do_poll()
            if val:
                partition = val.partition()
                offset = val.offset()
                topic = val.topic()
                val = val.value()
                id_val = f'{partition}<!>{offset}<!>{topic}'
                if self.debug:
                    logger.warning(
                        f'topic: {topic}, partition: {partition}, offset: {offset}, data: {val}'
                    )
                yield self._emit((id_val, val))
            else:
                yield gen.sleep(self.poll_interval)
            if self.stopped:
                break
        self._close_consumer()

    def start(self):
        global consumer
        if self.stopped:
            self.stopped = False
            self.consumer = ck.Consumer(self.cpars)
            self.consumer.subscribe(self.topics)
            tp = ck.TopicPartition(self.topics[0], 0, 0)

            # blocks for consumer thread to come up
            self.consumer.get_watermark_offsets(tp)
            consumer = self.consumer
            self.loop.add_callback(self.poll_kafka)

    def _close_consumer(self):
        if self.consumer is not None:
            consumer = self.consumer
            self.consumer = None
            consumer.unsubscribe()
            consumer.close()
        self.stopped = True


class FromKafkaBatched(Stream):
    """Base class for both local and cluster-based batched kafka processing"""

    def __init__(
        self,
        topics,
        consumer_params,
        poll_interval = '1s',
        partitions = 5,
        maxlen = 1000,
        **kwargs,
    ):
        self.consumer_params = consumer_params
        self.consumer_params['enable.auto.commit'] = False
        self.topics = topics
        self.partitions = partitions
        self.positions = {}
        self.poll_interval = convert_interval(poll_interval)
        self.stopped = True
        self.maxlen = maxlen

        super(FromKafkaBatched, self).__init__(ensure_io_loop = True, **kwargs)

    @gen.coroutine
    def poll_kafka(self):
        import confluent_kafka as ck

        try:
            while not self.stopped:
                out = []
                for tp in self.tps:
                    topic = tp[0]
                    partition = tp[1]
                    tp = ck.TopicPartition(topic, partition, 0)
                    print(tp)
                    try:
                        low, high = self.consumer.get_watermark_offsets(
                            tp, timeout = 0.1
                        )
                        low = self.consumer.committed(
                            [ck.TopicPartition(topic, partition)]
                        )[0].offset
                    except (RuntimeError, ck.KafkaException):
                        continue

                    current_position = self.positions.get(tp, 0)

                    lowest = max(current_position, low)
                    print(tp, high, lowest)
                    if high > lowest:
                        high = min(lowest + self.maxlen, high)
                        out.append(
                            (
                                self.consumer_params,
                                topic,
                                partition,
                                lowest,
                                high - 1,
                                self.maxlen,
                            )
                        )

                        self.positions[tp] = lowest

                    if len(out) == self.partitions:
                        for part in out:
                            yield self._emit(part)
                        out = []
                yield gen.sleep(self.poll_interval)
        finally:
            self.consumer.unsubscribe()
            self.consumer.close()

    def start(self):
        import confluent_kafka as ck

        if self.stopped:
            self.consumer = ck.Consumer(self.consumer_params)
            topics = self.consumer.list_topics().topics
            tps = []
            for t in self.topics:
                for p in topics[t].partitions.keys():
                    tps.append((t, p))

            self.tps = cycle(tps)
            self.stopped = False
            tp = ck.TopicPartition(self.topics[0], 0, 0)
            self.consumer.get_watermark_offsets(tp)
            self.loop.add_callback(self.poll_kafka)


@Stream.register_api(staticmethod)
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

    if dask:
        from distributed.client import default_client

        kwargs['loop'] = default_client().loop

    source = FromKafkaBatched(
        topics,
        consumer_params,
        poll_interval = poll_interval,
        partitions = partitions,
        maxlen = maxlen,
        **kwargs,
    )
    if dask:
        source = source.scatter()

    if start:
        source.start()

    return source.starmap(get_message_batch)


def get_message_batch(
    kafka_params, topic, partition, low, high, maxlen, timeout = None
):
    """Fetch a batch of kafka messages (keys & values) in given topic/partition
    This will block until messages are available, or timeout is reached.
    """
    import confluent_kafka as ck

    t0 = time.time()
    consumer = ck.Consumer(kafka_params)
    tp = ck.TopicPartition(topic, partition, low)
    consumer.assign([tp])
    out = []
    try:
        while True:
            msg = consumer.poll(0)
            if msg and msg.value() and msg.error() is None:
                if high >= msg.offset():
                    partition = msg.partition()
                    offset = msg.offset()
                    topic = msg.topic()
                    val = msg.value()
                    id_val = f'{partition}<!>{offset}<!>{topic}'
                    out.append((id_val, val))
                if high <= msg.offset() or len(out) == maxlen:
                    break
            else:
                time.sleep(0.1)
                if timeout is not None and time.time() - t0 > timeout:
                    break
    finally:
        consumer.close()
    return out


@Stream.register_api()
class partition_time(Stream):
    """
    Partition stream into tuples if waiting time expired.

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

    _graphviz_shape = 'diamond'

    def __init__(self, upstream, n, **kwargs):
        self.n = n
        self.buffer = []
        self.time = None
        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who = None):
        self.buffer.append(x)
        if self.time is None:
            self.time = datetime.now()

        if (datetime.now() - self.time).seconds >= self.n:
            self.time = None
            result, self.buffer = self.buffer, []
            return self._emit(tuple(result))
        else:
            return []


@Stream.register_api()
class foreach_map(Stream):
    """ 
    Apply a function to every element in a tuple in the stream.

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

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name = stream_name)

    def update(self, x, who = None):
        try:
            result = (self.func(e, *self.args, **self.kwargs) for e in x)
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result)


@Stream.register_api()
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

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name = stream_name)

    def update(self, x, who = None):
        try:

            @gen.coroutine
            def function(e, *args, **kwargs):
                return self.func(e, *args, **kwargs)

            @gen.coroutine
            def loop():
                r = yield [function(e, *self.args, **self.kwargs) for e in x]
                return r

            result = loop().result()
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result)
