from .core import Stream, convert_interval, logger
from .function import topic_partition_str
from .db.expiringdict import Database
from tornado import gen
from datetime import datetime
from typing import List, Dict
import confluent_kafka as ck
import time


class Source(Stream):
    _graphviz_shape = 'doubleoctagon'

    def __init__(self, **kwargs):
        self.stopped = True
        super(Source, self).__init__(**kwargs)

    def stop(self):
        if not self.stopped:
            self.stopped = True


def _close_consumer(consumer):
    try:
        consumer.close()
    except RuntimeError:
        pass


class KafkaOffset:
    def __init__(
        self,
        db=None,
        **kwargs,
    ):
        self.db = db
        if self.db is None:
            self.db = Database(**kwargs)
            logger.info(
                f'`db` is None, use `waterhealer.db.expiringdict.Database` with max_len={self.db.maxlen_memory}, max_age_seconds={self.db.maxage_memory}')

        logger.info(f'Use {self.db.__module__}.{self.db.__class__.__name__}')


@Stream.register_api(staticmethod)
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

    def __init__(
        self,
        topics: List[str],
        consumer_params: Dict,
        poll_interval: float = 0.1,
        start: bool = False,
        db=None,
        **kwargs,
    ):
        self.cpars = consumer_params
        KafkaOffset.__init__(self, db=db, **kwargs)
        self.cpars['enable.auto.commit'] = False
        self.consumer = None
        self.topics = topics
        self.poll_interval = poll_interval
        super(from_kafka, self).__init__(ensure_io_loop=True, **kwargs)
        self.stopped = True

        if start:
            self.start()

        self.last_poll = datetime.now()

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
                id_val = {
                    'partition': partition,
                    'offset': offset,
                    'topic': topic,
                }
                logger.debug(
                    f'topic: {topic}, partition: {partition}, offset: {offset}, data: {val}'
                )

                topic_partition = topic_partition_str(topic, partition)
                if self.db[topic_partition].get(offset, False):
                    continue

                self.db[topic_partition][offset] = False
                self.last_poll = datetime.now()
                yield self._emit((id_val, val))
            else:
                yield gen.sleep(self.poll_interval)
            if self.stopped:
                break
        self._close_consumer()

    def start(self):
        if self.stopped:
            self.stopped = False
            self.consumer = ck.Consumer(self.cpars)
            self.consumer.subscribe(self.topics)
            tp = ck.TopicPartition(self.topics[0], 0, 0)

            # blocks for consumer thread to come up
            self.consumer.get_watermark_offsets(tp)
            self.loop.add_callback(self.poll_kafka)

    def _close_consumer(self):
        if self.consumer is not None:
            _close_consumer(self.consumer)
        self.stopped = True

    def stop(self, sleep_after_close=2):
        self._close_consumer()
        time.sleep(sleep_after_close)


@Stream.register_api(staticmethod)
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

    def __init__(
        self,
        topics: List[str],
        consumer_params: Dict,
        batch_size: int = 100,
        batch_timeout: float = 10,
        poll_interval: float = 0.1,
        start: bool = False,
        db=None,
        **kwargs,
    ):
        self.cpars = consumer_params
        KafkaOffset.__init__(self, db=db, **kwargs)
        self.cpars['enable.auto.commit'] = False
        self.consumer = None
        self.topics = topics
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.buffer = []
        self.poll_interval = poll_interval
        super(from_kafka_batched, self).__init__(
            ensure_io_loop=True, **kwargs
        )
        self.stopped = True

        if start:
            self.start()

        self.last_poll = datetime.now()

    def do_poll(self):
        if self.consumer is not None:
            msg = self.consumer.poll(0)
            if msg and msg.value() and msg.error() is None:
                return msg

    @gen.coroutine
    def poll_kafka(self):
        last_push = datetime.now()
        while True:
            val = self.do_poll()
            if len(self.buffer) == self.batch_size or (
                len(self.buffer) > 0
                and (datetime.now() - last_push).seconds > self.batch_timeout
            ):
                L, self.buffer = self.buffer, []
                last_push = datetime.now()
                self.last_poll = datetime.now()
                yield self._emit(L)
            if val:
                partition = val.partition()
                offset = val.offset()
                topic = val.topic()
                val = val.value()
                id_val = {
                    'partition': partition,
                    'offset': offset,
                    'topic': topic,
                }
                logger.debug(
                    f'topic: {topic}, partition: {partition}, offset: {offset}, data: {val}'
                )

                topic_partition = topic_partition_str(topic, partition)
                if self.db[topic_partition].get(offset, False):
                    continue

                self.db[topic_partition][offset] = False
                self.buffer.append((id_val, val))
            else:
                yield gen.sleep(self.poll_interval)
            if self.stopped:
                break
        self._close_consumer()

    def start(self):
        if self.stopped:
            self.stopped = False
            self.consumer = ck.Consumer(self.cpars)
            self.consumer.subscribe(self.topics)
            tp = ck.TopicPartition(self.topics[0], 0, 0)

            # blocks for consumer thread to come up
            self.consumer.get_watermark_offsets(tp)
            self.loop.add_callback(self.poll_kafka)

    def _close_consumer(self):
        if self.consumer is not None:
            _close_consumer(self.consumer)
        self.stopped = True

    def stop(self, sleep_after_close=2):
        self._close_consumer()
        time.sleep(sleep_after_close)


class FromKafkaBatched(Source, KafkaOffset):
    """
    Base class for both local and cluster-based batched kafka processing.
    """

    def __init__(
        self,
        topics: List[str],
        consumer_params: Dict,
        poll_interval: int = 10,
        batch_size: int = 1000,
        db=None,
        **kwargs,
    ):
        self.cpars = consumer_params
        KafkaOffset.__init__(self, db=db, **kwargs)
        self.cpars['enable.auto.commit'] = False
        self.consumer = None
        self.topics = topics
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        super(FromKafkaBatched, self).__init__(ensure_io_loop=True, **kwargs)
        self.stopped = True
        self.last_poll = datetime.now()

    @gen.coroutine
    def poll_kafka(self):
        import confluent_kafka as ck

        def commit(_part):
            topic, part_no, _, _, offset = _part[1:]
            _tp = ck.TopicPartition(topic, part_no, offset + 1)
            self.consumer.commit(offsets=[_tp], asynchronous=True)

        self.npartitions, self.positions = [], []
        for topic in self.topics:
            kafka_cluster_metadata = self.consumer.list_topics(topic)
            len_partition = len(kafka_cluster_metadata.topics[topic].partitions)
            self.npartitions.append(len_partition)
            self.positions.append([0] * len_partition)

        tps = []
        for no, topic in enumerate(self.topics):
            tp = []
            for partition in range(self.npartitions[no]):
                tp.append(ck.TopicPartition(topic, partition))
            tps.append(tp)

        for no, topic in enumerate(self.topics):
            while True:
                try:
                    committed = self.consumer.committed(tps[no], timeout=1)
                except ck.KafkaException:
                    pass
                else:
                    for tp in committed:
                        self.positions[no][tp.partition] = tp.offset
                    break

        while not self.stopped:
            out = []

            for no, topic in enumerate(self.topics):
                for partition in range(self.npartitions[no]):
                    tp = ck.TopicPartition(topic, partition, 0)
                    try:
                        low, high = self.consumer.get_watermark_offsets(
                            tp, timeout=0.1
                        )
                    except (RuntimeError, ck.KafkaException):
                        continue
                    self.started = True
                    if 'auto.offset.reset' in self.cpars.keys():
                        if (
                            self.cpars['auto.offset.reset']
                            == 'latest'
                            and self.positions[no][partition] == -1001
                        ):
                            self.positions[no][partition] = high
                    current_position = self.positions[no][partition]
                    lowest = max(current_position, low)
                    if high > lowest + self.batch_size:
                        high = lowest + self.batch_size
                    if high > lowest:
                        out.append(
                            (
                                self.cpars,
                                topic,
                                partition,
                                lowest,
                                high - 1,
                                set(),
                            )
                        )
                        self.positions[no][partition] = high

            for part in out:
                for offset in range(part[-3], part[-2] + 1):
                    topic_partition = topic_partition_str(part[1], part[2])
                    if self.db[topic_partition].get(offset, False):
                        part[-1].add(offset)
                        continue

                    self.db[topic_partition][offset] = False
                self.last_poll = datetime.now()
                yield self._emit(part)

            yield gen.sleep(self.poll_interval)

    def start(self):
        import confluent_kafka as ck

        if self.stopped:
            self.stopped = False
            self.consumer = ck.Consumer(self.cpars)
            tp = ck.TopicPartition(self.topics[0], 0, 0)

            self.consumer.get_watermark_offsets(tp)
            self.loop.add_callback(self.poll_kafka)

    def _close_consumer(self):
        if self.consumer is not None:
            _close_consumer(self.consumer)
        self.stopped = True

    def stop(self, sleep_after_close=2):
        self._close_consumer()
        time.sleep(sleep_after_close)


def get_message_batch(
    kafka_params, topic, partition, low, high, done=set(), timeout=None
):
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
                partition = msg.partition()

                if partition in done:
                    continue

                offset = msg.offset()
                topic = msg.topic()
                val = msg.value()

                id_val = {
                    'partition': partition,
                    'offset': offset,
                    'topic': topic,
                }
                if high >= msg.offset():
                    out.append((id_val, val))
                if high <= msg.offset():
                    break
            else:
                time.sleep(0.1)
                if timeout is not None and time.time() - t0 > timeout:
                    break
    finally:
        consumer.close()
    return out


@Stream.register_api(staticmethod)
def from_kafka_batched_scatter(
    topics: List[str],
    consumer_params: Dict,
    poll_interval: int = 5,
    batch_size: int = 1000,
    dask: bool = False,
    db=None,
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
    if dask:
        from distributed.client import default_client

        kwargs['loop'] = default_client().loop
    source = FromKafkaBatched(
        topics=topics,
        consumer_params=consumer_params,
        poll_interval=poll_interval,
        batch_size=batch_size,
        db=db,
        **kwargs,
    )

    if dask:
        source = source.scatter()

    return source.starmap(get_message_batch)
