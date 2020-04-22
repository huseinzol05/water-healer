from tornado import gen
from streamz.core import Stream, convert_interval
from streamz.sources import Source
from itertools import cycle
from collections import defaultdict
from expiringdict import ExpiringDict
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import confluent_kafka as ck
from waterhealer.function import topic_partition_str


logger = logging.getLogger(__name__)


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
    maxlen_memory: int, (default=10000)
        max length of topic and partition dictionary for healing process.
    maxage_memory: int, (default=1800)
        max age for a partition stay in topic and partition dictionary.
    """

    def __init__(
        self,
        topics,
        consumer_params,
        poll_interval = 0.1,
        start = False,
        debug = False,
        maxlen_memory = 10000,
        maxage_memory = 1800,
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
        self.memory = defaultdict(
            lambda: ExpiringDict(
                max_len = maxlen_memory, max_age_seconds = maxage_memory
            )
        )

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
                if self.debug:
                    logger.warning(
                        f'topic: {topic}, partition: {partition}, offset: {offset}, data: {val}'
                    )

                self.memory[topic_partition_str(topic, partition)][
                    offset
                ] = False
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

        try:
            while not self.stopped:
                out = []
                for tp in self.tps:
                    topic = tp[0]
                    partition = tp[1]
                    tp = ck.TopicPartition(topic, partition, 0)
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

                        self.positions[tp] = high

                    if len(out) == self.partitions:
                        for part in out:
                            yield self._emit(part)
                        out = []
                yield gen.sleep(self.poll_interval)
        finally:
            self.consumer.unsubscribe()
            self.consumer.close()

    def start(self):

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
        if not source.asynchronous:
            source._set_asynchronous(False)
        source = source.scatter()

    if start:
        source.start()

    return source.starmap(get_message_batch)


def get_message_batch(
    kafka_params, topic, partition, low, high, maxlen, timeout = None
):
    """
    Fetch a batch of kafka messages (keys & values) in given topic/partition
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
                    id_val = {
                        'partition': partition,
                        'offset': offset,
                        'topic': topic,
                    }
                    out.append((id_val, val))
                if high <= msg.offset() or len(out) == maxlen:
                    break
            else:
                time.sleep(0.1)
                if timeout is not None and time.time() - t0 > timeout:
                    break
    finally:
        consumer.close()
    logger.warning(len(out))
    return out
