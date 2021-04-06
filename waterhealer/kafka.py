from waterhealer.core import Stream, convert_interval
from tornado import gen
from itertools import cycle
from collections import defaultdict
from expiringdict import ExpiringDict
from apscheduler.schedulers.background import BackgroundScheduler
from waterhealer.function import topic_partition_str
from datetime import datetime
import confluent_kafka as ck
import logging
import time

logger = logging.getLogger(__name__)


class Source(Stream):
    _graphviz_shape = 'doubleoctagon'

    def __init__(self, **kwargs):
        self.stopped = True
        super(Source, self).__init__(**kwargs)

    def stop(self):
        if not self.stopped:
            self.stopped = True


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
    maxlen_memory: int, (default=10_000_000)
        max length of topic and partition dictionary for healing process.
    maxage_memory: int, (default=3600)
        max age for a partition stay in topic and partition dictionary.
    """

    def __init__(
        self,
        topics,
        consumer_params,
        poll_interval = 0.1,
        start = False,
        debug = False,
        maxlen_memory = 10_000_000,
        maxage_memory = 3600,
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

    def stop(self, sleep_after_close = 2):
        self._close_consumer()
        time.sleep(sleep_after_close)


@Stream.register_api(staticmethod)
class from_kafka_batched(Source):
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
    batch_size: int
        batch size of polling
    batch_timeout: number
        timeout for batching if not reach size `batch_size`
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages
    start: bool, (default=False)
        Whether to start polling upon instantiation
    debug: bool, (default=False)
        If True, will print topic, partition and offset for each polled message.
    maxlen_memory: int, (default=10_000_000)
        max length of topic and partition dictionary for healing process.
    maxage_memory: int, (default=3600)
        max age for a partition stay in topic and partition dictionary.
    """

    def __init__(
        self,
        topics,
        consumer_params,
        batch_size = 100,
        batch_timeout = 10,
        poll_interval = 0.1,
        start = False,
        debug = False,
        maxlen_memory = 10_000_000,
        maxage_memory = 3600,
        **kwargs,
    ):
        self.cpars = consumer_params
        self.cpars['enable.auto.commit'] = False
        self.consumer = None
        self.topics = topics
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.buffer = []
        self.poll_interval = poll_interval
        super(from_kafka_batched, self).__init__(
            ensure_io_loop = True, **kwargs
        )
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
        last_push = datetime.now()
        while True:
            val = self.do_poll()
            if len(self.buffer) == self.batch_size or (
                len(self.buffer) > 0
                and (datetime.now() - last_push).seconds > self.batch_timeout
            ):
                L, self.buffer = self.buffer, []
                last_push = datetime.now()
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
                if self.debug:
                    logger.warning(
                        f'topic: {topic}, partition: {partition}, offset: {offset}, data: {val}'
                    )

                self.memory[topic_partition_str(topic, partition)][
                    offset
                ] = False
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
            consumer = self.consumer
            self.consumer = None
            consumer.unsubscribe()
            consumer.close()
        self.stopped = True

    def stop(self, sleep_after_close = 2):
        self._close_consumer()
        time.sleep(sleep_after_close)
