from tornado import gen
from datetime import datetime
from waterhealer.core import Stream, convert_interval
from waterhealer.function import topic_partition_str, str_topic_partition
import confluent_kafka as ck
import logging
import os
import time

LAST_UPDATED = datetime.now()
logger = logging.getLogger()


def get_memory(source, consumer=None, memory=None):

    if hasattr(source, 'memory'):
        return source, source.consumer, source.memory

    if isinstance(source, tuple):
        source = source[0]

    for upstream in source.upstreams:
        if hasattr(upstream, 'memory'):
            return upstream, upstream.consumer, upstream.memory
        return get_memory(upstream, consumer, memory)
    return source, consumer, memory


def get_error(source, error=None, last_poll=None):
    if hasattr(source, 'last_poll'):
        return source, source.error, source.last_poll

    if isinstance(source, tuple):
        source = source[0]

    for upstream in source.upstreams:
        if hasattr(upstream, 'last_poll'):
            return upstream, upstream.error, upstream.last_poll
        return get_error(upstream, error, last_poll)
    return source, error, last_poll


def get_source(source):
    if hasattr(source, 'stop'):
        return source

    if isinstance(source, tuple):
        source = source[0]

    for upstream in source.upstreams:
        if hasattr(upstream, 'stop'):
            return upstream
        return get_source(upstream)
    return source


@Stream.register_api()
class healing(Stream):
    _graphviz_shape = 'Mdiamond'

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
        if interval < 1:
            raise ValueError('`interval` must bigger than 0')
        self.ignore = ignore
        self.asynchronous = asynchronous
        self.interval = convert_interval(interval)
        self.partitions = []
        self.last = gen.moment
        _, self.consumer, self.memory = get_memory(upstream)

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, row, who=None):
        logger.debug(f'update, {row}')
        if len(row) == 2:
            if isinstance(row[0], dict):
                partition = row[0].get('partition', -1)
                offset = row[0].get('offset')
                topic = row[0].get('topic')
                topic_partition = topic_partition_str(topic, partition)
                m = self.memory.get(topic_partition)

                if partition < 0 or offset is None or topic is None:
                    logger.warning(f'{row[1]}, invalid water-healer data schema')
                elif m is None:
                    logger.error(
                        f'topic partition: {topic_partition}, offset: {offset}, reason: invalid topic and partition')
                elif offset not in m:
                    logger.warning(
                        f'topic partition: {topic_partition}, offset: {offset}, reason: offset expired or not exist')
                else:
                    m[offset] = True
        return self.last

    def commit(self):
        global LAST_UPDATED
        success = False
        now = datetime.now()
        if len(self.partitions) and self.consumer is not None:
            logger.debug(f'committing {self.partitions}')
            try:
                self.consumer.commit(
                    offsets=self.partitions, asynchronous=self.asynchronous
                )
                success = True
                LAST_UPDATED = now
            except Exception as e:
                if ignore:
                    logger.warning(str(e))
                    reason = str(e)
                else:
                    logger.exception(e)
                    raise
        logger.debug(f'healing successful: {success}, {now}')
        return success

    @gen.coroutine
    def cb(self):
        while True:
            if self.consumer is None:
                _, self.consumer, self.memory = get_memory(self.upstreams[0])
            for topic_partition in self.memory.keys():
                topic, partition = str_topic_partition(topic_partition)
                if len(self.memory[topic_partition]):
                    while True:
                        try:
                            low_offset, high_offset = self.consumer.get_watermark_offsets(
                                ck.TopicPartition(topic, partition)
                            )
                            current_offset = self.consumer.committed(
                                [ck.TopicPartition(topic, partition)]
                            )[0].offset
                            break
                        except Exception as e:
                            logger.warning(e)
                    for offset in sorted(self.memory[topic_partition].keys()):
                        if self.memory[topic_partition][offset]:
                            if current_offset >= high_offset:
                                logger.warning(
                                    f'topic partition: {topic_partition}, offset: {offset}, current offset already same as high offset')
                            elif offset < current_offset:
                                logger.warning(
                                    f'topic partition: {topic_partition}, offset: {offset}, current offset higher than message offset')
                            else:
                                self.partitions.append(
                                    ck.TopicPartition(topic, partition, offset + 1)
                                )
                        else:
                            break

            if self.commit():
                L = []
                for p in self.partitions:
                    self.memory[topic_partition_str(p.topic, p.partition)].pop(p.offset - 1)
                    L.append({'topic': p.topic, 'partition': p.partition, 'offset': p.offset - 1})
                self.last = self._emit(L)
                self.partitions = []
            yield self.last
            yield gen.sleep(self.interval)


def auto_shutdown(
    source,
    got_error: bool = True,
    got_dask: bool = True,
    graceful_offset: int = 3600,
    graceful_polling: int = 1800,
    interval: int = 5,
    sleep_before_shutdown: int = 2,
    auto_expired: int = 10800,
):
    """

    Parameters
    ----------
    source: waterhealer.core.Stream
        waterhealer.core.Stream object
    got_error: bool, (default=True)
        if dask streaming got an exception, automatically shutdown the script.
    got_dask: bool, (default=True)
        if True, will check Dask status, will shutdown if client status not in ('running','closing','connecting','newly-created').
    graceful_offset: int, (default=3600)
        automatically shutdown the script if water-healer not updated any offsets after `graceful_offset` period.
        To disable it, set it to 0.
    graceful_polling: int, (default=1800)
        automatically shutdown the script if kafka consumer not polling after `graceful_polling` period.
        To disable it, set it to 0.
    interval: int, (default=5)
        check heartbeat every `interval`.
    sleep_before_shutdown: int, (defaut=2)
        sleep (second) before shutdown.
    auto_expired: int, (default=10800)
        auto shutdown after `auto_expired`. Set to `0` to disable it.
        This is to auto restart the python script to flush out memory leaks.
    """
    start_time = datetime.now()

    def get_client(return_exception=False):
        error = 'no error'
        try:
            from distributed.client import default_client

            client = default_client()
        except Exception as e:
            error = str(e)
            logger.error(e)
            client = None

        if return_exception:
            return client, error
        else:
            return client

    def disconnect_client(client, timeout=10):
        try:
            client.close(timeout=timeout)
            return True
        except Exception as e:
            e = str(e)
            logger.error(e)
            return False

    def check_error():
        client = get_client()
        error_dask = False
        if client:
            try:
                for key, v in client.futures.copy().items():
                    if (
                        'dict' not in key
                        and 'str' not in key
                        and 'byte' not in key
                        and 'json_loads' not in key
                        and v.status == 'error'
                    ):
                        error_dask = True
                        break

            except Exception as e:
                print(e)

        source_ = get_source(source)

        if source_.error or error_dask:
            error = 'shutting down caused by exception.'
            logger.error(error)
            source_.stop()
            if error_dask:
                disconnect_client(client)
            time.sleep(sleep_before_shutdown)
            os._exit(1)

    def check_dask():
        client, error = get_client(return_exception=True)
        got_error, error_dask = False, False
        if client:
            try:
                if client.status not in (
                    'running',
                    'closing',
                    'connecting',
                    'newly-created',
                ):
                    got_error = True
                    error_dask = True
            except Exception as e:
                print(e)

        if 'No clients found' in error:
            got_error = True

        if got_error:
            source_ = get_source(source)
            source_.stop()
            if error_dask:
                disconnect_client(client)
            error = 'shutting down caused by disconnected dask cluster.'
            logger.error(error)

            time.sleep(sleep_before_shutdown)
            os._exit(1)

    def check_graceful_offset():
        if (datetime.now() - LAST_UPDATED).seconds > graceful_offset:
            error = 'shutting down caused by graceful offset timeout.'
            logger.error(error)

            source_ = get_source(source)
            source_.stop()
            client = get_client()
            if client:
                disconnect_client(client)
            time.sleep(sleep_before_shutdown)
            os._exit(1)

    def check_graceful_polling():

        source_ = get_source(source)
        if (datetime.now() - source_.last_poll).seconds > graceful_polling:
            error = 'shutting down caused by graceful polling timeout.'
            logger.error(error)
            source_.stop()
            client = get_client()
            if client:
                disconnect_client(client)
            time.sleep(sleep_before_shutdown)
            os._exit(1)

    while True:
        check_error()

        if got_dask:
            check_dask()

        if graceful_offset:
            check_graceful_offset()

        if graceful_polling:
            check_graceful_polling()

        if auto_expired > 0:
            if (datetime.now() - start_time).seconds > auto_expired:
                error = 'shutting down caused by expired.'
                logger.error(error)
                time.sleep(sleep_before_shutdown)
                os._exit(1)

        time.sleep(interval)
