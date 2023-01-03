from tornado import gen
from datetime import datetime
from .core import Stream, convert_interval, logger
from .function import (
    topic_partition_str,
    str_topic_partition,
    get_db,
    get_error,
    get_source,
    to_bool,
)
import confluent_kafka as ck
import os
import time

LAST_UPDATED = datetime.now()
COMMIT_COUNT = 0


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
        self.last_emit_id = None
        _, self.consumer, self.db = get_db(upstream)

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, row, emit_id=None, who=None):
        if self.last_emit_id is None:
            self.last_emit_id = emit_id
        logger.debug(f'update, {row}')
        if len(row) == 2:
            if isinstance(row[0], dict):
                partition = row[0].get('partition', -1)
                offset = row[0].get('offset')
                topic = row[0].get('topic')
                topic_partition = topic_partition_str(topic, partition)
                m = self.db.get(topic_partition)

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
        global LAST_UPDATED, COMMIT_COUNT
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
                COMMIT_COUNT += 1
            except Exception as e:
                if self.ignore:
                    logger.warning(str(e))
                    reason = str(e)
                else:
                    logger.exception(e)
                    raise
        logger.info(f'healing successful: {success}, {now}')
        return success

    def get_source_consumer(self):
        if self.consumer is None:
            _, self.consumer, self.db = get_db(self.upstreams[0])
        return self.consumer is not None

    @gen.coroutine
    def cb(self):
        while True:
            started = self.get_source_consumer()
            if started:
                delete_from_db = []
                for topic_partition in self.db.keys():
                    topic, partition = str_topic_partition(topic_partition)
                    if len(self.db[topic_partition]):
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

                        for offset in sorted(self.db[topic_partition].keys()):
                            if self.db[topic_partition][offset]:
                                if offset < current_offset:
                                    logger.warning(
                                        f'topic partition: {topic_partition}, offset: {offset}, current offset: {current_offset}, current offset higher than message offset')
                                    delete_from_db.append(
                                        ck.TopicPartition(topic, partition, offset + 1)
                                    )
                                else:
                                    self.partitions.append(
                                        ck.TopicPartition(topic, partition, offset + 1)
                                    )
                            else:
                                break

                if self.commit():
                    L = []
                    for p in self.partitions:
                        self.db[topic_partition_str(p.topic, p.partition)].pop(p.offset - 1)
                        L.append({'topic': p.topic, 'partition': p.partition, 'offset': p.offset - 1})
                    self.last = self._emit(L, emit_id=self.last_emit_id)
                    self.partitions = []
                    self.last_emit_id = None

                for p in delete_from_db:
                    self.db[topic_partition_str(p.topic, p.partition)].pop(p.offset - 1)

            yield self.last
            yield gen.sleep(self.interval)


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

        if auto_expired > 0 and (datetime.now() - start_time).seconds > auto_expired:
            error = 'shutting down caused by expired.'
            logger.error(error)
            time.sleep(sleep_before_shutdown)
            os._exit(1)

        if max_total_commit > 0 and COMMIT_COUNT > max_total_commit:
            error = 'shutting down caused by max total commit.'
            logger.error(error)
            time.sleep(sleep_before_shutdown)
            os._exit(1)

        time.sleep(interval)
