from tornado import gen
import confluent_kafka as ck
from typing import Tuple, Callable
from datetime import datetime
import streamz
import asyncio
import logging
import os
import time
from waterhealer.function import topic_partition_str
from apscheduler.schedulers.background import BackgroundScheduler

LAST_UPDATED = datetime.now()
LAST_INTERVAL = datetime.now()
PARTITIONS, COMMITS, REASONS = [], [], []
logger = logging.getLogger()


@gen.coroutine
def _healing(
    row, consumer, memory, ignore = False, asynchronous = True, interval = False
):
    global LAST_UPDATED, LAST_INTERVAL, PARTITIONS, COMMITS, REASONS

    if not consumer:
        raise Exception('consumer must not None')

    def commit(partitions):
        if len(partitions):
            try:
                consumer.commit(
                    offsets = partitions, asynchronous = asynchronous
                )
                success = True
                LAST_UPDATED = datetime.now()
                reason = 'success'
            except Exception as e:
                success = False
                if ignore:
                    logging.warning(str(e))
                    reason = str(e)
                else:
                    logger.exception(e)
                    raise

            return success, reason
        else:
            return False, 'length partitions is 0'

    success = False
    if not isinstance(row[0], dict):
        reason = 'invalid uuid'
        partition = None
        offset = None
        topic = None
    else:
        partition = row[0].get('partition', '')
        offset = row[0].get('offset')
        topic = row[0].get('topic')
        topic_partition = topic_partition_str(topic, partition)
        m = memory.get(topic_partition, [])

        if partition is None or offset is None or topic is None:
            reason = 'invalid uuid'
        elif not m:
            reason = 'invalid topic and partition'
        elif offset not in m:
            reason = 'offset expired or not exist'
        else:
            m[offset] = True
            offsets = list(m.keys())
            offsets.sort()

            partitions, reasons, commits = [], [], []

            if (offsets.index(offset) == 0 and interval == 0) or interval > 0:
                while True:
                    try:
                        low_offset, high_offset = consumer.get_watermark_offsets(
                            ck.TopicPartition(topic, partition)
                        )
                        current_offset = consumer.committed(
                            [ck.TopicPartition(topic, partition)]
                        )[0].offset
                        break
                    except Exception as e:
                        print(e)

                for o in offsets:
                    if m[o]:
                        if current_offset >= high_offset:
                            reason = 'current offset already same as high offset, skip'
                        elif o < current_offset:
                            reason = 'current offset higher than message offset, skip'
                        else:
                            partitions.append(
                                ck.TopicPartition(topic, partition, o + 1)
                            )
                            reason = f'committed topic: {topic} partition: {partition} offset: {o}'
                        reasons.append(reason)
                        commits.append((topic_partition, o))
                    else:
                        break

            if interval > 0:
                PARTITIONS.extend(partitions)
                COMMITS.extend(commits)
                REASONS.extend(reasons)

                if (datetime.now() - LAST_INTERVAL).seconds > interval:
                    LAST_INTERVAL = datetime.now()
                    success, reason = commit(PARTITIONS)
                    if success:
                        reason = REASONS
                        offset = COMMITS
                        commits = COMMITS
                else:
                    reason = 'waiting for interval to update offsets'
                    success = False
            else:
                success, reason = commit(partitions)
                if success:
                    reason = reasons
                    offset = commits
                else:
                    reason = 'current offset is not the smallest, waiting for smallest'
                    success = False

            if success:
                for o in commits:
                    topic_partition, o = o
                    memory[topic_partition].pop(o)

                if interval > 0:
                    PARTITIONS, COMMITS, REASONS = [], [], []

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
    source: Callable = None,
    ignore: bool = False,
    asynchronous: bool = True,
    interval: int = 10,
):
    """

    Parameters
    ----------
    row: tuple
        (uuid, value)
    source: waterhealer object
        waterhealer object to connect with kafka.
    ignore: bool, (default=False)
        if True, ignore any failed update offset.
    asynchronous: bool, (default=True)
        if True, it will update kafka offset async manner.
    interval: int, (default=10)
        Every interval, will update batch of kafka offsets. Set 0 to update every healing process.
    """

    if type(source) == streamz.dask.starmap:
        consumer = source.upstreams[0].upstreams[0].consumer
        memory = source.upstreams[0].upstreams[0].memory
    elif type(source) == streamz.core.starmap:
        consumer = source.upstreams[0].consumer
        memory = source.upstreams[0].memory
    else:
        consumer = source.consumer
        memory = source.memory
    result = _healing(
        row = row,
        consumer = consumer,
        memory = memory,
        ignore = ignore,
        asynchronous = asynchronous,
        interval = interval,
    )

    return result.result()


def healing_batch(
    rows: Tuple[Tuple],
    source: Callable = None,
    ignore: bool = False,
    asynchronous: bool = True,
):
    """

    Parameters
    ----------
    row: tuple of tuple
        ((uuid, value),)
    source: waterhealer object
        waterhealer object to connect with kafka.
    ignore: bool, (default=False)
        if True, ignore any failed update offset.
    asynchronous: bool, (default=True)
        if True, it will update kafka offset async manner.
    """
    if type(source) == streamz.dask.starmap:
        consumer = source.upstreams[0].upstreams[0].consumer
        memory = source.upstreams[0].upstreams[0].memory
    elif type(source) == streamz.core.starmap:
        consumer = source.upstreams[0].consumer
        memory = source.upstreams[0].memory
    else:
        consumer = source.consumer
        memory = source.memory

    @gen.coroutine
    def loop():
        r = yield [
            _healing(
                row = row,
                consumer = consumer,
                memory = memory,
                ignore = ignore,
                asynchronous = asynchronous,
            )
            for row in rows
        ]
        return r

    result = loop()

    return result.result()


def auto_shutdown(
    source,
    got_error: bool = True,
    got_dask: bool = True,
    graceful: int = 1800,
    interval: int = 5,
    sleep_before_shutdown: int = 15,
    debug: bool = True,
):
    """

    Parameters
    ----------
    source: source object
        async streamz source.
    got_error: bool, (default=True)
        if dask streaming got an exception, automatically shutdown the script.
    got_dask: bool, (default=True)
        if True, will check Dask status, will shutdown if client status not in ('running','closing','connecting','newly-created').
    graceful: int, (default=1800)
        automatically shutdown the script if water-healer not updated any offsets after `graceful` period. 
        To off it, set it to 0.
    interval: int, (default=5)
        check heartbeat every `interval`. 
    sleep_before_shutdown: int, (defaut=15)
        sleep (second) before shutdown.
    debug: bool, (default=False)
        If True, will print logging.error if got any error.
    """

    scheduler = BackgroundScheduler()
    start_time = datetime.now()

    def get_client():
        try:
            from distributed.client import default_client

            client = default_client()
        except Exception as e:
            logger.error(str(e))
            client = None
        return client

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

        if source.error or error_dask:
            if debug:
                logger.error('shutting down caused by exception.')
            source.destroy()
            source.stop()
            if error_dask:
                client.close()
            time.sleep(sleep_before_shutdown)
            os._exit(1)

    def check_dask():
        client = get_client()
        if client:
            try:
                if client.status not in (
                    'running',
                    'closing',
                    'connecting',
                    'newly-created',
                ):
                    if debug:
                        logger.error(
                            'shutting down caused by disconnected dask cluster.'
                        )
                    source.destroy()
                    source.stop()
                    client.close()
                    time.sleep(sleep_before_shutdown)
                    os._exit(1)
            except Exception as e:
                print(e)

    def check_graceful():
        if (datetime.now() - LAST_UPDATED).seconds > graceful:
            if debug:
                logger.error('shutting down caused by graceful timeout.')
            source.destroy()
            source.stop()
            client = get_client()
            if client:
                client.close()
            time.sleep(sleep_before_shutdown)
            os._exit(1)

    def check():
        check_error()

        if got_dask:
            check_dask()

        if graceful:
            check_graceful()

    if got_error:
        scheduler.add_job(check, 'interval', seconds = interval)

    scheduler.start()
