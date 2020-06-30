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

last_updated = datetime.now()
logger = logging.getLogger()


@gen.coroutine
def _healing(row, consumer, memory, ignore, asynchronous):
    global last_updated
    if not consumer:
        raise Exception('consumer must not None')
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
        m = memory.get(topic_partition_str(topic, partition), [])

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
            if offsets.index(offset) == 0:
                reasons, committed, successes = [], [], []
                for offset in offsets:
                    if m[offset]:
                        low_offset, high_offset = consumer.get_watermark_offsets(
                            ck.TopicPartition(topic, partition)
                        )
                        current_offset = consumer.committed(
                            [ck.TopicPartition(topic, partition)]
                        )[0].offset
                        reason = f'committed topic: {topic} partition: {partition} offset: {offset}'
                        success = False
                        if current_offset >= high_offset:
                            reason = 'current offset already same as high offset, skip'
                        elif offset < current_offset:
                            reason = 'current offset higher than message offset, skip'
                        else:
                            try:
                                consumer.commit(
                                    offsets = [
                                        ck.TopicPartition(
                                            topic, partition, offset + 1
                                        )
                                    ],
                                    asynchronous = asynchronous,
                                )
                                success = True
                                last_updated = datetime.now()
                            except Exception as e:
                                if ignore:
                                    logging.warning(str(e))
                                    reason = str(e)
                                else:
                                    logger.exception(e)
                                    raise
                        m.pop(offset)
                        reasons.append(reason)
                        committed.append(offset)
                        successes.append(success)
                    else:
                        break
                reason = reasons
                offset = committed
                success = successes
            else:
                reason = (
                    'current offset is not the smallest, waiting for smallest'
                )

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
    asynchronous: bool = False,
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
    asynchronous: bool, (default=False)
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
    result = _healing(
        row = row,
        consumer = consumer,
        memory = memory,
        ignore = ignore,
        asynchronous = asynchronous,
    )
    return result.result()


def healing_batch(
    rows: Tuple[Tuple],
    source: Callable = None,
    ignore: bool = False,
    asynchronous: bool = False,
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
    asynchronous: bool, (default=False)
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
    graceful: int = 1800,
    interval: int = 1,
    sleep_before_shutdown: int = 120,
    client = None,
    debug: bool = False,
):
    """

    Parameters
    ----------
    source: source object
        async streamz source.
    got_error: bool, (default=True)
        if streaming got an exception, automatically shutdown the script.
    graceful: int, (default=1800)
        automatically shutdown the script if water-healer not updated any offsets after `graceful` period. 
        To off it, set it to 0.
    interval: int, (default=1)
        check heartbeat every `interval`. 
    sleep_before_shutdown: int, (defaut=120)
        sleep (second) before shutdown.
    client: object, (default=None)
        should be a dask client, will shutdown if client status not in ('running','closing','connecting','newly-created').
    debug: bool, (default=False)
        If True, will print logging.error if got any error.
    """

    from apscheduler.schedulers.background import BackgroundScheduler

    scheduler = BackgroundScheduler()

    start_time = datetime.now()

    def check_error():
        try:
            for key, v in client.futures.copy().items():
                if (
                    'dict' not in key
                    and 'str' not in key
                    and 'byte' not in key
                    and 'json_loads' not in key
                    and v.status == 'error'
                ):
                    if debug:
                        logger.error(
                            f'shutting down caused by exception. Started auto_shutdown {str(start_time)}, ended {str(datetime.now())}'
                        )
                    time.sleep(sleep_before_shutdown)
                    os._exit(1)
        except:
            pass

    def check_graceful():
        if (datetime.now() - last_updated).seconds > graceful:
            if debug:
                logger.error(
                    f'shutting down caused by expired time. Started auto_shutdown {str(start_time)}, ended {str(datetime.now())}'
                )
            time.sleep(sleep_before_shutdown)
            os._exit(1)

    def check_dask():
        try:
            if client.status not in (
                'running',
                'closing',
                'connecting',
                'newly-created',
            ):
                if debug:
                    logger.error(
                        f'shutting down caused by disconnected dask cluster. Started auto_shutdown {str(start_time)}, ended {str(datetime.now())}'
                    )
                time.sleep(sleep_before_shutdown)
                os._exit(1)
        except:
            pass

    if got_error:
        scheduler.add_job(check_error, 'interval', seconds = interval)

    if graceful:
        scheduler.add_job(check_graceful, 'interval', seconds = interval)

    if client:
        scheduler.add_job(check_dask, 'interval', seconds = interval)

    scheduler.start()
