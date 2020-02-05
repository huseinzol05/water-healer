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
import confluent_kafka as ck
import logging
from expiringdict import ExpiringDict

logger = logging.getLogger(__name__)


class Source(Stream):
    _graphviz_shape = 'doubleoctagon'

    def __init__(self, **kwargs):
        self.stopped = True
        super(Source, self).__init__(**kwargs)

    def stop(self):  # pragma: no cover
        # fallback stop method - for poll functions with while not self.stopped
        if not self.stopped:
            self.stopped = True


def healing(
    row,
    stream = None,
    callback = None,
    ignore = False,
    silent = False,
    **kwargs,
):
    """

    Parameters
    ----------
    row: tuple
        (uuid, value)
    stream: waterhealer object
        waterhealer object to connect with kafka
    callback: function
        callback function after successful update
    ignore: bool, (default=False)
        if True, if uuid not in memory, it will not stop. 
        This is useful when you do batch processing, you might delete some rows after did some unique operations.
    silent: bool, (default=False)
        if True, will not print any log in this function.
    **kwargs:
        Keyword arguments to pass to callback.

    """
    if not stream:
        raise Exception('stream must not None')

    if not stream.memory.get(row[0]):
        msg = 'message id not in stream memory'
        if ignore:
            logger.warning(msg)
            return {'id': row[0], 'success': False}
        else:
            raise Exception(msg)

    c = stream.memory[row[0]]
    low_offset, high_offset = stream.consumer.get_watermark_offsets(
        ck.TopicPartition(c['topic'], c['partition'])
    )
    current_offset = stream.consumer.committed(
        [ck.TopicPartition(c['topic'], c['partition'])]
    )[0].offset

    success = False
    reason = 'committed %s %d' % (c['topic'], c['partition'])

    if current_offset >= high_offset:
        reason = 'current offset already same as high offset, skip'
    elif c['offset'] < current_offset:
        reason = 'current offset higher than message offset, skip'
    else:
        try:
            stream.consumer.commit(
                offsets = [
                    ck.TopicPartition(
                        c['topic'], c['partition'], c['offset'] + 1
                    )
                ],
                asynchronous = False,
            )
            success = True
        except Exception as e:
            if ignore:
                logging.warning(str(e))
            else:
                logger.exception(e)
                raise

        stream.memory.pop(row[0], None)

    if not silent:
        logging.info(reason)

    if callback:
        callback(row[1], **kwargs)
    return {'id': row[0], 'success': success, 'reason': reason}


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
    maxlen_memory: int, (100000)
        max size of memory (dict). Oldest automatically delete.
    maxage_memory: int, (1800)
        max age for each values in memory (dict). This will auto delete if the value stay too long in the memory.
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages
    start: bool, (False)
        Whether to start polling upon instantiation

    """

    def __init__(
        self,
        topics,
        consumer_params,
        maxlen_memory = 100_000,
        maxage_memory = 1800,
        poll_interval = 0.1,
        start = False,
        **kwargs,
    ):
        self.cpars = consumer_params
        self.cpars['enable.auto.commit'] = False
        self.consumer = None
        self.topics = topics
        self.poll_interval = poll_interval
        super(from_kafka, self).__init__(ensure_io_loop = True, **kwargs)
        self.stopped = True
        if start:
            self.start()
        self.memory = ExpiringDict(
            max_len = maxlen_memory, max_age_seconds = maxage_memory
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
                id_val = f'{partition}-{offset}-{topic}'
                self.memory[id_val] = {
                    'partition': partition,
                    'offset': offset,
                    'topic': topic,
                }
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
