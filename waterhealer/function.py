from json_logging import BaseJSONFormatter, _sanitize_log_msg
import traceback
import inspect
import sys

# https://github.com/bobbui/json-logging-python/blob/59dce199289fee3300ee4f44acce29b362b75cd6/json_logging/util.py#L87
if hasattr(sys, '_getframe'):
    def currentframe(_no_of_go_up_level): return sys._getframe(_no_of_go_up_level)
else:
    # pragma: no cover
    # noinspection PyBroadException
    def currentframe(_no_of_go_up_level):
        """
        Return the frame object for the caller's stack frame.
        """
        try:
            raise Exception
        except Exception:
            return sys.exc_info()[_no_of_go_up_level - 1].tb_frame.f_back


def str_topic_partition_offset(string):
    if isinstance(string, str):
        splitted = string.split('<!>')
        if len(splitted) != 3:
            r = None
        else:
            r = {
                'topic': splitted[0],
                'partition': splitted[1],
                'offset': splitted[2],
            }
    else:
        r = None
    return r


def topic_partition_offset_str(topic, partition, offset):
    return f'{topic}<!>{partition}<!>{offset}'


def topic_partition_str(topic, partition):
    return f'{topic}<!>{partition}'


def str_topic_partition(string):
    topic, partition = string.split('<!>')
    return topic, int(partition)


def to_bool(value):
    value = value.lower()
    if value in (1, 'true'):
        return True
    else:
        return False


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


def get_stream(source):

    for upstream in source.upstreams:
        if upstream is None:
            return source
        return get_stream(upstream)
    return source


class WaterHealerFormatter(BaseJSONFormatter):
    """
    Formatter for water-healer with emit id.
    """

    def get_exc_fields(self, record):
        if record.exc_info:
            exc_info = self.format_exception(record.exc_info)
        else:
            exc_info = record.exc_text
        return {
            'exc_info': exc_info,
            'filename': record.filename,
        }

    @classmethod
    def format_exception(cls, exc_info):
        return ''.join(traceback.format_exception(*exc_info)) if exc_info else ''

    def _format_log_object(self, record, request_util):
        json_log_object = super(WaterHealerFormatter, self)._format_log_object(record, request_util)

        json_log_object.update({
            'msg': _sanitize_log_msg(record),
            'type': 'log',
            'logger': record.name,
            'thread': record.threadName,
            'level': record.levelname,
            'module': record.module,
            'line_no': record.lineno,
        })

        if hasattr(record, 'props') and isinstance(record.props, dict):
            json_log_object.update(record.props)

        if 'emit_id' not in json_log_object:
            module = inspect.getmodule(inspect.currentframe().f_back)
            no_of_go_up_level = 11
            f = currentframe(no_of_go_up_level)
            function_name = None
            emit_id = None

            while True:
                f_locals = f.f_locals
                if record.threadName.startswith('Dask-') and 'key' in f_locals:
                    function_name, emit_id, _ = f_locals['key'].split('--')

                if 'emit_id' in f_locals or ('self' in f_locals and hasattr(f_locals['self'], 'last_emit_id')):
                    emit_id = f_locals.get('emit_id', None) or getattr(f_locals['self'], 'last_emit_id', None)
                    function_name = type(f_locals['self']).__name__
                    if hasattr(f_locals['self'], 'func'):
                        function_name = f"{function_name}.{f_locals['self'].func.__name__}"
                    break

                if f.f_back is not None:
                    f = f.f_back
                else:
                    break

            json_log_object.update({
                'function_name': function_name,
                'emit_id': emit_id
            })

        return json_log_object
