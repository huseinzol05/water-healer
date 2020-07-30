import functools
import logging

logger = logging.getLogger()


def topic_partition_offset_str(topic, partition, offset):
    return f'{topic}<!>{partition}<!>{offset}'


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


def insert_unique_uuid(row, uuids):
    try:
        partition = row.get('partition')
        offset = row.get('offset')
        topic = row.get('topic')
        uuids.add(topic_partition_offset_str(topic, partition, offset))
    except Exception as e:
        pass


def check_leakage(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        before_uuid = set()
        after_uuid = set()

        def insert(row, uuid):
            if isinstance(row, tuple):
                insert_unique_uuid(row[0], uuid)
            elif isinstance(row, dict):
                insert_unique_uuid(row.get('uuid'), uuid)

        for arg in args:
            if isinstance(arg, list) or isinstance(arg, tuple):
                for row in arg:
                    insert(row, before_uuid)
            else:
                insert(arg, before_uuid)

        value = func(*args, **kwargs)

        if isinstance(value, list) or isinstance(value, tuple):
            for arg in value:
                if isinstance(arg, list) or isinstance(arg, tuple):
                    for row in arg:
                        insert(row, after_uuid)
                else:
                    insert(arg, after_uuid)
        else:
            insert(value, after_uuid)

        not_in = before_uuid - after_uuid

        if len(not_in):
            not_in = [str_topic_partition_offset(i) for i in not_in]
            not_in = list(filter(None, not_in))
            before_uuid = list(before_uuid)
            after_uuid = list(after_uuid)

            func_name = f'{func.__module__}.{func.__name__}'

            message = {'reason': f'{func_name} leaking', 'not_in_uuid': not_in}

            logging.error(message['reason'], extra = message)

            # We did like this because when we distributed to dask worker, it is very hard to get `not_in` uuids.
            raise Exception(json.dumps(message))

        return value

    return wrapper_decorator
