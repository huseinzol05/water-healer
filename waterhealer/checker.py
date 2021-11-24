import functools
from .core import logger
from .function import topic_partition_offset_str, str_topic_partition_offset


def insert_unique_uuid(row, uuids):
    try:
        partition = row.get('partition')
        offset = row.get('offset')
        topic = row.get('topic')
        uuids.add(topic_partition_offset_str(topic, partition, offset))
    except Exception as e:
        pass


def check_leakage(func):
    """
    Check leakage for kafka offsets, only support below structure of data,
    1. List[Tuple[uuid, data]]
    2. List[Dict[uuid, **data]]

    Parameters
    ----------
    func: callable
        python function.
    """
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
            for row in value:
                insert(row, after_uuid)
        else:
            insert(row, after_uuid)

        not_in = before_uuid - after_uuid

        if len(not_in):
            not_in = [str_topic_partition_offset(i) for i in not_in]
            not_in = list(filter(None, not_in))
            before_uuid = [str_topic_partition_offset(i) for i in before_uuid]
            after_uuid = [str_topic_partition_offset(i) for i in after_uuid]

            func_name = f'{func.__module__}.{func.__name__}'

            message = {'reason': f'{func_name} leaking',
                       'not_in': not_in,
                       'before': before_uuid,
                       'after': after_uuid,
                       'leaking': True}
            logger.exception(message)
            raise Exception(f'{func_name} leaking')

        return value

    return wrapper_decorator
