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
