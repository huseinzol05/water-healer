def topic_partition_str(topic, partition):
    return f'{topic}<!>{partition}'


def str_topic_partition(string):
    topic, partition = string.split('<!>')
    return topic, int(partition)
