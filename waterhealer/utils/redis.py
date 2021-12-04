from redis_collections import Dict
from redis import StrictRedis


class DictHealer:
    def __init__(self, redis: StrictRedis, consumer: str, key: str = 'water-healer'):
        self.redis = redis
        self.consumer = consumer
        self.dict = Dict(redis=redis, key=key)
        self.list_partitions = self.dict.get(consumer, '').split(',')
        self.partitions = {p: Dict(redis=redis, key=f'{consumer}-{p}') for p in self.list_partitions}

    def __contains__(self, key):
        """
        Return True if the dict has a key, else return False.
        key in d
        """
        return key in self.partitions

    def __getitem__(self, key):
        """
        Return the item of the dict.
        Raises a KeyError if key is not in the map.
        d[key]
        """
        if key not in self.partitions:
            self.list_partitions = self.list_partitions + [key]
            self.dict[self.consumer] = ','.join(self.list_partitions)
            self.partitions[key] = Dict(redis=self.redis, key=f'{self.consumer}-{key}')
        return self.partitions[key]

    def pop(self, key, default=None):
        if self.__contains__(key):
            value = self.partitions.pop(key)
            self.list_partitions = list(self.partitions.keys())
            self.dict[self.consumer] = ','.join(self.list_partitions)
            self.redis.delete(f'{self.consumer}-{key}')
            return value
        else:
            return default

    def get(self, key, default=None):
        if self.__contains__(key):
            return self.__getitem__(key)
        else:
            return default

    def keys(self):
        return [p for p in self.list_partitions if len(p)]
