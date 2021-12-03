from redis_collections import Dict
from redis import StrictRedis


class DictHealer:
    def __init__(self, redis: StrictRedis, consumer: str, key: str = 'water-healer'):
        self.redis = redis
        self.consumer = consumer
        self.dict = Dict(redis=redis, key=key)
        self.list_consumers = self.dict.get(consumer, '').split(',')
        self.consumers = {c: Dict(redis=redis, key=f'{consumer}-{c}') for c in self.list_consumers}

    def __contains__(self, key):
        """
        Return True if the dict has a key, else return False.
        key in d
        """
        return key in self.consumers

    def __getitem__(self, key):
        """
        Return the item of the dict.
        Raises a KeyError if key is not in the map.
        d[key]
        """
        if key not in self.consumers:
            self.list_consumers = self.list_consumers + [key]
            self.dict[self.consumer] = ','.join(self.list_consumers)
            self.consumers[key] = Dict(redis=self.redis, key=f'{self.consumer}-{key}')
        return self.consumers[key]

    def pop(self, key, default=None):
        if self.__contains__(key):
            value = self.consumers.pop(key)
            self.list_consumers = list(self.consumers.keys())
            self.dict[self.consumer] = ','.join(self.list_consumers)
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
        return [c for c in self.list_consumers if len(c)]
