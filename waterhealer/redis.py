from .utils import pickle
from datetime import timedelta


class RedisCache:
    def __init__(self, r, max_age_seconds=3600):
        self.r = r
        self.max_age_seconds = max_age_seconds

    def __contains__(self, key):
        """
        Return True if the dict has a key, else return False.
        key in d
        """
        return self.r.exists(key)

    def __getitem__(self, key):
        """
        Return the item of the dict.
        Raises a KeyError if key is not in the map.
        d[key]
        """
        try:
            return pickle.loads(self.r[key])
        except Exception as e:
            print(e)

    def __setitem__(self, key, value):
        """
        Set d[key] to value.
        """
        value = pickle.dumps(value)
        self.r.setex(
            key, timedelta(seconds=self.max_age_seconds), value=value
        )

    def pop(self, key, default=None):
        if self.__contains__(key):
            value = self.__getitem__(key)
            self.r.delete(key)
            return value
        else:
            return default

    def get(self, key, default=None):
        if self.__contains__(key):
            return self.__getitem__(key)
        else:
            return default
