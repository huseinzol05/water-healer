import time
import sys
from typing import Any, Union
from threading import RLock, Thread
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import ordereddict
from collections import defaultdict
import os

MAXLEN_MEMORY = int(os.environ.get('MAXLEN_MEMORY', 10_000_000))
MAXAGE_MEMORY = int(os.environ.get('MAXAGE_MEMORY', 3600))

# https://github.com/KyRionY/expiringdict/blob/master/expiringdict/__init__.py


class ExpiringDict(OrderedDict):
    def __init__(
        self,
        max_len: int,
        max_age_seconds: int,
        items=None,
        auto_refresh: bool = False,
        auto_expired: bool = True,
        expired_sleep: float = 0.1,
        **kwargs,
    ):

        if not self.__is_instance_of_expiring_dict(items):
            self.__assertions(max_len, max_age_seconds)

        OrderedDict.__init__(self)
        self.max_len = max_len
        self.max_age = max_age_seconds
        self.lock = RLock()
        self.auto_refresh = auto_refresh
        self.expired_sleep = expired_sleep

        if sys.version_info >= (3, 5):
            self._safe_keys = lambda: list(self.keys())
        else:
            self._safe_keys = self.keys

        if items is not None:
            if self.__is_instance_of_expiring_dict(items):
                self.__copy_expiring_dict(max_len, max_age_seconds, items)
            elif self.__is_instance_of_dict(items):
                self.__copy_dict(items)
            elif self.__is_reduced_result(items):
                self.__copy_reduced_result(items)

            else:
                raise ValueError('can not unpack items')

        if auto_expired:
            self.thread = Thread(target=self._expiring_key, args=())
            self.thread.setDaemon(True)
            self.thread.start()

    def _expiring_key(self):
        while True:
            for key in self._safe_keys():
                try:
                    self.__getitem__(key, expired_check=True)
                except KeyError:
                    pass

            time.sleep(self.expired_sleep)

    def __contains__(self, key):
        """
        Return True if the dict has a key, else return False. 
        """
        try:
            with self.lock:
                item = OrderedDict.__getitem__(self, key)
                if time.time() - item[1] < self.max_age:
                    return True
                else:
                    del self[key]
        except KeyError:
            pass
        return False

    def __getitem__(self, key, with_age=False, expired_check=False):
        """
        Return the item of the dict.
        Raises a KeyError if key is not in the map.
        """
        with self.lock:
            item = OrderedDict.__getitem__(self, key)
            item_age = time.time() - item[1]
            if item_age < self.max_age:
                if self.auto_refresh and not expired_check:
                    set_time = time.time()
                    OrderedDict.__setitem__(self, key, (item[0], set_time))

                if with_age:
                    return item[0], item_age
                else:
                    return item[0]
            else:
                del self[key]
                raise KeyError(key)

    def __setitem__(self, key, value, set_time=None):
        """
        Set d[key] to value. 
        """
        with self.lock:
            if len(self) == self.max_len:
                try:
                    self.popitem(last=False)
                except KeyError:
                    pass
            if set_time is None:
                set_time = time.time()
            OrderedDict.__setitem__(self, key, (value, set_time))

    def pop(self, key, default=None):
        """
        Get item from the dict and remove it.
        Return default if expired or does not exist. Never raise KeyError.
        """
        with self.lock:
            try:
                item = OrderedDict.__getitem__(self, key)
                del self[key]
                return item[0]
            except KeyError:
                return default

    def ttl(self, key):
        """
        Return TTL of the `key` (in seconds).
        Returns None for non-existent or expired keys.
        """
        key_value, key_age = self.get(key, with_age=True)
        if key_age:
            key_ttl = self.max_age - key_age
            if key_ttl > 0:
                return key_ttl
        return None

    def get(self, key, default=None, with_age=False):
        """
        Return the value for key if key is in the dictionary, else default.
        """
        try:
            return self.__getitem__(key, with_age)
        except KeyError:
            if with_age:
                return default, None
            else:
                return default

    def items(self):
        """ Return a copy of the dictionary's list of (key, value) pairs. """
        r = []
        for key in self._safe_keys():
            try:
                r.append((key, self[key]))
            except KeyError:
                pass
        return r

    def items_with_timestamp(self):
        """ Return a copy of the dictionary's list of (key, value, timestamp) triples. """
        r = []
        for key in self._safe_keys():
            try:
                r.append((key, OrderedDict.__getitem__(self, key)))
            except KeyError:
                pass
        return r

    def values(self):
        """
        Return a copy of the dictionary's list of values.
        See the note for dict.items().
        """
        r = []
        for key in self._safe_keys():
            try:
                r.append(self[key])
            except KeyError:
                pass
        return r

    def fromkeys(self):
        """
        Create a new dictionary with keys from seq and values set to value.
        """
        raise NotImplementedError()

    def iteritems(self):
        """
        Return an iterator over the dictionary's (key, value) pairs.
        """
        raise NotImplementedError()

    def itervalues(self):
        """
        Return an iterator over the dictionary's values.
        """
        raise NotImplementedError()

    def viewitems(self):
        """
        Return a new view of the dictionary's items ((key, value) pairs).
        """
        raise NotImplementedError()

    def viewkeys(self):
        """
        Return a new view of the dictionary's keys.
        """
        raise NotImplementedError()

    def viewvalues(self):
        """
        Return a new view of the dictionary's values.
        """
        raise NotImplementedError()

    def __reduce__(self):
        reduced = self.__class__, (self.max_len, self.max_age, ('reduce_result', self.items_with_timestamp()))
        return reduced

    def __assertions(self, max_len, max_age_seconds):
        self.__assert_max_len(max_len)
        self.__assert_max_age_seconds(max_age_seconds)

    @staticmethod
    def __assert_max_len(max_len):
        assert max_len >= 1

    @staticmethod
    def __assert_max_age_seconds(max_age_seconds):
        assert max_age_seconds >= 0

    @staticmethod
    def __is_reduced_result(items):
        if len(items) == 2 and items[0] == 'reduce_result':
            return True
        return False

    @staticmethod
    def __is_instance_of_expiring_dict(items):
        if items is not None:
            if isinstance(items, ExpiringDict):
                return True
        return False

    @staticmethod
    def __is_instance_of_dict(items):
        if isinstance(items, dict):
            return True
        return False

    def __copy_expiring_dict(self, max_len, max_age_seconds, items):
        if max_len is not None:
            self.__assert_max_len(max_len)
            self.max_len = max_len
        else:
            self.max_len = items.max_len

        if max_age_seconds is not None:
            self.__assert_max_age_seconds(max_age_seconds)
            self.max_age = max_age_seconds
        else:
            self.max_age = items.max_age

        [self.__setitem__(key, value, set_time) for key, (value, set_time) in items.items_with_timestamp()]

    def __copy_dict(self, items):
        [self.__setitem__(key, value) for key, value in items.items()]

    def __copy_reduced_result(self, items):
        [self.__setitem__(key, value, set_time) for key, (value, set_time) in items[1]]


class Database:
    def __init__(
        self,
        maxlen_memory: int = MAXLEN_MEMORY,
        maxage_memory: int = MAXAGE_MEMORY,
        **kwargs,
    ):
        """
        Parameters
        ----------
        maxlen_memory: int, optional (default=int(os.environ.get('MAXLEN_MEMORY', 10_000_000)))
            max size of memory.
        maxage_memory: int, optional (default=int(os.environ.get('MAXAGE_MEMORY', 3600)))
            max age of memory in term of seconds.
        """
        self.maxlen_memory = maxlen_memory
        self.maxage_memory = maxage_memory
        self.db = defaultdict(
            lambda: ExpiringDict(
                max_len=maxlen_memory, max_age_seconds=maxage_memory, **kwargs
            )
        )

    def __contains__(self, key):
        """
        Return True if the dict has a key, else return False.
        key in d
        """
        return self.db.__contains__(key)

    def __getitem__(self, key):
        """
        Return the item of the dict.
        Raises a KeyError if key is not in the map.
        d[key]
        """
        return self.db.__getitem__(key)

    def pop(self, key, default=None):
        return self.db.pop(key, default)

    def get(self, key, default=None):
        return self.db.get(key, default)

    def keys(self):
        return self.db.keys()
