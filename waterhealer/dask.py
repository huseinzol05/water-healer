from __future__ import absolute_import, division, print_function

from operator import getitem

from tornado import gen

from dask.compatibility import apply
from distributed.client import default_client
from streamz.dask import DaskStream
from . import core


@DaskStream.register_api()
class map(DaskStream):
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, upstream)

    def update(self, x, who = None):
        client = default_client()
        result = client.submit(
            self.func, x, *self.args, **self.kwargs, pure = False
        )
        return self._emit(result)


@DaskStream.register_api()
class starmap(DaskStream):
    def __init__(self, upstream, func, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs

        DaskStream.__init__(self, upstream, stream_name = stream_name)

    def update(self, x, who = None):
        client = default_client()
        result = client.submit(apply, self.func, x, self.kwargs, pure = False)
        return self._emit(result)


@DaskStream.register_api()
class partition_time(DaskStream, core.partition_time):
    pass
