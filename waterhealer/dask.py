from __future__ import absolute_import, division, print_function

from operator import getitem

from tornado import gen

from dask.compatibility import apply
from distributed.client import default_client
from streamz.dask import DaskStream
from . import core


@DaskStream.register_api()
class partition_time(DaskStream, core.partition_time):
    pass
