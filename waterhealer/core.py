"""
Copyright (c) 2017, Continuum Analytics, Inc. and contributors
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

Neither the name of Continuum Analytics nor the names of any contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
THE POSSIBILITY OF SUCH DAMAGE.
"""

from tornado import gen
from streamz.core import Stream
from datetime import datetime
import time


@Stream.register_api()
class partition_time(Stream):
    """
    Partition stream into tuples if waiting time expired.

    Examples
    --------
    >>> source = Stream()
    >>> source.partition_time(3).sink(print)
    >>> for i in range(10):
    ...     source.emit(i)
    (0, 1, 2)
    (3, 4, 5)
    (6, 7, 8)
    """

    _graphviz_shape = 'diamond'

    def __init__(self, upstream, n, **kwargs):
        self.n = n
        self.buffer = []
        self.time = None
        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who = None):
        self.buffer.append(x)
        if self.time is None:
            self.time = datetime.now()

        if (datetime.now() - self.time).seconds >= self.n:
            self.time = None
            result, self.buffer = self.buffer, []
            return self._emit(tuple(result))
        else:
            return []


@Stream.register_api()
class foreach_map(Stream):
    """ 
    Apply a function to every element in a tuple in the stream.

    Parameters
    ----------
    func: callable
    *args :
        The arguments to pass to the function.
    **kwargs:
        Keyword arguments to pass to func
    Examples
    --------
    >>> source = Stream()
    >>> source.foreach_map(lambda x: 2*x).sink(print)
    >>> for i in range(3):
    ...     source.emit((i, i))
    (0, 0)
    (2, 2)
    (4, 4)
    """

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name = stream_name)

    def update(self, x, who = None):
        try:
            result = (self.func(e, *self.args, **self.kwargs) for e in x)
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result)


@Stream.register_api()
class foreach_async(Stream):
    """ 
    Apply a function to every element in a tuple in the stream, in async manner.

    Parameters
    ----------
    func: callable
    *args :
        The arguments to pass to the function.
    **kwargs:
        Keyword arguments to pass to func

    Examples
    --------
    >>> source = Stream()
    >>> source.foreach_async(lambda x: 2*x).sink(print)
    >>> for i in range(3):
    ...     source.emit((i, i))
    (0, 0)
    (2, 2)
    (4, 4)
    """

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name = stream_name)

    def update(self, x, who = None):
        try:

            @gen.coroutine
            def function(e, *args, **kwargs):
                return self.func(e, *args, **kwargs)

            @gen.coroutine
            def loop():
                r = yield [function(e, *self.args, **self.kwargs) for e in x]
                return r

            result = loop().result()
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result)
