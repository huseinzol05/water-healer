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

from __future__ import absolute_import, division, print_function

from collections import deque
from datetime import timedelta
import functools
import logging
import six
import sys
import threading
from time import time
import weakref

import toolz
from tornado import gen
from tornado.locks import Condition
from tornado.ioloop import IOLoop
from tornado.queues import Queue
from distributed import Future
import os
import uuid
import time

try:
    from tornado.ioloop import PollIOLoop
except ImportError:
    PollIOLoop = None  # dropped in tornado 6.0

try:
    from distributed.client import default_client as _dask_default_client
except ImportError:  # pragma: no cover
    _dask_default_client = None

from collections import Iterable

from streamz.compatibility import get_thread_identity
from streamz.orderedweakset import OrderedWeakrefSet
from .function import to_bool, WaterHealerFormatter

no_default = '--no-default--'

_global_sinks = set()

_html_update_streams = set()

thread_state = threading.local()

_io_loops = []

ENABLE_CHECKPOINTING = to_bool(os.environ.get('ENABLE_CHECKPOINTING', 'true'))
ENABLE_EMIT_LOGGING = to_bool(os.environ.get('ENABLE_EMIT_LOGGING', 'true'))
ENABLE_JSON_LOGGING = to_bool(os.environ.get('ENABLE_JSON_LOGGING', 'false'))
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO')

logger = logging.getLogger()
logger.setLevel(LOGLEVEL)

if ENABLE_JSON_LOGGING:
    try:
        import json_logging

        json_logging.init_non_web(custom_formatter=WaterHealerFormatter, enable_json=True)
        logger.handlers = logging.getLogger('json_logging').handlers
    except BaseException:
        logger.warning('json-logging not installed. Please install it by `pip install json-logging` and try again.')

ENABLE_OPENTELEMETRY_JAEGER = to_bool(os.environ.get('ENABLE_OPENTELEMETRY_JAEGER', 'false'))
OPENTELEMETRY_SERVICE_NAME = os.environ.get('OPENTELEMETRY_SERVICE_NAME', 'water-healer')
JAEGER_HOSTNAME = os.environ.get('JAEGER_HOSTNAME', 'localhost')
JAEGER_PORT = os.environ.get('JAEGER_PORT', '6831')

tracer = None
if ENABLE_OPENTELEMETRY_JAEGER:
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except BaseException:
        logger.warning(
            'opentelemetry-exporter-jaeger not installed. Please install it by `pip install opentelemetry-exporter-jaeger` and try again.')

    try:
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource.create({SERVICE_NAME: OPENTELEMETRY_SERVICE_NAME})
            )
        )
        jaeger_exporter = JaegerExporter(
            agent_host_name=JAEGER_HOSTNAME,
            agent_port=int(JAEGER_PORT),
        )

        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(jaeger_exporter)
        )
        tracer = trace.get_tracer(__name__)

    except Exception as e:
        logger.exception(f'error to initiate tracer, {str(e)}')


def get_io_loop(asynchronous=None):
    if asynchronous:
        return IOLoop.current()

    if _dask_default_client is not None:
        try:
            client = _dask_default_client()
        except ValueError:
            # No dask client found; continue
            pass
        else:
            return client.loop

    if not _io_loops:
        loop = IOLoop()
        thread = threading.Thread(target=loop.start)
        thread.daemon = True
        thread.start()
        _io_loops.append(loop)

    return _io_loops[-1]


def identity(x):
    return x


def get_args(client, key):
    def _get_args(key, dask_scheduler=None):
        ts = dask_scheduler.tasks.get(key)
        if ts:
            args = ts.run_spec
        else:
            args = None
        return {'task': args}

    s = client.run_on_scheduler(_get_args, key)
    if 'args' in s['task']:
        args = cloudpickle.loads(s['task']['args'])
    else:
        args = None

    if args:
        if isinstance(args, tuple) or isinstance(args, list):
            if isinstance(args[0], tuple) or isinstance(args[0], list):
                if isinstance(args[0][0], tuple) or isinstance(
                    args[0][0], list
                ):
                    data = args[0][0]
                else:
                    data = args[0]
            else:
                data = args
        else:
            data = [args]
    return data


class Stream(object):
    """
    A Stream is an infinite sequence of data

    Streams subscribe to each other passing and transforming data between them.
    A Stream object listens for updates from upstream, reacts to these updates,
    and then emits more data to flow downstream to all Stream objects that
    subscribe to it.  Downstream Stream objects may connect at any point of a
    Stream graph to get a full view of the data coming off of that point to do
    with as they will.

    Parameters
    ----------
    asynchronous: boolean or None
        Whether or not this stream will be used in asynchronous functions or
        normal Python functions.  Leave as None if you don't know.
        True will cause operations like emit to return awaitable Futures
        False will use an Event loop in another thread (starts it if necessary)
    ensure_io_loop: boolean
        Ensure that some IOLoop will be created.  If asynchronous is None or
        False then this will be in a separate thread, otherwise it will be
        IOLoop.current

    Examples
    --------
    >>> def inc(x):
    ...     return x + 1

    >>> source = Stream()  # Create a stream object
    >>> s = source.map(inc).map(str)  # Subscribe to make new streams
    >>> s.sink(print)  # take an action whenever an element reaches the end

    >>> L = list()
    >>> s.sink(L.append)  # or take multiple actions (streams can branch)

    >>> for i in range(5):
    ...     source.emit(i)  # push data in at the source
    '1'
    '2'
    '3'
    '4'
    '5'
    >>> L  # and the actions happen at the sinks
    ['1', '2', '3', '4', '5']
    """

    _graphviz_shape = 'ellipse'
    _graphviz_style = 'rounded,filled'
    _graphviz_fillcolor = 'white'
    _graphviz_orientation = 0

    str_list = ['func', 'predicate', 'n', 'interval']

    def __init__(
        self,
        upstream=None,
        upstreams=None,
        stream_name=None,
        loop=None,
        asynchronous=None,
        ensure_io_loop=False,
        checkpoint=False,
        **kwargs,
    ):
        self.downstreams = OrderedWeakrefSet()
        if upstreams is not None:
            self.upstreams = list(upstreams)
        else:
            self.upstreams = [upstream]

        self._set_asynchronous(asynchronous)
        self._set_loop(loop)
        if ensure_io_loop and not self.loop:
            self._set_asynchronous(False)
        if self.loop is None and self.asynchronous is not None:
            self._set_loop(get_io_loop(self.asynchronous))

        for upstream in self.upstreams:
            if upstream:
                upstream.downstreams.add(self)

        self.name = stream_name
        self._checkpoint = checkpoint
        self.checkpoint = {}
        self.error = False

    def _set_loop(self, loop):
        self.loop = None
        if loop is not None:
            self._inform_loop(loop)
        else:
            for upstream in self.upstreams:
                if upstream and upstream.loop:
                    self.loop = upstream.loop
                    break

    def _inform_loop(self, loop):
        """
        Percolate information about an event loop to the rest of the stream
        """
        if self.loop is not None:
            if self.loop is not loop:
                raise ValueError('Two different event loops active')
        else:
            self.loop = loop
            for upstream in self.upstreams:
                if upstream:
                    upstream._inform_loop(loop)
            for downstream in self.downstreams:
                if downstream:
                    downstream._inform_loop(loop)

    def _set_asynchronous(self, asynchronous):
        self.asynchronous = None
        if asynchronous is not None:
            self._inform_asynchronous(asynchronous)
        else:
            for upstream in self.upstreams:
                if upstream and upstream.asynchronous:
                    self.asynchronous = upstream.asynchronous
                    break

    def _inform_asynchronous(self, asynchronous):
        """
        Percolate information about an event loop to the rest of the stream
        """
        if self.asynchronous is not None:
            if self.asynchronous is not asynchronous:
                raise ValueError(
                    'Stream has both asynchronous and synchronous elements'
                )
        else:
            self.asynchronous = asynchronous
            for upstream in self.upstreams:
                if upstream:
                    upstream._inform_asynchronous(asynchronous)
            for downstream in self.downstreams:
                if downstream:
                    downstream._inform_asynchronous(asynchronous)

    def _add_upstream(self, upstream):
        """
        Add upstream to current upstreams, this method is overridden for
        classes which handle stream specific buffers/caches.
        """
        if self.upstreams == [None]:
            self.upstreams[0] = upstream
        else:
            self.upstreams.append(upstream)

    def _add_downstream(self, downstream):
        """
        Add downstream to current downstreams.
        """
        self.downstreams.add(downstream)

    def _remove_downstream(self, downstream):
        """
        Remove downstream from current downstreams.
        """
        self.downstreams.remove(downstream)

    def _remove_upstream(self, upstream):
        """
        Remove upstream from current upstreams, this method is overridden for
        classes which handle stream specific buffers/caches.
        """
        if len(self.upstreams) == 1:
            self.upstreams[0] = [None]
        else:
            self.upstreams.remove(upstream)

    @classmethod
    def register_api(cls, modifier=identity):
        """
        Add callable to Stream API

        This allows you to register a new method onto this class.  You can use
        it as a decorator.::

            >>> @Stream.register_api()
            ... class foo(Stream):
            ...     ...

            >>> Stream().foo(...)  # this works now

        It attaches the callable as a normal attribute to the class object.  In
        doing so it respsects inheritance (all subclasses of Stream will also
        get the foo attribute).

        By default callables are assumed to be instance methods.  If you like
        you can include modifiers to apply before attaching to the class as in
        the following case where we construct a ``staticmethod``.

            >>> @Stream.register_api(staticmethod)
            ... class foo(Stream):
            ...     ...

            >>> Stream.foo(...)  # Foo operates as a static method
        """

        def _(func):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                return func(*args, **kwargs)

            setattr(cls, func.__name__, modifier(wrapped))
            return func

        return _

    def start(self):
        """
        Start any upstream sources.
        """
        for upstream in self.upstreams:
            upstream.start()

    def __str__(self):
        s_list = []
        if self.name:
            s_list.append('{}; {}'.format(self.name, self.__class__.__name__))
        else:
            s_list.append(self.__class__.__name__)

        for m in self.str_list:
            s = ''
            at = getattr(self, m, None)
            if at:
                if not callable(at):
                    s = str(at)
                elif hasattr(at, '__name__'):
                    s = getattr(self, m).__name__
                elif hasattr(at.__class__, '__name__'):
                    s = getattr(self, m).__class__.__name__
                else:
                    s = None
            if s:
                s_list.append('{}={}'.format(m, s))
        if len(s_list) <= 2:
            s_list = [term.split('=')[-1] for term in s_list]

        text = '<'
        text += s_list[0]
        if len(s_list) > 1:
            text += ': '
            text += ', '.join(s_list[1:])
        text += '>'
        return text

    __repr__ = __str__

    def _ipython_display_(self, **kwargs):
        try:
            from ipywidgets import Output
            import IPython
        except ImportError:
            if hasattr(self, '_repr_html_'):
                return self._repr_html_()
            else:
                return self.__repr__()
        output = Output(_view_count=0)
        output_ref = weakref.ref(output)

        def update_cell(val):
            output = output_ref()
            if output is None:
                return
            with output:
                IPython.display.clear_output(wait=True)
                IPython.display.display(val)

        s = self.map(update_cell)
        _html_update_streams.add(s)

        self.output_ref = output_ref
        s_ref = weakref.ref(s)

        def remove_stream(change):
            output = output_ref()
            if output is None:
                return

            if output._view_count == 0:
                ss = s_ref()
                ss.destroy()
                _html_update_streams.remove(ss)  # trigger gc

        output.observe(remove_stream, '_view_count')

        return output._ipython_display_(**kwargs)

    def wait(self, sleep_between_gather=0.1):

        if ENABLE_CHECKPOINTING:
            try:
                from distributed.client import default_client
                client = default_client()
            except Exception as e:
                logger.warning(e)
                return

            def get_last(source):
                x = []

                def loop(s):
                    for downstream in s.downstreams:
                        if downstream._checkpoint:
                            name = type(downstream).__name__
                            if hasattr(downstream, 'func'):
                                name = f'{name}.{downstream.func.__name__}'
                            last = name
                            x.append(last)
                        loop(downstream)

                loop(source)
                return x[-1]

            last_name = get_last(self)

            while True:
                found = False
                for k in self.checkpoint.keys():
                    if last_name in k:
                        found = True
                        break
                if found:
                    break
                time.sleep(sleep_between_gather)

            for k, v in self.checkpoint.items():
                self.checkpoint[k] = client.gather(v)

    def _climb(self, source, name, x):

        for upstream in list(source.upstreams):
            if not upstream:
                source.checkpoint[name] = x
            else:
                new_name = type(upstream).__name__
                if hasattr(upstream, 'func'):
                    new_name = f'{new_name}.{upstream.func.__name__}'
                name = f'{new_name}.{name}'
                self._climb(upstream, name, x)

    def _emit(self, x, emit_id=None, **kwargs):

        if emit_id is None:
            emit_id = str(uuid.uuid4())

        name = type(self).__name__
        if hasattr(self, 'func'):
            name = f'{name}.{self.func.__name__}'

        if ENABLE_EMIT_LOGGING:
            logger.info({'function_name': name, 'data': str(x)[:10000]},
                        extra={'props': {'emit_id': emit_id}})

        if self._checkpoint and ENABLE_CHECKPOINTING:
            self._climb(self, name, x)

        result = []
        for downstream in list(self.downstreams):
            try:
                if tracer:
                    name = type(self).__name__
                    if hasattr(self, 'func'):
                        name = f'{name}.{self.func.__name__}'
                    with tracer.start_as_current_span(emit_id):
                        with tracer.start_as_current_span(name):
                            r = downstream.update(x, emit_id=emit_id, who=self)
                else:
                    r = downstream.update(x, emit_id=emit_id, who=self)
                if isinstance(r, list):
                    result.extend(r)
                else:
                    result.append(r)
            except Exception as e:
                self.error = True
                raise

        return [element for element in result if element is not None]

    def emit(self, x, asynchronous=False):
        """
        Push data into the stream at this point.

        This is typically done only at source Streams but can theortically be
        done at any point
        """
        ts_async = getattr(thread_state, 'asynchronous', False)

        if self.loop is None or asynchronous or self.asynchronous or ts_async:
            if not ts_async:
                thread_state.asynchronous = True
            try:
                result = self._emit(x)
                if self.loop:
                    return gen.convert_yielded(result)
            finally:
                thread_state.asynchronous = ts_async
        else:

            @ gen.coroutine
            def _():
                thread_state.asynchronous = True
                try:
                    result = yield self._emit(x)
                finally:
                    del thread_state.asynchronous

                raise gen.Return(result)

            sync(self.loop, _)

    def __call__(self, x, asynchronous=False):
        return self.emit(x, asynchronous=asynchronous)

    def update(self, x, emit_id=None, who=None):
        self._emit(x, emit_id=emit_id)

    def gather(self):
        """
        This is a no-op for core streamz.

        This allows gather to be used in both dask and core streams.
        """
        return self

    def connect(self, downstream):
        """
        Connect this stream to a downstream element.

        Parameters
        ----------
        downstream: Stream
            The downstream stream to connect to
        """
        self._add_downstream(downstream)
        downstream._add_upstream(self)

    def disconnect(self, downstream):
        """
        Disconnect this stream to a downstream element.

        Parameters
        ----------
        downstream: Stream
            The downstream stream to disconnect from
        """
        self._remove_downstream(downstream)

        downstream._remove_upstream(self)

    @ property
    def upstream(self):
        if len(self.upstreams) != 1:
            raise ValueError('Stream has multiple upstreams')
        else:
            return self.upstreams[0]

    def destroy(self, streams=None):
        """
        Disconnect this stream from any upstream sources.
        """
        if streams is None:
            streams = self.upstreams
        for upstream in list(streams):
            upstream.downstreams.remove(self)
            self.upstreams.remove(upstream)

    def scatter(self, **kwargs):
        from streamz.dask import scatter

        return scatter(self, **kwargs)

    def remove(self, predicate):
        """
        Only pass through elements for which the predicate returns False.
        """
        return self.filter(lambda x: not predicate(x))

    @ property
    def scan(self):
        return self.accumulate

    @ property
    def concat(self):
        return self.flatten

    def sink_to_list(self):
        """
        Append all elements of a stream to a list as they come in.

        Examples
        --------
        >>> source = Stream()
        >>> L = source.map(lambda x: 10 * x).sink_to_list()
        >>> for i in range(5):
        ...     source.emit(i)
        >>> L
        [0, 10, 20, 30, 40]
        """
        L = []
        self.sink(L.append)
        return L

    def frequencies(self, **kwargs):
        """
        Count occurrences of elements.
        """

        def update_frequencies(last, x):
            return toolz.assoc(last, x, last.get(x, 0) + 1)

        return self.scan(update_frequencies, start={}, **kwargs)

    def visualize(self, filename='mystream.png', **kwargs):
        """
        Render the computation of this object's task graph using graphviz.

        Requires ``graphviz`` to be installed.

        Parameters
        ----------
        filename : str, optional
            The name of the file to write to disk.
        kwargs:
            Graph attributes to pass to graphviz like ``rankdir="LR"``
        """
        from streamz.graph import visualize

        return visualize(self, filename, **kwargs)

    def to_dataframe(self, example):
        """
        Convert a stream of Pandas dataframes to a DataFrame.

        Examples
        --------
        >>> source = Stream()
        >>> sdf = source.to_dataframe()
        >>> L = sdf.groupby(sdf.x).y.mean().stream.sink_to_list()
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        """
        from .dataframe import DataFrame

        return DataFrame(stream=self, example=example)

    def to_batch(self, **kwargs):
        """
        Convert a stream of lists to a Batch.

        All elements of the stream are assumed to be lists or tuples.

        Examples
        --------
        >>> source = Stream()
        >>> batches = source.to_batch()
        >>> L = batches.pluck('value').map(inc).sum().stream.sink_to_list()
        >>> source.emit([{'name': 'Alice', 'value': 1},
        ...              {'name': 'Bob', 'value': 2},
        ...              {'name': 'Charlie', 'value': 3}])
        >>> source.emit([{'name': 'Alice', 'value': 4},
        ...              {'name': 'Bob', 'value': 5},
        ...              {'name': 'Charlie', 'value': 6}])
        """
        from .batch import Batch

        return Batch(stream=self, **kwargs)


@ Stream.register_api()
class sink(Stream):
    """
    Apply a function on every element.

    Examples
    --------
    >>> source = Stream()
    >>> L = list()
    >>> source.sink(L.append)
    >>> source.sink(print)
    >>> source.sink(print)
    >>> source.emit(123)
    123
    123
    >>> L
    [123]

    See Also
    --------
    map
    Stream.sink_to_list
    """

    _graphviz_shape = 'trapezium'

    def __init__(self, upstream, func, checkpoint=False, *args, **kwargs):
        self.func = func
        # take the stream specific kwargs out
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(
            self, upstream, stream_name=stream_name, checkpoint=checkpoint
        )
        _global_sinks.add(self)

    def update(self, x, emit_id=None, who=None):
        result = self.func(x, *self.args, **self.kwargs)
        if gen.isawaitable(result):
            return result
        else:
            return []


@ Stream.register_api()
class map(Stream):
    """
    Apply a function to every element in the stream.

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
    >>> source.map(lambda x: 2*x).sink(print)
    >>> for i in range(5):
    ...     source.emit(i)
    0
    2
    4
    6
    8
    """

    def __init__(self, upstream, func, checkpoint=False, *args, **kwargs):
        self.func = func
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(
            self, upstream, stream_name=stream_name, checkpoint=checkpoint
        )

    def update(self, x, emit_id=None, who=None):
        try:
            result = self.func(x, *self.args, **self.kwargs)
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result, emit_id=emit_id)


@ Stream.register_api()
class starmap(Stream):
    """
    Apply a function to every element in the stream, splayed out.

    See ``itertools.starmap``

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
    >>> source.starmap(lambda a, b: a + b).sink(print)
    >>> for i in range(5):
    ...     source.emit((i, i))
    0
    2
    4
    6
    8
    """

    def __init__(self, upstream, func, checkpoint=False, *args, **kwargs):
        self.func = func
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(
            self, upstream, stream_name=stream_name, checkpoint=checkpoint
        )

    def update(self, x, emit_id=None, who=None):
        y = x + self.args
        try:
            result = self.func(*y, **self.kwargs)
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result, emit_id=emit_id)


def _truthy(x):
    return not not x


@ Stream.register_api()
class filter(Stream):
    """
    Only pass through elements that satisfy the predicate.

    Parameters
    ----------
    predicate : function
        The predicate. Should return True or False, where
        True means that the predicate is satisfied.
    *args :
        The arguments to pass to the predicate.
    **kwargs:
        Keyword arguments to pass to predicate

    Examples
    --------
    >>> source = Stream()
    >>> source.filter(lambda x: x % 2 == 0).sink(print)
    >>> for i in range(5):
    ...     source.emit(i)
    0
    2
    4
    """

    def __init__(
        self, upstream, predicate, checkpoint=False, *args, **kwargs
    ):
        if predicate is None:
            predicate = _truthy
        self.predicate = predicate
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(
            self, upstream, stream_name=stream_name, checkpoint=checkpoint
        )

    def update(self, x, emit_id=None, who=None):
        if self.predicate(x, *self.args, **self.kwargs):
            return self._emit(x, emit_id=emit_id)


@Stream.register_api()
class accumulate(Stream):
    """
    Accumulate results with previous state.

    This performs running or cumulative reductions, applying the function
    to the previous total and the new element.  The function should take
    two arguments, the previous accumulated state and the next element and
    it should return a new accumulated state,
    - ``state = func(previous_state, new_value)`` (returns_state=False)
    - ``state, result = func(previous_state, new_value)`` (returns_state=True)

    where the new_state is passed to the next invocation. The state or result
    is emitted downstream for the two cases.

    Parameters
    ----------
    func: callable
    start: object
        Initial value, passed as the value of ``previous_state`` on the first
        invocation. Defaults to the first submitted element
    returns_state: boolean
        If true then func should return both the state and the value to emit
        If false then both values are the same, and func returns one value
    **kwargs:
        Keyword arguments to pass to func

    Examples
    --------
    A running total, producing triangular numbers

    >>> source = Stream()
    >>> source.accumulate(lambda acc, x: acc + x).sink(print)
    >>> for i in range(5):
    ...     source.emit(i)
    0
    1
    3
    6
    10

    A count of number of events (including the current one)

    >>> source = Stream()
    >>> source.accumulate(lambda acc, x: acc + 1, start=0).sink(print)
    >>> for _ in range(5):
    ...     source.emit(0)
    1
    2
    3
    4
    5

    Like the builtin "enumerate".

    >>> source = Stream()
    >>> source.accumulate(lambda acc, x: ((acc[0] + 1, x), (acc[0], x)),
    ...                   start=(0, 0), returns_state=True
    ...                   ).sink(print)
    >>> for i in range(3):
    ...     source.emit(0)
    (0, 0)
    (1, 0)
    (2, 0)
    """

    _graphviz_shape = 'box'

    def __init__(
        self,
        upstream,
        func,
        start=no_default,
        returns_state=False,
        checkpoint=False,
        **kwargs,
    ):
        self.func = func
        self.kwargs = kwargs
        self.state = start
        self.returns_state = returns_state
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        Stream.__init__(
            self, upstream, stream_name=stream_name, checkpoint=checkpoint
        )

    def update(self, x, emit_id=None, who=None):
        if self.state is no_default:
            self.state = x
            return self._emit(x, emit_id=emit_id)
        else:
            try:
                result = self.func(self.state, x, **self.kwargs)
            except Exception as e:
                logger.exception(e)
                raise
            if self.returns_state:
                state, result = result
            else:
                state = result
            self.state = state
            return self._emit(result, emit_id=emit_id)


@Stream.register_api()
class slice(Stream):
    """
    Get only some events in a stream by position. Works like list[] syntax.

    Parameters
    ----------
    start : int
        First event to use. If None, start from the beginnning
    end : int
        Last event to use (non-inclusive). If None, continue without stopping.
        Does not support negative indexing.
    step : int
        Pass on every Nth event. If None, pass every one.

    Examples
    --------
    >>> source = Stream()
    >>> source.slice(2, 6, 2).sink(print)
    >>> for i in range(5):
    ...     source.emit(0)
    2
    4
    """

    def __init__(
        self,
        upstream,
        start=None,
        end=None,
        step=None,
        checkpoint=False,
        **kwargs,
    ):
        self.state = 0
        self.star = start or 0
        self.end = end
        self.step = step or 1
        if any((_ or 0) < 0 for _ in [start, end, step]):
            raise ValueError('Negative indices not supported by slice')
        stream_name = kwargs.pop('stream_name', None)
        Stream.__init__(
            self, upstream, stream_name=stream_name, checkpoint=checkpoint
        )
        self._check_end()

    def update(self, x, emit_id=None, who=None):
        if self.state >= self.star and self.state % self.step == 0:
            self.emit(x)
        self.state += 1
        self._check_end()

    def _check_end(self):
        if self.end and self.state >= self.end:
            # we're done
            for upstream in self.upstreams:
                upstream._remove_downstream(self)


@Stream.register_api()
class partition(Stream):
    """
    Partition stream into tuples of equal size.

    Examples
    --------
    >>> source = Stream()
    >>> source.partition(3).sink(print)
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
        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, emit_id=None, who=None):
        self.buffer.append(x)
        if len(self.buffer) == self.n:
            result, self.buffer = self.buffer, []
            return self._emit(tuple(result), emit_id=emit_id)
        else:
            return []


@Stream.register_api()
class sliding_window(Stream):
    """
    Produce overlapping tuples of size n.

    Parameters
    ----------
    return_partial : bool
        If True, yield tuples as soon as any events come in, each tuple being
        smaller or equal to the window size. If False, only start yielding
        tuples once a full window has accrued.

    Examples
    --------
    >>> source = Stream()
    >>> source.sliding_window(3, return_partial=False).sink(print)
    >>> for i in range(8):
    ...     source.emit(i)
    (0, 1, 2)
    (1, 2, 3)
    (2, 3, 4)
    (3, 4, 5)
    (4, 5, 6)
    (5, 6, 7)
    """

    _graphviz_shape = 'diamond'

    def __init__(self, upstream, n, return_partial=True, **kwargs):
        self.n = n
        self.buffer = deque(maxlen=n)
        self.partial = return_partial
        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, emit_id=None, who=None):
        self.buffer.append(x)
        if self.partial or len(self.buffer) == self.n:
            return self._emit(tuple(self.buffer), emit_id=emit_id)
        else:
            return []


def convert_interval(interval):
    if isinstance(interval, str):
        import pandas as pd

        interval = pd.Timedelta(interval).total_seconds()
    return interval


@Stream.register_api()
class timed_window(Stream):
    """
    Emit a tuple of collected results every interval.

    Every ``interval`` seconds this emits a tuple of all of the results
    seen so far.  This can help to batch data coming off of a high-volume
    stream.
    """

    _graphviz_shape = 'octagon'

    def __init__(self, upstream, interval, **kwargs):
        self.interval = convert_interval(interval)
        self.buffer = []
        self.last = gen.moment
        self.last_emit_id = None

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, emit_id=None, who=None):
        self.buffer.append(x)
        if self.last_emit_id is None:
            self.last_emit_id = emit_id
        return self.last

    @gen.coroutine
    def cb(self):
        while True:
            L, self.buffer = self.buffer, []
            self.last = self._emit(L, emit_id=self.last_emit_id)
            yield self.last
            yield gen.sleep(self.interval)
            self.last_emit_id = None


@Stream.register_api()
class delay(Stream):
    """
    Add a time delay to results.
    """

    _graphviz_shape = 'octagon'

    def __init__(self, upstream, interval, **kwargs):
        self.interval = convert_interval(interval)
        self.queue = Queue()
        self.last_emit_id = None

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    @gen.coroutine
    def cb(self):
        while True:
            last = time()
            x = yield self.queue.get()
            yield self._emit(x, emit_id=self.last_emit_id)
            duration = self.interval - (time() - last)
            if duration > 0:
                yield gen.sleep(duration)
            self.last_emit_id = None

    def update(self, x, emit_id=None, who=None):
        if self.last_emit_id is None:
            self.last_emit_id = emit_id
        return self.queue.put(x)


@Stream.register_api()
class rate_limit(Stream):
    """
    Limit the flow of data.

    This stops two elements of streaming through in an interval shorter
    than the provided value.

    Parameters
    ----------
    interval: float
        Time in seconds
    """

    _graphviz_shape = 'octagon'

    def __init__(self, upstream, interval, **kwargs):
        self.interval = convert_interval(interval)
        self.next = 0

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

    @gen.coroutine
    def update(self, x, emit_id=None, who=None):
        now = time()
        old_next = self.next
        self.next = max(now, self.next) + self.interval
        if now < old_next:
            yield gen.sleep(old_next - now)
        yield self._emit(x, emit_id=emit_id)


@Stream.register_api()
class buffer(Stream):
    """
    Allow results to pile up at this point in the stream.

    This allows results to buffer in place at various points in the stream.
    This can help to smooth flow through the system when backpressure is
    applied.
    """

    _graphviz_shape = 'diamond'

    def __init__(self, upstream, n, **kwargs):
        self.queue = Queue(maxsize=n)
        self.last_emit_id = None

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, emit_id=None, who=None):
        if self.last_emit_id is None:
            self.last_emit_id = emit_id
        return self.queue.put(x)

    @gen.coroutine
    def cb(self):
        while True:
            x = yield self.queue.get()
            yield self._emit(x, emit_id=self.last_emit_id)
            self.last_emit_id = None


@Stream.register_api()
class zip(Stream):
    """
    Combine streams together into a stream of tuples.

    We emit a new tuple once all streams have produce a new tuple.

    See also
    --------
    combine_latest
    zip_latest
    """

    _graphviz_orientation = 270
    _graphviz_shape = 'triangle'

    def __init__(self, *upstreams, **kwargs):
        self.maxsize = kwargs.pop('maxsize', 10)
        self.condition = Condition()
        self.literals = [
            (i, val)
            for i, val in enumerate(upstreams)
            if not isinstance(val, Stream)
        ]

        self.buffers = {
            upstream: deque()
            for upstream in upstreams
            if isinstance(upstream, Stream)
        }

        upstreams2 = [
            upstream for upstream in upstreams if isinstance(upstream, Stream)
        ]

        Stream.__init__(self, upstreams=upstreams2, **kwargs)

    def _add_upstream(self, upstream):
        # Override method to handle setup of buffer for new stream
        self.buffers[upstream] = deque()
        super(zip, self)._add_upstream(upstream)

    def _remove_upstream(self, upstream):
        # Override method to handle removal of buffer for stream
        self.buffers.pop(upstream)
        super(zip, self)._remove_upstream(upstream)

    def pack_literals(self, tup):
        """
        Fill buffers for literals whenever we empty them.
        """
        inp = list(tup)[::-1]
        out = []
        for i, val in self.literals:
            while len(out) < i:
                out.append(inp.pop())
            out.append(val)

        while inp:
            out.append(inp.pop())

        return tuple(out)

    def update(self, x, emit_id=None, who=None):
        L = self.buffers[who]  # get buffer for stream
        L.append(x)
        if len(L) == 1 and all(self.buffers.values()):
            tup = tuple(self.buffers[up][0] for up in self.upstreams)
            for buf in self.buffers.values():
                buf.popleft()
            self.condition.notify_all()
            if self.literals:
                tup = self.pack_literals(tup)
            return self._emit(tup, emit_id=emit_id)
        elif len(L) > self.maxsize:
            return self.condition.wait()


@Stream.register_api()
class combine_latest(Stream):
    """
    Combine multiple streams together to a stream of tuples.

    This will emit a new tuple of all of the most recent elements seen from
    any stream.

    Parameters
    ----------
    emit_on : stream or list of streams or None
        only emit upon update of the streams listed.
        If None, emit on update from any stream

    See Also
    --------
    zip
    """

    _graphviz_orientation = 270
    _graphviz_shape = 'triangle'

    def __init__(self, *upstreams, **kwargs):
        emit_on = kwargs.pop('emit_on', None)
        self._initial_emit_on = emit_on

        self.last = [None for _ in upstreams]
        self.missing = set(upstreams)
        if emit_on is not None:
            if not isinstance(emit_on, Iterable):
                emit_on = (emit_on,)
            emit_on = tuple(
                upstreams[x] if isinstance(x, int) else x for x in emit_on
            )
            self.emit_on = emit_on
        else:
            self.emit_on = upstreams
        Stream.__init__(self, upstreams=upstreams, **kwargs)

    def _add_upstream(self, upstream):
        # Override method to handle setup of last and missing for new stream
        self.last.append(None)
        self.missing.update([upstream])
        super(combine_latest, self)._add_upstream(upstream)
        if self._initial_emit_on is None:
            self.emit_on = self.upstreams

    def _remove_upstream(self, upstream):
        # Override method to handle removal of last and missing for stream
        if self.emit_on == upstream:
            raise RuntimeError(
                "Can't remove the ``emit_on`` stream since that"
                'would cause no data to be emitted. '
                'Consider adding an ``emit_on`` first by '
                'running ``node.emit_on=(upstream,)`` to add '
                'a new ``emit_on`` or running '
                '``node.emit_on=tuple(node.upstreams)`` to '
                'emit on all incoming data'
            )
        self.last.pop(self.upstreams.index(upstream))
        self.missing.remove(upstream)
        super(combine_latest, self)._remove_upstream(upstream)
        if self._initial_emit_on is None:
            self.emit_on = self.upstreams

    def update(self, x, emit_id=None, who=None):
        if self.missing and who in self.missing:
            self.missing.remove(who)

        self.last[self.upstreams.index(who)] = x
        if not self.missing and who in self.emit_on:
            tup = tuple(self.last)
            return self._emit(tup, emit_id=emit_id)


@Stream.register_api()
class flatten(Stream):
    """
    Flatten streams of lists or iterables into a stream of elements.

    Examples
    --------
    >>> source = Stream()
    >>> source.flatten().sink(print)
    >>> for x in [[1, 2, 3], [4, 5], [6, 7, 7]]:
    ...     source.emit(x)
    1
    2
    3
    4
    5
    6
    7

    See Also
    --------
    partition
    """

    def update(self, x, emit_id=None, who=None):
        L = []
        for item in x:
            y = self._emit(item, emit_id=emit_id)
            if isinstance(y, list):
                L.extend(y)
            else:
                L.append(y)
        return L


@Stream.register_api()
class unique(Stream):
    """
    Avoid sending through repeated elements.

    This deduplicates a stream so that only new elements pass through.
    You can control how much of a history is stored with the ``history=``
    parameter.  For example setting ``history=1`` avoids sending through
    elements when one is repeated right after the other.

    Parameters
    ----------
    history : int or None, optional
        number of stored unique values to check against
    key : function, optional
        Function which returns a representation of the incoming data.
        For example ``key=lambda x: x['a']`` could be used to allow only
        pieces of data with unique ``'a'`` values to pass through.
    hashable : bool, optional
        If True then data is assumed to be hashable, else it is not. This is
        used for determining how to cache the history, if hashable then
        either dicts or LRU caches are used, otherwise a deque is used.
        Defaults to True.

    Examples
    --------
    >>> source = Stream()
    >>> source.unique(history=1).sink(print)
    >>> for x in [1, 1, 2, 2, 2, 1, 3]:
    ...     source.emit(x)
    1
    2
    1
    3
    """

    def __init__(
        self,
        upstream,
        maxsize=None,
        key=identity,
        hashable=True,
        **kwargs,
    ):
        self.key = key
        self.maxsize = maxsize
        if hashable:
            self.seen = dict()
            if self.maxsize:
                from zict import LRU

                self.seen = LRU(self.maxsize, self.seen)
        else:
            self.seen = []

        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, emit_id=None, who=None):
        y = self.key(x)
        emit = True
        if isinstance(self.seen, list):
            if y in self.seen:
                self.seen.remove(y)
                emit = False
            self.seen.insert(0, y)
            if self.maxsize:
                del self.seen[self.maxsize:]
            if emit:
                return self._emit(x, emit_id=emit_id)
        else:
            if self.seen.get(y, '~~not_seen~~') == '~~not_seen~~':
                self.seen[y] = 1
                return self._emit(x, emit_id=emit_id)


@Stream.register_api()
class union(Stream):
    """
    Combine multiple streams into one.

    Every element from any of the upstreams streams will immediately flow
    into the output stream.  They will not be combined with elements from
    other streams.

    See also
    --------
    Stream.zip
    Stream.combine_latest
    """

    def __init__(self, *upstreams, **kwargs):
        super(union, self).__init__(upstreams=upstreams, **kwargs)

    def update(self, x, emit_id=None, who=None):
        return self._emit(x, emit_id=emit_id)


@Stream.register_api()
class pluck(Stream):
    """
    Select elements from elements in the stream.

    Parameters
    ----------
    pluck : object, list
        The element(s) to pick from the incoming element in the stream
        If an instance of list, will pick multiple elements.

    Examples
    --------
    >>> source = Stream()
    >>> source.pluck([0, 3]).sink(print)
    >>> for x in [[1, 2, 3, 4], [4, 5, 6, 7], [8, 9, 10, 11]]:
    ...     source.emit(x)
    (1, 4)
    (4, 7)
    (8, 11)

    >>> source = Stream()
    >>> source.pluck('name').sink(print)
    >>> for x in [{'name': 'Alice', 'x': 123}, {'name': 'Bob', 'x': 456}]:
    ...     source.emit(x)
    'Alice'
    'Bob'
    """

    def __init__(self, upstream, pick, **kwargs):
        self.pick = pick
        super(pluck, self).__init__(upstream, **kwargs)

    def update(self, x, emit_id=None, who=None):
        if isinstance(self.pick, list):
            return self._emit(tuple([x[ind] for ind in self.pick]), emit_id=emit_id)
        else:
            return self._emit(x[self.pick], emit_id=emit_id)


@Stream.register_api()
class collect(Stream):
    """
    Hold elements in a cache and emit them as a collection when flushed.

    Examples
    --------
    >>> source1 = Stream()
    >>> source2 = Stream()
    >>> collector = collect(source1)
    >>> collector.sink(print)
    >>> source2.sink(collector.flush)
    >>> source1.emit(1)
    >>> source1.emit(2)
    >>> source2.emit('anything')  # flushes collector
    ...
    [1, 2]
    """

    def __init__(self, upstream, cache=None, **kwargs):
        if cache is None:
            cache = deque()
        self.cache = cache

        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, emit_id=None, who=None):
        self.cache.append(x)

    def flush(self, _=None):
        out = tuple(self.cache)
        self._emit(out, emit_id=emit_id)
        self.cache.clear()


@Stream.register_api()
class zip_latest(Stream):
    """
    Combine multiple streams together to a stream of tuples.

    The stream which this is called from is lossless. All elements from
    the lossless stream are emitted reguardless of when they came in.
    This will emit a new tuple consisting of an element from the lossless
    stream paired with the latest elements from the other streams.
    Elements are only emitted when an element on the lossless stream are
    received, similar to ``combine_latest`` with the ``emit_on`` flag.

    See Also
    --------
    Stream.combine_latest
    Stream.zip
    """

    def __init__(self, lossless, *upstreams, **kwargs):
        upstreams = (lossless,) + upstreams
        self.last = [None for _ in upstreams]
        self.missing = set(upstreams)
        self.lossless = lossless
        self.lossless_buffer = deque()
        Stream.__init__(self, upstreams=upstreams, **kwargs)

    def update(self, x, emit_id=None, who=None):
        idx = self.upstreams.index(who)
        if who is self.lossless:
            self.lossless_buffer.append(x)

        self.last[idx] = x
        if self.missing and who in self.missing:
            self.missing.remove(who)

        if not self.missing:
            L = []
            while self.lossless_buffer:
                self.last[0] = self.lossless_buffer.popleft()
                L.append(self._emit(tuple(self.last), emit_id=emit_id))
            return L


@Stream.register_api()
class latest(Stream):
    """
    Drop held-up data and emit the latest result.

    This allows you to skip intermediate elements in the stream if there is
    some back pressure causing a slowdown.  Use this when you only care about
    the latest elements, and are willing to lose older data.

    This passes through values without modification otherwise.

    Examples
    --------
    >>> source.map(f).latest().map(g)  # doctest: +SKIP
    """

    _graphviz_shape = 'octagon'

    def __init__(self, upstream, **kwargs):
        self.condition = Condition()
        self.next = []
        self.last_emit_id = None

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, emit_id=None, who=None):
        self.next = [x]
        if self.last_emit_id is None:
            self.last_emit_id = emit_id
        self.loop.add_callback(self.condition.notify)

    @gen.coroutine
    def cb(self):
        while True:
            yield self.condition.wait()
            [x] = self.next
            yield self._emit(x, emit_id=self.last_emit_id)
            self.last_emit_id = None


@Stream.register_api()
class to_kafka(Stream):
    """
    Writes data in the stream to Kafka

    This stream accepts a string or bytes object. Call ``flush`` to ensure all
    messages are pushed. Responses from Kafka are pushed downstream.

    Parameters
    ----------
    topic : string
        The topic which to write
    producer_config : dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers: Connection string (host:port) to Kafka

    Examples
    --------
    >>> from streamz import Stream
    >>> ARGS = {'bootstrap.servers': 'localhost:9092'}
    >>> source = Stream()
    >>> kafka = source.map(lambda x: str(x)).to_kafka('test', ARGS)
    <to_kafka>
    >>> for i in range(10):
    ...     source.emit(i)
    >>> kafka.flush()
    """

    def __init__(self, upstream, topic, producer_config, **kwargs):
        import confluent_kafka as ck

        self.topic = topic
        self.producer = ck.Producer(producer_config)

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)
        self.stopped = False
        self.polltime = 0.2
        self.loop.add_callback(self.poll)
        self.futures = []

    @gen.coroutine
    def poll(self):
        while not self.stopped:
            # executes callbacks for any delivered data, in this thread
            # if no messages were sent, nothing happens
            self.producer.poll(0)
            yield gen.sleep(self.polltime)

    def update(self, x, emit_id=None, who=None):
        future = gen.Future()
        self.futures.append(future)

        @gen.coroutine
        def _():
            while True:
                try:
                    # this runs asynchronously, in C-K's thread
                    self.producer.produce(self.topic, x, callback=self.cb)
                    return
                except BufferError:
                    yield gen.sleep(self.polltime)
                except Exception as e:
                    future.set_exception(e)
                    return

        self.loop.add_callback(_)
        return future

    @gen.coroutine
    def cb(self, err, msg):
        future = self.futures.pop(0)
        if msg is not None and msg.value() is not None:
            future.set_result(None)
            yield self._emit(msg.value(), emit_id=emit_id)
        else:
            future.set_exception(err or msg.error())

    def flush(self, timeout=-1):
        self.producer.flush(timeout)


def sync(loop, func, *args, **kwargs):
    """
    Run coroutine in loop running in separate thread.
    """
    # This was taken from distrbuted/utils.py

    # Tornado's PollIOLoop doesn't raise when using closed, do it ourselves
    if PollIOLoop and (
        (isinstance(loop, PollIOLoop) and getattr(loop, '_closing', False))
        or (hasattr(loop, 'asyncio_loop') and loop.asyncio_loop._closed)
    ):
        raise RuntimeError('IOLoop is closed')

    timeout = kwargs.pop('callback_timeout', None)

    e = threading.Event()
    main_tid = get_thread_identity()
    result = [None]
    error = [False]

    @gen.coroutine
    def f():
        try:
            if main_tid == get_thread_identity():
                raise RuntimeError('sync() called from thread of running loop')
            yield gen.moment
            thread_state.asynchronous = True
            future = func(*args, **kwargs)
            if timeout is not None:
                future = gen.with_timeout(timedelta(seconds=timeout), future)
            result[0] = yield future
        except Exception:
            error[0] = sys.exc_info()
        finally:
            thread_state.asynchronous = False
            e.set()

    loop.add_callback(f)
    if timeout is not None:
        if not e.wait(timeout):
            raise gen.TimeoutError('timed out after %s s.' % (timeout,))
    else:
        while not e.is_set():
            e.wait(10)
    if error[0]:
        six.reraise(*error[0])
    else:
        return result[0]


@Stream.register_api()
class partition_time(Stream):
    """
    Emit a tuple of collected results every interval
    Every ``interval`` seconds this emits a tuple of all of the results
    seen so far.  This can help to batch data coming off of a high-volume
    stream. This interface will only emit if size of buffer bigger than 0.
    """

    _graphviz_shape = 'octagon'

    def __init__(self, upstream, interval, **kwargs):
        self.interval = convert_interval(interval)
        self.buffer = []
        self.last = gen.moment
        self.last_emit_id = None

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, emit_id=None, who=None):
        self.buffer.append(x)
        if self.last_emit_id is None:
            self.last_emit_id = emit_id
        return self.last

    @gen.coroutine
    def cb(self):
        while True:
            L, self.buffer = self.buffer, []
            if len(L):
                self.last = self._emit(L, emit_id=self.last_emit_id)
            yield self.last
            yield gen.sleep(self.interval)
            self.last_emit_id = None


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

    def __init__(self, upstream, func, checkpoint=False, *args, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(
            self, upstream, stream_name=stream_name, checkpoint=checkpoint
        )

    def update(self, x, emit_id=None, who=None):
        try:
            result = (self.func(e, *self.args, **self.kwargs) for e in x)
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result, emit_id=emit_id)


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

    def __init__(self, upstream, func, checkpoint=False, *args, **kwargs):
        self.func = func
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(
            self, upstream, stream_name=stream_name, checkpoint=checkpoint
        )

    def update(self, x, emit_id=None, who=None):
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
            return self._emit(result, emit_id=emit_id)
