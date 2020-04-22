import sys
import functools
from prometheus_client import start_http_server, Counter, Summary, Histogram
from distributed.client import default_client
from time import time
import logging
from tornado import gen

logger = logging.getLogger(__name__)


def _evolve(
    source, name, time_execution_metrics = True, data_size_metrics = True
):
    for c in source.downstreams:
        c.total_before = 0
        c.total_after = 0
        c.total_error = 0
        if hasattr(c, 'kwargs'):
            c.ignore_error = c.kwargs.pop('ignore_error', False)
        else:
            c.ignore_error = False

        if hasattr(c, 'func'):
            func_name = c.func.__name__
            c.got_func = True

        else:
            func_name = (
                str(c).replace('<', '').replace('>', '').replace('; ', '_')
            )
            c.got_func = False

        c.is_dask = '.dask.' in str(type(c))

        f = f'{name}__{func_name}'
        c.name = f

        if c.got_func:

            c.total_before = Counter(f'total_before_{f}', f'total input {f}')
            c.total_after = Counter(f'total_after_{f}', f'total output {f}')
            c.total_error = Counter(f'total_error_{f}', f'total total {f}')
            c.time_execution_metrics = time_execution_metrics
            if c.time_execution_metrics:
                c.total_time = Summary(
                    f'total_time_{f}', f'summary of time execution {f}'
                )
                c.total_time_histogram = Histogram(
                    f'total_time_histogram_{f}',
                    f'histogram of time execution {f}',
                )

            c.data_size_metrics = data_size_metrics
            if c.data_size_metrics:
                c.data_size_before = Summary(
                    f'data_size_before_{f}',
                    f'summary of input data size {f} (KB)',
                )
                c.data_size_after = Summary(
                    f'data_size_after_{f}',
                    f'summary of output data size {f} (KB)',
                )
                c.data_size_before_histogram = Histogram(
                    f'data_size_before_histogram_{f}',
                    f'histogram of input data size {f} (KB)',
                )
                c.data_size_after_histogram = Histogram(
                    f'total_time_after_histogram_{f}',
                    f'histogram of output data size {f} (KB)',
                )

            if c.is_dask:

                def additional(self, x, who = None):
                    client = default_client()

                    result = client.submit(
                        self.func, x, *self.args, **self.kwargs
                    )
                    return self._emit(result)

            else:

                def additional(self, x, who = None):
                    self.total_before.inc()
                    if self.data_size_metrics:
                        self.data_size_before.observe(sys.getsizeof(x) / 1000)
                        self.data_size_before_histogram.observe(
                            sys.getsizeof(x) / 1000
                        )

                    before = time()
                    try:
                        result = self.func(x)
                        after = time() - before
                    except Exception as e:
                        logger.exception(e)
                        if self.ignore_error:
                            self.total_error.inc()
                        else:
                            raise
                    else:
                        self.total_after.inc()

                        if self.time_execution_metrics:
                            self.total_time.observe(after)
                            self.total_time_histogram.observe(after)

                        if self.data_size_metrics:
                            self.data_size_after.observe(
                                sys.getsizeof(x) / 1000
                            )
                            self.data_size_after_histogram.observe(
                                sys.getsizeof(x) / 1000
                            )

                        return self._emit(result)

            c.update = functools.partial(additional, c)
        c = _evolve(
            c,
            name = f,
            time_execution_metrics = time_execution_metrics,
            data_size_metrics = data_size_metrics,
        )

    return source


def metrics(
    source,
    time_execution_metrics: bool = True,
    data_size_metrics: bool = True,
    port: int = 8000,
):
    """

    Parameters
    ----------
    source: source object
        streamz source.
    time_execution_metrics: bool, (default=True)
        added time execution metrics.
    data_size_metrics: bool, (default=True)
        data size metrics.
    port: int, (default=8000)
        default port for prometheus exporter.
        
    """
    start_http_server(port)
    source = _evolve(
        source,
        name = 'source',
        time_execution_metrics = time_execution_metrics,
        data_size_metrics = data_size_metrics,
    )
    return source
