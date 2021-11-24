from distributed.diagnostics.plugin import WorkerPlugin
from distributed.protocol.pickle import dumps, loads
from distributed.utils_comm import pack_data
from datetime import datetime
import os
import traceback
import logging
import json
from typing import List


def remote_logging(client, persistent_class,
                   status_finish: List[str] = ['error'],
                   plugin_name: str = 'error-logging'):
    """
    Remote logging for dask worker using dask plugin.
    Only support dask>=2021.1.0 and distributed>=2021.1.0

    Parameters
    ----------
    client: distributed.client.Client
    persistent_class: class
        Must have `persist` and `constructor` methods.
    status_finish: List[str], optional (default=['error'])
    plugin_name: str, optional (default='error-logging')
        Name for plugin.
    """
    if not hasattr(persistent_class, 'persist'):
        raise ValueError('persistent_class class must has `persist` method.')

    class ErrorLogger(WorkerPlugin, persistent_class):
        def __init__(self):
            persistent_class.__init__(self)

        def setup(self, worker):
            self.worker = worker

        def transition(self, key, start, finish, *args, **kwargs):
            if finish in status_finish:
                function_name, emit_id, _ = key.split('--')
                ts = self.worker.tasks[key]
                exc = ts.exception
                trace = ts.traceback
                trace = traceback.format_tb(trace.data)
                error = str(exc.data)
                if 'SerializedTask' in str(type(ts.runspec)):
                    function = loads(ts.runspec.function)
                    args = ts.runspec.args
                    args = loads(args) if args else ()
                    kwargs = ts.runspec.kwargs
                    kwargs = loads(kwargs) if kwargs else ()
                else:
                    function, args, kwargs = ts.runspec
                function_name = f'{function.__module__}.{function.__name__}'

                data = {}
                for dep in ts.dependencies:
                    try:
                        data[dep.key] = self.worker.data[dep.key]
                    except BaseException:
                        pass

                args2 = pack_data(args, data, key_types=(bytes, str))
                kwargs2 = pack_data(kwargs, data, key_types=(bytes, str))

                data = {
                    'function': function_name,
                    'args': args2,
                    'kwargs': kwargs2,
                    'exception': trace,
                    'error': error,
                    'key': key,
                    'emit_id': emit_id
                }
                self.persist(data, datetime.now())

    plugin = ErrorLogger()
    client.register_worker_plugin(plugin, name=plugin_name)
