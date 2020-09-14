from distributed.diagnostics.plugin import WorkerPlugin
from distributed.protocol.pickle import dumps, loads
from distributed.utils_comm import pack_data
from datetime import datetime
import os
import traceback
import logging
import json


def error_logging(client, persistent, name: str = 'worker-plugin'):
    if not hasattr(persistent, 'persist'):
        raise ValueError('persistent class must has `persist` method.')

    class ErrorLogger(WorkerPlugin, persistent):
        def __init__(self):
            self.logger = logging.getLogger()
            persistent.__init__(self)

        def setup(self, worker):
            self.worker = worker

        def transition(self, key, start, finish, *args, **kwargs):
            if finish == 'error':
                exc = self.worker.exceptions[key]
                trace = self.worker.tracebacks[key]
                trace = traceback.format_tb(trace.data)
                error = str(exc.data)
                func, args, kwargs = self.worker.tasks[key]
                func_name = f'{func.__module__}.{func.__name__}'

                data = {}
                for k in self.worker.dependencies[key]:
                    try:
                        data[k] = self.worker.data[k]
                    except:
                        pass

                args2 = pack_data(args, data, key_types = (bytes, str))
                kwargs2 = pack_data(kwargs, data, key_types = (bytes, str))

                data = {
                    'function': func_name,
                    'args': args2,
                    'kwargs': kwargs2,
                    'exception': trace,
                    'error': error,
                    'key': key,
                }
                # now = datetime.strftime(datetime.now(), '%y-%m-%d-%H-%M-%S')

                # filename = f'{func_name}-{now}.json'

                self.persist(data, datetime.now())

    async def register_worker_plugin(plugin, name = None):
        responses = await client.scheduler.broadcast(
            msg = dict(op = 'plugin-add', plugin = dumps(plugin), name = name)
        )
        return responses

    plugin = ErrorLogger()
    client.sync(register_worker_plugin, plugin = plugin, name = name)
