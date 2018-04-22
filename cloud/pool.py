import fn
import asyncio, time, threading
from threading import Thread
from concurrent.futures import Future, ThreadPoolExecutor

################################################################################

class AsyncThread:
    def __init__(self):
        self.loop = None
        self.pool = ThreadPoolExecutor()
        self.thread = threading.Thread(target=self.run)
        self.thread.start()
        while self.loop is None:
            time.sleep(0.1)

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)

    def _add_task(self, future, coro):
        task = self.loop.create_task(coro)
        future.set_result(task)

    def put(self, coro):
        future = Future()
        p = fn.partial(self._add_task, future, coro)
        self.loop.call_soon_threadsafe(p)
        return future.result()

    def get(self, future, timeout=None, resolution=0.2, throw=True):
        end = None if timeout is None else time.time() + timeout
        while not future.done():
            time.sleep(resolution)
            if end is not None and end < time.time():
                return None
        try:
            return future.result()
        except Exception as e:
            if throw: raise e

    def wait(self, futures, timeout):
        fut = self.put(asyncio.wait(list(futures), timeout=timeout))
        return self.get(fut, timeout=timeout, throw=False)

    def cancel(self, task):
        self.loop.call_soon_threadsafe(task.cancel)

    def execute(self, fun, *args, **kwargs):
        return self.loop.run_in_executor(self.pool, fn.partial(fun, *args, **kwargs))

    def __enter__(self):
        self.pool.__enter__()
        return self
    
    def __exit__(self, cls, value, traceback):
        self.pool.__exit__(cls, value, traceback)

################################################################################

def async_exe(pool, fun, *args, **kwargs):
    if isinstance(pool, AsyncThread):
        return pool.execute(fun, *args, **kwargs)
    else:
        return asyncio.get_event_loop().run_in_executor(None, fn.partial(fun, *args, **kwargs))

################################################################################