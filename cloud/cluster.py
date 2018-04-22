from .openstack import Instance, ClosingContext, RETRY


import fn

from .pool import AsyncThread
from . import ssh

import asyncio, uuid, collections
from concurrent.futures import ThreadPoolExecutor

info, handle = fn.logs(__name__, 'info', 'handle')

################################################################################

class Worker:
    def __init__(self, instance, port, pid):
        self.instance = instance
        self.port = port
        self.pid = pid
        self.close = None

################################################################################

class JetStreamCluster(ClosingContext):

    async def _start_scheduler(self, future, *args, **kwargs):
        address = await self._address(future)
        return await ssh.start_scheduler(address, *args, **kwargs)

    def __init__(self, name, image, flavor, port=8786, **kwargs):
        self.name = str(uuid.uuid4()) if name is None else name
        self.ssh_options = kwargs
        self.runner = AsyncThread()
        sc = self.runner.put(Instance.create(self.name, image=image, flavor=flavor))
        pid = self.runner.put(self._start_scheduler(sc, port, **kwargs))
        info('Started scheduler instance and process')
        self.workers = [Worker(sc, port, pid)]

    def __str__(self):
        return 'JetStreamCluster({})'.format(repr(self.name))
    
    def scheduler(self):
        return self.workers[0].instance.result()

    async def _address(self, instance):
        inst = await instance
        return await self.runner.execute(RETRY(inst.address))

    async def _close_worker(self, worker, **kwargs):
        options = self.ssh_options.copy()
        options.update(kwargs)
        address = await self._address(worker.instance)
        pid = await worker.pid
        await ssh.stop_client(address, pid, signal='INT', **options)

    def instances(self, *, workers=None):
        workers = self.workers if workers is None else workers
        out = []
        for w in workers:
            try: out.append(w.instance.result())
            except Exception as e: handle(e)
        return out

    def close(self, *, workers=None):
        workers = self.workers if workers is None else workers
        for w in workers:
            w.close = w.close if w.close is None else self.runner.put(self._close_worker(w))
        self.runner.wait([w.close for w in workers], timeout=60)    
        self.runner.wait([w.instance for w in workers], timeout=60)
        deleted = self.runner.pool.map(lambda i: i.delete(), self.instances(workers=workers))
        return len(workers) - len(tuple(deleted))

    def stop_worker(self, w):
        return self.close(workers=[w])[0]

    async def _run_work(self, instance, port, **kwargs):
        '''run_worker(host, port, saddr, sport, nthreads, nprocs, event, log=None, python=None, interface='eth0', **kwargs)'''
        saddr = await self._address(self.workers[0].instance)
        waddr = await self._address(instance)
        return await ssh.run_worker(waddr, port, saddr, self.workers[0].port, 1, 1, **kwargs)

    def add_worker(self, image, flavor, port=8000, **kwargs):
        '''
        wait for instance.status() to be active
        and wait for instance.ip()
        dask-worker {SCHEDULERIP}:8786 --nthreads 0 --nprocs 1
            --listen-address tcp://{WORKERETH}:8001
            --contact-address tcp://{WORKERIP}:8001
        '''
        name = '{}-{}'.format(self.name, len(self.workers))
        inst = self.runner.put(Instance.create(name, image=image, flavor=flavor))
        pid = self.runner.put(self._run_work(inst, port, **kwargs))
        self.workers.append(Worker(inst, port, pid))

    #@gen.coroutine
    def scale_up(self, n, **kwargs):
        """ Bring the total count of tasks up to ``n``
        This can be implemented either as a function or as a Tornado coroutine.
        """
        with log_errors():
            kwargs2 = toolz.merge(self.worker_kwargs, kwargs)
            yield [self._start_worker(**kwargs2) for i in range(n - len(self.scheduler.tasks))]

            # clean up any closed worker
            self.tasks = [w for w in self.tasks if w.status != 'closed']

    #@gen.coroutine
    def scale_down(self, tasks):
        """ Remove ``workers`` from the cluster

        Given a list of worker addresses this function should remove those
        workers from the cluster.  This may require tracking which jobs are
        associated to which worker address.

        This can be implemented either as a function or as a Tornado coroutine.
        """
        with log_errors():
            # clean up any closed worker
            self.workers = [w for w in self.workers if w.status != 'closed']
            workers = set(workers)

            # we might be given addresses
            if all(isinstance(w, str) for w in workers):
                workers = {w for w in self.workers if w.worker_address in workers}

            # stop the provided workers
            yield [self._stop_worker(w) for w in workers]

################################################################################

