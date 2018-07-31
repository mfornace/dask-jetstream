import asyncio, uuid, distributed
import fn

from .openstack import Instance, retry_openstack
from .future import AsyncThread, failed, result, block
from . import ssh

################################################################################

info, error = fn.logs(__name__, 'info', 'error')

################################################################################

class Worker:
    def __init__(self, instance, port, pid):
        self.instance = instance
        self.port = port
        self.pid = pid
        self.close = None

    def __str__(self):
        if failed(self.pid) or failed(self.instance): s = 'error'
        elif not self.instance.done(): s = 'booting'
        elif not self.pid.done(): s = 'connecting'
        elif self.close is None: s = 'active'
        elif self.close.done(): s = 'closed'
        else: s = 'closing'
        return '(status={}, port={}, pid={}, instance={})'.format(s, self.port, result(self.pid), result(self.instance))

################################################################################

class JetStreamCluster(fn.ClosingContext):

    async def _start_scheduler(self, future, port, volume, **kwargs):
        address = await self._address(future)
        if volume is not None:
            inst = await future
            volume = await self.runner.execute(retry_openstack(inst.attach_volume), volume)
        return await ssh.start_scheduler(address, port, volume, **kwargs)

    def __init__(self, name, flavor, image, port=8786, *, python=None, volume=None, preload='', ssh={}):
        self.name = str(uuid.uuid4()) if name is None else name
        self.exe_options = dict(python=python, preload=preload)
        self.ssh_options = ssh
        self.runner = AsyncThread()
        self.image = image
        sc = self.runner.put(Instance.create(self.name, image=image, flavor=flavor))
        pid = self.runner.put(self._start_scheduler(sc, port=port, volume=volume, **self.exe_options,  **ssh))
        info('Started scheduler instance and process')
        self.workers = [Worker(sc, port, pid)]

    def __str__(self):
        return 'JetStreamCluster({})'.format(repr(self.name))

    def load_scheduler(self):
        return block(self.workers[0].instance)

    async def _address(self, instance):
        inst = await instance
        return await self.runner.execute(retry_openstack(inst.address))

    async def _close_worker(self, worker, signal='INT', sleep=None, **kwargs):
        sleep = (0.1, 0.25, 0.5, 1, 1, 1, 2, 5) if sleep is None else sleep
        options = self.ssh_options.copy()
        options.update(kwargs)
        address = await self._address(worker.instance)
        pid = await worker.pid
        await ssh.stop_client(address, pid, signal=signal, sleep=sleep, **options)

    def instances(self, *, workers=None):
        workers = self.workers if workers is None else workers
        out = []
        for w in workers:
            try:
                out.append(w.instance.result())
            except Exception as e:
                out.append(None)
                error('Failed to create instance', exception=e)
        return out

    @property
    def scheduler_address(self):
        [addr] = self.runner.wait([self._address(self.workers[0].instance)], timeout=None)[0]
        return '%s:%d' % (addr.result(), self.workers[0].port)

    def __getstate__(self):
        out = dict(self.__dict__)
        out.pop('runner')
        out['workers'] = [() for w in self.workers]

    def close(self, *, workers=None, timeouts=(60, 60)):
        workers = self.workers if workers is None else workers
        for w in workers:
            w.close = self.runner.put(self._close_worker(w)) if w.close is None else w.close
        self.runner.wait([w.close for w in workers], timeout=timeouts[0])
        self.runner.wait([w.instance for w in workers], timeout=timeouts[1])
        inst = self.instances(workers=workers)
        tuple(self.runner.pool.map(lambda i: None if i is None else i.close(), inst))
        return tuple(w for (i, w) in zip(inst, workers) if i is None)

    def stop_worker(self, w):
        return self.close(workers=[w])[0]

    async def _start_worker(self, instance, port, **kwargs):
        '''ok'''
        addrs = await asyncio.gather(*map(self._address, (instance, self.workers[0].instance)))
        options = self.ssh_options.copy()
        options.update(kwargs)
        return await ssh.start_worker(*zip(addrs, [port, self.workers[0].port]), **self.exe_options, **options)

    def add_workers(self, flavor, image=None, ports=(8000,), **kwargs):
        '''
        wait for instance.status() to be active
        and wait for instance.ip()
        dask-worker {SCHEDULERIP}:8786 --nthreads 0 --nprocs 1
            --listen-address tcp://{WORKERETH}:8001
            --contact-address tcp://{WORKERIP}:8001
        '''
        out = []
        name = '{}-{}'.format(self.name, len(self.workers))
        image = self.image if image is None else image
        inst = self.runner.put(Instance.create(name, image=image, flavor=flavor))
        for port in ports:
            pid = self.runner.put(self._start_worker(inst, port, **kwargs))
            out.append(Worker(inst, port, pid))
            self.workers.append(out[-1])
        return out

    def client(self, attempts=10, **kwargs):
        '''Wait for scheduler to be initialized and return Client(self)'''
        block(self.workers[0].pid)
        self.workers[0].pid.result() # check for error in startup
        for i in reversed(range(attempts)):
            try:
                return distributed.Client(self, **kwargs)
            except (TimeoutError, ConnectionRefusedError, OSError) as e:
                if i == 0: raise e

    # async def scale_up(self, n, **kwargs):
    #     """ Bring the total count of tasks up to ``n``
    #     This can be implemented either as a function or as a Tornado coroutine.
    #     """
    #     with error.context('Failed to scale_up workers'):
    #         #kwargs2 = toolz.merge(self.worker_kwargs, kwargs)
    #         self.add_worker()
    #         #yield [self._start_worker(**kwargs2) for i in range(n - len(self.scheduler.tasks))]

    #         # clean up any closed worker
    #         #self.tasks = [w for w in self.tasks if w.status != 'closed']

    # async def scale_down(self, tasks):
    #     """ Remove ``workers`` from the cluster

    #     Given a list of worker addresses this function should remove those
    #     workers from the cluster.  This may require tracking which jobs are
    #     associated to which worker address.

    #     This can be implemented either as a function or as a Tornado coroutine.
    #     """
    #     with error.context('Failed to scale_down workers'):
    #         self.stop_worker()
    #         # clean up any closed worker
    #         #self.workers = [w for w in self.workers if w.status != 'closed']
    #         #workers = set(workers)

    #         # we might be given addresses
    #         #if all(isinstance(w, str) for w in workers):
    #         #    workers = {w for w in self.workers if w.worker_address in workers}

    #         # stop the provided workers
    #         #yield [self._stop_worker(w) for w in workers]

################################################################################

