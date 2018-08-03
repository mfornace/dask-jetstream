import asyncio, uuid, distributed, logging
import fn

from .openstack import Instance, FloatingIP, retry_openstack
from .future import AsyncThread, failed, result, block
from .script import scheduler_script, worker_script

log = logging.getLogger(__name__)

################################################################################

class JetStreamCluster(fn.ClosingContext):
    async def _scheduler(self, flavor, volume):
        host = str(await self.scheduler[0])
        script = scheduler_script(host, self.scheduler[1], volume, python=self.python, preload=self.preload)
        log.debug(fn.message('Submitting scheduler script', contents=script))
        return await Instance.create(self.name, image=self.image, flavor=flavor, ip=self.scheduler[0], userdata=script)

    def __init__(self, name, flavor, image, port=8786, *, preload=None, python=None, volume=None):
        self.name = str(uuid.uuid4()) if name is None else name
        self.python = python
        self.runner = AsyncThread()
        self.image = image
        self.scheduler = (self.runner.execute(retry_openstack(FloatingIP.create), 'public'), port)
        self.preload = preload
        self.instances = [self.runner.put(self._scheduler(flavor, volume))]

    async def _close(self, instance):
        inst = await instance
        await self.runner.execute(retry_openstack(inst.close))

    def close(self, *, instances=None):
        '''Stop a set of workers which defaults to all workers'''
        instances = self.instances if instances is None else instances
        return [self.runner.put(self._close(i)) for i in self.instances]

    async def _worker(self, name, image, flavor, port=8785, preload=None):
        scheduler = await self.scheduler[0], self.scheduler[1]
        host = self.runner.execute(retry_openstack(FloatingIP.create), 'public')
        ip = await host
        script = worker_script((ip, port), scheduler=scheduler, python=self.python, preload=preload)
        log.debug(fn.message('Submitting worker script', contents=script))
        return await Instance.create(name, image=image, flavor=flavor, ip=host, userdata=script)

    def add_worker(self, flavor, image=None, preload=None):
        '''
        wait for instance.status() to be active
        and wait for instance.ip()
        dask-worker {SCHEDULERIP}:8786 --nthreads 0 --nprocs 1
            --listen-address tcp://{WORKERETH}:8001
            --contact-address tcp://{WORKERIP}:8001
        '''
        name = '{}-{}'.format(self.name, len(self.instances))
        image = self.image if image is None else image
        self.instances.append(self.runner.put(self._worker(name, image, flavor, preload=preload)))

    @property
    def scheduler_address(self):
        [addr] = self.runner.wait([self.scheduler[0]], timeout=None)[0]
        return '%s:%d' % (addr.result(), self.scheduler[1])

    def client(self, attempts=10, **kwargs):
        '''Wait for scheduler to be initialized and return Client(self)'''
        block(self.workers[0])
        for i in reversed(range(attempts)):
            try:
                return distributed.Client(self, **kwargs)
            except (TimeoutError, ConnectionRefusedError, OSError) as e:
                if i == 0: raise e

    def __str__(self):
        return 'JetStreamCluster({}, {})'.format(repr(self.name), len(self.instances))

    __repr__ = __str__

    #def __getstate__(self):
    #    out = dict(self.__dict__)
    #    out.pop('runner')
    #    out['workers'] = [() for w in self.workers]

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

