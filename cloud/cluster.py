import asyncio, uuid, distributed, logging, time
import fn

from .ostack import create_server, close_server, create_ip
from .future import AsyncThread, failed, result, block
from .script import scheduler_script, worker_script

log = logging.getLogger(__name__)

################################################################################

class JetStreamCluster(fn.ClosingContext):
    async def _scheduler(self, ip, port, flavor, volume):
        script = scheduler_script(ip, port, volume, python=self.python, preload=self.preload)
        print(script)
        log.debug(fn.message('Submitting scheduler script', contents=script))
        return await create_server(self.conn, name=self.name, network=self.network,
            image=self.image, flavor=flavor, ip=ip, user_data=script)

    def __init__(self, conn, name, flavor, image, network, port=8786, *, preload=None, python=None, volume=None):
        self.name = str(uuid.uuid4()) if name is None else name
        self.python = python
        self.conn = conn
        self.runner = AsyncThread()
        self.image = image
        self.network = network
        self.preload = preload
        ip = create_ip(conn)
        self.instances = [(ip, port, self.runner.put(self._scheduler(ip, port, flavor, volume)))]

    async def _close(self, instance):
        await self.runner.execute(close_server, self.conn, await instance[2])
        self.conn.delete_floating_ip(instance[0])

    def close(self, *, instances=None):
        '''Stop a set of workers which defaults to all workers'''
        instances = self.instances if instances is None else instances
        return [self.runner.put(self._close(i)) for i in self.instances]

    async def _worker(self, name, ip, script, *, image, flavor):
        log.debug(fn.message('Submitting worker script', contents=script))
        await self.instances[0][2]
        return await create_server(self.conn, name=name, image=image,
            flavor=flavor, ip=ip, network=self.network, user_data=script)

    def add_worker(self, flavor, image=None, port=8785, preload=None):
        '''
        wait for instance.status() to be active
        and wait for instance.ip()
        dask-worker {SCHEDULERIP}:8786 --nthreads 0 --nprocs 1
            --listen-address tcp://{WORKERETH}:8001
            --contact-address tcp://{WORKERIP}:8001
        '''
        name = '{}-{}'.format(self.name, len(self.instances))
        image = self.image if image is None else image
        ip = create_ip(self.conn)
        assert not any(ip == i[0] for i in self.instances)
        try:
            script = worker_script((ip, port), scheduler=self.instances[0][:2], python=self.python, preload=preload)
            inst = self.runner.put(self._worker(name, ip, script, image=image, flavor=flavor))
            self.instances.append((ip, port, inst))
        except Exception:
            self.conn.delete_floating_ip(ip)
            raise

    @property
    def scheduler_address(self):
        return '%s:%d' % self.instances[0][:2]

    def client(self, attempts=10, **kwargs):
        '''Wait for scheduler to be initialized and return Client(self)'''
        block(self.instances[0][2])
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

