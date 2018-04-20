from distributed.deploy.ssh import SSHCluster
from .openstack import Instance

import uuid

import fn

info = fn.logs(__name__, 'info')

###################################################################

class JetStreamCluster(SSHCluster):
    def __init__(self, image, flavor, port=8786, nthreads=0, nprocs=1,
                 ssh_user=None, ssh_port=22, ssh_key=None,
                 nohost=False, logdir=None, python=None):
        self.name = str(uuid.uuid4())
        self.instances = [Instance(self.name, image=image, flavor=flavor)]
        addr = self.instances[0].ip()

        # dask-scheduler --port 8000
        super().__init__(addr, port, [], nthreads, nprocs, ssh_username=ssh_user, 
            ssh_port=ssh_port, ssh_private_key=ssh_key,
            nohost=nohost, logdir=logdir, remote_python=python)
        
    def shutdown(self):
        super().shutdown()
        for i in self.instances: i.delete()

    def add_worker(self, image, flavor):
        '''
        wait for instance.status() to be active
        and wait for instance.ip()
        dask-worker {SCHEDULERIP}:8786 --nthreads 0 --nprocs 1 
            --listen-address tcp://{WORKERETH}:8001 
            --contact-address tcp://{WORKERIP}:8001
        '''
        name = '{}-{}'.format(self.name, len(self.instances))
        inst = Instance(name, image=image, flavor=flavor)
        self.instances.append(inst)
        super().add_worker(inst.ip())

    def stop_worker(self, w):
        pass

    #@gen.coroutine
    def scale_up(self, n, **kwargs):
        """ Bring the total count of workers up to ``n``
        This can be implemented either as a function or as a Tornado coroutine.
        """
        with log_errors():
            kwargs2 = toolz.merge(self.worker_kwargs, kwargs)
            yield [self._start_worker(**kwargs2)
                   for i in range(n - len(self.scheduler.workers))]

            # clean up any closed worker
            self.workers = [w for w in self.workers if w.status != 'closed']

    #@gen.coroutine
    def scale_down(self, workers):
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


####################################################

