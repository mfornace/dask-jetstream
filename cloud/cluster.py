logger = logging.getLogger(__name__)

###################################################################

class JetStreamCluster:
    def __init__(self, workers, nthreads, nprocs, **kwargs):
        self.scheduler = Instance()
        s_addr = None
        s_port = None
        self.workers = [Instance() for w in workers]
        w_addrs = []
        self.ssh = SSHCluster(s_addr, s_port, w_addrs, nthreads, nprocs, **kwargs)

    def start(self, **kwargs):
        pass

    def shutdown(self):
        self.ssh.shutdown()

    def start_scheduler(self):
        instance = Instance(name, image=image, flavor=flavor)
        ssh.remote_submit(address, 'local', settings, 
            python='/usr/anaconda3/bin/python3', user='root'):

    def start_worker(self):
        instance = Instance(name, image=image, flavor=flavor)
        ssh.remote_submit(address, 'local', settings, 
            python='/usr/anaconda3/bin/python3', user='root'):
        # wait for instance.status() to be active
        # and wait for instance.ip()

    def stop_worker(self, w):
        pass

    @gen.coroutine
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

    @gen.coroutine
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

