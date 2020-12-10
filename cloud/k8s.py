import uuid, logging, time, shortuuid
from concurrent.futures import ThreadPoolExecutor
import fn

from .ostack import create_server, close_server, create_ip

log = logging.getLogger(__name__)

FRONT_CMD = '''#!/usr/bin/env bash
docker run -d --restart=unless-stopped --privileged  -p 80:80 -p 443:443 -v /opt/rancher:/var/lib/rancher rancher/rancher:latest
'''

################################################################################

class K8sCluster:
    def __init__(self, conn, name, flavor, image, network, *, threads=16, launch=False):
        self.name = str(uuid.uuid4()) if name is None else name
        self.conn = conn
        self.pool = ThreadPoolExecutor(threads)
        self.image = image
        self.network = network

        servers = [s for s in conn.list_servers() if s.status == 'ACTIVE']

        if launch:
            assert not any(self.name in s.name for s in servers)
            self.workers = []

            ip = create_ip(conn)
            log.info('starting front-end at {}'.format(ip))
            self.front = create_server(self.conn, name=self.name+'-front',
                network=self.network, image=self.image, flavor=flavor,
                ip=ip, user_data=FRONT_CMD)

            cmd = input(('Wait for the IP {} to appear in the browser. Then set up the '
                         'cluster and input the docker run command here with the etcd '
                         'and control plane layers activated in the toggle').format(ip))

            ip = create_ip(conn)
            log.info('starting scheduler at {}'.format(ip))
            self.scheduler = create_server(self.conn, name=self.name+'-scheduler',
                network=self.network, image=self.image, flavor=flavor,
                ip=ip, user_data='#!/bin/bash\n' + cmd.strip())

            servers = [s for s in conn.list_servers() if s.status == 'ACTIVE']

        self.front = next(s for s in servers if s.name == self.name + '-front')
        self.scheduler = next(s for s in servers if s.name == self.name + '-scheduler')
        self.workers = [s for s in servers if (self.name + '-worker') in s.name]

    def _close(self, instance):
        ip = instance.interface_ip
        close_server(self.conn, instance)
        self.conn.delete_floating_ip(ip)
        try:
            del self.workers[next(i for i, w in enumerate(self.workers)
                             if w.id == instance.id)]
        except StopIteration:
            pass
        return ip

    def stop_worker(self, name):
        '''Stop a single worker'''
        self._close(next(w for w in self.workers if w.name == name))

    def close(self):
        '''Stop all workers and the head nodes'''
        self.instances = [self.front, self.scheduler] + self.workers
        return [self.pool.submit(self._close, i) for i in instances]

    def _worker(self, script, *, image, flavor):
        ip = create_ip(self.conn)
        assert not any(ip == i.interface_ip for i in self.workers)
        try:
            log.info('creating worker at {}'.format(ip))
            server = create_server(self.conn, name=self.name + '-worker-' + ip.replace('.', '-'),
                image=image, flavor=flavor, ip=ip, network=self.network, user_data=script)
            server.interface_ip  = ip
            self.workers.append(server)
            return ip
        except Exception as e:
            log.info('failed to create worker at {}: {}'.format(ip, e))
            self.conn.delete_floating_ip(ip)
            raise

    def add_worker(self, *, flavor, script, image=None):
        '''Add a single worker (asynchronous)'''
        image = self.image if image is None else image
        return self.pool.submit(self._worker, script, image=image, flavor=flavor)

    def scale_up(self, n, *, flavor, script, image=None):
        '''Add workers to get up to n total workers'''
        tasks = [self.add_worker(flavor=flavor, script=script, image=image) for _ in range(len(self.workers), n)]
        return [t.result() for t in tasks]

    def refresh(self):
        '''Refresh fetched data -- probably better to just remake cluster though'''
        servers = {v.id : v for v in self.conn.list_servers()}
        for w in self.workers:
            w.update(servers.get(w.id, {}))

    def stop_all_workers(self):
        '''Remove all workers'''
        tasks = [self.pool.submit(self._close, i) for i in self.workers]
        return [t.result() for t in tasks]

    def __str__(self):
        return 'K8sCluster(%r, %s, %d)' % (self.name, self.front.interface_ip, len(self.workers))

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

