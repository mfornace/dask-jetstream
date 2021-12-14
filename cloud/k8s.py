import uuid, logging, time, typing
from concurrent.futures import ThreadPoolExecutor
import fn

from .ostack import create_server, close_server, create_ip

log = logging.getLogger(__name__)

FRONT_CMD = '''#!/usr/bin/env bash
docker run -d --restart=unless-stopped --privileged  -p 80:80 -p 443:443 -v /opt/rancher:/var/lib/rancher rancher/rancher:{}
'''

################################################################################

class K8sInstance(typing.NamedTuple):
    server: object
    connection: object

    @property
    def name(self):
        return self.server.name

    @property
    def id(self):
        return self.server.id

    @property
    def interface_ip(self):
        return self.server.interface_ip

    def close(self):
        ip = self.interface_ip
        close_server(self.connection, self.server)
        self.connection.delete_floating_ip(ip)
        return ip

    def __repr__(self):
        return 'K8sInstance(%r, %r)' % (self.id, self.server.interface_ip)

    def __str__(self):
        return '(%r, %r)' % (self.id, self.server.interface_ip)


################################################################################

class K8sCluster:
    def all_active_servers(self):
        return [K8sInstance(s, c) for c in self.connections for s in c.list_servers() if s.status == 'ACTIVE']

    def __init__(self, connections, name, flavor, image, network, *, threads=16, rancher_tag='stable', launch=False):
        self.name = str(uuid.uuid4()) if name is None else name
        self.connections = list(connections)
        self.pool = ThreadPoolExecutor(threads)
        self.image = image
        self.network = network

        servers = self.all_active_servers()

        if launch:
            assert not any(self.name in s.name for s in servers)
            self.workers = []

            ip = create_ip(connections[0])
            log.info('starting front-end at {}'.format(ip))
            self.front = create_server(self.connections[0], name=self.name+'-front',
                network=self.network, image=self.image, flavor=flavor,
                ip=ip, user_data=FRONT_CMD.format(rancher_tag))

            cmd = input(('Wait for the IP {} to appear in the browser. Then set up the '
                         'cluster and input the docker run command here with the etcd '
                         'and control plane layers activated in the toggle').format(ip))

            ip = create_ip(connections[0])
            log.info('starting scheduler at {}'.format(ip))
            self.scheduler = create_server(self.connections[0], name=self.name+'-scheduler',
                network=self.network, image=self.image, flavor=flavor,
                ip=ip, user_data='#!/bin/bash\n' + cmd.strip())

            servers = self.all_active_servers()

        self.front = next(s for s in servers if s.name == self.name + '-front')
        self.scheduler = next(s for s in servers if s.name == self.name + '-scheduler')
        self.workers = [s for s in servers if (self.name + '-worker') in s.name]

    def remake_scheduler():
            ip = create_ip(conn)
            log.info('starting scheduler at {}'.format(ip))
            self.scheduler = create_server(self.conn, name=self.name+'-scheduler',
                network=self.network, image=self.image, flavor=flavor,
                ip=ip, user_data='#!/bin/bash\n' + cmd.strip())

    def _close(self, instance):
        ip = instance.close()
        try:
            del self.workers[next(i for i, w in enumerate(self.workers)
                             if w.id == instance.id)]
        except StopIteration:
            pass
        return ip

    def stop_workers(self, names):
        '''Stop workers given a list of their names'''
        tasks = [self.pool.submit(self._close, w) for w in self.workers if w.name in names]
        return [t.result() for t in tasks]

    def close(self):
        '''Stop all workers and the head nodes'''
        self.instances = [self.front, self.scheduler] + self.workers
        return [self.pool.submit(self._close, i) for i in self.instances]

    def _worker(self, conn, *, script, image, flavor):
        ip = create_ip(conn)
        assert not any(ip == i.interface_ip for i in self.workers)
        try:
            log.info('creating worker at {}'.format(ip))
            server = create_server(conn, name=self.name + '-worker-' + ip.replace('.', '-'),
                image=image, flavor=flavor, ip=ip, network=self.network, user_data=script)
            server.interface_ip  = ip
            self.workers.append(server)
            return ip
        except Exception as e:
            log.info('failed to create worker at {}: {}'.format(ip, e))
            conn.delete_floating_ip(ip)
            raise

    def add_worker(self, conn, *, flavor, script, image=None):
        '''Add a single worker (asynchronous)'''
        if conn not in self.connections:
            self.connections.append(conn)
        image = self.image if image is None else image
        return self.pool.submit(self._worker, conn, script=script, image=image, flavor=flavor)

    def scale_up(self, conn, n, *, flavor, script, image=None):
        '''Add workers to get up to n total workers'''
        tasks = [self.add_worker(conn, flavor=flavor, script=script, image=image) for _ in range(len(self.workers), n)]
        return [t.result() for t in tasks]

    def refresh(self):
        '''Refresh fetched data -- probably better to just remake cluster though'''
        servers = {v.id : v for c in self.connections for v in c.list_servers()}
        for w in self.workers:
            w.server.update(servers.get(w.id, {}))

    def stop_all_workers(self):
        '''Remove all workers'''
        tasks = [self.pool.submit(self._close, i) for i in self.workers]
        return [t.result() for t in tasks]

    def __str__(self):
        return 'K8sCluster(%r, %s, %d workers)' % (self.name, self.front.interface_ip, len(self.workers))

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

