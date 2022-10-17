import uuid, logging, time, typing
from concurrent.futures import ThreadPoolExecutor
import fn

from .ostack import create_server, close_server, create_ip

log = logging.getLogger(__name__)

SCRIPT = r'''#!/usr/bin/env bash
cd /home/ubuntu
/home/ubuntu/miniconda3/bin/python fks.py --set-dns --sleep 30
'''

################################################################################

class FksInstance(typing.NamedTuple):
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
        return 'FksInstance(%r, %r)' % (self.id, self.server.interface_ip)

    def __str__(self):
        return '(%r, %r)' % (self.id, self.server.interface_ip)


################################################################################

class FksCluster:
    def all_active_servers(self):
        return [FksInstance(s, c) for c in self.connections for s in c.list_servers() if s.status == 'ACTIVE']

    def __init__(self, connections, name, image, network, *, threads=16):
        self.name = str(uuid.uuid4()) if name is None else name
        self.connections = list(connections)
        self.pool = ThreadPoolExecutor(threads)
        self.image = image
        self.network = network

        servers = self.all_active_servers()
        self.workers = [s for s in servers if (self.name + '-worker') in s.name]

    def _close(self, instance):
        ip = instance.close()
        try:
            del self.workers[next(i for i, w in enumerate(self.workers) if w.id == instance.id)]
        except StopIteration:
            pass
        return ip

    def stop_workers(self, names):
        '''Stop workers given a list of their names'''
        tasks = [self.pool.submit(self._close, w) for w in self.workers if w.name in names]
        return [t.result() for t in tasks]

    def close(self):
        '''Stop all workers and the head nodes'''
        return [self.pool.submit(self._close, i) for i in self.workers]

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

    def scale_up(self, conn, n, *, flavor, image=None):
        '''Add workers to get up to n total workers'''
        tasks = [self.add_worker(conn, flavor=flavor, script=SCRIPT, image=image) for _ in range(len(self.workers), n)]
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
        return 'FksCluster(%r, %d workers)' % (self.name, len(self.workers))

    __repr__ = __str__

################################################################################

