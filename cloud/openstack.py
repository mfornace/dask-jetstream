"""
Utilities for dealing with OpenStack
"""
import json, time, itertools, logging
from concurrent.futures import ThreadPoolExecutor

from novaclient.exceptions import BadRequest, Conflict, NotFound
from keystoneauth1.exceptions import RetriableConnectionFailure
import os_client_config

import fn
from .future import async_exe

log = logging.getLogger(__name__)

################################################################################

FLAVORS, VOLUMES, NETWORKS, IMAGES = set(), {}, {}, {}

def set_config(dict_like):
    VOLUMES.update(dict_like['volumes'])
    NETWORKS.update(dict_like['networks'])
    FLAVORS.update(dict_like['flavors'])
    IMAGES.update(dict_like['images'])

def lookup(where, key):
    '''Disambiguates the key repeatedly, if a list takes the first element'''
    while key in where:
        key = where[key]
        key = key if isinstance(key, str) else key[0]
    return key

def close_openstack(pool=None, graceful=True):
    '''Close all instances and IPs'''
    work = Instance.list() + FloatingIP.list()
    if not work:
        return ()
    if pool is None:
        pool = ThreadPoolExecutor(len(work))
    return tuple(pool.map(lambda i: i.close(graceful=graceful), work))

################################################################################

def check_version():
    '''Check version since novaclient 10 or greater has missing method'''
    import novaclient
    novaclient.__version__ = getattr(novaclient, '__version__', '9.1.1')
    from novaclient.v2.servers import Server
    if not hasattr(Server, 'add_floating_ip'):
        raise ImportError('Use novaclient 9.1.1 or lower')

check_version()

################################################################################

_make_client = fn.lru_cache(4)(os_client_config.make_client)

def as_nova(nova=None):
    '''Return a nova client from options or a nova client itself'''
    nova = 'compute' if nova is None else nova
    return _make_client(nova) if isinstance(nova, str) else nova

def as_neutron(neutron=None):
    '''Return a neutron client from options or a neutron client itself'''
    neutron = 'network' if neutron is None else neutron
    return _make_client(neutron) if isinstance(neutron, str) else neutron

################################################################################

EXCEPTIONS = [BadRequest, ConnectionRefusedError, RetriableConnectionFailure, Conflict]

def retry_openstack(function, timeout=120, exceptions=None):
    '''Reattempt OpenStack calls that give given exception types'''
    exc = tuple(EXCEPTIONS if exceptions is None else exceptions)
    @fn.wraps(function)
    def retryable(*args, **kwargs):
        start = time.time()
        for i in itertools.count():
            try:
                return function(*args, **kwargs)
            except exc as e:
                if time.time() > start + timeout: raise e
            time.sleep(2 ** (i-2))
    return retryable

################################################################################

def report(instance=1, flavor=0, ip=0, image=0, as_dict=False):
    '''Return string describing all services'''
    f = lambda c: list(map(str, c.list()))
    C = [Instance, Flavor, FloatingIP, Image]
    B = [instance, flavor, ip, image]
    N = ['Instances', 'Flavors', 'IPs', 'Images']
    d = {n: f(c) for (n, c, b) in zip(N, C, B) if b}
    return d if as_dict else json.dumps(d, indent=4)

################################################################################

class OS:
    '''An OpenStack wrapping base class'''
    def __init__(self, os, name=None):
        if name is not None:
            if type(os).__name__ != name:
                raise TypeError('Expected type %s but got %s' % (name, type(os).__name__))
        self.os = os

    def __getattr__(self, name):
        return getattr(self.os, name)

    @property
    def name(self):
        return getattr(self.os, 'name', 'unnamed')

    def __str__(self):
        return "{}('{}')".format(type(self).__name__, self.name)

################################################################################

class Flavor(OS):
    @classmethod
    def list(cls, nova=None):
        return list(map(cls, as_nova(nova).flavors.list()))

    def __init__(self, flavor, nova=None):
        if isinstance(flavor, str):
            assert flavor in FLAVORS
            flavor = as_nova(nova).flavors.find(name=flavor)
        super().__init__(flavor, 'Flavor')

################################################################################

class Image(OS):
    @classmethod
    def list(cls, nova=None):
        return list(map(cls, as_nova(nova).glance.list()))

    def __init__(self, image='ubuntu', nova=None):
        if isinstance(image, str):
            image = lookup(IMAGES, image)
            image = as_nova(nova).glance.find_image(image)
        assert image is not None
        super().__init__(image, 'Image')

################################################################################

class FloatingIP(OS, fn.ClosingContext):
    @classmethod
    def list(cls, neutron=None):
        return list(map(FloatingIP, as_neutron(neutron).list_floatingips()['floatingips']))

    def __init__(self, ip, neutron=None):
        self.neutron = as_neutron(neutron)
        if isinstance(ip, str):
            ip = {i['floating_ip_address'] : i for i in self.neutron.list_floatingips()['floatingips']}[ip]
        super().__init__(ip, 'dict')

    @classmethod
    def create(cls, net='public', neutron=None):
        '''openstack floating ip create public'''
        body = {'floating_network_id': Network(net).id}
        neutron = as_neutron(neutron)
        return cls(neutron.create_floatingip(dict(floatingip=body))['floatingip'], neutron)

    def __str__(self):
        return self.address

    def __repr__(self):
        return "FloatingIP('%s')" % self.address

    def status(self):
        self.os = self.neutron.find_resource_by_id('floatingip', self.id)
        return self.os['status'].lower()

    def close(self, graceful=True):
        with fn.ErrorContext(log, 'Failed to close FloatingIP'):
            retry_openstack(self.neutron.delete_floatingip)(self.id)

for k, v in dict(address='floating_ip_address', id='id').items():
    setattr(FloatingIP, k, property(lambda self, v=v: self.os[v]))

################################################################################

class Instance(OS, fn.ClosingContext):
    '''
    openstack server add security group  ${OS_USERNAME}-api-U-1 global-ssh
    openstack server remove remove security group ${OS_USERNAME}-api-U-1 global-ssh
    '''

    @classmethod
    def list(cls, nova=None):
        return list(map(cls, as_nova(nova).servers.list()))

    def __init__(self, instance, nova=None):
        '''Instance is ID, name, or instance'''
        self.nova = as_nova(nova)
        if isinstance(instance, str):
            try:
                instance = self.nova.servers.find(id=instance)
            except NotFound:
                instance = self.nova.servers.find(name=instance)
        super().__init__(instance, 'Server')
        self._ip = None
        self.volumes = []

    @classmethod
    async def create(cls, name, image, flavor, *, ip=None, pool=None, net=None, nova=None,
        userdata=None, key='mfornace-api-key', groups=['mfornace-global-ssh']):
        '''openstack server create ${OS_USERNAME}-api-U-1 \
            --flavor m1.tiny \
            --image IMAGE-NAME \
            --key-name ${OS_USERNAME}-api-key \
            --security-group global-ssh \
            --nic net-id=${OS_USERNAME}-api-net
        '''
        nics = [{'net-id': Network(net).id}]
        log.info(fn.message('Creating OpenStack instance', image=str(image), flavor=str(flavor)))
        os = await async_exe(pool, as_nova(nova).servers.create, name=name, image=Image(image).os,
            flavor=Flavor(flavor).os, key_name=key, nics=nics, security_groups=groups, userdata=userdata)
        out = Instance(os)
        with fn.ErrorContext(log, 'Failed to create IP'):
            if ip is None:
                ip = await async_exe(pool, FloatingIP.create, 'public')
            else:
                ip = await ip
        with fn.ErrorContext(log, 'Failed to associate IP with server'):
            await async_exe(pool, retry_openstack(out.add_ip), ip)
        return out

    def attach_volume(self, volume, device=None):
        '''Attach a volume that already exists'''
        volume = lookup(VOLUMES, volume)
        self.nova.volumes.create_server_volume(self.id, volume, device=device)
        self.volumes.append(volume)
        return volume

    def detach_volume(self, volume):
        '''Detach a volume by index or ID'''
        volume = lookup(VOLUMES, volume)
        volume = volume if isinstance(volume, str) else self.volumes[volume]
        self.nova.volume.delete_server_volume(self.id, volume)
        self.volumes.remove(volume)
        return volume

    def add_ip(self, ip):
        '''openstack server add floating ip ${OS_USERNAME}-api-U-1 your.ip.number.here'''
        out = self.os.add_floating_ip(ip.address)
        #as_neutron().update_floatingip(ip.id, {'floatingip': {'port_id': self.id}})
        self._ip = ip
        return out

    def status(self):
        '''Search for current status'''
        try:
            self.os = next(i for i in self.nova.servers.list() if i.id == self.id)
            return self.os.status.lower()
        except StopIteration:
            return 'missing'

    def find_ip(self, neutron=None):
        '''Search for ip if not cached'''
        try:
            if self._ip is None:
                ips = {i['floating_ip_address'] : i for i in as_neutron(neutron).list_floatingips()['floatingips']}
                self._ip = FloatingIP(next(ips[i] for n in self.os.networks.values() for i in n if i in ips))
            return self._ip
        except StopIteration:
            raise BadRequest('IP not found')

    def ip(self, retry=True, neutron=None):
        return retry_openstack(self.find_ip)(neutron) if retry else self.find_ip(neutron)

    def address(self, retry=True, neutron=None):
        return self.ip(retry=retry, neutron=neutron).address

    def prune(self, pid=True):
        '''Remove workers that are errored'''
        f = (lambda w: w.pid) if pid else (lambda w: w.instance)
        self.workers = [w for w in self.workers if not f(w).done() or f(w).exception() is None]

    def create_image(self, name, public=False, **metadata):
        metadata['visibility'] = 'public' if public else 'private'
        return self.os.create_image(name, metadata=metadata)

    def close(self, graceful=True, neutron=None):
        '''should never throw unless interrupted
        tries to shut down instance first
        openstack server remove floating ip ${OS_USERNAME}-api-U-1 your.ip.number.here
        openstack server delete ${OS_USERNAME}-api-U-1
        '''
        try:
            self.stop()
            for i in range(30):
                if not graceful or self.status() != 'active': break
                time.sleep(1)
        except Exception: # for instance, Conflict if already stopped
            pass
        ips = {i['floating_ip_address'] : i for i in as_neutron(neutron).list_floatingips()['floatingips']}
        [FloatingIP(ips[ip]).close() for n in self.os.networks.values() for ip in n if ip in ips]
        with fn.ErrorContext(log, 'Failed to close Instance'):
            retry_openstack(self.nova.servers.delete)(self.os)

    def __str__(self):
        ip = ', ip={}'.format(self._ip) if self._ip else ''
        return "('{}'{})".format(self.name, ip)

    def __repr__(self):
        return 'Instance' + str(self)

for _i in 'suspend resume start stop reboot'.split():
    def command(self, *, _cmd=_i):
        '''Run command on instance'''
        getattr(self.nova.servers, _cmd)(self.os)
    setattr(Instance, _i, command)

################################################################################

class Router(OS):
    '''
    openstack router unset --external-gateway ${OS_USERNAME}-api-router
    openstack router remove subnet ${OS_USERNAME}-api-router ${OS_USERNAME}-api-subnet1
    openstack router delete ${OS_USERNAME}-api-router
    '''
    @classmethod
    def create(cls, neutron=None):
        return cls(as_neutron(neutron).create_router(dict(router={}))['router'])

################################################################################

class Network(OS):
    @classmethod
    def list(cls, nova=None):
        return list(map(cls, as_nova(nova).neutron.list()))

    def __init__(self, net=None, nova=None):
        if net is None or isinstance(net, str):
            net = as_nova(nova).neutron.find_network(NETWORKS.get(net, net))
        super().__init__(net)

    @classmethod
    def create(cls, net, nova=None):
        return cls(as_nova(nova).neutron.find_network(NETWORKS.get(net, net)))

################################################################################

