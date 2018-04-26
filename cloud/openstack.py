"""
Utilities for dealing with OpenStack
"""
from novaclient.exceptions import BadRequest, Conflict, NotFound
from keystoneauth1.exceptions import RetriableConnectionFailure
import os_client_config
import json, time, itertools

import fn
from .future import async_exe

################################################################################

info, error = fn.logs(__name__, 'info', 'error')

################################################################################

def check_version():
    import novaclient
    novaclient.__version__ = getattr(novaclient, '__version__', '9.1.1')
    from novaclient.v2.servers import Server
    if not hasattr(Server, 'add_floating_ip'):
        raise ImportError('Use novaclient 9.1.1 or lower')

check_version()

################################################################################

_make_client = fn.lru_cache(4)(os_client_config.make_client)

def as_nova(nova=None):
    nova = 'compute' if nova is None else nova
    return _make_client(nova) if isinstance(nova, str) else nova

def as_neutron(neutron=None):
    neutron = 'network' if neutron is None else neutron
    return _make_client(neutron) if isinstance(neutron, str) else neutron

################################################################################

def retry_openstack(function):
    @fn.wraps(function)
    def retryable(*args, **kwargs):
        start = time.time()
        for i in itertools.count():
            try:
                return function(*args, **kwargs)
            except (BadRequest, ConnectionRefusedError, RetriableConnectionFailure) as e:
                if time.time() > start + 120: raise e
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

class ClosingContext:
    def __enter__(self):
        return self

    def __exit__(self, cls, value, traceback):
        self.close()

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

    def __str__(self):
        return "{}('{}')".format(type(self).__name__, self.os.name)

################################################################################

class Flavor(OS):
    @classmethod
    def list(cls, nova=None):
        return list(map(cls, as_nova(nova).flavors.list()))

    lookup = {'m1.tiny', 's1.large', 's1.xlarge', 's1.xxlarge', 'm1.small',
              'm1.medium', 'm1.large', 'm1.xlarge', 'm1.xxlarge'}

    def __init__(self, flavor, nova=None):
        if isinstance(flavor, str):
            assert flavor in self.lookup
            flavor = as_nova(nova).flavors.find(name=flavor)
        super().__init__(flavor, 'Flavor')

################################################################################

class Image(OS):
    lookup = {
        'dask-agent': 'ubuntu-16-conda3-dask',
        'ubuntu-16-conda3-dask': [
            '27a174a7-046f-41a6-8952-2b86a28ff599', # fix autograd
            'a66e5c39-14db-4284-adf2-282727237361',
        ][0],
        'ubuntu-16-anaconda3-dask': [
            'c89afc1e-ea4a-48da-9d62-6834411fee4a', # has rosetta
            'bab52ac7-1529-4558-9c38-45b4285ef275',
            '02d98b3f-ab1f-48c7-b75d-25f853688d44', 
        ][0],
        'ubuntu-16-anaconda3-plus2': 'd6547a8c-2760-4861-aadc-974b9519f114',
        'nupack': 'd844d19a-defc-4430-af96-d74474f6f69a',
        'pna-agent': 'd1e1f474-edaf-449b-bee0-62f6c297a3b2', #  '4a61d296-aa4d-43f1-b822-d6d23d422af3', # '3ffaffe2-d07b-4bd8-831d-e82dff12e9fd', # 'ce178ebd-00bb-4a1b-9d2e-dc3217b0d3ba',
        'ubuntu': 'ubuntu-16',
        'ubuntu-16': '7505ea37-2fbb-499f-b150-c18fade5ce26',
        'ubuntu-14': 'JS-API-Featured-Ubuntu14-Feb-23-2017',
        'ubuntu-old': '32bf80f9-7368-4b05-97b5-2e8fff0f6e80',
        'centos': 'centos-7',
        'centos-7': 'JS-API-Featured-Centos7-Dec-12-2017',
        'jupyter-tf': 'Python3_Jupyter_Tensorflow',
        'ubuntu-tf': 'Ubuntu 14.04 TensorFlow Py3.5'
    }

    @classmethod
    def list(cls, nova=None):
        return list(map(cls, as_nova(nova).glance.list()))

    def __init__(self, image='ubuntu', nova=None):
        if isinstance(image, str):
            while image in self.lookup:
                image = self.lookup[image]
            image = as_nova(nova).glance.find_image(image)
        assert image is not None
        super().__init__(image, 'Image')

################################################################################

class FloatingIP(OS, ClosingContext):
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

    def status(self):
        self.os = self.neutron.find_resource_by_id('floatingip', self.id)
        return self.os['status'].lower()

    def close(self):
        try:
            retry_openstack(self.neutron.delete_floatingip)(self.id)
        except Exception as e:
            return error('Failed to close FloatingIP', exception=e)

for k, v in dict(address='floating_ip_address', id='id').items():
    setattr(FloatingIP, k, property(lambda self, v=v: self.os[v]))

################################################################################

class Instance(OS, ClosingContext):
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

    @classmethod
    async def create(cls, name, image, flavor, *, pool=None, net=None, nova=None, 
        userdata=None, key='mfornace-api-key', groups=['mfornace-global-ssh']):
        '''openstack server create ${OS_USERNAME}-api-U-1 \
            --flavor m1.tiny \
            --image IMAGE-NAME \
            --key-name ${OS_USERNAME}-api-key \
            --security-group global-ssh \
            --nic net-id=${OS_USERNAME}-api-net
        '''
        nics = [{'net-id': Network(net).id}]
        info('Creating OpenStack instance', image=str(image), flavor=str(flavor))
        os = await async_exe(pool, as_nova(nova).servers.create, name=name, image=Image(image).os,
            flavor=Flavor(flavor).os, key_name=key, nics=nics, security_groups=groups, userdata=userdata)
        out = Instance(os)
        with error.context('Failed to create IP'):
            ip = await async_exe(pool, FloatingIP.create, 'public')
        with error.context('Failed to associate IP with server'):
            await async_exe(pool, retry_openstack(out.add_ip), ip)
        return out

    def add_ip(self, ip):
        '''openstack server add floating ip ${OS_USERNAME}-api-U-1 your.ip.number.here'''
        out = self.os.add_floating_ip(ip.address)
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

    def create_image(self, name, public=False, **metadata):
        metadata['visibility'] = 'public' if public else 'private'
        return self.os.create_image(name, metadata=metadata)

    def close(self, neutron=None):
        '''never throws
        openstack server remove floating ip ${OS_USERNAME}-api-U-1 your.ip.number.here
        openstack server delete ${OS_USERNAME}-api-U-1
        '''
        ips = {i['floating_ip_address'] : i for i in as_neutron(neutron).list_floatingips()['floatingips']}
        [FloatingIP(ips[ip]).close() for n in self.os.networks.values() for ip in n if ip in ips]
        try:
            retry_openstack(self.nova.servers.delete)(self.os)
        except Exception as e:
            return error('Failed to close Instance', exception=e)

    def __str__(self):
        ip = ', ip={}'.format(self._ip) if self._ip else ''
        return "('{}'{})".format(self.name, ip)

for _i in 'suspend resume start stop reboot'.split():
    def command(self, *, _cmd=_i):
        '''Run command on instance'''
        getattr(self.nova.servers, _cmd)(self.os)
    setattr(Instance, _i, command)

################################################################################

class Volume(OS):
    '''
    nova.volumes.create_server_volume(server_id, volume_id, device=None, tag=None)
    create_server_volume, get, update, delete
    '''
    def __init__(self):
        pass

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

    lookup = {None: 'mfornace-api-net', 'public': 'public'}

    def __init__(self, net=None, nova=None):
        if net is None or isinstance(net, str):
            net = as_nova(nova).neutron.find_network(self.lookup.get(net, net))
        super().__init__(net)

    @classmethod
    def create(cls, net, nova=None):
        return cls(as_nova(nova).neutron.find_network(cls.lookup.get(net, net)))

################################################################################

# class Security_Group(OS):
#     lookup = {i['name'] for i in neutron.list_security_groups()['security_groups']}
#
#     def __init__(self, group):
#         if group not in lookup:
#             group = neutron.create_security_group(dict(name=group, protocol='icmp'))
#         super().__init__(group)
#
#     @classmethod
#     def list(cls):
#         return list(map(Security_Group, neutron.list_security_groups()['security_groups']))
#
#     def close(self):