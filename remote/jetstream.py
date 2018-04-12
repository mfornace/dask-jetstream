"""
Utilities for dealing with JetStream
"""

from neutronclient.v2_0.client import Client as Neutron_Client
from novaclient.exceptions import BadRequest, Conflict, NotFound

import os_client_config
import time
from functools import partial

#password='6Y3-WkX-dU8-ELk',
                             #       auth_url='https://tacc.jetstream-cloud.org:5000/v3',
                                      # auth_url='https://tacc.jetstream-cloud.org:5000/v3',
                                    #password='97u-qLc-2Yx-dEz',
                                    #username='mfornace',
                                    #project_name='TG-MCB170016',
                                    #region_name='RegionOne')
nova = os_client_config.make_client('compute')

neutron = os_client_config.make_client('network')
                                      # username='mfornace',
                                      # password='97u-qLc-2Yx-dEz',
                                      # project_name='TG-MCB170016',
                                      # region_name='RegionOne')

################################################################################

class OS:
    def __init__(self, os, name=None):
        if name is not None:
            if type(os).__name__ != name:
                raise TypeError('Expected type %s but got %s' % (name, type(os).__name__))
        self.os = os

    def __getattr__(self, name):
        return getattr(self.os, name)

    @classmethod
    def list(cls):
        return list(map(cls, cls._get(nova).list()))

    @classmethod
    def all(cls):
        return [cls(i) for i in cls.list()]

    def __str__(self):
        return type(self).__name__ + '(' + self.os.name + ')'

################################################################################

class Flavor(OS):
    _get = lambda nova: nova.flavors
    lookup = {'m1.tiny', 's1.large', 's1.xlarge', 's1.xxlarge', 'm1.small',
              'm1.medium', 'm1.large', 'm1.xlarge', 'm1.xxlarge'}

    def __init__(self, flavor):
        if isinstance(flavor, str):
            assert flavor in self.lookup
            flavor = nova.flavors.find(name=flavor)
        super().__init__(flavor, 'Flavor')

################################################################################

class Image(OS):
    _get = lambda nova: nova.glance
    lookup = {
        'nupack': 'd844d19a-defc-4430-af96-d74474f6f69a',
        'pna-agent': 'd1e1f474-edaf-449b-bee0-62f6c297a3b2', #  '4a61d296-aa4d-43f1-b822-d6d23d422af3', # '3ffaffe2-d07b-4bd8-831d-e82dff12e9fd', # 'ce178ebd-00bb-4a1b-9d2e-dc3217b0d3ba',
        'ubuntu-14': 'JS-API-Featured-Ubuntu14-Feb-23-2017',
        'ubuntu': '32bf80f9-7368-4b05-97b5-2e8fff0f6e80',
        'centos': 'JS-API-Featured-Centos7-Dec-12-2017',
        'jupyter-tf': 'Python3_Jupyter_Tensorflow',
        'ubuntu-tf': 'Ubuntu 14.04 TensorFlow Py3.5'
    }

    def __init__(self, image='ubuntu'):
        if isinstance(image, str):
            name = self.lookup.get(image, image)
            image = nova.glance.find_image(name)
        assert image is not None
        super().__init__(image, 'Image')

################################################################################

class Floating_IP(OS):
    def __init__(self, ip=None, net='public', server=None):
        if ip is None:
            body = {'floating_network_id': Network(net).id}
            # openstack floating ip create public
            ip = neutron.create_floatingip(dict(floatingip=body))['floatingip']
        elif isinstance(ip, str):
            ip = {i['floating_ip_address'] : i for i in neutron.list_floatingips()['floatingips']}[ip]
        super().__init__(ip, 'dict')
        if server is not None:
            for attempt in range(320):
                try:
                    # openstack server add floating ip ${OS_USERNAME}-api-U-1 your.ip.number.here
                    server.add_floating_ip(self.address)
                    break
                except BadRequest as e:
                    time.sleep(5)
                    print(self.address, e)
                    exc = e
            else:
                raise exc

    @classmethod
    def list(cls): return list(map(Floating_IP, neutron.list_floatingips()['floatingips']))

    def __str__(self):
        return 'Floating_IP(%s, %s)' % (self.address, self.status())

    def __repr__(self): return self.address

    def status(self):
        self.os = neutron.find_resource_by_id('floatingip', self.id)
        return self.os['status'].lower()

    def delete(self): neutron.delete_floatingip(self.id)

for k, v in dict(address='floating_ip_address', id='id').items():
    setattr(Floating_IP, k, property(lambda self, v=v: self.os[v]))

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
#     def delete(self):

################################################################################

class Router(OS):
    def __init__(self, router=None):
        if router is None:
            router = neutron.create_router(dict(router={}))['router']
        super().__init__(router)
        # openstack router unset --external-gateway ${OS_USERNAME}-api-router
        # openstack router remove subnet ${OS_USERNAME}-api-router ${OS_USERNAME}-api-subnet1
        # openstack router delete ${OS_USERNAME}-api-router

################################################################################

class Network(OS):
    _get = lambda nova: nova.neutron

    lookup = {None: 'mfornace-api-net', 'public': 'public'}

    def __init__(self, net=None):
        if net is None or isinstance(net, str):
            net = nova.neutron.find_network(self.lookup.get(net, net))
        super().__init__(net, 'Network')

################################################################################

class Instance(OS):
    _get = lambda nova: nova.servers

    def __init__(self, instance, image=None, flavor=None, key='mfornace-api-key', net=None):
        """
        openstack server create ${OS_USERNAME}-api-U-1 \
            --flavor m1.tiny \
            --image IMAGE-NAME \
            --key-name ${OS_USERNAME}-api-key \
            --security-group global-ssh \
            --nic net-id=${OS_USERNAME}-api-net
        """
        if image is not None:
            nics = [{'net-id': Network(net).id}]
            instance = nova.servers.create(name=instance, image=Image(image).os,
                flavor=Flavor(flavor).os, key_name=key, nics=nics, security_groups=['mfornace-global-ssh'])
            super().__init__(instance)
            try:
                ip = Floating_IP(net='public', server=self.os)
                self.ips = {ip}
            except BaseException as e:
                try: self.delete()
                except: pass
                raise e
            return
        if isinstance(instance, str):
            try:
                instance = nova.servers.find(id=instance)
            except NotFound:
                instance = nova.servers.find(name=instance)
        super().__init__(instance, 'Server')

    # openstack server add security group  ${OS_USERNAME}-api-U-1 global-ssh
    def add_security_group(self, group): raise NotImplemented
    # openstack server remove remove security group ${OS_USERNAME}-api-U-1 global-ssh
    def remove_security_group(self, group): raise NotImplemented

    def status(self):
        try:
            self.os = next(i for i in nova.servers.list() if i.id == self.id)
            return self.os.status.lower()
        except StopIteration:
            return 'dead'

    def ip(self):
        ips = {i['floating_ip_address'] : i for i in neutron.list_floatingips()['floatingips']}
        return Floating_IP(next(ips[i] for n in self.os.networks.values() for i in n if i in ips))

    def image(self, name, public=False, **metadata):
        metadata['visibility'] = 'public' if public else 'private'
        return Image(nova.images.find(id=self.os.create_image(name, metadata=metadata)))

    def delete(self):
        ips = {i['floating_ip_address'] : i for i in neutron.list_floatingips()['floatingips']}
        for n in self.os.networks.values():
            for ip in n:
                if ip in ips:
                    Floating_IP(ips[ip]).delete()
                # openstack server remove floating ip ${OS_USERNAME}-api-U-1 your.ip.number.here
        # openstack server delete ${OS_USERNAME}-api-U-1
        nova.servers.delete(self.os)

    def reboot(self):
        self.os.reboot()

    def __repr__(self):
        try: ip = str(self.ip())
        except StopIteration: ip = 'ERROR'
        return 'Instance(\'%s\', %s, %s)' % (self.name, self.status(), ip)

    def __str__(self): return self.__repr__()

for i in 'suspend resume start stop'.split():
    setattr(Instance, i, lambda self, *, _cmd=i: getattr(nova.servers, _cmd)(self.os))

################################################################################

def report():
    #print(('\n' + '-'*35 + '\n').join('id:   %s\nname: %s' % (f.id, f.name) for f in Flavor.list()))
    print('Instances')
    for inst in Instance.list():
        print(inst)
    #print('Floating IPs')
    #for ip in Floating_IP.list():
    #    print(ip)
    #for im in Image.list():
    #    print(im)
    print()


################################################################################

