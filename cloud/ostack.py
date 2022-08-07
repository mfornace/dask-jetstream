"""
Utilities for dealing with OpenStack

connection.list_flavors()
connection.list_networks()
connection.list_images()

connection.compute.stop_server(server)
connection.compute.start_server(server)
connection.compute.suspend_server(server)
"""
import time, itertools, logging, base64
from concurrent.futures import ThreadPoolExecutor

from keystoneauth1.exceptions import RetriableConnectionFailure
import openstack

import fn

log = logging.getLogger(__name__)

################################################################################

FLAVORS, VOLUMES, NETWORKS, IMAGES = set(), {}, {}, {}

DEFAULT_OS_KEY = ''
DEFAULT_OS_GROUPS = []
DEFAULT_CONNECTION = None

def connection(conn=None):
    '''Return the default connection or else the given connection'''
    global DEFAULT_CONNECTION
    if conn is None:
        if DEFAULT_CONNECTION is None:
            DEFAULT_CONNECTION = openstack.connect()
        return DEFAULT_CONNECTION
    if isinstance(conn, openstack.connection.Connection):
        return conn
    raise TypeError('Expected None or Connection object')

def set_config(dict_like):
    VOLUMES.update(dict_like['volumes'])
    NETWORKS.update(dict_like['networks'])
    FLAVORS.update(dict_like['flavors'])
    IMAGES.update(dict_like['images'])

def lookup(where, key):
    '''Disambiguates the key repeatedly, if a list takes the last element'''
    while key in where:
        key = where[key]
        key = key if isinstance(key, str) else key[-1]
    return key

def close_openstack(conn, graceful=True):
    '''Synchronously close all instances and IPs'''
    ips = conn.list_floating_ips()
    servers = conn.list_servers()
    if not ips and not servers:
        return []

    tasks = []
    with ThreadPoolExecutor(len(ips) + len(servers)) as pool:
        for i in ips:
            tasks.append(pool.submit(conn.delete_floating_ip, i))
        while any(not t.done() for t in tasks):
            time.sleep(0.2)
        for i in servers:
            tasks.append(pool.submit(close_server, conn, i, graceful=graceful))
        while any(not t.done() for t in tasks):
            time.sleep(0.2)
        conn.delete_unattached_floating_ips()
        return [t.result() for t in tasks]

################################################################################

EXCEPTIONS = [ConnectionRefusedError, RetriableConnectionFailure, openstack.exceptions.ResourceTimeout]

def retry(function, timeout=3600, exceptions=None):
    '''Reattempt OpenStack calls that give given exception types'''
    exc = tuple(EXCEPTIONS if exceptions is None else exceptions)
    @fn.wraps(function)
    def retryable(*args, **kwargs):
        start = time.time()
        for i in itertools.count():
            try:
                return function(*args, **kwargs)
            except exc as e:
                log.info(fn.message(str(e)))
                if time.time() > start + timeout:
                    raise
            time.sleep(2 ** (i/2-1))
    return retryable

################################################################################

# def report(instance=1, flavor=0, ip=0, image=0, as_dict=False):
#     '''Return string describing all services'''
#     f = lambda c: list(map(str, c.list()))
#     C = [Instance, Flavor, FloatingIP, Image]
#     B = [instance, flavor, ip, image]
#     N = ['Instances', 'Flavors', 'IPs', 'Images']
#     d = {n: f(c) for (n, c, b) in zip(N, C, B) if b}
#     return d if as_dict else json.dumps(d, indent=4)

################################################################################

def get_flavor(conn, flavor):
    # if isinstance(flavor, str):
    #     assert flavor in FLAVORS, (flavor, FLAVORS)
    out = conn.compute.find_flavor(flavor)
    assert out is not None
    return out

################################################################################

def get_image(conn, image='ubuntu'):
    '''Find an image. Use config defaults'''
    if isinstance(image, str):
        image = lookup(IMAGES, image)
    assert image is not None
    out = conn.compute.find_image(image)
    assert out is not None
    return out

################################################################################

def get_network(conn, net):
    assert net is None or isinstance(net, str)
    out = connection(conn).get_network(NETWORKS.get(net, net))
    assert out is not None
    return out

################################################################################

def get_server_ip(conn, server):
    server = getattr(server, 'id', server)
    return conn.get_server_public_ip(conn.get_server(server))

def create_ip(conn):
    return conn.create_floating_ip().floating_ip_address

def attach_ip(conn, server, ip: str):
    '''Attach an IP to a server'''
    def fetch():
        s = conn.get_server(server.id)
        assert s is not None
        return s
    server = retry(fetch, exceptions=(AssertionError,))()
    _ = conn.add_ips_to_server(server, ips=[ip])
    retry(lambda i: conn.get_server_public_ip(conn.get_server(i)))(server.id)

def get_server(conn, name_or_id):
    return conn.compute.get_server(name_or_id)

def submit_server(conn, name, image, flavor, network, security_groups=None, user_data=None, key_name=None, nics=None):
    net = get_network(conn, network).id
    if nics is None:
        nics = [{'net-id': net}]
    if security_groups is None:
        security_groups = [dict(name=g) for g in DEFAULT_OS_GROUPS]
    if key_name is None:
        key_name = DEFAULT_OS_KEY
    if user_data is not None:
        user_data = base64.b64encode(user_data.encode()).decode()
    kwargs = dict(
        name=name, image_id=get_image(conn, image).id,
        flavor_id=get_flavor(conn, flavor).id,
        security_groups=security_groups, user_data=user_data,
        networks=[{"uuid": net}], key_name=key_name, nics=nics)
    try:
        log.info('Creating server with keywords %r' % kwargs)
        return conn.compute.create_server(**kwargs)
    except Exception:
        log.error('Failed to create server with keywords %r' % kwargs)
        raise

def close_server(conn, server, graceful=True):
    '''
    Close a single instance
    - `conn`: the OpenStack connection
    - `server`: the server or server id
    '''
    server = getattr(server, 'id', server)
    if graceful:
        s = conn.compute.find_server(server)
        if s is not None and s.status != 'SHUTOFF':
            conn.compute.stop_server(s)
    s = conn.get_server(server)
    print('deleting', s)
    if s is None:
        return False
    # try:
    #     print('deleting with ips', s)
    #     return conn.delete_server(s, delete_ips=True)
    # except openstack.exceptions.SDKException as e:
    #     print('deleting without ips', s)
    #     log.warning(fn.message(str(e)))
    return conn.delete_server(s, delete_ips=False)

################################################################################

def create_server(conn, *, name, image, flavor, network, ip=None, security_groups=None, user_data=None, key_name=None, nics=None):
    '''Create a server. If an IP is given, attach it to the server'''
    server = submit_server(conn, name=name, image=image, flavor=flavor, network=network,
        security_groups=security_groups, user_data=user_data, key_name=key_name, nics=nics)
    try:
        s = retry(conn.compute.wait_for_server)(server, wait=0.01)
        if ip is not None:
            attach_ip(conn, server, ip)
        return s
    except Exception:
        try:
            close_server(conn, server, graceful=False)
            if ip is not None:
                conn.delete_floating_ip(ip)
        except Exception as e:
            log.error('Could not close server or IP {} because of exception {}'.format(server.id, e))
        raise

################################################################################

def create_image(conn, server, name, public=False, suspend=True, metadata=None):
    metadata = {} if metadata is None else dict(metadata)
    metadata['visibility'] = 'public' if public else 'private'
    server=conn.compute.find_server(server.id)
    if suspend:
        conn.compute.suspend_server(server)
        conn.compute.wait_for_server(server, 'SUSPENDED')
    return conn.compute.create_server_image(server=server, name=name, metadata=metadata)
