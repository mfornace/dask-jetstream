import asyncio, sys, functools, logging, string, boto3, fn, dask, distributed

from . import templates

log = logging.getLogger(__name__)

################################################################################

def retry(function, timeouts, exceptions=()):
    @functools.wraps(function)
    async def retry_function(*args, **kwargs):
        x = StopIteration
        for i, t in enumerate(timeouts):
            try:
                return await asyncio.wait_for(function(*args, **kwargs), timeout=t)
            except (asyncio.TimeoutError, TimeoutError) as e:
                x = log.info(fn.message('Timed out while trying function', exception=e, attempt=i, allowed=len(timeouts)))
            except exceptions as e:
                x = log.info(fn.message('Exception encountered while retrying function', exception=e, attempt=i))
                await asyncio.sleep(t)
        raise x
    return retry_function

################################################################################

async def mount_volume(conn, volume_id, mount='/mnt/volume', user='$USER'):
    '''Mount a volume on a running instance via SSH'''
    # You can google this, but it seems to be the only way of identifying the disk:
    cmd = 'ls /dev/disk/by-id/*%s*' % volume_id[:20]
    out = await conn.run(cmd, check=True)
    # Sometimes part is added i.e. id-part1 etc. and it seems like that is the one we want
    dev = max(out.stdout.strip().split(), key=len)
    cmd = ' && '.join((
        'sudo mkdir -p {v}',
        'sudo mount {d} {v}',
        'sudo chown -R {u} {v}',
        'sudo chmod -R g+rw {v}'
    )).format(d=dev, v=mount, u=user)
    print(cmd)
    await conn.run(cmd, check=True)
    return dev

################################################################################

def configure():
    '''Returns an executable string setting the dask config to a copy of the local one'''
    dask.config.refresh()
    return templates.config.substitute(config=repr(dask.config.config))

################################################################################

def preload_cloudwatch(session, group, stream=None, interval=60):
    '''Reset the distributed logger with one going to AWS CloudWatch'''
    session = boto3.Session() if session is None else session
    cred = session.get_credentials()
    if cred is None:
        raise ValueError('No AWS credentials found')
    with open(fn.cloudwatch.__file__) as f:
        mod = f.read()
    return configure() + mod + templates.cloudwatch.substitute(group=repr(group), stream=repr(stream),
        interval=interval, region=session.region_name,
        access=repr(cred.access_key), secret=repr(cred.secret_key))

################################################################################

def shebang(python=None):
    '''Returns shebang for the top of a remote Python script with newline afterwards'''
    if python is None:
        return '#!/usr/bin/env python{}\n'.format(sys.version_info.major)
    else:
        return '#!{}\n'.format(python)

################################################################################

def scheduler_script(host, port, volume=None, python=None, preload=''):
    '''
    Returns a Python executable script with shebang included
    The script will write a dask.yml in the home directory (perhaps in /root).
    '''
    if volume is None:
        visit = None
        path = ''
    else:
        visit = fn.partial(mount_volume, volume_id=volume, mount='/mnt/volume')
        path = '/mnt/volume/dask-scheduler'
    return shebang(python) + configure() + templates.scheduler.substitute(preload=preload or '', host=host, port=port, path=repr(path))

################################################################################

def worker_script(worker, scheduler, *, python=None, preload='', interfaces=('eth0', 'en0', 'ens3')):
    '''
    worker and scheduler are pairs of (IP, port)
    The script will write a dask.yml in the home directory (perhaps in /root).
    dask-worker {SCHEDULERIP}:8786 --nthreads 0 --nprocs 1 --listen-address tcp://{WORKERETH}:8001 --contact-address tcp://{WORKERIP}:8001
    interfaces is a list of possible IP interfaces that should be tried in order
    '''
    host, port = worker
    shost, sport = scheduler
    return shebang(python) + configure() + templates.worker.substitute(preload=preload or '',
        interfaces=repr(interfaces), host=host, port=port, shost=shost, sport=sport)
