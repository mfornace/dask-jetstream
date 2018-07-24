import asyncio, asyncssh, sys, functools, logging
import boto3

import fn

[info, error] = fn.logs(__name__, 'info', 'error')

################################################################################

def redirect_ssh_logs(file):
    '''Switch SSH loggers to only write to a file'''
    sh = logging.FileHandler(file)
    for log in map(logging.getLogger, ('asyncssh', __name__)):
        log.handlers = [sh]
        log.propagate = False

################################################################################

def retry(function, timeouts, exceptions=()):
    @functools.wraps(function)
    async def retry_function(*args, **kwargs):
        x = StopIteration
        for i, t in enumerate(timeouts):
            try:
                return await asyncio.wait_for(function(*args, **kwargs), timeout=t)
            except (asyncio.TimeoutError, TimeoutError) as e:
                x = info('Timed out while trying function', exception=e, attempt=i, allowed=len(timeouts))
            except exceptions as e:
                x = info('Exception encountered while retrying function', exception=e, attempt=i)
                await asyncio.sleep(t)
        raise error('Failed to complete retryable function', exception=x)
    return retry_function

################################################################################

connect = retry(asyncssh.connect, [1,2,4,8] + 20 * [16],
                exceptions=(ConnectionRefusedError, OSError))

################################################################################

async def stop_client(host, pid, signal='INT', port=22, sleep=(), env={}, **kwargs):
    '''Returns True if process was killed or False if process did not exist
    Raises SystemError if process existed and could not be killed
    '''
    info('Killing remote SSH process', host=host, port=port, signal=signal, pid=pid)
    cmd = 'kill -{} {}'.format(signal, int(pid))
    async with await connect(host, port, **kwargs) as conn:
        if (await conn.run(cmd, env=env, check=False)).exit_status:
            return False
        for t in sleep:
            await asyncio.sleep(t)
            if (await conn.run(cmd, env=env, check=False)).exit_status:
                return True
        raise OSError('Process {} could not be killed'.format(pid))

################################################################################

async def start_client(cmd, host, ssh_port=22, env={}, visit=None, **kwargs):
    '''Returns PID'''
    #cmd = "$SHELL -c 'echo $$; exec {} '".format(cmd)
    cmd = "sh -c 'nohup {} </dev/null >/dev/null 2>&1 & echo $!'".format(cmd.replace(r"'", r"'\''").replace(r'"', r"'\"'"))
    info('Connecting to client', host=host, port=ssh_port, kwargs=str(kwargs))
    info('Running command', cmd=cmd)
    async with await connect(host, ssh_port, **kwargs) as conn: # not sure why I have to await here
        info('Starting SSH command')
        if visit is not None:
            await visit(conn)
        result = await conn.run(cmd, env=env, check=True)

        info('Finished SSH command', result=str(result))
        return int(result.stdout.split('\n')[0])

################################################################################

WATCHTOWER_SCRIPT = """
import watchtower, logging, boto3, distributed
session = boto3.Session(region_name='{region}', aws_access_key_id={access}, aws_secret_access_key={secret})
ch = watchtower.CloudWatchLogHandler(log_group={group}, stream_name={stream}, send_interval={interval}, boto3_session=session)
ch.setLevel(logging.INFO)
logging.getLogger('distributed').handlers = [ch]
"""

def preload_watchtower(session, group, stream=None, interval=60):
    '''Reset the distributed logger with one going to watchtower'''
    session = boto3.Session() if session is None else session
    cred = session.get_credentials()
    if cred is None:
        raise ValueError('No AWS credentials found')
    return WATCHTOWER_SCRIPT.format(group=repr(group), stream=repr(stream),
        interval=interval, region=session.region_name,
        access=repr(cred.access_key), secret=repr(cred.secret_key))

################################################################################

def python_exec(script, python=None):
    python = python or 'python{}'.format(sys.version_info.major)
    return '{} -c "exec(\'\'\'{}\'\'\')"'.format(python, repr(script)[1:-1])

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

SCHEDULE_SCRIPT = """
from distributed.cli.dask_scheduler import go
import sys
sys.argv[0] = 'ignore_this.py'
sys.argv += ['--port', str({port})]
if '{path}': sys.argv += ['--local-directory', '{path}']
go()
"""

def start_scheduler(host, port, volume=None, python=None, preload='', **kwargs):
    if volume is None:
        visit = None
        path = ''
    else:
        visit = fn.partial(mount_volume, volume_id=volume, mount='/mnt/volume')
        path = '/mnt/volume/dask-scheduler'
    script = preload + SCHEDULE_SCRIPT.format(port=port, path=path)
    return start_client(python_exec(script, python), host, visit=visit, **kwargs)

################################################################################

# A wrapper around dask-worker to provide some more flexibility
WORK_SCRIPT = """
from distributed.cli.dask_worker import go
from distributed.utils import get_ip_interface
import os, sys, psutil
allowed = tuple(psutil.net_if_addrs().keys())
ip = next(get_ip_interface(i) for i in {interfaces} if i in allowed)
sys.argv[0] = 'ignore_this.py'
sys.argv += ['%s:%d' % ('{shost}', {sport})]
sys.argv += ['--listen-address', 'tcp://%s:%d' % (ip, {port})]
sys.argv += ['--contact-address', 'tcp://%s:%d' % ('{host}', {port})]
sys.argv += ['--nprocs', '1']
sys.argv += ['--nthreads', str(os.cpu_count())]
go()
"""

def start_worker(address, scheduler, *, preload='', python=None, interfaces=['eth0', 'en0', 'ens3'], **kwargs):
    '''
    address and scheduler are pairs of (IP, port)
    dask-worker {SCHEDULERIP}:8786 --nthreads 0 --nprocs 1 --listen-address tcp://{WORKERETH}:8001 --contact-address tcp://{WORKERIP}:8001
    interfaces is a list of possible IP interfaces that should be tried in order
    '''
    host, port = address
    shost, sport = scheduler
    script = preload + WORK_SCRIPT.format(interfaces=repr(interfaces), port=port, host=host, shost=shost, sport=sport)
    return start_client(python_exec(script, python), host, **kwargs)

################################################################################

def test():
    x = []
    def event():
        x.append(1)
        return len(x) < 50
    loop = asyncio.get_event_loop()

    tasks = [loop.create_task(start_scheduler('127.0.0.1', 8000, event, username='Mark', password='bjorko91')) for i in range(1)]
    tasks += [loop.create_task(start_worker('127.0.0.1', 8000, 'schedip', 'sport', 2, 1, event, username='Mark', password='bjorko91')) for i in range(1)]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

    for i, r in enumerate(t.result() for t in tasks):
        if isinstance(r, Exception):
            print('Task %d failed: %s' % (i, str(r)))
        elif r != 0:
            print('Task %d exited with status %s' % (i, r))
        else:
            print('Task %d succeeded' % i)
