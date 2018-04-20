from functools import partial
import asyncio, asyncssh, sys, time
from queue import Queue


def python_exe(exe):
    return exe or 'python{}'.format(sys.version_info.major)

################################################################################

async def run_client(cmd, host, output, keep_going, port=22, **kwargs):
    output('connecting to {}:{}'.format(host, port))
    output('running command: {}'.format(cmd))
    async with asyncssh.connect(host, port, **kwargs) as conn:
        async with conn.create_process(cmd, stderr=asyncssh.STDOUT) as proc:
            while keep_going():
                out = (await proc.stdout.read()).rstrip()
                if out: output(out)
        return proc.exit_status

################################################################################

def run_logged(log, cmd, host, *args, **kwargs):
    if log is not None:
        cmd = 'mkdir -p {} && {} &> {}/dask.log'.format(log, cmd, log, host)
    return run_client(cmd, host, *args, **kwargs) 


def run_scheduler(host, port, keep_going, log=None, python=None, **kwargs):
    cmd = '{} -m distributed.cli.dask_scheduler --port {}'.format(python_exe(python), port)
    output = lambda msg: print('[scheduler]: {}'.format(msg))    
    return run_logged(log, cmd, host, output, keep_going, **kwargs)


def run_worker(host, port, saddr, sport, nthreads, nprocs, keep_going, log=None, python=None, interface='eth0', **kwargs):
    '''dask-worker {SCHEDULERIP}: 8786 --nthreads 0 --nprocs 1 --listen-address tcp://{WORKERETH}:8001 --contact-address tcp://{WORKERIP}:8001'''
    py = python_exe(python)
    cmd = '{} -m distributed.cli.dask_worker {}:{} --nthreads {} --nprocs {}'
    cmd = cmd.format(py, saddr, sport, nthreads, nprocs)
    eth = '''`{} -c "from distributed.utils import get_ip_interface; print(get_ip_interface('{}'))"`'''.format(py, interface)
    cmd = '{} --listen-address tcp://{}:{} --contact-address tcp://{}:{}'.format(cmd, eth, port, host, port)

    print(cmd)
    cmd = 'ls'

    output = lambda msg: print('[worker]: {}'.format(msg))
    return run_logged(log, cmd, host, output, keep_going, **kwargs)



x = []
def keep_going():
    x.append(1)
    return len(x) < 50

################################################################################

try:
    loop = asyncio.get_event_loop()

    tasks = [loop.create_task(run_scheduler('127.0.0.1', 8000, keep_going, username='Mark', password='bjorko91')) for i in range(1)]
    tasks += [loop.create_task(run_worker('127.0.0.1', 8000, 'schedip', 'sport', 2, 1, keep_going, username='Mark', password='bjorko91')) for i in range(1)]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

    for i, r in enumerate(t.result() for t in tasks):
        if isinstance(r, Exception):
            print('Task %d failed: %s' % (i, str(r)))
        elif r != 0:
            print('Task %d exited with status %s' % (i, r))
        else:
            print('Task %d succeeded' % i)
            
except (OSError, asyncssh.Error) as exc:
    sys.exit('SSH connection failed: ' + str(exc))

################################################################################
