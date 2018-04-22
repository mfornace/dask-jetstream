import itertools, functools
import asyncio, asyncssh, sys, time
from queue import Queue

import fn

info = fn.logs(__name__, 'info')

################################################################################

def python_exe(exe):
    return exe or 'python{}'.format(sys.version_info.major)

################################################################################

def retry(function, timeouts, exceptions=()):
    @functools.wraps(function)
    async def retry_function(*args, **kwargs):
        x = StopIteration
        for i, t in enumerate(timeouts):
            info(t, 'Attempting retryable function', attempt=i, allowed=len(timeouts))
            try:
                return await asyncio.wait_for(function(*args, **kwargs), timeout=t)
            except (asyncio.TimeoutError, TimeoutError) as e:
                x = e
            except exceptions as e:
                info('Exception encountered while retrying function', exception=str(e))
                x = e
                await asyncio.sleep(t)
        raise x
    return retry_function

################################################################################

connect = retry(asyncssh.connect, [1,2,4,8] + 20 * [16], exceptions=(ConnectionRefusedError,))

async def start_client(cmd, host, port=22, **kwargs):
    '''Returns PID'''
    cmd = "$SHELL -c 'echo $$; exec {} '".format(cmd)
    info('Connecting to client', host=host, port=port, kwargs=str(kwargs))
    info('Running command', cmd=cmd)
    async with await connect(host, port, **kwargs) as conn: # not sure why I have to await here
        info('Startin SSH command')
        result = await conn.run(cmd, check=True)
        info('Finished SSH command', result=str(result))
        return int(result.stdout.split('\n')[0])

async def stop_client(host, pid, signal='INT', port=22, **kwargs):
    info('Killing client', host=host, port=port, signal=signal, pid=pid)
    cmd = 'kill -{} {}'.format(signal, int(pid))
    async with await connect(host, port, **kwargs) as conn:
        result = await conn.run(cmd, check=False)
        return result.exit_code

################################################################################

def start_scheduler(host, port, python=None, **kwargs):
    cmd = '{} -m distributed.cli.dask_scheduler --port {} &'.format(python_exe(python), port)
    return start_client(cmd, host, **kwargs)


WORKER_CMD = """
from distributed.cli.dask_worker import go
from distributed.utils import get_ip_interface
import sys

ip = get_ip_interface({interface})
sys.argv += ['--listen-address', 'tcp://{{ip}}:{port}'.format(ip, port)]
print(sys.argv)
go()
"""

def start_worker(host, port, saddr, sport, nthreads, nprocs, event, log=None, python=None, interface='eth0', **kwargs):
    '''dask-worker {SCHEDULERIP}: 8786 --nthreads 0 --nprocs 1 --listen-address tcp://{WORKERETH}:8001 --contact-address tcp://{WORKERIP}:8001'''
    cmd =  r'{} exec(\"{}\")'.format(python_exe(python), WORKER_CMD.format(port=port, interface=interface))
    cmd = '{} {}:{} --nthreads {} --nprocs {}'
    cmd = cmd.format(py, saddr, sport, nthreads, nprocs)
    cmd = '{} --listen-address tcp://{}:{} --contact-address tcp://{}:{}'.format(cmd, eth, port, host, port)

    output = lambda msg: print('[worker]: {}'.format(msg))
    return start_client(cmd, host, output, event, **kwargs)

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

################################################################################
