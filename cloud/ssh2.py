import fn, logging, sys

import paramiko, socket, tenacity as tn
from paramiko.buffered_pipe import PipeTimeout
from paramiko.ssh_exception import SSHException, PasswordRequiredException


TIMEOUTS = (PipeTimeout, socket.timeout)# (SSHException, PasswordRequiredException)

error = fn.logs(__name__, 'error')

################################################################################

@fn.lru_cache(maxsize=2)
def client():
    '''Make paramiko client'''
    logging.getLogger('paramiko').setLevel(logging.WARN)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    return ssh

################################################################################

def python_exe(exe):
    return exe or 'python{}'.format(sys.version_info.major)

def retry_until(predicate):
    '''Retry until predicate returns True'''
    def check(_):
        r = predicate()
        print('predicate', r)
        return r
    return tn.retry_if_not_result(check)

################################################################################

async def ssh_loop(outputs, feed, check):
    try:
        [feed(ln) for o in outputs for ln in o.read().split(b'\n') if ln]
    except TIMEOUTS as e:
        pass
    return check()

################################################################################

async def watch_channel(channel, function, *args):
    pred = retry_until(channel.exit_status_ready)
    tn.retry(wait=tn.wait_fixed(1), retry=pred & tn.retry_if_result(bool))(function)(*args)

    if not channel.exit_status_ready(): # manual shut down
        tn.retry(wait=tn.wait_fixed(1), retry=pred, stop=tn.stop_after_delay(60))(channel.send)(b'\x03')

    return channel.recv_exit_status()

################################################################################

async def ssh_connect(ssh, cmd, label, check, put, address, user, port, 
                      key=None, compress=True, timeout=20, banner_timeout=20, **kwargs):
    retry = tn.retry(wait=tn.wait_exponential(multiplier=1, max=10),
        stop=tn.stop_after_delay(120), retry=tn.retry_if_exception_type(TIMEOUTS))

    try:
        retry(ssh.connect)(hostname=address, username=user, port=port, key_filename=key,
                compress=compress, timeout=timeout, banner_timeout=banner_timeout, **kwargs)
    except tn.RetryError as e:
        raise error('SSH connection timed out', exception=e)

    stdin, stdout, stderr = ssh.exec_command("$SHELL -i -c \'%s\'" % cmd, get_pty=True)

    with stdin.channel as i, stderr.channel as e, stdout.channel as o:
        for c in (e, o): 
            c.settimeout(0.1)
        feed = lambda ln: put((label, ln))
        print('watching channel')
        return await watch_channel(o, ssh_loop, (stdin, stdout, stderr), feed, check)

################################################################################

def start_scheduler(ssh, inputs, outputs, address, port, log=None, python=None, **kwargs):
    cmd = '{} -m distributed.cli.dask_scheduler --port {}'.format(python_exe(python), port)

    if log is not None:
        cmd = 'mkdir -p {} && {} &> {}/dask_scheduler_{}:{}.log'.format(log, cmd, log, address, port)

    label = 'scheduler {}:{}'.format(address, port)
    print(label)
    cmd = 'ls -l\n'
    print(cmd)
    return ssh_connect(ssh, cmd, label, inputs.empty, outputs.put, address, port=port, **kwargs)

################################################################################

def start_worker(inputs, outputs, log, saddr, sport, waddr, nthreads, nprocs, port, python=None, interface='eth0', **kwargs):
    '''dask-worker {SCHEDULERIP}: 8786 --nthreads 0 --nprocs 1 --listen-address tcp://{WORKERETH}:8001 --contact-address tcp://{WORKERIP}:8001'''
    py = python_exe(python)
    cmd = '{} -m distributed.cli.dask_worker {}:{} --nthreads {} --nprocs {}'
    cmd = cmd.format(py, saddr, sport, waddr, nthreads, nprocs)
    eth = '''`{} -c "from distributed.utils import get_ip_interface; print(get_ip_interface('{}'))"`'''.format(py, interface)
    cmd = '{} --listen-address tcp://{}:{} --contact-address tcp://{}:{}'.format(cmd, eth, port, waddr, port)

    if log is not None:
        cmd = 'mkdir -p {} && {} &> {}/dask_scheduler_{}.log'.format(log, cmd, log, waddr)

    label = 'worker {}'.format(waddr)
    async_ssh(cmd, label, inputs, outputs, waddr, port, **kwargs)
