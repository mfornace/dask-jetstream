'''
Remote submission of SSH jobs
'''
import io, os, uuid, tarfile, json, subprocess, functools, time
import remote

################################################################################

try:
    import paramiko
    @functools.lru_cache(maxsize=2)
    def client(hostname, port=22, user=None, password=None, pkey=None, timeout=None):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_system_host_keys()
        try:
            ssh.connect(hostname, port=port, username=user, password=password, pkey=pkey, timeout=timeout)
        except Exception as e:
            print(e)
            raise e
        return ssh
except ImportError:
    def client(hostname, port=22, user=None, password=None, pkey=None, timeout=None): pass

################################################################################

def remote_submit(host, mode, settings, *, python, user=None, strict=True, **kwargs):
    assert mode in 'pbs local'.split()

    try:
        ssh = client(host, user=user, **kwargs)
    except (paramiko.ssh_exception.NoValidConnectionsError, paramiko.ssh_exception.BadHostKeyException, OSError):
        return False
    gz = 'source_{}.gz'.format(uuid.uuid4())

    with fn.temporary_path() as path, ssh.open_sftp() as ftp:
        with (path/'remote.json').open('w') as f:
            json.dump(settings, f, indent=4)
        with tarfile.open(str(path/gz), mode='w:gz') as tf:
            add = lambda a, n: tf.add(str(n), arcname=a)
            add('remote.json', path/'remote.json')
        ftp.put(str(fn.module_path(remote)/'run_agent.py'), 'run_agent.py')
        ftp.put(str(path/gz), gz)

    cmd = ' '.join(['source .bash_profile;', python, 'run_agent.py', mode, gz, '&& rm' if strict else '; rm', gz])

    _, out, err = ssh.exec_command(cmd)
    code = out.channel.recv_exit_status()

    if code != 0:
        print(''.join(out), '\n', ''.join(err))
        raise OSError('Bad exit code {} from host {}'.format(code, host))
    return True

################################################################################

def parse_qstat(output):
    import pandas
    jobs = output.split('\nJob Id:')
    def process(lns):
        ret = {'id': str(lns[0].strip())}
        for ln in map(lambda ln: ln.split(' = '), lns[1:]):
            try:
                ret[ln[0].strip().lower()] = ln[1].strip()
            except IndexError:
                pass
        return ret
    return pandas.DataFrame(list(map(process, map(lambda j: j.split('\n'), jobs))))


def remote_qstat(host, user=None, **kwargs):
    ssh = client(host, user=user, **kwargs)
    _, out, err = ssh.exec_command('qstat -f')
    code = out.channel.recv_exit_status()
    if code != 0:
        print(''.join(out), '\n', ''.join(err))
        raise OSError('Bad exit code {} from host {}'.format(code, host))
    return parse_qstat(''.join(out))


def local_qstat(args='-f'):
    return parse_qstat(subprocess.check_output(['qstat'] + args.split()).decode())

################################################################################