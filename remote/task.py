from . import file_db, lpickle, config, process
from fn import env

import time, multiprocessing, os, sys, signal, tarfile, copy, inspect

################################################################################

def run_pickle(job_database, item, tmp, platform, restart=False, global_platform=True):
    os.chdir(str(tmp)) # paths are relative so this is easier
    sys.path.insert(0, str(tmp))

    item['attempts'] += 1
    assert item['status'] == 'running'
    job_database.set(item, 'attempts', item['attempts'])
    platform['path'] = str(tmp)
    if global_platform:
        config.PLATFORM = platform
        args = ()
    else:
        args = (platform,)
    with (tmp/'function.lp').open('rb') as f:
        function = lpickle.load(f)
    try:
        function(*args)
    except KeyboardInterrupt:
        try:
            if restart:
                os.chdir(str(tmp)) # make sure still here
                job_database.replace(item, function, tmp)
                job_database.set(item, 'checkpoints', item['checkpoints'] + 1)
                print('**** restarted ****')
        finally:
            print('**** resetting ****')
            sys.exit(signal.SIGINT)
    except Exception as e:
        print('**** error ****', type(e), e)
        raise e

################################################################################

class Task:

    def __init__(self, item, job_database, *, method='spawn'):
        """GPU context acquisition seems to fail if process is forked"""
        self.restart = item['restart']
        self.item = copy.deepcopy(item)
        self.file = self.item.pop('file')
        self.file_database = file_db.File_Database(item['file-database'])
        self.job_database = job_database
        self.method = method
        self.tmp = env.temporary_path()
        self.worker = None, None

    def acquire(self, agent):
        if self.job_database.set_if(self.item, 'status', 'queued', 'running'):
            self.item['status'] = 'running'
            self.item['agents'] = self.item.get('agents', []) + [agent]
            self.job_database.set(self.item, 'agents', self.item['agents'])
            return True
        return False

    def start(self, platform, stdout=None, stderr=None):
        """Load resources and launch function"""
        self.file_database.extract(self.file, self.tmp.path)
        self.worker = process.start(target=run_pickle, args=(self.job_database,
            self.item, self.tmp.path, platform, self.restart),
            stdout=stdout, stderr=stderr, method=self.method)

    def define_resources(self, platform):
        try:
            return lpickle.loadz(self.item['get'])(platform['resources'])
        except (KeyError, TypeError):
            return {k: v for (k, v) in self.item.items() if k in platform['resources']}

    def wait(self, timeout=None):
        """Wait for the process to finish, return if it finished"""
        self.worker.join(timeout)
        return self.worker.exitcode is not None

    def status(self):
        code = self.worker.exitcode
        if code is None:
            return 'running'
        if abs(code) in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
            return 'queued' if self.restart else 'timeout'
        return 'error' if code else 'complete'

    def release(self, delay):
        """Release the process by sending SIGINT, wait delay, return if it finished"""
        if process.is_alive(self.worker):
            os.kill(self.worker.pid, signal.SIGINT)
            self.wait(delay)
        if process.is_alive(self.worker):
            self.worker.terminate()
        self.tmp.cleanup()

################################################################################
