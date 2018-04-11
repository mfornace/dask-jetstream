"""Defines process objects"""

from . import lpickle, _REMOTE_ROOT
from fn import env

import time
import subprocess, sys, multiprocessing, os

_SCRIPT = R"""
import sys
sys.path.append(str(sys.argv[-2]))
from remote import lpickle

with open(sys.argv[-1], 'rb') as f:
    function, args, kwargs, tmp = lpickle.load(f)
tmp.cleanup()
function(*args, **kwargs)
"""

def fork(target, *, daemon=None):
    def create(*args, **kwargs):
        return multiprocessing.Process(target=target, args=args, kwargs=kwargs, daemon=daemon)
    return create

def fork_map(function, iterable, processes=None):
    with multiprocessing.Pool(processes=processes) as pool:
        return pool.map(lpickle.wrap(function), tuple(iterable))

class Process:
    def __init__(self, popen, tmp=None):
        self.popen = popen
        self.tmp = tmp

    @property
    def pid(self):
        return self.popen.pid

    def join(self, timeout=None):
        try:
            self.popen.wait(timeout)
        except subprocess.TimeoutExpired:
            pass

    def is_alive(self):
        return self.exitcode is None

    @property
    def exitcode(self):
        return self.popen.returncode

    def terminate(self):
        self.popen.kill()


def spawn_interpreter(*, target, args=[], kwargs={}, stdout=None, stderr=None):
    tmp = env.temporary_path()
    path = tmp.path
    (path/'run.py').write_text(_SCRIPT)
    with (path/'function.lp').open('wb') as f:
        lpickle.dump((target, args, kwargs, tmp), f)
    cmd = [sys.executable, path/'run.py', os.path.dirname(_REMOTE_ROOT), path/'function.lp']
    proc = subprocess.Popen(map(str, cmd), stdout=stdout, stderr=stderr, preexec_fn=os.setsid)
    return Process(proc, tmp)


def start(*, target, args=[], kwargs={}, stdout=None, stderr=None, method=None):
    if method == 'spawn':
        return spawn_interpreter(target=target, args=args, kwargs=kwargs, stdout=stdout, stderr=stderr)
    ctx = multiprocessing if method is None else multiprocessing.get_context(method)
    proc = ctx.Process(target=target, args=args, kwargs=kwargs)
    proc.start()
    return proc

def is_alive(worker):
    if hasattr(worker, 'dead'): return not worker.dead
    if hasattr(worker, 'is_alive'): return worker.is_alive()

try:
    import blahhhhh
    try:
        __IPYTHON__
    except NameError:
        from gevent import monkey
        monkey.patch_all()

    from gevent import joinall as join_all, spawn

except ImportError:
    import threading

    def join_all(workers):
        for w in workers: w.join()

    def spawn(function, *args, **kwargs):
        ret = threading.Thread(target=function, args=args, kwargs=kwargs)
        ret.start()
        return ret
