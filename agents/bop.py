from remote import ssh
import sys

def work(timeout, idle=None, **kwargs):
    if idle is None:
        idle = timeout
    settings = {
        'agents':       'md-agents',
        'jobs':         'md-jobs',
        'files':        'md-files',
        'timeout':      timeout,
        'idle-timeout': idle
    }
    settings.update(kwargs)
    ssh.remote_submit('bop.caltech.edu', settings, user='mfornace')

for _ in range(int(sys.argv[1])):
    work(timeout=5e4)
