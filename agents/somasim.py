from remote import ssh

import argparse, time

###############################################################################

parser = argparse.ArgumentParser(description='Launch an agent on Somasim')
parser.add_argument('shared',    type=int, help='number of fourgpu agents')
parser.add_argument('dedicated', type=int, nargs='?', help='number of pierce agents', default=0)
parser.add_argument('period',    type=int, nargs='?', help='seconds to repeat', default=0)
args = parser.parse_args()

###############################################################################

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
    ssh.remote_submit('somasim.caltech.edu', 'pbs', settings, user='mfornace', 
                      python='/home/mfornace/local/anaconda3/bin/python3')

while True:
    for _ in range(args.dedicated):
        work(timeout=4e5)

    for _ in range(args.shared):
        work(timeout=1.42e4, idle=1e3, queue='fourgpu')

    if not args.period: break
    time.sleep(args.period)
