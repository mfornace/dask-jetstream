from remote import ssh
import argparse

###############################################################################

parser = argparse.ArgumentParser(description='Launch an agent on Comet')
parser.add_argument('number',      type=int,   help='number of agents')
parser.add_argument('platform',    type=str,   help='gpu or cpu')
parser.add_argument('--timeout',   type=float, help='number of seconds allowed', default=12*3600)
parser.add_argument('--idle',      type=float, help='number of idle seconds allowed')
args = parser.parse_args()

assert args.timeout < 48*3600

def work():
    settings = {
        'agents':       'md-agents',
        'jobs':         'md-jobs',
        'files':        'md-files',
        'timeout':      args.timeout,
        'account':      'CIT161',
        'idle-timeout': args.timeout if args.idle is None else args.idle
    }
    if args.platform == 'gpu':
        settings.update({'queue': 'gpu', 'gpus': 4, 'cpus': 24})
    else:
        settings.update({'queue': 'compute', 'cpus': 16, 'gpus': 0})

    ssh.remote_submit('comet.sdsc.xsede.org', settings, user='mfornace')


for _ in range(args.number):
    work()
