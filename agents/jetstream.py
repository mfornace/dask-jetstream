from remote import ssh
from remote.jetstream import Instance

import argparse, time, uuid

###############################################################################

parser = argparse.ArgumentParser(description='Launch an agent on JetStream')
parser.add_argument('name',      type=str, help='base name for instance')
parser.add_argument('number',    type=int, help='number of instances')
parser.add_argument('image',     type=str, help='type of image')
parser.add_argument('flavor',    type=str, help='flavor of instance')
parser.add_argument('timeout',   type=str, nargs='?', help='timeout ending in (s|m|h|d)', default='144h')
ARGS = parser.parse_args()

timeout = float(eval(ARGS.timeout[:-1])) * dict(s=1, m=60, h=3600, d=3600*24)[ARGS.timeout[-1]]

###############################################################################

settings = {
    'agents':       'md-agents',
    'jobs':         'md-jobs',
    'output':       'agent.o',
    'files':        'md-files',
    'timeout':      timeout,
    'idle-timeout': timeout
}

def early_exit(event, s, sleep=0):
    if event.is_set():
        print('%s -- exiting early' % s)
        return True
    if sleep: time.sleep(sleep)
    return False


def work(s, event):
    name = ARGS.name + '-' + str(uuid.uuid4())
    print('%s -- initialized as %s' % (s, name))
    instance = Instance(name, image=ARGS.image, flavor=ARGS.flavor)
    stop = lambda t: early_exit(event, s, t)

    try:
        if stop(0): return
        print('%s -- booted with id %s' % (s, instance.id))
        while instance.status() != 'active':
            if stop(5): return

        ip = instance.ip()
        while ip.status() != 'active':
            if stop(5): return
        print('%s -- started at %s' % (s, ip.address))

        for n in range(1, 360):
            if ssh.remote_submit(ip.address, 'local', settings, python='/usr/anaconda3/bin/python3', user='root'):
                print('%s -- submitted after %d tries' % (s, n))
                n = int(timeout / 5) + 1
                for _ in range(n):
                    if stop(timeout / n): return
                print('%s -- completed' % s)
                break
            for _ in range(4):
                if stop(5): return
            print('%s -- failed SSH connection on attempt %d' % (s, n))
        else:
            print('%s -- could not make SSH connection' % s)

    except Exception as e:
        print(type(e))
        raise e
    finally:
        instance.delete()
        print('%s -- deleted' % s)

if False:
    from threading import Event, Thread
else:
    from multiprocessing import Event, Process as Thread

event = Event()
workers = [Thread(target=work, args=(('[%d]' % i).ljust(4), event)) for i in range(ARGS.number)]

for w in workers:
    w.start()

try:
    while workers:
        time.sleep(5)
        workers = [w for w in workers if w.is_alive()]
except KeyboardInterrupt as e:
    event.set()
    print('   -- deleting workers due to interrupt')
    for w in workers: w.join()
