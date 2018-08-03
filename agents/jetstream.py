import fn
from remote import ssh
from remote.jetstream import Instance

import time, uuid, threading, multiprocessing, logging

log = logging.getLogger()

###############################################################################

def early_exit(event, index, sleep=0):
    '''Check if an event is set, if not sleep a while'''
    if event.is_set():
        log.info(fn.message('%s -- exiting early' % index))
        return True
    if sleep: time.sleep(sleep)
    return False

###############################################################################

def boot_instance(instance, index, event):
    '''Start the instance'''
    stop = lambda t: early_exit(event, index, t)
    if stop(0): return
    log.info(fn.message('%s -- booted with id %s' % (index, instance.id))

    while instance.status() != 'active': # Wait to become active
        if stop(5): return

    ip = instance.ip() # Wait for IP to become active
    while ip.status() != 'active':
        if stop(5): return

    log.info(fn.message('%s -- started at %s' % (index, ip.address))

###############################################################################

def launch_scheduler(event, index, address, timeout):
    '''Launch scheduler on a running instance'''
    stop = lambda t: early_exit(event, index, t)
    for n in range(1, 360):
        if ssh.remote_submit(address, 'local', settings, python='/usr/anaconda3/bin/python3', user='root'):
            log.info(fn.message('%s -- submitted after %d tries' % (index, n))
            n = int(timeout / 5) + 1
            for _ in range(n):
                if stop(timeout / n): return
            log.info(fn.message('%s -- completed' % index)
            break
        for _ in range(4):
            if stop(5): return
        log.info(fn.message('%s -- failed SSH connection on attempt %d' % (index, n))
    else:
        log.info(fn.message('%s -- could not make SSH connection' % index)

###############################################################################

def work(index, name, event, image, flavor):
    log.info(fn.message('%s -- initialized as %s' % (index, name))
    instance = Instance(name, image=image, flavor=flavor)
    try:
        boot_instance(instance, index, event)
    except Exception as e:
        log.error([type(e), e])
        raise e
    finally:
        instance.delete()
        log.info(fn.message('%s -- deleted' % index)

###############################################################################

class JetStreamCluster:
    def __init__(self, name, threading):
        if threading:
            self.worker = threading.Thread
            self.event = threading.Event()
        else:
            self.worker = multiprocessing.Process
            self.event = multiprocessing.Event()
        self.name = name.format(uuid.uuid4())
        self.scheduler = None
        self.workers = []

    def add_workers(self, n, image, flavor):
        names = ['{}-{}'.format(self.name, i) for i in range(n)]
        indices = [('[%d]' % i).ljust(4) for i in range(n)]
        workers = [(name, self.worker(target=work, args=(idx, name, self.event, image, flavor))) for idx, name in zip(indices, names)]
        for _, w in workers:
            w.start()
        self.workers.extend(workers)

    def poll(self):
        self.workers = [w for w in self.workers if w.is_alive()]
        return len(self.workers)

    def stop(self):
        log.info(fn.message('deleting workers due to stop() command')
        self.event.set()
        for w in self.workers: w.join()
        self.event.clear()
        self.workers = []

