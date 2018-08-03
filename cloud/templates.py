contents = r'''
#script=watchtower
import watchtower, logging, boto3, distributed
if __name__ == '__main__':
    session = boto3.Session(region_name='{region}', aws_access_key_id={access}, aws_secret_access_key={secret})
    ch = watchtower.CloudWatchLogHandler(log_group={group}, stream_name={stream}, send_interval={interval}, boto3_session=session)
    ch.setLevel(logging.INFO)
    logging.getLogger('distributed').handlers.append(ch)

################################################################################

#script=scheduler
import os, sys, json, getpass, resource, pathlib, dask, distributed, logging
from distributed.cli.dask_scheduler import go

cfg = dask.config.config['distributed']
cfg['scheduler']['allowed-failures'] = 5
cfg['worker']['profile']['interval'] = '200ms'
cfg['comm']['timeouts']['connect'] = '300s'
cfg['comm']['timeouts']['tcp'] = '600s'
cfg['admin']['tick']['interval'] = '1s'
cfg['admin']['tick']['limit'] = '30s'

if __name__ == '__main__':
    resource.setrlimit(resource.RLIMIT_NOFILE, (131072, 131072))
    with pathlib.Path('~/config.json').expanduser().open('w') as f:
        info = dict(ip='{host}', pid=os.getpid(), pwd=os.getcwd(), user=getpass.getuser(), port={port}, path={path})
        json.dump(info, f, sort_keys=True, indent=4)

{preload}

if __name__ == '__main__':
    sys.argv[0] = 'dask-scheduler'
    sys.argv += ['--port', str({port})]
    if {path}: sys.argv += ['--local-directory', {path}]
    logging.getLogger('distributed.scheduler').info('Executing go with arguments ' + str(sys.argv))
    go()

################################################################################

#script=worker
# A wrapper around dask-worker to provide some more flexibility
# Processes must be set to 1 because of limitation on listen address
import os, sys, psutil, json, getpass, resource, pathlib, multiprocessing, dask, distributed, logging
from distributed.cli.dask_worker import go
from distributed.utils import get_ip_interface

cfg = dask.config.config['distributed']
cfg['scheduler']['allowed-failures'] = 5
cfg['worker']['profile']['interval'] = '200ms'
cfg['comm']['timeouts']['connect'] = '300s'
cfg['comm']['timeouts']['tcp'] = '600s'
cfg['admin']['tick']['interval'] = '1s'
cfg['admin']['tick']['limit'] = '30s'

if __name__ == '__main__':
    resource.setrlimit(resource.RLIMIT_NOFILE, (131072, 131072))
    with pathlib.Path('~/config.json').expanduser().open('w') as f:
        info = dict(ip='{host}', pid=os.getpid(), pwd=os.getcwd(), user=getpass.getuser(), port={port})
        json.dump(info, f, sort_keys=True, indent=4)

{preload}

if __name__ == '__main__':
    allowed = tuple(psutil.net_if_addrs().keys())
    ip = next(get_ip_interface(i) for i in {interfaces} if i in allowed)

    sys.argv[0] = 'dask-worker'
    sys.argv += ['%s:%d' % ('{shost}', {sport})]
    sys.argv += ['--listen-address', 'tcp://%s:%d' % (ip, {port})]
    sys.argv += ['--contact-address', 'tcp://%s:%d' % ('{host}', {port})]
    sys.argv += ['--nprocs', '1']
    sys.argv += ['--nthreads', str(multiprocessing.cpu_count())]
    sys.argv += ['--no-bokeh']
    sys.argv += ['--no-nanny'] # can't spawn processes with nanny
    sys.argv += ['--reconnect']
    sys.argv += ['--resources', 'THREADS=%d' % multiprocessing.cpu_count()]
    logging.getLogger('distributed.worker').info('Executing go with arguments ' + str(sys.argv))
    go()

################################################################################
'''
