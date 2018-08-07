from string import Template

################################################################################

cloudwatch = Template(r'''
import logging, boto3, distributed
if __name__ == '__main__':
    session = boto3.Session(region_name='$region', aws_access_key_id=$access, aws_secret_access_key=$secret)
    ch = CloudWatchHandler(group=$group, stream=$stream, interval=$interval, session=session, level=logging.INFO)
    logging.getLogger('distributed').addHandler(ch)
''')

################################################################################

config = Template(r'''
import dask
if __name__ == '__main__':
    dask.config.set(dict($config))
''')

################################################################################

scheduler = Template(r'''
import os, sys, yaml, getpass, pathlib, dask, distributed, logging, resource
from distributed.cli.dask_scheduler import go

if __name__ == '__main__':
    os.chdir(pathlib.Path.home())
    resource.setrlimit(resource.RLIMIT_NOFILE, (131072, 131072))
    info = dict(ip='$host', pid=os.getpid(), pwd=os.getcwd(), user=getpass.getuser(), port=$port, path=$path)
    dask.config.set(cloud=info)
    with pathlib.Path('~/dask.yml').expanduser().open('w') as f:
        yaml.dump(dask.config.config, f)

$preload

if __name__ == '__main__':
    sys.argv = ['dask-scheduler']
    sys.argv += ['--port', str($port)]
    if $path: sys.argv += ['--local-directory', $path]
    logging.getLogger('distributed.scheduler').info('Executing go with arguments ' + str(sys.argv))
    go()
''')

################################################################################

# A wrapper around dask-worker to provide some more flexibility
# Processes must be set to 1 because of limitation on listen address
worker = Template(r'''
import os, sys, psutil, yaml, getpass, pathlib, multiprocessing, dask, distributed, logging, resource
from distributed.cli.dask_worker import go
from distributed.utils import get_ip_interface

if __name__ == '__main__':
    os.chdir(pathlib.Path.home())
    resource.setrlimit(resource.RLIMIT_NOFILE, (131072, 131072))
    info = dict(ip='$host', pid=os.getpid(), pwd=os.getcwd(), user=getpass.getuser(), port=$port)
    dask.config.set(cloud=info)
    with pathlib.Path('~/dask.yml').expanduser().open('w') as f:
        yaml.dump(dask.config.config, f)

$preload

if __name__ == '__main__':
    allowed = tuple(psutil.net_if_addrs().keys())
    ip = next(get_ip_interface(i) for i in $interfaces if i in allowed)

    sys.argv = ['dask-worker']
    sys.argv += ['%s:%d' % ('$shost', $sport)]
    sys.argv += ['--listen-address', 'tcp://%s:%d' % (ip, $port)]
    sys.argv += ['--contact-address', 'tcp://%s:%d' % ('$host', $port)]
    sys.argv += ['--nprocs', '1']
    sys.argv += ['--nthreads', str(multiprocessing.cpu_count())]
    sys.argv += ['--no-bokeh']
    sys.argv += ['--no-nanny'] # can't spawn processes with nanny
    sys.argv += ['--reconnect']
    sys.argv += ['--resources', 'THREADS=%d' % multiprocessing.cpu_count()]
    logging.getLogger('distributed.worker').info('Executing go with arguments ' + str(sys.argv))
    go()
''')
