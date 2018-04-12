'''
Utilities for determining limits and capabilities
'''

from fn import split_evenly

import os, getpass, platform, json

###############################################################################

# Deduce GPUs
try:
    import pyopencl
    _GPUS = {}
    for p in pyopencl.get_platforms():
        for i, d in enumerate(p.get_devices()):
            if d.type == getattr(pyopencl.device_type, 'GPU', None):
                _GPUS[str(i)] = d.name
except ImportError:
    _GPUS = None

###############################################################################

# Deduce RAM
try:
    import psutil, warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        _RAM = psutil.virtual_memory().total / 1024**3
except ImportError:
    _RAM = 0

if _RAM == 0:
    try:
        meminfo = dict((i.split()[0].rstrip(':'), int(i.split()[1])) for i in open('/proc/meminfo').readlines())
        _RAM = meminfo['MemTotal'] / 1024**3 * 1000 # conversion factor maybe not exactly correct
    except FileNotFoundError:
        _RAM = None

###############################################################################

ENV_MAP = [
    ('HOST',                    'node'),
    ('HOSTNAME',                'node'),
    ('USER',                    'user'),
    ('HOME',                    'home'),
    ('PWD',                     'pwd'),
    ('PBS_O_WORKDIR',           'submit-dir'),
    ('TMPDIR',                  'tmp'),
    ('PBS_QUEUE',               'queue'),
    ('PBS_JOBID',               'id'),
    ('PBS_JOBNAME',             'job'),
    ('PBS_NODEFILE',            'nodefile'),
    ('PBS_ARRAYID',             'array-id'),
    ('PBS_VNODENUM',            'vnode'),
    ('PBS_NODENUM',             'node-index'),
    ('PBS_OPATH',               'pbs-path'),
    ('PBS_NUM_PPN',             'cpus'),
    ('SLURM_JOBID',	            'id'),
    ('SLURM_SUBMIT_DIR',        'submit-dir'),
    ('SLURM_SUBMIT_HOST',	    'host-node'),
    ('SLURM_JOB_NODELIST',	    'node'),
    ('SLURM_ARRAY_TASK_ID',	    'task-id'),
    ('SLURM_JOB_CPUS_PER_NODE',	'cpus'),
    ('SLURM_NNODES',	        'nodes'),
]

__PLATFORM__ = None
PLATFORM = None

###############################################################################

def reset_platform():
    global PLATFORM, __PLATFORM__
    if __PLATFORM__ is not None:
         PLATFORM = __PLATFORM__

def current_platform():
    return default_platform_info() if PLATFORM is None else PLATFORM

def split_platform(i, n):
    '''Divide the resources in the platform into n chunks'''
    global PLATFORM
    p = current_platform()
    p['resources']['cpus'] = split_evenly(p['resources']['cpus'], n)[i]
    gpus = p['resources']['gpus']
    p['resources']['gpus'] = split_evenly(gpus, n)[i]
    if hasattr(gpus, 'items'):
        p['resources']['gpus'] = {k: gpus[k] for k in p['resources']['gpus']}
    PLATFORM = p

###############################################################################

def read_json(s):
    if not os.path.isfile(s):
        return {}
    try:
        with open(s, 'r') as ifs:
            return json.load(ifs)

def default_platform_info(override=None):
    if override is None:
        override = read_json(os.path.join(os.path.expanduser('~'), 'remote.json'))
    env = dict(os.environ)
    info = {v: env[k] for (k, v) in ENV_MAP if k in env}
    info.update(override)
    resources = info.setdefault('resources', {})
    info.setdefault('node', platform.node())
    info.setdefault('user', getpass.getuser())
    info.setdefault('cpus', os.cpu_count())
    info.setdefault('heartbeat', 240)
    if _GPUS is not None:
        info.setdefault('gpus', len(_GPUS))
        resources.setdefault('gpus', _GPUS)
    elif 'gpus' in info:
        resources.setdefault('gpus', list(range(info['gpus'])))
    if 'cpus' in info:
        resources.setdefault('cpus', list(range(info['cpus'])))
    if _RAM is not None:
        info.setdefault('ram', _RAM)
    if 'ram' in info:
        resources.setdefault('ram', info['ram'])
    return info

###############################################################################
