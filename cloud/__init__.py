import os as _os, inspect as _in, sys as _sys

from .ostack import *
from .cluster import *
from .script import *
from .cloudwatch import *

import psutil

def mem_percent():
    return psutil.virtual_memory().percent


def task_distribution(who_has):
    out = {}
    for k, v in who_has.items():
        for ip in v:
            out.setdefault(ip, []).append(k)
        if not v:
            out.setdefault(None, []).append(k)
    return out

def module_versions():
    ver = lambda v: getattr(v, 'version', None) or getattr(v, '__version__', None)
    return {k: ver(v) for k, v in sorted(_sys.modules.items()) if isinstance(ver(v), str)}
