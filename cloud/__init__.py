import os as _os, inspect as _in

from .openstack import *
from .cluster import *
from .script import *

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
