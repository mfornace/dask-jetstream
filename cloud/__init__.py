import os as _os, inspect as _in

_REMOTE_ROOT = _os.path.dirname(_os.path.abspath(_in.getfile(_in.currentframe())))

from . import ssh, pickler