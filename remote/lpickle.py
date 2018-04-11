try:
    from cloudpickle import loads, dumps, load, dump as _dump
    implementation = 'cloudpickle'
except ImportError:
    from pickle import loads, dumps, load, dump as _dump
    implementation = 'pickle'

################################################################################

import functools, zlib

################################################################################

class wrap:
    """
    Simple class to circumvent e.g. multiprocessing using lpickle from standard
    library. Reads a function and arguments into a bytestring, and unpacks them
    when __call__ is requested
    """
    def __init__(self, function, *args, **kwargs):
        self.data = dumps((function, args, kwargs))

    def __call__(self, *args, **kwargs):
        if isinstance(self.data, bytes):
            self.data = loads(self.data)
        return self.data[0](*self.data[1], *args, **self.data[2], **kwargs)

################################################################################

LINKS = None

def register_link(link):
    LINKS.append(link)

class _Link_Context:
    def __enter__(self):
        global LINKS
        LINKS, self.links = [], LINKS

    def __exit__(self, cls, value, traceback):
        global LINKS
        LINKS, self.links = self.links, LINKS

################################################################################

@functools.wraps(_dump)
def dump(obj, file, *args, **kwargs):
    tracker = _Link_Context()
    with tracker:
        _dump(obj, file, *args, **kwargs)
    file.flush()
    return tracker.links

################################################################################

@functools.wraps(loads)
def loadz(data, *args, **kwargs):
    return loads(zlib.decompress(data), *args, **kwargs)

@functools.wraps(dumps)
def dumpz(data, *args, **kwargs):
    return zlib.compress(dumps(data, *args, **kwargs))

################################################################################
