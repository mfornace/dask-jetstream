try:
    from cloudpickle import loads, dumps, load, dump
    implementation = 'cloudpickle'
except ImportError:
    from pickle import loads, dumps, load, dump
    implementation = 'pickle'

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

@functools.wraps(loads)
def loadz(data, *args, **kwargs):
    return loads(zlib.decompress(data), *args, **kwargs)

@functools.wraps(dumps)
def dumpz(data, *args, **kwargs):
    return zlib.compress(dumps(data, *args, **kwargs))

################################################################################
