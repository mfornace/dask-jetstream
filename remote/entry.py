from fn import env, colored
from .utility import command
import remote

import types, uuid, collections, sys, copy, json, datetime, time, functools

###############################################################################

def _serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif type(obj).__name__ == 'Quantity':
        return str(obj)
    elif isinstance(obj, bytes):
        return obj.decode()
    raise TypeError('Type {} not serializable'.format(type(obj)))

###############################################################################

class Entry:
    def __init__(self, key, database, *, verbose=False):
        self.verbose = verbose
        self.database = database
        self.attributes = {'status': 'running', 'git': command.git_info()}
        self.key = database.__setitem__(key, {'status': 'running'})

        self.info = collections.OrderedDict() # log
        self.trace = [('', self.info)]        # current scope in log
        self._log = [0]                       # next index available in log
        self._scope = []                      #
        self.filenames = {'entry.json'}       #

        self['prefix'] = '{}-{}/'.format(*self.key)

    def copy(self, key=None):
        ret = copy.deepcopy(self)
        ret.key = ret.key[0] if key is None else key
        try:
            ret.key = ret.database.copy_item(self.key, ret.key)[0]
        except KeyError:
            time.sleep(0.05) # in case of 'time' clash
            ret.key = ret.database.copy_item(self.key, ret.key)[0]
        ret.key = (ret.key['name'], ret.key['time'])
        ret['prefix'] = '{}-{}/'.format(*ret.key)
        ret.database.remove_files(ret.key, self.attributes['prefix'] + 'entry.json')
        return ret

    def _print(self, string):
        if self.verbose and len(string) < 200:
            print((len(self.trace) - 1) * '  ' + '-- ' + string)

    def put_path(self, key, path):
        filename = self.attributes['prefix'] + str(env.mangled_path(path.name, lambda p: str(p) in self.filenames))
        self[key] = filename
        self.filenames.add(filename)
        self.database.put_file(self.key, filename, path)

    def put_pickle(self, key, obj):
        with env.temporary_path('{}.lp'.format(key)) as path:
            with path.open('wb') as f:
                remote.lpickle.dump(obj, f)
            self.put_path(key, path)

    def log(self, key, value):
        assert not isinstance(value, env.Path)
        self.trace[-1][1][key] = value
        return value

    def __setitem__(self, key, value):
        assert not isinstance(value, env.Path) and value != ''
        self.log('set-' + key, value)
        key = key.split('.')
        scope = self._scope + key
        self._print('Set {} to {}'.format(colored('.'.join(scope), 'blue'), colored(str(value), 'blue')))

        place = self.attributes
        for i, s in enumerate(scope):
            if i + 1 == len(scope):
                self.database.set(self.key, scope, value)
                place[s] = value
            elif s not in place:
                self.database.set(self.key, scope[:i+1], {})
                place[s] = {}
                place = place[s]

    def write(self, file=None, string=False, *args, indent=4, **kwargs):
        item = self.info.copy()
        item['module-code'] = command.current_module_sources()
        if file is None:
            with env.temporary_path('entry.json') as path:
                with path.open('w') as file:
                    json.dump(item, file, *args, default=_serial, indent=indent, **kwargs)
                self.database.put_file(self.key, self.attributes['prefix'] + 'entry.json', path, replace=True)
        else:
            json.dump(item, fp, *args, default=_serial, indent=indent, **kwargs)
        if string:
            return json.dumps(item, *args, default=_serial, indent=indent, **kwargs)

    def __getstate__(self):
        self.write()
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __enter__(self):
        self['platform'] = str(remote.current_platform()) # BUG
        self['status'] = 'running'
        return self

    def __exit__(self, cls, value, traceback):
        if value is not None:
            self['status'] = type.__name__
            self['traceback'] = str(traceback)
            self['error'] = str(value)
        else:
            self['status'] = 'complete'
        self.dump()

    def __call__(self, name):
        return Scoped_Entry(self, name)

    def __lshift__(self, message, verbose=True):
        if verbose: self._print(message)
        key = 'log-' + str(self._log[-1]).zfill(4)
        self.trace[-1][1][key] = message
        self._log[-1] += 1
        return Traced_Entry(self, message)

################################################################################

class Scoped_Entry:
    def __init__(self, entry, name):
        self.name = name
        self.entry = entry

    def __setitem__(self, key, value):
        self.entry[key] = value

    def __getattr__(self, key):
        return getattr(self.entry, key)

    def __enter__(self):
        self._scope.append(self.name)
        return self

    def __exit__(self, cls, value, tb):
        if value is not None and cls != GeneratorExit:
            err = str(value) if cls != KeyboardInterrupt else 'interrupt'
            self._print(colored('{}: {}'.format(cls.__name__, err) + err, 'red'))
            self.__setitem__('error', err)
            self.__setitem__('error_type', cls.__name__)
        self._scope.pop()

################################################################################

class Traced_Entry:
    def __init__(self, entry, name):
        self.name = name
        self.entry = entry

    def __setitem__(self, key, value):
        self.entry[key] = value

    def __getattr__(self, key):
        return getattr(self.entry, key)

    def __enter__(self):
        key = self.name
        i = 0
        while key in self.trace[-1][1]:
            i += 1
            key = '{}-{}'.format(self.name, i)

        inner = collections.OrderedDict()
        self.trace[-1][1][key] = inner
        self.trace.append((self.name, inner))
        self._log.append(0)

    def __exit__(self, cls, value, tb):
        if value is not None and cls != GeneratorExit:
            err = str(value) if type != KeyboardInterrupt else 'interrupt'
            self._print(colored('{}: {}'.format(cls.__name__, err), 'red'))
            self.__lshift__(err, False)
        self._log.pop()
        self.trace.pop()

################################################################################

def entry_context(func):
    @functools.wraps(func)
    def decorated(db, *args, **kwargs):
        with db:
            return func(db, *args, **kwargs)
    return decorated

################################################################################
