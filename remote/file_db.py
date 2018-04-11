from fn import env
from . import aws, storage, lpickle

import botocore, tarfile, uuid

###############################################################################

class File_Database(aws.Database):
    fields = [('path', str)]

    def __init__(self, name):
        super().__init__(name)
        self.storage = storage.Storage(aws.Database.__getitem__(self, 'BUCKET')['name'])

    def clean_up(self):
        for i in self.scan(lambda i: i['count'] == 0):
            self.files.delete_items(i['path'])

    def __setitem__(self, path, os_path, links=(), replace=False):
        """Put an OS path on the database"""
        os_path = env.Path(os_path)
        links = {'self'}
        links.update(links)
        if replace and str(path) in self.storage:
            swap = self.storage.send_file('{}.{}.swap'.format(path, uuid.uuid4()), os_path)
            self.storage.move_file(swap, path)
        else:
            path = self.storage.send_file(path, os_path)
        prev = aws.Database.__setitem__(self, path, dict(count=1, links=links), replace=replace)
        if replace and prev:
            self.release(*prev['links'])

    @property
    def __delitem__(self):
        raise AttributeError('Use "release" to delete file entries')

    def loads(self, path):
        return self.storage.load_string(path)

    def load(self, path, os_path=None):
        if os_path is None:
            ret = env.temporary_path(env.Path(path).name)
            self.load(path, ret.path)
            return ret
        self.storage.load_file(path, os_path)

    def unpickle(self, path):
        with self.load(path) as os_path:
            with os_path.open('rb') as f:
                return lpickle.load(f)

    def extract(self, path, os_path=None, mode='r:gz'):
        if os_path is None:
            ret = env.temporary_path()
            self.extract(path, ret.path)
            return ret
        with self.load(path) as tarpath:
            with tarfile.open(str(tarpath), mode=mode) as tf:
                tf.extractall(path=str(os_path))

    def _release(self, keys):
        """Recursively yield dead keys"""
        for k in keys:
            if k == 'self': continue
            key = self._key(k)
            count = self.add(key, 'count', -1)
            if count <= 1:
                if count == 0:
                    print('WARNING', count, key)
                try:
                    self._release(self[key]['links'])
                except KeyError:
                    raise KeyError('{}[{}]'.format(key, 'links'))
                yield k, key

    def release(self, *keys):
        """Release ownership on all the keys given"""
        dels = dict(self._release(keys))
        for key in dels.values(): self._delete_key(key)
        self.storage.delete_files(*dels.keys())
        return len(dels)

    def __iter__(self):
        return iter(filter(lambda i: i['path'] != 'BUCKET', aws.Database.__iter__(self)))

    def acquire(self, key):
        if key != 'self':
            self.add(key, 'count', +1)

    def clobber(self):
        n = 0
        if input('Clobber all the files? (yes|no)\n') != 'yes':
            return n
        for k in self.keys():
            self.storage.delete_files(k)
            self._delete_key({'path':k})
            n += 1
        return n

    def __str__(self):
        return "File_Database ({}):\n    ".format(self['BUCKET']['name']) + '\n    '.join('{}) {}'.format(i['count'], i['path']) for i in self if i['path'] != 'BUCKET')

###############################################################################
