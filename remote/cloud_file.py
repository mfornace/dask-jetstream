import fn, s3fs
from . import pickler
import uuid, os, pathlib

################################################################################

# fs = s3fs.S3FileSystem()
# fs.ls('my-bucket')
# ['my-file.txt']
# >>> with fs.open('my-bucket/my-file.txt', 'rb') as f:
# ...     print(f.read())

################################################################################

class Cloud_File:
    '''Descriptor denoting a file on S3'''
    def __init__(self, name, filesystem=None):
        self.tmp = None
        self.db_name = str(name)
        self._name = os.path.basename(name)
        self.filesystem = filesystem
        self._downloaded = False

    @property
    def path(self):
        if self.tmp is None:
            self.tmp = fn.temporary_path()
            self.database.load_file(self.db_name, self.tmp.path/self._name)
        return self.tmp.path/self._name

    def upload(self):
        if self.tmp is not None:
            self.database.send_file(self.db_name, self.path)

    def __getstate__(self):
        self.upload()
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.tmp = None

    def __str__(self):
        return str(self.db_name)

    def __enter__(self):
        return self.path

    def __exit__(self, cls, value, traceback):
        self.upload()
        self.tmp.cleanup()
        self.tmp = None

################################################################################

class Positioned_File:
    '''
    Temporarily positioned file object
    '''
    def __init__(self, file, pos=None):
        self.file = file
        self.old = file.tell()
        file.flush()
        if pos is not None: 
            file.seek(pos)

    def __enter__(self):
        return self.file

    def __exit__(self, cls, value, traceback):
        self.file.seek(self.old)

################################################################################

class Buffered_List(Cloud_File):
    def __init__(self, name, database):
        super().__init__(name, database)
        self.positions = []
        self._file = None

    def __enter__(self):
        self._file = self.path.open('+b')
        return self

    def __exit__(self, cls, value, traceback):
        self._file.close()
        super().__exit__(cls, value, traceback)

    def append(self, value):
        self.positions.append(self._file.tell())
        pickler.dump(value, self._file)

    def __getitem__(self, index):
        with Positioned_File(self._file, self.positions[index]) as f:
            return pickler.load(f)

    def __getstate__(self):
        if self._file is not None:
            self._file.flush()
        state = super().__getstate__().copy()
        state['_file'] = self._file is not None
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self._file = self.path.open('+b') if self._file else None

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.tmp = fn.temporary_path()
        self.database.load_file(self.db_name, self.path)

    def items(self):
        with Positioned_File(self._file) as f:
            for i in self.positions:
                f.seek(i)
                yield pickler.load(f)

    def __iter__(self):
        return iter(self.positions)

################################################################################
