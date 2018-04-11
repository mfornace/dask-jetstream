from fn import env, colored

import boto3, botocore
import threading, os, sys, tempfile, tarfile, itertools, io

s3 = boto3.resource('s3')
_client = boto3.client('s3')

###############################################################################

def watch_progress(fn, *, size=None, scale=1.e6):
    size = env.path_size(fn) if size is None else size
    size /= scale
    fn = os.path.basename(fn)
    seen = 0
    lock = threading.Lock()
    def call(bytes_amount):
        nonlocal seen
        with lock:
            seen += bytes_amount / scale
            percent = (seen / size) * 100
            msg = '%s  %.1f / %.1f MB (%.2f%%)' % (fn, seen, size, percent)
            sys.stdout.write(colored('\r' + msg.center(80), 'yellow'))
            sys.stdout.flush()
    return call

###############################################################################

class Storage:
    client = _client

    def __init__(self, name):
        self.bucket = s3.Bucket(name)

    @property
    def name(self):
        return self.bucket.name

    def __iter__(self):
        return self.bucket.objects.all()

    def keys(self, prefix=None):
        if prefix is None:
            return (obj.key for obj in self.bucket.objects.all())
        else:
            return (obj.key for obj in self.bucket.objects.filter(Prefix=prefix))

    def __getitem__(self, key):
        ret = self.bucket.Object(key)
        ret.load()
        return ret

    def __contains__(self, key):
        try:
            self.bucket.Object(key).load()
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise e

    def _clear_path(self, name):
        name = '/'.join(i for i in name.split('/') if i)
        for f in itertools.accumulate(i + '/' for i in name.split('/')[:-1]):
            response = self.bucket.put_object(Body='', Key=f)
        return name

    def send_file(self, name, path, live=env.interactive()):
        path = str(path)
        name = self._clear_path(name)
        callback = watch_progress(path) if live else None
        self.bucket.Object(name).upload_file(path, Callback=callback)
        if live: print()
        return name

    def delete_files(self, *names):
        if not names: return 0
        self.bucket.delete_objects(Delete={'Objects': [{'Key': n} for n in names]})
        return len(names)

    def upload(self, name, buffer_size=32768):
        return Upload(self.bucket.Object(name), buffer_size)

    def load_string(self, name, live=env.interactive()):
        buffer = io.BytesIO()
        self.load_file(name, buffer, live=live)
        buffer.flush()
        return buffer.getvalue().decode()

    def load_file(self, name, path, live=env.interactive()):
        obj = self.bucket.Object(name)
        callback = watch_progress(str(path), size=obj.content_length) if live else None
        try:
            if hasattr(path, 'write'):
                ret = obj.download_fileobj(path, Callback=callback)
            else:
                ret = obj.download_file(str(path), Callback=callback)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise ValueError('Name {} is missing from the s3 bucket'.format(name))
            else:
                raise e
        if live: print()
        return ret

    def __getstate__(self):
        state = self.__dict__.copy()
        state['bucket'] = self.name
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.bucket = s3.Bucket(self.bucket)

    def move_file(self, old, new):
        self.bucket.Object(new).copy_from(CopySource='{}/{}'.format(self.name, old))
        self.bucket.Object(old).delete()

###############################################################################

class Upload:
    def __init__(self, obj, buffer_size):
        self.count, self.upload = None, None
        self.obj = obj
        self.buffer_size = buffer_size
        self.buffer = None

    def flush(self):
        self.buffer.flush()
        part = self.upload(str(self.count))
        part.upload(Body=self.buffer)
        self.count += 1
        self.buffer.seek(0)
        self.buffer.truncate()

    def write(self, s):
        if self.buffer.tell() > self.buffer_size:
            self.flush()
        self.buffer.write(s)

    def __enter__(self):
        self.upload = self.obj.initiate_multipart_upload()
        self.count = 0
        self.buffer = io.StringIO()
        return self

    def __exit__(self, cls, value, traceback):
        try:
            self.flush()
            self.upload.complete()
        except Exception as e:
            self.upload.abort()
            raise e
        finally:
            self.count = None
            self.upload = None

###############################################################################
