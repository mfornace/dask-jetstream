from . import chrono_db, task, lpickle
from fn import env

import os, tarfile, decimal, sys


class Job_Database(chrono_db.Chronological_Database):

    def get_task(self, item):
        return task.Task(item, self)

    def reset_if_running(self, key):
        return self.set_if(key, 'status', 'running', 'queued')

    def release_with_status(self, key, status, remove=False):
        """Release a job, it must have been running"""
        item = self[key]
        if not self.set_if(item, 'status', 'running', status):
            raise ValueError('status {} should be running for release'.format(item['status']))
        if remove and 'file' in item and status == 'complete':
            self.files.release(item['file'])
            self.remove(item, 'links', {item['file']})

    def tar(self, path, function, paths):
        with (path/'function.lp').open('wb') as f:
            links = lpickle.dump(function, f)
        with tarfile.open(str(path/'job.tar.gz'), 'w:gz') as tf:
            tf.add(str(path/'function.lp'), arcname='function.lp')
            for path in map(str, paths):
                tf.add(path, arcname=os.path.basename(path))
        return links

    def replace(self, item, function, path):
        with env.temporary_path() as tmp:
            paths = [path/name for name in self[item]['paths']]
            links = self.tar(tmp, function, paths)
            self.put_file(item, '{}-{}.tar.gz'.format(item['name'], item['time']), tmp/'job.tar.gz', links=links, replace=True)

    def output(self, key):
        try:
            links = key['links']
        except (TypeError, KeyError):
            links = self[key]['links']
        try:
            return next(self.files.loads(l) for l in links if l.endswith('.o'))
        except StopIteration:
            return ''

    def put(self, key, function, *, paths=(), prefix='job-', restart=False, get=None, **item):
        item.update({
            'paths': tuple(map(os.path.basename, map(str, paths))),
            'prefix': str(prefix),
            'status': 'unavailable',
            'file-database': self.files.name,
            'attempts': 0,
            'checkpoints': 0,
            'restart': bool(restart),
            'get': None if get is None else lpickle.dumpz(get)
        })
        with env.temporary_path() as tmp:
            key = self.__setitem__(key, item)
            filename = '{}-{}.tar.gz'.format(*key)
            self.set(key, 'file', filename)
            self.put_file(key, filename, tmp/'job.tar.gz', links=self.tar(tmp, function, paths))
            self.set(key, 'status', 'queued')
            return key

    def _vstr(self, v): # chrono_db.Chronological_Database
        kvp = lambda s: '\n    {}: {}'.format(s, v[s])
        return super()._vstr(v) + ''.join(map(kvp, ['status', 'file', 'attempts', 'checkpoints']))


for k in ['running', 'complete', 'queued', 'error']:
    def running(self, status_name=k):
        return self.where('status', status_name)
    setattr(Job_Database, k, property(running))
