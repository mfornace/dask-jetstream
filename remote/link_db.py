from . import aws, file_db, file

################################################################################

class Link_Database(aws.Database):
    files = None

    def copy_item(self, old, new):
        item = self[old]
        new = self._key(new)
        item.update(new)
        self[item] = item
        for link in item['links']:
            self.files.acquire(link)
        return new, item

    def put_file(self, key, path, os_path, replace=False, links=()):
        self.add(key, 'links', {path})
        self.files.__setitem__(path, os_path, replace=replace, links=links)

    def remove_files(self, key, *paths):
        ret = self.files.release(*paths)
        self.remove(key, 'links', set(paths))
        return ret

    def __setitem__(self, key, item):
        item = item.copy()
        item['links'] = {'self'}.union(item.get('links', ()))
        super().__setitem__(key, item)

    def __delitem__(self, key):
        try:
            self.files.release(*self[key]['links'])
        except KeyError:
            print('Warning: key {} has already been released'.format(key))
        super().__delitem__(key)

    def _vstr(self, v):
        return ''.join('\n    -- ' + l for l in v['links'] if l != 'self')

################################################################################
