from . import aws, link_db, file_db
from fn import env

import boto3
import decimal

###############################################################################

class Chronological_Database(link_db.Link_Database):
    fields = [('name', str), ('time', decimal.Decimal)]

    def __init__(self, name):
        super().__init__(name)
        self.files = file_db.File_Database(super().__getitem__(['FILES', 0])['database-name'])

    def by_time(self, predicate):
        expr = aws.compose(predicate, boto3.dynamodb.conditions.Key('time'))
        return self.table.scan(FilterExpression=expr)['Items']

    def by_name(self, predicate):
        expr = aws.compose(predicate, boto3.dynamodb.conditions.Key('name'))
        return self.table.query(KeyConditionExpression=expr)['Items']

    def count(self, predicate):
        return len(self.scan(predicate))

    def clear_name(self, name):
        items = self.by_name(lambda k: k == name)
        return env.count(self.__delitem__(i) for i in items)

    def latest(self, name):
        items = self.by_name(lambda k: k == name)
        return aws.convert_from_db(max(items, key=lambda i: i['time']))

    def _key(self, key):
        if isinstance(key, str):
            return dict(name=key, time=env.unix_time())
        elif hasattr(key, 'items'):
            return dict(name=key['name'], time=key.get('time', env.unix_time()))
        else:
            return {f[0]: f[1](k) for f, k in zip(self.fields, key)}

    def __iter__(self):
        return iter(sorted(filter(lambda i: i['name'] != 'FILES', super().__iter__()), key=lambda i: i['time']))

    def __setitem__(self, key, item):
        """
        1) adds time attribute if not present
        2) sends given tarfile or tarfile of given path to self.files ['file']
        """
        key = self._key(key)
        item = item.copy()
        item.update(key)
        super().__setitem__(key, item)
        return item['name'], item['time']

    def __delitem__(self, key):
        super().__delitem__(self.__getitem__(key))

###############################################################################

for k, v in {'hour': 3600, 'day': 3600*24, 'minute': 60, 'month': 2629800}.items():
    def get(self, n=1, seconds=v):
        return self.scan(lambda i: i['time'] > (env.unix_time() - n * seconds))
    setattr(Chronological_Database, 'last_' + k, get)
