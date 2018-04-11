from fn import env, ilen, try_except

import boto3
from botocore.exceptions import ClientError
import decimal, datetime, uuid, collections, time, functools, itertools, copy, sys

###############################################################################

dynamodb = boto3.resource('dynamodb')

TO_DB = []
def _to_db(value):
    for f in TO_DB:
        try: return f(value)
        except Exception: pass
    return value

###############################################################################

def convert_to_db(t):
    t = _to_db(t)
    if hasattr(t, 'keys'):
        ret = dict(t)
        if isinstance(t, collections.OrderedDict):
            ret['insertion-order'] = list(t.keys())
        for (k, v) in ret.items():
            ret[k] = convert_to_db(v)
        return ret
    elif t == '':
        return 'null'
    elif t == b'':
        return b'null'
    elif isinstance(t, datetime.datetime):
        return env.unix_time(t)
    elif isinstance(t, (str, bytes)):
        return t
    elif hasattr(t, 'union'):
        return set(map(convert_to_db, t))
    elif hasattr(t, '__len__'):
        return list(map(convert_to_db, t))
    elif isinstance(t, float):
        return decimal.getcontext().create_decimal_from_float(t)
    elif isinstance(t, uuid.UUID):
        return str(t)
    else:
        return t

def convert_from_db(t):
    if hasattr(t, 'keys'):
        if 'insertion-order' in t:
            ret = collections.OrderedDict((k, t[k]) for k in t['insertion-order'])
        else:
            ret = dict(t)
        for (k, v) in ret.items():
            ret[k] = convert_from_db(v)
        return ret
    elif t == 'null':
        return ''
    elif t == b'null':
        return b''
    elif isinstance(t, (str, bytes)):
        return t
    elif hasattr(t, '__len__'):
        return type(t)(convert_from_db(i) for i in t)
    elif isinstance(t, decimal.Decimal):
        return t if t % 1 != 0 else int(t)
    elif isinstance(t, boto3.dynamodb.types.Binary):
        return t.value
    else:
        return t

###############################################################################

def create_table(name):
    table = dynamodb.create_table(
        TableName=name,
        KeySchema=[
            {'AttributeName': 'name', 'KeyType': 'HASH'},
            {'AttributeName': 'time', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'name', 'AttributeType': 'S'},
            {'AttributeName': 'time', 'AttributeType': 'N'},
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    return table.item_count

###############################################################################

def _to_expr(t):
    if isinstance(t, datetime.datetime):
        return env.to_unix_time(t)
    elif isinstance(t, float):
        return decimal.getcontext().create_decimal_from_float(t)
    return t


class Expression:
    def __init__(self, expr):     self.expr = expr
    def  __eq__(self, value):     return Expression(self.expr.eq(_to_expr(value)))
    def  __ne__(self, value):     return Expression(self.expr.ne(_to_expr(value)))
    def  __lt__(self, value):     return Expression(self.expr.lt(_to_expr(value)))
    def __le__(self, value):      return Expression(self.expr.le(_to_expr(value)))
    def __gt__(self, value):      return Expression(self.expr.gt(_to_expr(value)))
    def __ge__(self, value):      return Expression(self.expr.ge(_to_expr(value)))
    def __and__(self, value):     return Expression(self.expr & value.expr)
    def __or__(self, value):      return Expression(self.expr | value.expr)
    def __invert__(self):         return Expression(~self.expr)
    def startswith(self, value):  return Expression(self.expr.begins_with(value))
    def between(self, low, high): return Expression(self.expr.between(_to_expr(low), _to_expr(high)))
    def type(self, value):        return Expression(self.expr.attribute_type(value))
    def exists(self):             return Expression(self.expr.exists())
    def contains(self, value):    return Expression(self.expr.contains(_to_expr(value)))
    def __getitem__(self, value): return Expression(self.expr(value))
    def __getattr__(self, value): return Expression(self.expr(value))

def compose(pred, expr):
    with decimal.localcontext() as ctx:
        ctx.prec = 28
        return pred(Expression(expr)).expr

###############################################################################

def retry(function, pauses):
    @functools.wraps(function)
    def decorated(*args, **kwargs):
        for p in pauses:
            try:
                return function(*args, **kwargs)
            except ClientError as e:
                if e.response['Error']['Code'] != 'ProvisionedThroughputExceededException':
                    raise e
                time.sleep(p)
        raise IOError('Timeout for retried operation')
    return decorated

class Table:
    fields = ('scan', 'query', 'put_item', 'get_item', 'update_item', 'delete_item')

    def __init__(self, name, min_pause=0.5, max_pause=1.e4, scale=2, pauses=None):
        self._table = dynamodb.Table(name)
        if pauses is None:
            self.pauses = tuple(itertools.takewhile(lambda x: x < max_pause,
                (min_pause * scale**i for i in itertools.count())))
        else:
            self.pauses = tuple(pauses)
        for i in self.fields:
            setattr(self, i, retry(getattr(self._table, i), self.pauses))

    def __getstate__(self):
        ret = {k: v for (k, v) in self.__dict__.items() if k not in self.fields}
        ret['_table'] = self.name
        return ret

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.__init__(self._table, pauses=self.pauses)

    @property
    def name(self):
        return self._table.name

    @property
    def item_count(self):
        return self._table.item_count

###############################################################################

class Database:
    fields = ()

    def __init__(self, name, predicate=None):
        self.table = Table(name)
        self.predicate = predicate

    @property
    def name(self):
        return self.table.name

    @property
    def field_names(self):
        return tuple(i[0] for i in self.fields)

    def tuple_key(self, key):
        key = self._key(key)
        return tuple(key[f[0]] for f in self.fields)

    def _key(self, key):
        if hasattr(key, 'items'):
            return {k: t(key[k]) for (k, t) in self.fields}
        elif isinstance(key, (tuple, list)):
            return {ft[0]: ft[1](k) for (ft, k) in zip(self.fields, key)}
        else:
            return {self.fields[0][0]: self.fields[0][1](key)}

    def _iter(self, predicate=None, value=None, *, convert=True):
        pred2 = predicate if value is None else lambda i: i[predicate] == value
        if self.predicate is None and pred2 is None:
            kws = {}
        else:
            if self.predicate is not None and pred2 is not None:
                pred = lambda i: self.predicate(i) & pred2(i)
            else:
                pred = self.predicate if pred2 is None else pred2
            kws = {'FilterExpression': compose(pred, boto3.dynamodb.conditions.Attr)}
        convert = convert_from_db if convert else lambda x: x

        batch = self.table.scan(**kws)
        yield from map(convert, batch['Items'])

        next_ = lambda: self.table.scan(**kws, ExclusiveStartKey=batch['LastEvaluatedKey'])
        while 'LastEvaluatedKey' in batch:
            batch = next_()
            yield from map(convert, batch['Items'])

    def __iter__(self):
        return self._iter()

    def scan(self, predicate, value=None, *, convert=True):
        return list(self._iter(predicate, value, convert=convert))

    def delete(self, predicate, value=None):
        for i in self.scan(predicate, value):
            k = self._key(i)
            print(', '.join(map(str, k.values())))
            self.__delitem__(k)

    def __getitem__(self, key):
        k = self._key(key)
        r = self.table.get_item(Key=k)
        try:
            return convert_from_db(r['Item'])
        except KeyError:
            raise KeyError(str(k))

    def get(self, key, default=None):
        return try_except(lambda: self[key], KeyError, default=default)

    def __setitem__(self, key, value, replace=False):
        with decimal.localcontext() as ctx:
            ctx.prec = 28
            item = convert_to_db(value)
        item.update(self._key(key))
        if replace:
            ret = self.table.put_item(Item=item, ReturnValues='ALL_OLD')
        else:
            try:
                ret = self.table.put_item(Item=item, ReturnValues='ALL_OLD',
                    ConditionExpression='attribute_not_exists (#P)',
                    ExpressionAttributeNames={'#P': self.fields[0][0]})
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e
                raise KeyError('Key {} already exists'.format(key))

        return ret.get('Attributes', {})

    def __delitem__(self, key):
        try:
            self.table.delete_item(Key=self._key(key))
        except ClientError as e:
            print('203', e.response['Error']['Code'])
            raise KeyError(str(key))

    def set(self, key, attr, new):
        key = self._key(key)
        values = {':new': convert_to_db(new)}
        if isinstance(attr, str):
            update = 'SET #P = :new'
            names = {'#P': attr}
        else:
            names = collections.OrderedDict(('#' + chr(ord('A')+i), v) for (i, v) in enumerate(attr))
            update = 'SET ' + '.'.join(names.keys()) + ' = :new'
        try:
            self.table.update_item(Key=key, UpdateExpression=update,
                ExpressionAttributeNames=names, ExpressionAttributeValues=values)
        except ClientError as e:
            print('219', e.response['Error']['Code'])
            raise KeyError(str(key))

    def set_if(self, key, attr, old, new):
        try:
            self.table.update_item(
                Key=self._key(key),
                UpdateExpression='SET #P = :new',
                ConditionExpression='#P = :old',
                ExpressionAttributeNames={'#P': attr},
                ExpressionAttributeValues={':new': convert_to_db(new), ':old': convert_to_db(old)}
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                print('233', e.response['Error']['Code'])
                raise e
            return False

    def add(self, key, attr, value):
        ret = self.table.update_item(
            Key=self._key(key),
            UpdateExpression='ADD #P :value',
            ExpressionAttributeNames={'#P': attr},
            ExpressionAttributeValues={':value': value},
            ReturnValues='UPDATED_OLD'
        )
        try:
            return convert_from_db(ret['Attributes'].popitem()[1])
        except KeyError:
            return None

    def remove(self, key, attr, value):
        ret = self.table.update_item(
            Key=self._key(key),
            UpdateExpression='DELETE #P :value',
            ExpressionAttributeNames={'#P': attr},
            ExpressionAttributeValues={':value': value},
            ReturnValues='UPDATED_OLD'
        )
        return convert_from_db(ret['Attributes'].popitem()[1])

    def keys(self):
        if len(self.field_names) == 1:
            return tuple(x[self.field_names[0]] for x in self.__iter__())
        return tuple(tuple(x[f] for f in self.field_names) for x in self.__iter__())

    def items(self):
        if len(self.field_names) == 1:
            return ((x[self.field_names[0]], x) for x in self.__iter__())
        else:
            return ((tuple(x[f] for f in self.field_names), x) for x in self.__iter__())

    def __len__(self):
        return self.table.item_count if self.predicate is None else ilen(self)

    def _kstr(self, k):
        return (', '.join(map(str, k)) if len(self.fields) > 1 else str(k)) + ':'

    def _vstr(self, v):
        return ''

    def __str__(self):
        return '\n\n'.join(self._kstr(k) + self._vstr(v) for (k, v) in self.items())

    def _delete_key(self, key):
        return self.table.delete_item(Key=key)

    def clobber(self):
        """Delete all the items"""
        if input('Clobber all the items? (yes|no)\n') != 'yes':
            return 0
        return ilen(self.__delitem__(k) for k in self.keys())

    def __repr__(self):
        return "{}.{}('{}')".format(type(self).__module__, type(self).__name__, self.name)

    def counts(self, visitor='status'):
        """Count the occurrences of a result for a given visitor"""
        f = visitor if callable(visitor) else lambda i: i.get(visitor)
        ret = {}
        for i in map(f, self):
            ret[i] = ret.get(i, 0) + 1
        return ret

    def report(self, visitor='status', out=None):
        """Print information to a file object"""
        out = sys.stdout if out is None else out
        for k, v in sorted(self.counts(visitor).items(), key=lambda kv: kv[1], reverse=True):
            out.write('\n    {}:'.format(k).ljust(20))
            out.write(str(v))
        out.write('\n')
        out.flush()

    def where(self, predicate, value=None):
        """Return new database with predicate as a filter"""
        pred = predicate if value is None else lambda i: i[predicate] == value
        ret = copy.copy(self)
        ret.predicate = pred if ret.predicate is None else lambda i: self.predicate(i) & pred(i)
        return ret

    def containing(self, value):
        return self.where(lambda i: i[self.fields[0][0]].contains(value))
