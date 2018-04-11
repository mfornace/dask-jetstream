from . import aws, job_db
from fn import env, try_except

import boto3
import time

###############################################################################

class Agent_Database(aws.Database):
    fields = [('name', str)]

    def release(self, key):
        """Register that an agent is unavailable"""
        item = self.__getitem__(key)
        ret = self.set_if(item, 'status', 'running', 'unavailable')
        for job in item['work']:
            try:
                db = job_db.Job_Database(job[0])
                db.reset_if_running(job[1:])
            except Exception:
                pass
        self.set(item, 'work', [])
        return ret

    def __setitem__(self, key, item, *args, replace=True, **kwargs):
        """Load an agent"""
        item.setdefault('now', env.unix_time())
        return aws.Database.__setitem__(self, key, item, *args, replace=replace, **kwargs)

    def _vstr(self, v):
        """Print agents"""
        kvp = lambda s: '\n    {}: {}'.format(s, v.get(s))
        return ''.join(map('\n    {}'.format, v['work'])) + ''.join(map(kvp, ['status', 'user', 'cpus', 'gpus']))

    def kill(self, key):
        """Manually request that an agent stop listening"""
        self.set(key, 'kill', True)

    def ping(self, key, heartbeat):
        """Ping an agent to register that it is still alive"""
        return self.table.update_item(
            Key=self._key(key),
            UpdateExpression='SET #T = :time, #H = :heartbeat',
            ConditionExpression='#P = :run',
            ExpressionAttributeNames={'#P': 'status', '#T': 'now', '#H': 'heartbeat'},
            ExpressionAttributeValues={':run': 'running', ':time': env.unix_time(), ':heartbeat': aws.convert_to_db(heartbeat)},
            ReturnValues='UPDATED_OLD'
        )['Attributes']

    def cleanup(self, heartbeat=None):
        """Clean up agents that haven't responded in a while"""
        ret = [0, []]
        for item in self.scan(lambda a: a['status'] == 'running'):
            if 'now' not in item:
                print(item['name'], 'has no heartbeat')
            elif (env.unix_time() - item['now']) > 2 * item.get('heartbeat', heartbeat):
                ret[1].append(item['name'])
                ret[0] += bool(self.release(item))
        return list(ret)

    def zombies(self, jobs, heartbeat=None):
        runs = {jobs.tuple_key(j): j for j in jobs.scan('status', 'running')}
        for a in self.scan('status', 'running'):
            if 'now' in a and (env.unix_time() - a['now']) < 2 * a.get('heartbeat', heartbeat):
                for w in a.get('work', []):
                    try_except(lambda: runs.pop(tuple(w[1:])), KeyError)
        return runs

###############################################################################

for k in ['running', 'unavailable']:
    def running(self, status_name=k):
        return self.where('status', status_name)
    setattr(Agent_Database, k, property(running))

###############################################################################

def heartbeat(database, key, timeout, period):
    end = None if timeout is None else time.time() + timeout
    while True:
        print('heartbeat:', env.unix_time())
        try:
            database.ping(key, period)
        except boto3.exceptions.Boto3Error:
            return
        if end is not None and end < time.time() + period:
            break
        time.sleep(period)

###############################################################################
