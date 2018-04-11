from . import agent_db, config, process
from fn import env, flush, try_except, colored

import time, sys, copy, collections, decimal

###############################################################################

def slicable(t):
    return tuple(t.items()) if hasattr(t, 'items') else t

Work_Item = collections.namedtuple('Work_Item', ['database', 'key', 'process'])

###############################################################################

def _run_task(agent, task, platform, job, jobs, delay, timeout):
    """Greenlet to run a task and return resources to the agent"""
    with env.temporary_path() as tmp:
        with (tmp/'stdout.o').open('w') as out:
            out.write(jobs.output(job))
            flush('agent =', agent.key, file=out)
            flush('start time =', env.time_string(env.utc()), file=out)
            task.start(platform, stdout=out, stderr=out)
            start = time.time()
            task.wait(None if timeout is None else timeout - start - delay)
            task.release(delay)
            agent.merge_platform(platform)
            stat = task.status()
            flush('end-time =', env.time_string(env.utc()), file=out)
            output = '{}-{}.o'.format(task.item['name'], task.item['time'])
            jobs.put_file(task.item, output, tmp/'stdout.o', links=[], replace=True)
            jobs.add(task.item, 'walltime', decimal.getcontext().create_decimal_from_float(time.time() - start))
            agent.write('result', (stat, task.worker.exitcode, output, jobs.tuple_key(job)))
            jobs.release_with_status(job, stat)
    agent.refresh()

###############################################################################

class Agent:
    def __init__(self, platform, agents):
        """
        Initialize self, database entry, platform, heartbeat
        Platform properies:
            'timeout'
            'idle-timeout'
            'node'
            'stop'
            'host'
            'heartbeat'
            'output'
        """
        self.agents = agents
        self.platform = config.default_platform_info(platform)
        self.timeout = self.platform.get('timeout')
        self.idle_timeout = self.platform.get('idle-timeout')
        if self.timeout is not None:
            self.platform['stop'] = time.time(), self.timeout
        self.key = [self.platform['node']]
        if self.platform.get('host', self.platform['node']) != self.platform['node']:
            self.key.insert(0, self.platform['host'])
        self.key = '-'.join(self.key)
        try_except(lambda: self.agents.release(self.key), KeyError)
        self.heartbeat = process.fork(agent_db.heartbeat, daemon=False)(self.agents, self.key, self.timeout, self.platform['heartbeat'])
        self.work = []
        self.refusal = 0
        self._out_name, self.out = self.platform.get('output'), None

    def write(self, key, value):
        """Write a key value pair to the output stream"""
        msg = '{}: {}'.format(key, value)
        flush(colored(msg, 'green') if self.out == sys.__stdout__ else msg, file=self.out)

    def refresh(self):
        """Refresh database entry and kill self if requested"""
        item = dict(status='running')
        self.work = list(filter(lambda w: process.is_alive(w.process), self.work))
        item['work'] = [(w.database.name, *w.key) for w in self.work]
        item.update(self.platform)
        if self.agents.__setitem__(self.key, item).get('kill'):
            raise KeyboardInterrupt
        self.agents.ping(self.key, self.platform['heartbeat'])

    def __enter__(self):
        """Start the heartbeat and open the output stream"""
        self.refresh()
        self.heartbeat.start()
        self.out = sys.stdout if self._out_name is None else open(self._out_name, 'w')
        return self

    def ordered_jobs(self, db):
        """List of queued jobs in order that they should be run"""
        queued = db.scan(lambda j: j['status'] == 'queued')
        queued = [j for j in queued if j['name'] != 'FILES']
        return sorted(queued, key=lambda j: j['time'])

    def accept_job(self, job, launch, db):
        """Try to acquire and launch a job, return if successful"""
        save = copy.deepcopy(self.platform)
        task = db.get_task(job)
        try:
            platform = self.split_platform(task.define_resources(self.platform))
            self.write('platform', str(platform))
            self.write('platform2', str(self.platform))
            assert task.acquire(self.key)
            launch(job, task, platform)
            return True
        except Exception as e:
            self.write('refusal-{}'.format(self.refusal), e)
            self.refusal += 1
            self.platform = save
            return False

    def listen(self, db, period=60, delay=300.0, idle_timeout=None):
        """
        db     = job database to listen to
        period = sleep period if there are no db
        """
        self.write('timeout', self.timeout)
        self.write('idle-timeout', self.idle_timeout)
        end = None if self.timeout is None else time.time() + self.timeout - delay

        def launch(job, task, platform):
            proc = process.spawn(_run_task, self, task, platform, job, db, delay, end)
            self.write('job', db.tuple_key(job))
            self.work.append(Work_Item(db, db.tuple_key(job), proc))

        if idle_timeout is None:
            idle_timeout = self.idle_timeout

        last_work = time.time()
        while end is None or time.time() < end - delay:
            if self.work:
                last_work = time.time()
            elif idle_timeout is not None and time.time() - last_work > idle_timeout:
                self.write('exit-reason', 'idle')
                return
            self.refresh()
            if not any(self.accept_job(j, launch, db) or (end is not None and time.time() + period > end) for j in self.ordered_jobs(db)):
                time.sleep(period)
        self.write('exit-reason', 'timeout')


    def __exit__(self, cls, value, traceback):
        """Finalize database, try to join workers, close stream"""
        self.heartbeat.terminate()
        self.write('error-type', None if cls is None else cls.__name__)
        self.write('error', value)
        try:
            self.write('exit-time', env.unix_time())
            process.join_all([w.process for w in self.work])
        finally:
            for db, key, _ in self.work:
                db.reset_if_running(key)
            self.agents.release(self.key)
            if self._out_name is not None:
                self.out.close()

    def split_platform(self, requested):
        """Add resources to a job and subtract them from self"""
        left, right = copy.deepcopy(self.platform['resources']), {}
        for k, v in requested.items():
            if k not in left:
                raise ValueError('Key {} missing'.format(k))
            elif not hasattr(left[k], '__iter__'):
                left[k] -= v
                right[k] = type(left[k])(v)
            elif isinstance(v, int): # take any N items
                if v > len(left[k]):
                    raise ValueError('Need {} but only had {} for {}'.format(v, len(left[k]), k))
                right[k] = type(left[k])(slicable(left[k])[:v])
                left[k] = type(left[k])(slicable(left[k])[v:])
            elif hasattr(left[k], 'items'): # take the specified keys
                right[k] = {i: left[k].pop(i) for i in v}
            else: # take the specified elements
                right[k] = [left[k].pop(i) for i in v]
        self.platform['resources'] = left
        ret = copy.deepcopy(self.platform)
        ret['resources'] = right
        return ret

    def merge_platform(self, platform):
        """Add resources from a job back into self"""
        left, right = self.platform['resources'], platform['resources']
        for k, v in right.items():
            if hasattr(left[k], 'update'):
                left[k].update(v)
            else:
                left[k] = v + left[k]

###############################################################################
