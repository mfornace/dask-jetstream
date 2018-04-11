from . import ssh, entries, platform_db, simulations
from pna.utility import context


class _Remote_PBS:
    def __init__(self, entry, key, item, task, slots):
        self.starting_entry = entry
        self.key = key
        self.item = item
        self.task = task
        self.slots = slots

    def __call__(self, **settings):
        self.item.update(settings)
        with platform_db.Platform(self.item, slots=self.slots) as platform_info:
            self.item['platform'] = platform_info
            db = entries.Database_Entry(self.key, entry=self.starting_entry, verbose=True)
            with db as entry:
                entry << 'Starting database entry ' + self.key[0]
                for (k, v) in self.item.items(): entry[k] = v
                entry['status'] = 'running'
                dt = context.time_it(lambda: self.task(entry))
                entry['finish-time'] = context.utc()
                entry << 'Finished database entry after {} seconds'.format(dt)


def remote_pbs(name, host, task, heartbeat=240, entry=None, *, user, slots, timeout, nppn, memory, queue=None):
    """
    task = (db) -> None
    kwargs are for submit_pbs
    """
    key = name, context.unix_time()
    item = dict(status='queued', scheduler='pbs', host=str(host), task=str(task), heartbeat=heartbeat,
        user=user, timeout=timeout, nppn=nppn, memory=memory, queue=queue)
    simulations[key] = item
    run = _Remote_PBS(entry, key, item, task, slots)
    obj = run, dict(timeout=timeout, nppn=nppn, memory=memory, queue=queue)
    ssh.submit_ssh([host], [obj], exe='source .bash_profile; source .bashrc; handle_pna', user=user)
    return key
