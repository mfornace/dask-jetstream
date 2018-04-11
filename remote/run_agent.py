#!/home/mfornace/local/anaconda3/bin/python3
import os, sys, uuid, shutil, tarfile, argparse, tempfile, json

HOME = os.path.expanduser('~')
PATH = os.path.join(HOME, 'jobs', str(uuid.uuid4()))

###############################################################################

parser = argparse.ArgumentParser(description='submit a remote job')
parser.add_argument('mode',    type=str, help='pbs or local')
parser.add_argument('tarball', type=str, help='a tarball containing the remote source')
args = parser.parse_args()

###############################################################################

# Put the jobs in place
os.makedirs(PATH)
with tarfile.open(args.tarball, 'r:gz') as tf:
    tf.extractall(PATH)
    shutil.rmtree(os.path.join(HOME, '.aws'), ignore_errors=True)
    shutil.move(os.path.join(PATH, 'aws'), os.path.join(HOME, '.aws'))
sys.path.insert(0, PATH)

def read_json(s):
    if not os.path.isfile(s):
        return {}
    with open(s, 'r') as ifs:
        return json.load(ifs)

###############################################################################

settings = read_json(os.path.join(HOME, 'remote.json'))
settings.update(read_json(os.path.join(PATH, 'remote.json')))
settings.update(read_json('/usr/local/src/remote.json'))

###############################################################################

with open(os.path.join(PATH, 'remote.json'), 'w') as f:
    json.dump(settings, f, indent=4)

def daemon():
    from remote import agent, job_db, agent_db
    with agent.Agent(settings, agent_db.Agent_Database(settings['agents'])) as a:
        a.listen(job_db.Job_Database(settings['jobs']))

if args.mode == 'pbs':
    from remote.pbs import qsub
    qsub(PATH, **settings)
elif args.mode == 'local':
    from fn.env import launch_daemon
    launch_daemon(daemon)
else:
    raise ValueError(args.mode)

###############################################################################
