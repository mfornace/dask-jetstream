#from gevent import monkey
#monkey.patch_all()

import os, sys, uuid, tarfile, argparse, tempfile

add_path = os.path.dirname(os.path.dirname(__file__))
if add_path: sys.path.append(add_path)

###############################################################################

parser = argparse.ArgumentParser(description='Launch an agent (remotely)')
parser.add_argument('agents',      type=str,   help='name of agents database')
parser.add_argument('jobs',        type=str,   help='name of jobs database')
parser.add_argument('--tarball',   type=str,   help='path to package tarball')
parser.add_argument('--host',      type=str,   help='name of host')
parser.add_argument('--timeout',   type=float, help='number of seconds allowed')
parser.add_argument('--heartbeat', type=float, help='number of seconds between heartbeats', default=60)
parser.add_argument('--gpus',      type=int,   help='gpu device number')
parser.add_argument('--cpus',      type=int,   help='cpu device number')
parser.add_argument('--memory',    type=float, help='gigabytes of memory to use')
args = parser.parse_args()

###############################################################################

with tempfile.TemporaryDirectory() as tmp:
    if args.tarball is not None:
        with tarfile.open(args.tarball, 'r:gz') as tf:
            tf.extractall(tmp)
        sys.path.insert(0, tmp)

    from remote import agent, job_db, agent_db

    platform = {}
    for key in ('gpus', 'cpus', 'memory', 'timeout', 'host', 'heartbeat'):
        if getattr(args, key, None) is not None:
            platform[key] = getattr(args, key)

    jobs = job_db.Job_Database(args.jobs)
    agents = agent_db.Agent_Database(args.agents)

    try:
        with agent.Agent(platform, agents) as a:
            a.listen(jobs)
    except KeyboardInterrupt:
        pass

###############################################################################
