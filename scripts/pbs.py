'''PBS submission from a process on the cluster'''
import fn, sys, os, pathlib, datetime, tempfile, subprocess, shutil

###############################################################################

if shutil.which('sbatch') is None:
    _CMD, _EXT = 'qsub', '{}.pbs'
    _SCRIPT = """#!/bin/bash
#PBS -S /bin/bash
#PBS -V
{timeout.ln}{nppn.ln}{memory.ln}{queue.ln}{path.ln}{output.ln}{error.ln}{name.ln}
source ~/.bash_profile
{python} remote.py {submit_time}

"""
    _COMMANDS = {
        'name':     '#PBS -N {}',
        'output':   '#PBS -o {}',
        'queue':    '#PBS -q {}',
        'timeout':  '#PBS -l walltime={}',
        'nppn':     '#PBS -l nodes={}:ppn={}',
        'memory':   '#PBS -l mem={}',
        'path':     '#PBS -d {}',
        'error':    '#PBS -e {}',
    }
else:
    _CMD, _EXT = 'sbatch', '{}.sh'
    _SCRIPT = """#!/bin/bash
#SBATCH --export=ALL
{name.ln}{output.ln}{queue.ln}{nodes.ln}{cpus.ln}{timeout.ln}{gpus.ln}{account.ln}{memory.ln}
source ~/.bash_profile
{python} remote.py {submit_time}

"""
    _COMMANDS = {
        'name':      '#SBATCH --job-name="{}"',
        'output':    '#SBATCH --output="{}"',
        'error':     '#SBATCH --error="{}"',
        'queue':     '#SBATCH --partition={}',
        'timeout':   '#SBATCH --time={}',
        'nodes':     '#SBATCH --nodes={}',
        'cpus':      '#SBATCH --ntasks-per-node={}',
        'gpus':      '#SBATCH --gres=gpu:{}',
        'gpu-flags': '#SBATCH --gres-flags={}',
        'path':      '#SBATCH --workdir={}',
        'account':   '#SBATCH --account={}',
        'memory':    '#PBS -l mem={}',
    }

###############################################################################

class Line:
    def __init__(self, value, ln):
        self.value = value
        if value is None:
            self.ln = ''
        elif isinstance(value, (tuple, list)):
            self.ln = ln.format(*value) + '\n'
        else:
            self.ln = ln.format(value) + '\n'

    def __str__(self):
        return str(self.value)

def submission_script(script_inputs):
    kwargs = {k: Line(script_inputs.get(k), v) for (k, v) in _COMMANDS.items()}
    return _SCRIPT.format(python=sys.executable, submit_time=fn.unix_time(), **kwargs)

###############################################################################

_RUN = """
from remote import agent, job_db, agent_db
import json, sys

with open('remote.json', 'r') as f:
    platform = json.load(f)

jobs = job_db.Job_Database(platform['jobs'])
agents = agent_db.Agent_Database(platform['agents'])

with agent.Agent(platform, agents) as a:
    a.write('submit-time', sys.argv[-1])
    a.listen(jobs, platform.get('period', 60.0), platform.get('delay', 300.0))
"""

###############################################################################

def qsub(path, *, nodes, cpus, timeout, gpus=None, queue=None, account=None, memory=None, **_):
    """
    path: directory where to run, should exist
    """
    path = fn.Path(path)
    (path/'remote.py').write_text(_RUN)

    job_name = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    script_inputs = {
        'name':     job_name,
        'output':   'out.o',
        'timeout':  fn.duration_string(timeout),
        'nodes':    nodes,
        'cpus':     cpus,
        'gpus':     gpus,
        'nppn':     (nodes, cpus),
        'queue':    queue,
        'account':  account,
        'path':     path,
        'memory':   memory,
        'error':    'err.e',
    }
    script = submission_script({k:v for k,v in script_inputs.items() if v is not None})
    (path/_EXT.format(job_name)).write_text(script)
    return subprocess.check_output([_CMD, _EXT.format(job_name)], cwd=str(path))

###############################################################################
