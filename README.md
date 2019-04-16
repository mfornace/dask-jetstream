## Introduction
`dask-jetstream` is a Python 3 software package for running jobs in parallel on the NSF XSEDE JetStream cluster using the dask.distributed framework. Besides code comments, the documentation for this software is in current development.

Below are some notes for performing some common tasks and commands.

## To install AWS command line on Mac

```bash
brew install awscli
```

## Example Python egg installation
`setup.py bdist_egg`

```
conda update
pip3 install -U pip distributed
pip3 install -U 'asyncssh[bcrypt,gssapi,libnacl,pyOpenSSL]'
```

## Check IP interfaces to get worker local IP

```python
import psutil
psutil.net_if_addrs().keys()
# --> dict_keys(['lo', 'ens3', 'docker0'])
psutil.net_if_addrs()['ens3'][0].address # --> 10.0.0.4, ==WORKERETH
```

## Queuing with dask

```python
import queue
reaction_q = queue.Queue()
remote_q = client.scatter(reaction_q)
sim_dataset_q = client.map(run_reaction, remote_q)
result_q = client.gather(sim_dataset_q)
```

## openstack

```bash
export PATH=/usr/anaconda3/bin:$PATH
python3 -m distributed.cli.dask_scheduler --show
python3 -m distributed.cli.dask_worker ${SCHEDULERIP}:8786 --nthreads 0 --nprocs 1 --listen-address tcp://${WORKERETH}:8001 --contact-address tcp://${WORKERIP}:8001
```

```bash
apt update
apt install nmap
nmap -p 22 ${INSTANCEIP} # check for SSH access
```

## Open ports for TCP if not open already

```bash
openstack security group rule create mfornace-global-ssh --protocol tcp --dst-port 8000:9000 --remote-ip 0.0.0.0/0
```

## Image creation from Ubuntu 16
Reboot instance first. We should redo the pip installs to be conda sometime.
```bash
sudo apt update
sudo dpkg --configure -a
sudo apt upgrade -f # takes a while
sudo apt dist-upgrade

wget https://repo.continuum.io/archive/Anaconda3-5.1.0-Linux-x86_64.sh
bash *.sh # use /usr/anaconda as the prefix

# conda install -c omnia autograd doesnt work, too old version, had to install from setup.py
sudo /usr/anaconda/bin/conda update --prefix /usr/anaconda anaconda && \
sudo /usr/anaconda/bin/conda update -n base conda && \
sudo /usr/anaconda/bin/conda install numpy scipy pandas matplotlib && \
sudo /usr/anaconda/bin/conda install -c omnia openmm && \
sudo /usr/anaconda/bin/conda install -c anaconda netcdf4 && \
sudo /usr/anaconda/bin/conda install pytorch torchvision -c pytorch && \
sudo /usr/anaconda/bin/conda upgrade --all && \
sudo /usr/anaconda/bin/pip install -U pip && \
sudo /usr/anaconda/bin/pip install -U asyncssh autograd awscli bokeh boto3 cloudpickle dask distributed Flask gitpython Gpy ipyparallel joblib keras more_itertools nbdime neutron paramiko parmed psutil python-cinderclient python-dateutil python-designateclient python-editor python-glanceclient python-heatclient python-json-logger python-keystoneclient python-mimeparse python-neutronclient python-novaclient python-openstackclient python-swiftclient s3fs scikit-learn seaborn tenacity tensorboard tensorflow-tensorboard tensorflow termcolor theano watchtower xarray yolk3k protobuf keystoneauth1
# sudo /usr/anaconda/bin/pip install -U asyncssh autograd awscli bokeh boto3 cloudpickle dask distributed Flask gitpython Gpy ipyparallel joblib keras more_itertools nbdime neutron paramiko parmed psutil python-cinderclient==3.5.0 python-dateutil==2.7.2 python-designateclient==2.9.0 python-editor==1.0.3 python-glanceclient==2.11.0 python-heatclient==1.15.0 python-json-logger python-keystoneclient==3.16.0 python-mimeparse==1.6.0 python-neutronclient==6.8.0 python-novaclient==9.1.1 python-openstackclient==3.15.0 python-swiftclient==3.5.0 s3fs scikit-learn seaborn tenacity tensorboard==1.7.0 tensorflow-tensorboard==1.5.1 tensorflow==1.7.0 termcolor theano watchtower xarray yolk3k
# pip install -U pycuda pyopencl only if has GPU

# make sure python2 has numpy
wget http://ambermd.org/downloads/install_ambertools.sh
bash install_ambertools.sh -v 3 --prefix /usr --non-conda

# download https://www.rosettacommons.org/download.php?token=a64t4Q78m&file=rosetta_bin_linux_3.9_bundle.tgz and extract on own computer
# scp it to the instance host as rosetta
mv rosetta /usr/rosetta
```

Then on the instance:
```python
instance.suspend()
# wait a minute ... then
image_id = instance.create_image('ubuntu-16-anaconda3-plus2', public=False, skip_atmosphere='yes')
print(cloud.Image(image_id))
```

Apparently user should be `ubuntu` after this setup so put this as userdata:
```bash
#!/usr/bin/env bash
source /usr/amber18/amber.sh
export PATH=/usr/anaconda/bin:$PATH
export ROSETTA=/usr/rosetta # into bashrc
export ROSETTA_BINEXT=.static.linuxgccrelease # into bashrc
```

Actually user data doesn't register I guess so pass this in as `env` to
the cluster if AMBER or something else is needed. Usually I don't rely on `$PATH`
so that should be fine to ignore.
```python
#    'PATH': '/usr/anaconda/bin:/usr/amber18/bin:$PATH'
env = {
    'AMBERHOME': '/usr/amber18',
    'PYTHONPATH': '/usr/amber18/lib/python3.6/site-packages/',
    'LD_LIBRARY_PATH': '/usr/amber18/lib'
}
```

## SSH troubleshooting
To remove an remote IP if SSH fails due to remote host changing, run
`ssh-keygen -R YOURIP`.

Otherwise you can connect without host checking via
`ssh -o UserKnownHostsFile=/dev/null YOURIP`.

## Partial Rosetta
I copied these files along with `rna_denovo` and `rna_extract` executables to the instance.
Otherwise you get all of Rosetta which is like 30 GB.

```bash
scp -r database/chemical
scp -r database/scoring/weights
scp -r database/scoring/rna
scp -r database/scoring/score_functions/hbonds
scp -r database/scoring/score_functions/carbon_hbond
scp -r database/sampling/rna
scp -r database/input_output
```

## Environment variables
Make sure this line is in `/etc/ssh/sshd_config`
```bash
AcceptEnv *
```

## Image scheduler
```python
i = cluster.instances[0].result() # get an Instance object
i.suspend()
# wait 5 minutes or so; status should say suspended eventually
image_id = i.create_image('ubuntu-16-conda3-dask', public=False, skip_atmosphere='yes')
# wait 5 minutes or so
i.resume()
i.close()
```

# openstack volume create --size 256 --type default --image 5a5235ed-628b-43cc-a6d5-3ff54439abf2 ubuntu-16-conda3-dask-scheduler

## Volume
```bash
openstack volume type list
openstack volume list
# 2559f0ae-7116-46b6-9ccd-5feecdadd3d5 | default | True      |
openstack limits show
openstack volume create --size 256 --type default --image 27a174a7-046f-41a6-8952-2b86a28ff599 ubuntu-16-conda3-dask-scheduler
```
After attaching the volume should live in /dev/disk/by-id/{something}
My device ID was scsi-0QEMU_QEMU_HARDDISK_b2414c9c-ff4a-4a7c-a.
```bash
ls /dev/disk/by-id/${DEVICE_ID} # 1 time
sudo mkfs.ext4 /dev/disk/by-id/${DEVICE_ID} # 1 time
sudo mkdir -p /mnt/volume
sudo mount /dev/disk/by-id/${DEVICE_ID} /mnt/volume
sudo umount /mnt/volume
```
Then detach the volume.
'ls /dev/disk/by-id/*%s*' % volume_id[:20]

## Seeing limits/resource restrictions
```bash
openstack limits show --absolute
```

### Maximum ports

This appears to only be modifiable by the administrator, not the user.
```bash
neutron quota-show -c port
neutron quota-update --port 100 # doesnt work unless administrator
```

## Caching in notebook
```bash
pip install git+https://github.com/rossant/ipycache.git
```

## Setting up cluster

```python
print(cloud.Flavor.list())
cluster = cloud.JetStreamCluster(None, 'm1.large', 'dask-agent', volume=None, python='/usr/anaconda/bin/python', # volume='dask-scheduler'
                                 preload=preload, ssh=dict(username='ubuntu', env=env, known_hosts=None))

for i in range(4):
    cluster.add_worker('m1.large')

import time
for i in range(80):
    print(i)
    #print(*map(str, cluster.workers), sep='\n')
    while not cluster.workers[-1].pid.done():
        time.sleep(1)
    cluster.add_worker('m1.xlarge')

client = cluster.client()
```

## Uploading modules

```python
!bash eggs.sh
client.upload_file('eggs/fn-0.1.0-py3.6.egg')
client.upload_file('eggs/cloud-0.1.0-py3.6.egg')
client.upload_file('eggs/md-0.1.0-py3.6.egg')
```

## OpenStack creation/deletion

```python
fn.stream_logs()
cloud.ssh.redirect_ssh_logs('ssh.log')
fs = fn.s3.FileSystem()
cloud.set_config(yaml.load(open('config.yml')))

print(cloud.close_openstack(graceful=True))
```

## Instance files
- /etc/security/limits.conf - to set the open file limit
- /var/log/cloud-init-output.log - user script output
- /var/log/cloud-init.log - another user script output
- /var/lib/cloud/instance/scripts/part-001 - where the script is

Useful commmands
```bash
ps aux | grep python # get the cloud-init PIDs
sudo tail -f /var/log/cloud-init-output.log # look at logs
sudo cat /root/dask.yml # cat the config
```
