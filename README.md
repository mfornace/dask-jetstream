`setup.py bdist_egg`

now egg is in dist

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
apt update
dpkg --configure -a
apt upgrade -f # takes a while
apt dist-upgrade

wget https://repo.continuum.io/archive/Anaconda3-5.1.0-Linux-x86_64.sh
bash *.sh # use /usr/anaconda as the prefix

conda update --prefix /usr/anaconda anaconda
conda update -n base conda
conda install numpy scipy pandas matplotlib
conda install -c omnia openmm
# conda install -c omnia autograd doesnt work, too old version, had to install from setup.py
conda install -c anaconda netcdf4
conda upgrade --all

pip install -U pip
# pip install -U pycuda pyopencl only if has GPU
pip install -U asyncssh autograd awscli bokeh boto3 cloudpickle dask distributed Flask gitpython Gpy ipyparallel joblib keras more_itertools nbdime neutron paramiko parmed psutil python-cinderclient==3.5.0 python-dateutil==2.7.2 python-designateclient==2.9.0 python-editor==1.0.3 python-glanceclient==2.11.0 python-heatclient==1.15.0 python-json-logger python-keystoneclient==3.16.0 python-mimeparse==1.6.0 python-neutronclient==6.8.0 python-novaclient==9.1.1 python-openstackclient==3.15.0 python-swiftclient==3.5.0 s3fs scikit-learn seaborn tenacity tensorboard==1.7.0 tensorflow-tensorboard==1.5.1 tensorflow==1.7.0 termcolor theano watchtower xarray yolk3k 


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
i = cluster.workers[0].instance.result()
i.suspend()
# wait 5 minutes or so; status should say suspended eventually
image_id = i.create_image('ubuntu-16-conda3-dask', public=False, skip_atmosphere='yes')
# wait 5 minutes or so
i.resume()
i.close()
```