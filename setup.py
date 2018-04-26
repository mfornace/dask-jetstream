from setuptools import setup, find_packages

setup(
    name='cloud',
    version='0.1.0',
    description='Remote cluster codes for INSPIRE',
    packages=find_packages(exclude=['*tests*']),
)