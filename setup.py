from setuptools import find_packages, setup

setup(
    name='crudLib',
    packages=find_packages(include=['crudLib']),
    version='2.0.0',
    description='Mongo datalake management Python library',
    author='Nivetha Rajkumar',
    install_requires=['uuid', 'pandas', 'pymongo']
)
