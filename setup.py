from setuptools import find_packages, setup

setup(
    name='crudLib',
    packages=find_packages(include=['crudLib']),
    version='0.1.0',
    description='My first Python library',
    author='Nivetha Rajkumar',
    install_requires=['uuid', 'pandas', 'pymongo']
)
