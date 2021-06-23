# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('./requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='wan-pyredis',
    version='0.0.1',
    description='python的redis的扩展应用',
    author='tianyuzhiyou',
    packages=find_packages(),
    install_requires=required,
)
