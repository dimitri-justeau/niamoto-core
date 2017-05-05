# coding: utf-8

import os
from setuptools import find_packages, setup


this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, 'README.md'), encoding='utf-8') as file:
    long_description = file.read()

setup(
    name='Niamoto',
    version='0.1',
    description='Niamoto core application',
    author='Dimitri Justeau',
    author_email='dimitri.justeau@gmail.com',
    url='https://github.com/dimitri-justeau/niamoto-core/',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'niamoto=niamoto.bin.cli:niamoto_cli',
        ],
    },
 )
