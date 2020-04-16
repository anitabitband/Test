#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
from setuptools import setup, find_packages

NAME = 'yoink'
HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, NAME, '_version.py')) as f:
    VERSION = f.readlines()[-1].split()[-1].strip("\"'")
with open(os.path.join(HERE, 'README.md')) as f:
    README = f.read()

INSTALL_REQUIRES = [
    'requests==2.23.0',
    'Sphinx==3.0.1',
    'sphinx-rtd-theme==0.4.3',
    'pycapo==0.2.1.post1'
]

TEST_REQUIRES = [
]

setup(
    name=NAME,
    version=VERSION,
    description='grab files from NRAO Archive',
    long_description=README,
    author='Stephan Witz',
    author_email='switz@nrao.edu',
    url='TBD',
    license="GPL",
    install_requires=INSTALL_REQUIRES,
    test_requires=TEST_REQUIRES,
    test_suite='yoink.tests',
    keywords=['TBD'],
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2'
    ],
    entry_points={
        'console_scripts': ['yoink = yoink.commands:main']
    },
)
