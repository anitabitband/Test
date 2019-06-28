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
    'requests>=2.6.0',
    'Sphinx>=1.3',
    'sphinx_rtd_theme>=0.1.7',
    'pycapo==0.2.0',
    'mysqlclient==1.3.10',
    'sqlacodegen==1.1.6',
    'SQLAlchemy==1.2.1',
    'zope.sqlalchemy==0.7.7',
    'psycopg2==2.7.3'
]

TEST_REQUIRES = [
    'tox==3.1.3',
    'tox-pyenv==1.1.0',
    'pyinstaller==3.2',
    'pytest==3.7.0',
    'pytest-runner==4.2'
]

NAME = 'yoink'
HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, NAME, '_version.py')) as f:
    VERSION = f.readlines()[-1].split()[-1].strip("\"'")
setup(
    name=NAME,
    version=VERSION,
    description='grab files from NGAS',
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
