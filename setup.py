#!/usr/bin/env python

import os
import sys
import glob
from setuptools import setup


def get_zookeeper_paths():
    zk_path = None

    if sys.platform == 'darwin':
        homebrew_path = glob.glob('/usr/local/Cellar/zookeeper/*/libexec/')
        if homebrew_path:
            zk_path = homebrew_path[0]
    elif sys.platform == 'linux2':
        linux_default = '/usr/share/java/'
        if os.path.exists(linux_default):
            zk_path = linux_default

    if zk_path:
        return zk_path

    raise Exception('ZOOKEEPER_PATH must be in environment for tests to run')

if 'ZOOKEEPER_PATH' not in os.environ:
    os.environ['ZOOKEEPER_PATH'] = get_zookeeper_paths()

try:
    import multiprocessing  # Seems to fix http://bugs.python.org/issue15881
except ImportError:
    pass


setup(
    name='durabledict',
    version='0.9.0',
    author='DISQUS',
    author_email='opensource@disqus.com',
    url='http://github.com/disqus/durabledict/',
    description='Dictionary-style access to different types of models.',
    packages=['durabledict'],
    zip_safe=False,
    tests_require=[
        'Django',
        'nose',
        'mock',
        'redis',
        'kazoo',
    ],
    test_suite='nose.collector',
    include_package_data=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
