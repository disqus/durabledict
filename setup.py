#!/usr/bin/env python
import os
import sys

from setuptools import setup, find_packages

ZOOKEEPER_PATHS = {
    'darwin': '/usr/local/Cellar/zookeeper/3.4.5/libexec/',  # assume homebrew
    'linux2': '/usr/share/java/',
}

if sys.platform in ZOOKEEPER_PATHS:
    os.environ.setdefault('ZOOKEEPER_PATH', ZOOKEEPER_PATHS[sys.platform])

try:
    import multiprocessing  # Seems to fix http://bugs.python.org/issue15881
except ImportError:
    pass

setup_requires = []

if 'nosetests' in sys.argv[1:]:
    setup_requires.append('nose')

setup(
    name='durabledict',
    version='0.7.2',
    author='DISQUS',
    author_email='opensource@disqus.com',
    url='http://github.com/disqus/durabledict/',
    description='Dictionary-style access to different types of models.',
    packages=find_packages(),
    zip_safe=False,
    setup_requires=[
        'nose'
    ],
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
