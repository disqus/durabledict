#!/usr/bin/env python

import sys

from setuptools import setup, find_packages

try:
    import multiprocessing  # Seems to fix http://bugs.python.org/issue15881
except ImportError:
    pass

setup_requires = []

if 'nosetests' in sys.argv[1:]:
    setup_requires.append('nose')

setup(
    name='modeldict',
    version='0.3.1',
    author='DISQUS',
    author_email='opensource@disqus.com',
    url='http://github.com/disqus/modeldict/',
    description = 'Dictionary-style access to different types of models.',
    packages=find_packages(),
    zip_safe=False,
    tests_require=[
        'Django',
        'nose',
        'mock',
        'redis',
        'kazoo'
    ],
    test_suite = 'nose.collector',
    include_package_data=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)