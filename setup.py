#!/usr/bin/env python

import os
import sys
import glob
from setuptools import setup


try:
    import multiprocessing  # Seems to fix http://bugs.python.org/issue15881
except ImportError:
    pass

setup(
    name='durabledict',
    version='0.9.4',
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
