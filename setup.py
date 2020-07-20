#!/usr/bin/env python

import os.path

from setuptools import setup


# get the version to include in setup()
dir_ = os.path.abspath(os.path.dirname(__file__))
with open(f'{dir_}/activemq_manager/__init__.py') as fh:
    for line in fh:
        if '__VERSION__' in line:
            exec(line)


setup(
    name='py-activemq-manager',
    version=__VERSION__,
    license='MIT',
    author='Kyle Smith',
    author_email='smithk86@gmail.com',
    description='gather information about activemq via the jolokia api',
    packages=['activemq_manager'],
    install_requires=[
        'asyncio-concurrent-functions',
        'asyncio-pool',
        'httpx==0.12.1'
    ],
    tests_require=[
        'docker',
        'pytest',
        'pytest-asyncio',
        'stomp.py'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Framework :: AsyncIO',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
