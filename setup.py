#!/usr/bin/env python

import os.path

from setuptools import setup  # type: ignore


# get the version to include in setup()
dir_ = os.path.abspath(os.path.dirname(__file__))
with open(f'{dir_}/activemq_manager/__init__.py') as fh:
    for line in fh:
        if '__VERSION__' in line:
            exec(line)


setup(
    name='py-activemq-manager',
    version=__VERSION__,  # type: ignore
    license='MIT',
    author='Kyle Smith',
    author_email='smithk86@gmail.com',
    description='gather information about activemq via the jolokia api',
    packages=['activemq_manager'],
    setup_requires=[
        'pytest-runner'
    ],
    install_requires=[
        'asyncio-pool',
        'dateparser',
        'httpx'
    ],
    tests_require=[
        'docker',
        'mypy',
        'pytest',
        'pytest-asyncio',
        'pytest-mypy',
        'stomp.py',
        'types-dateparser'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Framework :: AsyncIO',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
