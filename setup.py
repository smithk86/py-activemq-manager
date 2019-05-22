#!/usr/bin/env python

from setuptools import setup

setup(
    name='py-activemq-manager',
    version='2.0.0-dev',
    license='MIT',
    author='Kyle Smith',
    author_email='smithk86@gmail.com',
    description='gather information about activemq via the web console',
    packages=['activemq_manager'],
    install_requires=[
        'aiohttp',
        'asyncinit',
        'asyncio-pool'
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
