#!/usr/bin/env python

from setuptools import setup

setup(
    name='activemq-console-parser',
    version='2.0.0-dev',
    license='MIT',
    author='Kyle Smith',
    author_email='smithk86@gmail.com',
    description='gather information about activemq via the web console',
    packages=['activemq_console_parser'],
    install_requires=[
        'beautifulsoup4',
        'lxml',
        'aiohttp'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
