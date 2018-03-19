#!/usr/bin/env python

from setuptools import setup

setup(
    name='activemq-console-parser',
    version='0.5.1',
    license='MIT',
    author='Kyle Smith',
    author_email='smithk86@gmail.com',
    description='gather information about activemq via the web console',
    packages=['activemq_console_parser'],
    install_requires=[
        'requests',
        'beautifulsoup4',
        'lxml==4.1.1'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
