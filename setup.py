#!/usr/bin/env python

from setuptools import setup

setup(
    name='activemq-console-parser',
    version='1.0.2',
    license='MIT',
    author='Kyle Smith',
    author_email='smithk86@gmail.com',
    description='gather information about activemq via the web console',
    packages=['activemq_console_parser'],
    install_requires=[
        'requests',
        'beautifulsoup4'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
