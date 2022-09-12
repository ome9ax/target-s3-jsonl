#!/usr/bin/env python

from setuptools import setup

setup(
    install_requires=[
        'adjust-precision-for-schema',
        'jsonschema==4.16.0',
        'boto3==1.24.62',
        'backoff==2.1.2'
    ]
)
