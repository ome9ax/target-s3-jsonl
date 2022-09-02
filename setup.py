#!/usr/bin/env python

from setuptools import setup

setup(
    install_requires=[
        'target-core==0.0.6',
        'jsonschema==4.14.0',
        'boto3==1.24.57',
        'backoff==2.1.2'
    ]
)
