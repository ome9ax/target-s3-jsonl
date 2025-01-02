#!/usr/bin/env python

from setuptools import setup

setup(
    install_requires=[
        # 'target-core==0.1.0',
        'target-core @ git+https://git@gitlab.com/adamroderick/target-core.git',
        'boto3==1.26.82'
    ]
)
