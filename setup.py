#!/usr/bin/env python

from setuptools import setup

setup(
    install_requires=[
        # 'target-core==0.0.5'
        'target-core @ git+ssh://git@gitlab.com/singer-core/target-core@13-rest-api-support#egg=target-core'
    ]
)
