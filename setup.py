#!/usr/bin/env python

from setuptools import setup

with open('README.md', 'r', encoding='utf-8') as f:
    readme = f.read()

setup(
    name='target-s3-jsonl',
    version='0.0.0',
    description='Singer.io target for writing JSON Line files and upload to S3',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Eddy ∆',
    author_email='edrdelta@gmail.com',
    url='https://github.com/ome9ax/target-s3-jsonl',
    keywords=['singer', 'singer.io', 'tap', 'target', 'etl', 'json', 'jsonl', 'aws', 's3'],
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only',
    ],
    py_modules=['target_s3_jsonl'],
    install_requires=[
        'jsonschema==2.6.0',
        'singer-python==5.6.1', # NOTE: >5.7.0 broken
        'boto3==1.18.15',
    ],
    packages=['target_s3_jsonl'],
    package_data = {},
    include_package_data=True,
    entry_points='''
        [console_scripts]
        target-s3-jsonl=target_s3_jsonl:main
    ''',
)
