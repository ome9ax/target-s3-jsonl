#!/usr/bin/env python

from setuptools import setup

with open('README.md', 'r', encoding='utf-8') as f:
    readme = f.read()

setup(
    name='target-s3-jsonl',
    version='0.0.4',
    description='Singer.io target for writing JSON Line files and upload to S3',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Eddy ∆',
    author_email='edrdelta@gmail.com',
    url='https://github.com/ome9ax/target-s3-jsonl',
    keywords=['singer', 'singer.io', 'tap', 'target', 'etl', 'json', 'jsonl', 'aws', 's3'],
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
    ],
    py_modules=['target_s3_jsonl'],
    python_requires='>=3.8',
    install_requires=[
        'jsonschema==3.2.0',
        'boto3==1.18.16',
        "backoff==1.11.1",
    ],
    packages=['target_s3_jsonl'],
    include_package_data=True,
    package_data = {
        "target_s3_jsonl": [
            "logging.conf"
        ]
    },
    entry_points='''
        [console_scripts]
        target-s3-jsonl=target_s3_jsonl:main
    ''',
)
