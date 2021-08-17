'''Tests for the target_s3_jsonl.main module'''
# Standard library imports
from copy import deepcopy
from pathlib import Path
import json

# Third party imports
from pytest import fixture, raises
import boto3
from botocore.errorfactory import ClientError
from moto import mock_s3

# Package imports
from target_s3_jsonl.s3 import create_client, upload_file


@fixture
def config():
    '''Use custom parameters set'''

    with open(Path('tests', 'resources', 'config.json'), 'r', encoding='utf-8') as config_file:
        return json.load(config_file)


@mock_s3
def test_create_client(config):
    '''TEST : simple upload_files call'''

    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket=config['s3_bucket'])

    client = create_client(config)
    client.put_object(Bucket=config['s3_bucket'], Key='Eddy is', Body='awesome!')
    body = conn.Object(config['s3_bucket'], 'Eddy is').get()[
        'Body'].read().decode("utf-8")

    assert body == 'awesome!'

    with raises(Exception):
        config_copy = deepcopy(config)
        config_copy['aws_endpoint_url'] = 'xx'

        client = create_client(config_copy)
        client.put_object(Bucket=config_copy['s3_bucket'], Key='Eddy is', Body='awesome!')
        body = conn.Object(config_copy['s3_bucket'], 'Eddy is').get()[
            'Body'].read().decode("utf-8")


@mock_s3
def test_upload_file(config):
    '''TEST : simple upload_files call'''

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['s3_bucket'])

    client = create_client(config)

    file_key = str(Path('tests', 'resources', 'config.json'))
    upload_file(
        file_key,
        client,
        config.get('s3_bucket'),
        'dummy/remote_config.json',
        encryption_type=config.get('encryption_type'),
        encryption_key=config.get('encryption_key'))

    try:
        client.head_object(Bucket=config.get('s3_bucket'), Key=file_key)
    except ClientError:
        pass

    with raises(Exception):
        upload_file(
            file_key,
            client,
            config.get('s3_bucket'),
            'dummy/remote_config_dummy.json',
            encryption_type='dummy',
            encryption_key=config.get('encryption_key'))

    upload_file(
        file_key,
        client,
        config.get('s3_bucket'),
        'dummy/remote_config_kms.json',
        encryption_type='kms',
        encryption_key=config.get('encryption_key'))

    try:
        client.head_object(Bucket=config.get('s3_bucket'), Key=file_key)
    except ClientError:
        pass
