'''Tests for the target_s3_jsonl.main module'''
# Standard library imports
from copy import deepcopy
from pathlib import Path
import json
import os

import boto3
from moto import mock_s3, mock_sts
from pytest import fixture, raises

# Package imports
from target_s3_jsonl.s3 import create_client, upload_file, log_backoff_attempt


@fixture
def config():
    '''Use custom parameters set'''

    with open(Path('tests', 'resources', 'config.json'), 'r', encoding='utf-8') as config_file:
        return json.load(config_file)


@fixture
def config_assume_role():
    with open(Path('tests', 'resources', 'config_assume_role.json'), 'r', encoding='utf-8') as f:
        return json.load(f)


@fixture(scope='module')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    moto_credentials_file_path = Path('tests', 'resources', 'aws_credentials')
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = str(moto_credentials_file_path)

    os.environ['AWS_ACCESS_KEY_ID'] = 'that_key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'no_big_secret'


def test_log_backoff_attempt(caplog):
    '''TEST : simple upload_files call'''

    log_backoff_attempt({'tries': 2})

    assert 'Error detected communicating with Amazon, triggering backoff: 2 try' in caplog.text


@mock_sts
@mock_s3
def test_create_client_with_assumed_role(config_assume_role, caplog):
    """Assert client is created with assumed role when role_arn is specified"""
    create_client(config_assume_role)
    assert caplog.text.endswith('Creating s3 client with role TestAssumeRole\n')


@mock_s3
def test_create_client(aws_credentials, config):
    '''TEST : simple upload_files call'''

    conn = boto3.resource('s3', region_name='us-east-1', endpoint_url='https://s3.amazonaws.com')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket=config['s3_bucket'])

    client = create_client(config)
    client.put_object(Bucket=config['s3_bucket'], Key='Eddy is', Body='awesome!')
    body = conn.Object(config['s3_bucket'], 'Eddy is').get()['Body'].read().decode("utf-8")

    assert body == 'awesome!'

    with raises(Exception):
        config_copy = deepcopy(config)
        config_copy['aws_endpoint_url'] = 'xXx'

        client = create_client(config_copy)
        client.put_object(Bucket=config_copy['s3_bucket'], Key='Eddy is', Body='awesome!')
        body = conn.Object(config_copy['s3_bucket'], 'Eddy is').get()['Body'].read().decode("utf-8")

    # NOTE: AWS Profile based authentication
    config_copy = deepcopy(config)
    config_copy['aws_profile'] = 'dummy'
    config_copy.pop('aws_access_key_id')
    config_copy.pop('aws_secret_access_key')
    os.environ.pop('AWS_ACCESS_KEY_ID')
    os.environ.pop('AWS_SECRET_ACCESS_KEY')

    client = create_client(config_copy)
    client.put_object(Bucket=config_copy['s3_bucket'], Key='Look!', Body='No access key!')
    body = conn.Object(config_copy['s3_bucket'], 'Look!').get()['Body'].read().decode("utf-8")

    assert body == 'No access key!'


@mock_s3
def test_upload_file(config):
    '''TEST : simple upload_files call'''

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['s3_bucket'])

    client = create_client(config)

    file_path = str(Path('tests', 'resources', 'messages.json'))
    s3_key = 'dummy/messages.json'
    upload_file(
        client,
        file_path,
        config.get('s3_bucket'),
        s3_key,
        encryption_type=config.get('encryption_type'),
        encryption_key=config.get('encryption_key'))

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=s3_key)
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 613
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    # NOTE: 'kms' encryption_type with default encryption_key
    s3_key = 'dummy/messages_kms.json'
    upload_file(
        client,
        file_path,
        config.get('s3_bucket'),
        s3_key,
        encryption_type='kms')

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=s3_key)
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 613
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    # NOTE: 'kms' encryption_type with encryption_key
    s3_key = 'dummy/messages_kms.json'
    upload_file(
        client,
        file_path,
        config.get('s3_bucket'),
        s3_key,
        encryption_type='kms',
        encryption_key='xXx')

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=s3_key)
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 613
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    # NOTE: 'dummy' encryption_type
    with raises(Exception):
        upload_file(
            client,
            file_path,
            config.get('s3_bucket'),
            'dummy/messages_dummy.json',
            encryption_type='dummy',
            encryption_key=config.get('encryption_key'))
