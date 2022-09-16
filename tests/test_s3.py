'''Tests for the target.s3 module'''
# Standard library imports
import sys
from os import environ
from asyncio import run
from copy import deepcopy
from re import match
import lzma
from pathlib import Path
import datetime
import boto3
from botocore.client import BaseClient
# from botocore.stub import Stubber
# from botocore.exceptions import ClientError
# from aiobotocore.session import get_session

from io import BytesIO
import json
import gzip

# from pytest import patch
# Third party imports
from pytest import fixture, raises, mark
from moto import mock_s3, mock_sts

# Package imports
# from target.file import save_json
from target_s3_json.s3 import (
    _log_backoff_attempt, config_compression, create_session, get_encryption_args, put_object, upload_file, config_s3, main
)

# from .conftest import clear_dir

# import shutil
# import signal
# import subprocess as sp
# import sys
# import time
# import requests
# from aiohttp.web_exceptions import HTTPError
# from aiohttp import ClientSession, ClientResponse, BasicAuth, ClientResponseError, ClientError, TCPConnector


# _proxy_bypass = {
#   "http": None,
#   "https": None,
# }


# async def start_service(session, service_name, host, port):
#     moto_svr_path = shutil.which("moto_server")
#     args = [sys.executable, moto_svr_path, service_name, "-H", host,
#             "-p", str(port)]
#     process = sp.Popen(args, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.DEVNULL)
#     url = "http://{host}:{port}".format(host=host, port=port)

#     for _ in range(30):
#         if process.poll() is not None:
#             break

#         async with session.request('get', url, timeout=0.1, proxies=_proxy_bypass) as _:
#             try:
#                 # we need to bypass the proxies due to monkeypatches
#                 # requests.get(url, timeout=0.1, proxies=_proxy_bypass)
#                 # response.status
#                 break
#             # except requests.exceptions.RequestException:
#             except (HTTPError, ConnectionError):
#                 time.sleep(0.1)
#     else:
#         stop_process(process)
#         raise AssertionError("Can not start service: {}".format(service_name))

#     return process


# def stop_process(process, timeout=20):
#     try:
#         process.send_signal(signal.SIGTERM)
#         process.communicate(timeout=timeout / 2)
#     except sp.TimeoutExpired:
#         process.kill()
#         outs, errors = process.communicate(timeout=timeout / 2)
#         exit_code = process.returncode
#         msg = "Child process finished {} not in clean way: {} {}" \
#             .format(exit_code, outs, errors)
#         raise RuntimeError(msg)


# @fixture(scope='session')
# def s3_server():
#     host = 'localhost'
#     port = 5002
#     url = 'http://{host}:{port}'.format(host=host, port=port)

#     session = ClientSession(
#             # NOTE: overwrite the default connector to customise the default connection settings applied to the queries
#             connector=TCPConnector(
#                 # NOTE: max concurrent connections to the end point
#                 limit_per_host=16,
#                 # NOTE: limit on the client connections total count. 100 by default
#                 # limit=limit_connections_count,
#                 # NOTE: live connection duration
#                 keepalive_timeout=30),
#             connector_owner=True)


#     process = run(start_service(session, 's3', host, port))
#     yield url
#     stop_process(session, process)


# @fixture(scope='session')
# def s3_server():
#     host = 'localhost'
#     port = 5002
#     url = 'http://{host}:{port}'.format(host=host, port=port)
#     process = start_service('s3', host, port)
#     yield url
#     stop_process(process)


@fixture
def config_raw(temp_path):
    '''Use custom configuration set'''

    return {
        's3_bucket': 'BUCKET',
        'aws_access_key_id': 'ACCESS-KEY',
        'aws_secret_access_key': 'SECRET',
        'add_metadata_columns': False,
        'work_dir': f'{temp_path}/tests/output',
        'memory_buffer': 2000000,
        'compression': 'none',
        'timezone_offset': 0,
        'path_template': '{stream}-{date_time}.json'
    }


@fixture
def config(patch_datetime, config_raw):
    '''Use custom configuration set'''

    return config_raw | {
        # 'date_time': dt.strptime('2021-08-11 06:39:38.321056+00:00', '%Y-%m-%d %H:%M:%S.%f%z'),
        'date_time': datetime.datetime.utcnow(),
        'work_path': Path(config_raw['work_dir']),
        'concurrency_max': 1000,
        'open_func': open
    }


@fixture
def config_assume_role(config):
    '''Use custom configuration set'''

    return config | {
        'role_arn': 'arn:aws:iam::123456789012:role/TestAssumeRole'
    }


# @fixture
# @mock_s3
# def bucket(config):
#     conn = boto3.resource('s3', region_name='us-east-1', endpoint_url='https://s3.amazonaws.com')
#     # We need to create the bucket since this is all in Moto's 'virtual' AWS account
#     conn.create_bucket(Bucket=config['s3_bucket'])
#     return conn

# NOTE: Explore https://github.com/aio-libs/aiobotocore/issues/440


@fixture(scope='module')
def aws_credentials():
    '''Mocked AWS Credentials for moto.'''
    moto_credentials_file_path = Path('tests', 'resources', 'aws_credentials')
    environ['AWS_SHARED_CREDENTIALS_FILE'] = str(moto_credentials_file_path)

    environ['AWS_ACCESS_KEY_ID'] = 'that_key'
    environ['AWS_SECRET_ACCESS_KEY'] = 'no_big_secret'


def test_log_backoff_attempt(caplog):
    '''TEST : simple _log_backoff_attempt call'''

    _log_backoff_attempt({'tries': 99})
    pat = r'INFO     root:s3.py:\d{2} Error detected communicating with Amazon, triggering backoff: 99 try\n'

    assert match(pat, caplog.text)


def test_config_compression(config):
    '''TEST : simple config_compression call'''

    assert f"{config.get('compression')}".lower() in {'', 'none', 'gzip', 'lzma'}

    with raises(Exception):
        config_compression(config | {'compression': 'dummy'})


@mark.parametrize("compression,extention,open_func", [('none', '', open), ('gzip', '.gz', gzip.compress), ('lzma', '.xz', lzma.compress)])
def test_config_compression_open_func(config, compression, extention, open_func):
    '''TEST : simple config_compression call'''

    assert config_compression(config | {'compression': compression}) == config | {
        'compression': compression,
        'path_template': config['path_template'] + extention,
        'open_func': open_func
    }


@mock_sts
@mock_s3
def test_create_client_with_assumed_role(caplog, config_assume_role: dict):
    '''Assert client is created with assumed role when role_arn is specified'''

    create_session(config_assume_role)
    assert caplog.text.endswith('Creating s3 session with role TestAssumeRole\n')


@mock_s3
def test_create_session(aws_credentials, config):
    '''TEST : simple create_session call'''

    conn = boto3.resource('s3', region_name='us-east-1', endpoint_url='https://s3.amazonaws.com')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket=config['s3_bucket'])

    # async with get_session().create_client('s3', region_name='us-east-1', end_point_url=s3_server) as client:
    #     with patch('aiobotocore.AioSession.create_client') as mock:
    #         mock.return_value = client

    client: BaseClient = create_session(config).client('s3')
    client.put_object(Bucket=config['s3_bucket'], Key='Eddy is', Body='awesome!')
    body = conn.Object(config['s3_bucket'], 'Eddy is').get()['Body'].read().decode("utf-8")

    assert body == 'awesome!'

    # NOTE: Test jsonl upload
    file_metadata = {
        'absolute_path': Path('tests', 'resources', 'messages.json'),
        'relative_path': 'dummy/messages.json'}

    stream_src = [
        {"c_pk": 1, "c_varchar": "1", "c_int": 1, "c_time": "04:00:00"},
        {"c_pk": 2, "c_varchar": "2", "c_int": 2, "c_time": "07:15:00"},
        {"c_pk": 3, "c_varchar": "3", "c_int": 3, "c_time": "23:00:03"}]
    # stream_bin = b''.join(json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in stream_src)
    # client.put_object(Body=gzip.GzipFile(fileobj=BytesIO(stream_bin), mode='w'), Bucket=config['s3_bucket'], Key=file_metadata['relative_path'])

    # with gzip.open(BytesIO([json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in stream_src]), 'wt', encoding='utf-8') as output_data:
    #     client.put_object(Body=output_data, Bucket=config['s3_bucket'], Key=file_metadata['relative_path'])

    stream_bin = b''.join(json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in stream_src)
    client.put_object(Body=gzip.compress(stream_bin), Bucket=config['s3_bucket'], Key=file_metadata['relative_path'])
    body = conn.Object(config['s3_bucket'], file_metadata['relative_path']).get()['Body'].read()
    # stream_txt = gzip.decompress(body)
    with gzip.open(BytesIO(body), 'rt', encoding='utf-8') as input_data:
        assert [json.loads(item) for item in input_data] == stream_src

    with raises(Exception):
        config_copy = deepcopy(config)
        config_copy['aws_endpoint_url'] = 'xXx'

        client = create_session(config_copy).client('s3', endpoint_url=config_copy.get('aws_endpoint_url'))
        client.put_object(Bucket=config_copy['s3_bucket'], Key='Eddy is', Body='awesome!')
        body = conn.Object(config_copy['s3_bucket'], 'Eddy is').get()['Body'].read().decode("utf-8")

    # NOTE: AWS Profile based authentication
    config_copy = deepcopy(config)
    config_copy['aws_profile'] = 'dummy'
    config_copy.pop('aws_access_key_id')
    config_copy.pop('aws_secret_access_key')
    environ.pop('AWS_ACCESS_KEY_ID')
    environ.pop('AWS_SECRET_ACCESS_KEY')

    client = create_session(config_copy).client('s3')
    client.put_object(Bucket=config_copy['s3_bucket'], Key='Look!', Body='No access key!')
    body = conn.Object(config_copy['s3_bucket'], 'Look!').get()['Body'].read().decode("utf-8")

    assert body == 'No access key!'


def test_get_encryption_args(config):
    '''TEST : simple get_encryption_args call'''

    encryption_desc, encryption_args = get_encryption_args(config)
    assert encryption_args == {}
    assert encryption_desc == ''

    encryption_desc, encryption_args = get_encryption_args(config | {'encryption_type': 'kms'})
    assert encryption_args == {'ExtraArgs': {'ServerSideEncryption': 'aws:kms'}}
    assert encryption_desc == ' using default KMS encryption'

    encryption_desc, encryption_args = get_encryption_args(config | {'encryption_type': 'kms', 'encryption_key': 'SECRET'})
    assert encryption_args == {'ExtraArgs': {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'SECRET'}}
    assert encryption_desc == " using KMS encryption key ID '{}'".format('SECRET')

    with raises(Exception):
        encryption_desc, encryption_args = get_encryption_args(config | {'encryption_type': 'dummy'})


@mock_s3
def test_put_object(config):
    '''TEST : simple put_object call'''
    config |= {'compression': 'gzip', 'open_func': gzip.compress}

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['s3_bucket'])

    client: BaseClient = create_session(config).client('s3')

    file_metadata = {
        'absolute_path': Path('tests', 'resources', 'messages.json.gz'),
        'relative_path': 'dummy/messages.json.gz'}

    stream_data = [
        {"c_pk": 1, "c_varchar": "1", "c_int": 1, "c_time": "04:00:00"},
        {"c_pk": 2, "c_varchar": "2", "c_int": 2, "c_time": "07:15:00"},
        {"c_pk": 3, "c_varchar": "3", "c_int": 3, "c_time": "23:00:03"}]

    run(put_object(
        config,
        file_metadata,
        stream_data,
        client))

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=file_metadata['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 102
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    # NOTE: 'kms' encryption_type with default encryption_key
    file_metadata = {
        'absolute_path': Path('tests', 'resources', 'messages.json.gz'),
        'relative_path': 'dummy/messages_kms.json.gz'}
    run(put_object(
        config | {'encryption_type': 'kms', 'encryption_key': None},
        file_metadata,
        stream_data,
        client))

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=file_metadata['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 102
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    # NOTE: 'kms' encryption_type with encryption_key
    file_metadata = {
        'absolute_path': Path('tests', 'resources', 'messages.json.gz'),
        'relative_path': 'dummy/messages_kms.json.gz'}
    run(put_object(
        config | {'encryption_type': 'kms', 'encryption_key': 'xXx'},
        file_metadata,
        stream_data,
        client))

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=file_metadata['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 102
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    # NOTE: 'dummy' encryption_type
    with raises(Exception):
        run(put_object(
            config | {'encryption_type': 'dummy'},
            file_metadata | {'relative_path': 'dummy/messages_dummy.json.gz'},
            stream_data,
            client))


@mock_s3
def test_upload_file(config, temp_path):
    '''TEST : simple upload_file call'''

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['s3_bucket'])

    client: BaseClient = create_session(config).client('s3')

    temp_file: Path = Path(temp_path.join('test.json.gz'))
    temp_file.write_bytes(Path('tests', 'resources', 'messages.json').read_bytes())

    file_metadata = {
        'absolute_path': temp_file,
        'relative_path': 'dummy/messages.json'}

    run(upload_file(
        config | {'local': True, 'client': client, 'remove_file': False},
        file_metadata))

    assert 'Contents' not in client.list_objects_v2(Bucket=config['s3_bucket'], Prefix=file_metadata['relative_path'], MaxKeys=1)
    assert file_metadata['absolute_path'].exists()

    run(upload_file(
        config | {'client': client, 'remove_file': False},
        file_metadata))

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=file_metadata['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 613
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    # NOTE: 'kms' encryption_type with default encryption_key
    file_metadata = {
        'absolute_path': temp_file,
        'relative_path': 'dummy/messages_kms.json'}
    run(upload_file(
        config | {'client': client, 'remove_file': False, 'encryption_type': 'kms', 'encryption_key': None},
        file_metadata))

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=file_metadata['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 613
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    # NOTE: 'kms' encryption_type with encryption_key
    file_metadata = {
        'absolute_path': temp_file,
        'relative_path': 'dummy/messages_kms.json'}
    run(upload_file(
        config | {'client': client, 'encryption_type': 'kms', 'encryption_key': 'xXx'},
        file_metadata))

    head = client.head_object(Bucket=config.get('s3_bucket'), Key=file_metadata['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 613
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    assert not file_metadata['absolute_path'].exists()

    # NOTE: 'dummy' encryption_type
    # with raises(Exception):
    #     run(upload_file(
    #         config | {'client': client, 'encryption_type': 'dummy'},
    #         file_metadata | {'relative_path': 'dummy/messages_dummy.json'}))


def test_config_s3(config_raw):

    config = deepcopy(config_raw)
    config['temp_dir'] = config.pop('work_dir')
    config['naming_convention'] = config.pop('path_template')
    assert config_s3(config) == config_raw | {'concurrency_max': 1000}

    config = deepcopy(config_raw)
    config['temp_dir'] = config.pop('work_dir')
    config.pop('path_template')
    config['naming_convention'] = '{stream}-{timestamp}.json'
    assert config_s3(config) == config_raw | {'concurrency_max': 1000, 'path_template': '{stream}-{date_time:%Y%m%dT%H%M%S}.json'}

    config = deepcopy(config_raw)
    config['temp_dir'] = config.pop('work_dir')
    config.pop('path_template')
    config['naming_convention'] = '{stream}-{date}.json'
    assert config_s3(config) == config_raw | {'concurrency_max': 1000, 'path_template': '{stream}-{date_time:%Y%m%d}.json'}

    config.pop('s3_bucket')
    with raises(Exception):
        config_s3(config)


@mock_s3
def test_main(capsys, patch_datetime, patch_sys_stdin, patch_argument_parser, config_raw, state, file_metadata):
    '''TEST : simple main call'''

    conn = boto3.resource('s3', region_name='us-east-1', endpoint_url='https://s3.amazonaws.com')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket=config_raw['s3_bucket'])

    main(lines=sys.stdin)

    captured = capsys.readouterr()
    assert captured.out == json.dumps(state) + '\n'

    for file_info in file_metadata.values():
        assert not file_info['path'][1]['absolute_path'].exists()

    client: BaseClient = create_session(config_raw).client('s3')

    head = client.head_object(Bucket=config_raw.get('s3_bucket'), Key=file_metadata['tap_dummy_test-test_table_one']['path'][1]['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 42
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    head = client.head_object(Bucket=config_raw.get('s3_bucket'), Key=file_metadata['tap_dummy_test-test_table_two']['path'][1]['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 150
    assert head['ResponseMetadata']['RetryAttempts'] == 0

    head = client.head_object(Bucket=config_raw.get('s3_bucket'), Key=file_metadata['tap_dummy_test-test_table_three']['path'][1]['relative_path'])
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200
    assert head['ContentLength'] == 192
    assert head['ResponseMetadata']['RetryAttempts'] == 0
