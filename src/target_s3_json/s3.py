#!/usr/bin/env python3
from os import environ
from pathlib import Path
import sys
# from re import Pattern, compile, match
import argparse
import json
import gzip
import lzma
import backoff
from typing import Callable, Dict, Any, List, TextIO  # , Generator
from asyncio import Semaphore, gather, shield, to_thread, run

from boto3.session import Session
from botocore.exceptions import ClientError
from botocore.client import BaseClient
# from aiobotocore.session import get_session
# from s3fs import S3FileSystem

from target.stream import Loader
from target import file
from target.file import config_file

from target._logger import get_logger
LOGGER = get_logger()


def _log_backoff_attempt(details: Dict) -> None:
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


def _retry_pattern() -> Callable:
    return backoff.on_exception(
        backoff.expo,
        ClientError,
        max_tries=5,
        on_backoff=_log_backoff_attempt,
        factor=10)


def config_compression(config_default: Dict) -> Dict:
    config: Dict[str, Any] = {
        'compression': 'none'
    } | config_default

    if f"{config.get('compression')}".lower() == 'gzip':
        config['open_func'] = gzip.compress
        config['path_template'] = config['path_template'] + '.gz'

    elif f"{config.get('compression')}".lower() == 'lzma':
        config['open_func'] = lzma.compress
        config['path_template'] = config['path_template'] + '.xz'

    elif f"{config.get('compression')}".lower() in {'', 'none'}:
        config['open_func'] = open

    else:
        raise NotImplementedError(
            "Compression type '{}' is not supported. "
            "Expected: 'none', 'gzip', or 'lzma'"
            .format(f"{config.get('compression')}".lower()))

    return config


@_retry_pattern()
def create_session(config: Dict) -> Session:
    LOGGER.debug('Attempting to create AWS session')

    # Get the required parameters from config file and/or environment variables
    aws_access_key_id = config.get('aws_access_key_id') or environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('aws_secret_access_key') or environ.get('AWS_SECRET_ACCESS_KEY')
    aws_session_token = config.get('aws_session_token') or environ.get('AWS_SESSION_TOKEN')
    aws_profile = config.get('aws_profile') or environ.get('AWS_PROFILE')
    aws_endpoint_url = config.get('aws_endpoint_url')
    role_arn = config.get('role_arn')

    endpoint_params = {'endpoint_url': aws_endpoint_url} if aws_endpoint_url else {}

    # AWS credentials based authentication
    if aws_access_key_id and aws_secret_access_key:
        # aws_session = get_session(
        aws_session: Session = Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token)
    # AWS Profile based authentication
    else:
        # aws_session = get_session()
        aws_session = Session(profile_name=aws_profile)

    # AWS credentials based authentication assuming specific IAM role
    if role_arn:
        role_name = role_arn.split('/', 1)[1]
        sts: BaseClient = aws_session.client('sts', **endpoint_params)
        resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=f'role-name={role_name}-profile={aws_profile}')
        credentials = {
            'aws_access_key_id': resp['Credentials']['AccessKeyId'],
            'aws_secret_access_key': resp['Credentials']['SecretAccessKey'],
            'aws_session_token': resp['Credentials']['SessionToken'],
        }
        aws_session = Session(**credentials)
        LOGGER.info(f'Creating s3 session with role {role_name}')

    return aws_session


def get_encryption_args(config: Dict[str, Any]) -> tuple:
    if config.get('encryption_type', 'none').lower() == "none":
        # No encryption config (defaults to settings on the bucket):
        encryption_desc = ''
        encryption_args = {}
    elif config.get('encryption_type', 'none').lower() == 'kms':
        if config.get('encryption_key'):
            encryption_desc = " using KMS encryption key ID '{}'".format(config.get('encryption_key'))
            encryption_args = {'ExtraArgs': {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': config.get('encryption_key')}}
        else:
            encryption_desc = ' using default KMS encryption'
            encryption_args = {'ExtraArgs': {'ServerSideEncryption': 'aws:kms'}}
    else:
        raise NotImplementedError(
            "Encryption type '{}' is not supported. "
            "Expected: 'none' or 'KMS'"
            .format(config.get('encryption_type')))
    return encryption_desc, encryption_args


@_retry_pattern()
# def write(config: Dict[str, Any], file_meta: Dict, file_data: List) -> None:
# async def upload_file(config: Dict[str, Any], file_metadata: Dict, file_data: List, client: Any) -> None:
async def put_object(config: Dict[str, Any], file_metadata: Dict, stream_data: List, client: BaseClient) -> None:
    encryption_desc, encryption_args = get_encryption_args(config)

    LOGGER.info("Uploading %s to bucket %s at %s%s",
                str(file_metadata['absolute_path']), config.get('s3_bucket'), file_metadata['relative_path'], encryption_desc)

    # with config['open_func'](file_metadata['absolute_path'], 'at', encoding='utf-8') as output_file:
    # await client.put_object(
    await to_thread(client.put_object,
                    Body=config['open_func'](  # NOTE: gzip.compress, lzma.compress
                        b''.join(json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in stream_data)),
                    Bucket=config.get('s3_bucket'),
                    Key=file_metadata['relative_path'],
                    **encryption_args.get('ExtraArgs', {}))

    # resp = await client.get_object_acl(Bucket=config.get('s3_bucket'), Key=file_metadata['relative_path'])
    # print(resp)
    # LOGGER.debug("S3 Upload %s uploaded to %s at %s%s",
    #             str(file_metadata['absolute_path']), config.get('s3_bucket'), file_metadata['relative_path'], encryption_desc)

#     with BytesIO(output_data, 'wb') as output_file:
#         with config['open_func'](output_file, 'at', encoding='utf-8') as input_data:
#             input_data.writelines((json.dumps(record) + '\n' for record in stream_data))

#     with BytesIO(b''.join(json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in file_data), 'wb') as output_file:
#     # with BytesIO((b'' + json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in file_data), 'wb') as output_file:
#         with config['open_func'](output_file, 'at', encoding='utf-8') as output_data:

#             await client.put_object(
#                 Bucket=config.get('s3_bucket'),
#                 Key=file_metadata['relative_path'],
#                 Body=output_data,
#                 ExtraArgs=encryption_args)

#             # await client.upload_file(
#             #     str(file_metadata['absolute_path']),
#             #     config.get('s3_bucket'),
#             #     file_metadata['relative_path'],
#             #     ExtraArgs=encryption_args)


# # NOTE: https://github.com/aws/aws-cli/issues/3784
# async def search(client: BaseClient, bucket: str, prefix: str, regex_path: str) -> Generator:
#     '''
#     perform a flat listing of the files within a bucket
#     '''
#     regex_pattern: Pattern = compile(regex_path)

#     paginator = client.get_paginator('list_objects_v2')
#     files_metadata = paginator.paginate(Bucket=bucket, Prefix=prefix)
#     for file_path in map(lambda x: x.get('Key', ''), await to_thread(files_metadata.search, 'Contents')):
#         if match(regex_pattern, file_path):
#             yield file_path


# async def sync(
#     client: BaseClient, semaphore: Semaphore, source_bucket: str, source_key: str, target_bucket: str, target_key: str, overwrite: bool = False) -> None:
#     async with semaphore:
#         LOGGER.debug(f'S3 Bucket Sync - "s3://{source_bucket}/{source_key}" to "s3://{target_bucket}/{target_key}" begins.')
#         if not overwrite and 'Contents' in client.list_objects_v2(Bucket=target_bucket, Prefix=target_key, MaxKeys=1):
#             LOGGER.info(f'S3 Bucket Sync - "s3://{target_bucket}/{target_key}" already exists.')
#         else:
#             await to_thread(client.copy, {'Bucket': source_bucket, 'Key': source_key}, target_bucket, target_key)
#             LOGGER.info(f'S3 Bucket Sync - "s3://{source_bucket}/{source_key}" to "s3://{target_bucket}/{target_key}" copy completed.')


@_retry_pattern()
# def write(config: Dict[str, Any], file_meta: Dict, file_data: List) -> None:
# async def upload_file(config: Dict[str, Any], file_metadata: Dict, file_data: List, client: Any) -> None:
async def upload_file(config: Dict[str, Any], file_metadata: Dict, client: BaseClient, remove_file: bool = False) -> Dict:
    encryption_desc, encryption_args = get_encryption_args(config)

    async with config['semaphore']:
        # await client.upload_file(
        await to_thread(client.upload_file,
                        str(file_metadata['absolute_path']),
                        config.get('s3_bucket'),
                        file_metadata['relative_path'],
                        **encryption_args)

    LOGGER.info('%s uploaded to bucket %s at %s%s',
                str(file_metadata['absolute_path']), config.get('s3_bucket'), file_metadata['relative_path'], encryption_desc)

    if remove_file:
        # NOTE: Remove the local file(s)
        file_metadata['absolute_path'].unlink()  # missing_ok=False
    return file_metadata


async def upload_files(file_data: Dict, config: Dict[str, Any]) -> None:
    if not config.get('local', False):
        # async with create_session(config).create_client('s3', **({'endpoint_url': config.get('aws_endpoint_url')}
        #                                                          if config.get('aws_endpoint_url') else {})) as client:
        client: BaseClient = create_session(config).client('s3', **({'endpoint_url': config.get('aws_endpoint_url')}
                                                           if config.get('aws_endpoint_url') else {}))
        # for stream, file_metadata in file_data.items():
        #     for path in file_metadata['path'].values():
        #         if path['absolute_path'].exists():
        #             await upload_file(config, path, client)
        #             # run(upload_file(config, path, client))
        #             LOGGER.info("Target Core: {} file {} uploaded to {}".format(stream, path['relative_path'], config.get('s3_bucket')))

        #             # NOTE: Remove the local file(s)
        #             path['absolute_path'].unlink()  # missing_ok=False

        semaphore = Semaphore(config['concurrency_max'])
        await gather(*[
            shield(upload_file(config | {'semaphore': semaphore}, path, client, remove_file=True))
            for stream, file_metadata in file_data.items()
            for path in file_metadata['path'].values()
            if path['absolute_path'].exists()])


def config_s3(config_default: Dict[str, Any], datetime_format: Dict[str, str] = {
        'date_time_format': ':%Y%m%dT%H%M%S',
        'date_format': ':%Y%m%d'}) -> Dict[str, Any]:

    if 'temp_dir' in config_default:
        LOGGER.warning('`temp_dir` configuration option is deprecated and support will be removed in the future, use `work_dir` instead.')
        config_default['work_dir'] = config_default.pop('temp_dir')

    if 'naming_convention' in config_default:
        LOGGER.warning(
            '`naming_convention` configuration option is deprecated and support will be removed in the future, use `path_template` instead.'
            ', `{timestamp}` key pattern is now replaced by `{date_time}`'
            ', and `{date}` key pattern is now replaced by `{date_time:%Y%m%d}`')
        config_default['path_template'] = config_default.pop('naming_convention') \
            .replace('{timestamp:', '{date_time:').replace('{date:', '{date_time:') \
            .replace('{timestamp}', '{date_time%s}' % datetime_format['date_time_format']) \
            .replace('{date}', '{date_time%s}' % datetime_format['date_format'])

    if 'concurrency_max' not in config_default:
        config_default['concurrency_max'] = 1000

    missing_params = {'s3_bucket'} - set(config_default.keys())
    if missing_params:
        raise Exception(f'Config is missing required settings: {missing_params}')

    return config_default


def main(loader: type[Loader] = Loader, lines: TextIO = sys.stdin) -> None:
    '''Main'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()
    config = file.config_compression(config_file(config_s3(json.loads(Path(args.config).read_text(encoding='utf-8')))))

    s3_loader: Loader = loader(config)
    s3_loader.run(lines)

    run(upload_files(s3_loader.stream_data, s3_loader.config))


# def write(config: Dict[str, Any], file_meta: Dict, file_data: List) -> None:
#     s3_fs: S3FileSystem = S3FileSystem(anon=False, s3_additional_kwargs={'ACL': 'bucket-owner-full-control'}, asynchronous=False)
#     # s3_fs.set_session()

#     with s3_fs.open(file_meta['absolute_path'], 'wb') as output_file:
#         with config['open_func'](output_file, 'at', encoding='utf-8') as output_data:
#             output_file.writelines((json.dumps(record) + '\n' for record in file_data))


# from io import BytesIO
# from pyarrow import parquet
# from pyarrow.json import read_json
# import pandas
# def write_parquet(config: Dict[str, Any], file_meta: Dict, file_data: List) -> None:
#     s3_fs: S3FileSystem = S3FileSystem(anon=False, s3_additional_kwargs={'ACL': 'bucket-owner-full-control'}, asynchronous=False)

#     # NOTE: Create parquet file using Pandas
#     pandas.json_normalize(file_data).to_parquet(path = file_meta['absolute_path'], filesystem = s3_fs)
#     # NOTE: Synchronous Alternative without df middle step, read_json not yet as efficient as pandas. Worth keeping on check.
#     with BytesIO(b''.join(json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in file_data)) as data:
#         parquet.write_table(read_json(data), file_meta['relative_path'], filesystem=s3_fs)
