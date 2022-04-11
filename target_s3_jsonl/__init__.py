#!/usr/bin/env python3

__version__ = '2.0.0'

import sys
import argparse

# Package imports
from target import (
    emit_state,
    persist_lines,
    config_file,
    s3
)

from target.file import config_compression

from target_s3_jsonl.logger import get_logger
LOGGER = get_logger()

CONFIG_PARAMS = {
    'local',
    's3_bucket',
    's3_key_prefix',
    'aws_profile',
    'aws_endpoint_url',
    'aws_access_key_id',
    'aws_secret_access_key',
    'aws_session_token',
    'encryption_type',
    'encryption_key'
}


def get_s3_config(config_path):
    config = config_file(config_path)

    missing_params = {'s3_bucket'} - set(config.keys())
    if missing_params:
        raise Exception(f'Config is missing required settings: {missing_params}')

    return config


def upload_files(file_data, config):
    if not config.get('local', False):
        s3_client = s3.create_client(config)
        for stream, file_info in file_data.items():
            if file_info['file_name'].exists():
                s3.upload_file(
                    s3_client,
                    str(file_info['file_name']),
                    config.get('s3_bucket'),
                    file_info['target_key'],
                    encryption_type=config.get('encryption_type'),
                    encryption_key=config.get('encryption_key'))
                LOGGER.debug("{} file {} uploaded to {}".format(stream, file_info['target_key'], config.get('s3_bucket')))

                # NOTE: Remove the local file(s)
                file_info['file_name'].unlink()


def upload_files(file_data, config):
    if not config.get('local', False):
        s3_client = s3.create_client(config)
        for stream, file_info in file_data.items():
            if file_info['file_name'].exists():
                s3.upload_file(
                    s3_client,
                    str(file_info['file_name']),
                    config.get('s3_bucket'),
                    file_info['target_key'],
                    encryption_type=config.get('encryption_type'),
                    encryption_key=config.get('encryption_key'))
                LOGGER.debug("{} file {} uploaded to {}".format(stream, file_info['target_key'], config.get('s3_bucket')))

                # NOTE: Remove the local file(s)
                file_info['file_name'].unlink()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    config = config_compression(get_s3_config(args.config))

    state, file_data = persist_lines(sys.stdin, config)

    # NOTE: Upload created files to S3
    upload_files(file_data, config)

    emit_state(state)
    LOGGER.debug('Exiting normally')


if __name__ == '__main__':  # pragma: no cover
    main()
