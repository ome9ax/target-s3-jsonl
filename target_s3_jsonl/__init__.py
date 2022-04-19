#!/usr/bin/env python3

__version__ = '2.0.0'

from pathlib import Path
import sys
import argparse
from asyncio import run
from typing import Any, Dict

# Package imports
from target import (
    emit_state,
    writelines,
    config_file,
    s3
)

from target.file import config_compression

from target.logger import get_logger
LOGGER = get_logger(Path(__file__).parent / 'logging.conf')

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


def get_s3_config(config_path: str, datetime_format: Dict[str, str] = {
        'date_time_format': ':%Y%m%dT%H%M%S',
        'date_format': ':%Y%m%d'}) -> Dict[str, Any]:
    config = config_compression(config_file(config_path, datetime_format=datetime_format))

    if 'temp_dir' in config:
        LOGGER.warning('`temp_dir` configuration option is deprecated and support will be removed in the future, use `work_dir` instead.')
        config['work_dir'] = config.pop('temp_dir')
        config['work_path'] = Path(config['work_dir'])
    if 'naming_convention' in config:
        LOGGER.warning(
            '`naming_convention` configuration option is deprecated and support will be removed in the future, use `path_template` instead.'
            ', `{timestamp}` key pattern is now replaced by `{date_time}`'
            ', and `{date}` key pattern is now replaced by `{date_time:%Y%m%d}`')
        config['path_template'] = config.pop('naming_convention') \
            .replace('{timestamp:', '{date_time:').replace('{date:', '{date_time:') \
            .replace('{timestamp}', '{date_time%s}' % datetime_format['date_time_format']) \
            .replace('{date}', '{date_time%s}' % datetime_format['date_format'])

    missing_params = {'s3_bucket'} - set(config.keys())
    if missing_params:
        raise Exception(f'Config is missing required settings: {missing_params}')

    return config


def upload_files(file_data: Dict[str, Any], config: Dict[str, Any]) -> None:
    if not config.get('local', False):
        s3_client = s3.create_client(config)
        for stream, file_info in file_data.items():
            if file_info['absolute_path'].exists():
                s3.upload_file(
                    s3_client,
                    str(file_info['absolute_path']),
                    config.get('s3_bucket'),
                    file_info['relative_path'],
                    encryption_type=config.get('encryption_type'),
                    encryption_key=config.get('encryption_key'))
                LOGGER.debug("{} file {} uploaded to {}".format(stream, file_info['relative_path'], config.get('s3_bucket')))

                # NOTE: Remove the local file(s)
                file_info['absolute_path'].unlink()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    config = get_s3_config(args.config)

    state, file_data = run(writelines(sys.stdin, config))

    # NOTE: Upload created files to S3
    upload_files(file_data, config)

    emit_state(state)
    LOGGER.debug('Exiting normally')


if __name__ == '__main__':  # pragma: no cover
    main()
