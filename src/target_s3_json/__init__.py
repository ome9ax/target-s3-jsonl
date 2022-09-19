#!/usr/bin/env python3

__version__ = '2.0.0'

# from pathlib import Path

# Package imports
# from target._logger import get_logger
from .s3 import main

# LOGGER = get_logger(Path(__file__).with_name('logging.conf'))

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


if __name__ == '__main__':
    main()
