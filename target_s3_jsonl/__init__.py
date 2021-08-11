#!/usr/bin/env python3

__version__ = '0.0.4'

import argparse
import gzip
import lzma
import io
import json
from pathlib import Path
import sys
import tempfile
from datetime import datetime

from jsonschema import Draft4Validator, FormatChecker
from decimal import Decimal

from target_s3_jsonl import s3
from target_s3_jsonl.logger import get_logger

LOGGER = get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def float_to_decimal(value):
    '''Walk the given data structure and turn all instances of float into
    double.'''
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def get_target_key(message, naming_convention=None, timestamp=None, prefix=None):
    """Creates and returns an S3 key for the message"""
    if not naming_convention:
        naming_convention = '{stream}-{timestamp}.jsonl'

    # replace simple tokens
    key = naming_convention.format(
        stream=message['stream'],
        timestamp=timestamp if timestamp is not None else datetime.now().strftime('%Y%m%dT%H%M%S'),
        date=datetime.now().strftime('%Y%m%d'),
        time=datetime.now().strftime('%H%M%S'))

    # NOTE: Replace dynamic tokens
    # TODO: replace dynamic tokens such as {date(<format>)} with the date formatted as requested in <format>

    return str(Path(key).parent / f'{prefix}{Path(key).name}') if prefix else key


def save_file(file_data, compression):
    if any(file_data['file_data']):
        if compression == 'gzip':
            with open(file_data['file_name'], 'ab') as output_file:
                with gzip.open(output_file, 'wt', encoding='utf-8') as output_data:
                    output_data.writelines(file_data['file_data'])
        if compression == 'lzma':
            with open(file_data['file_name'], 'ab') as output_file:
                with lzma.open(output_file, 'wt', encoding='utf-8') as output_data:
                    output_data.writelines(file_data['file_data'])
        else:
            with open(file_data['file_name'], 'a', encoding='utf-8') as output_file:
                output_file.writelines(file_data['file_data'])
        del file_data['file_data'][:]


def upload_files(file_data, config):
    s3_client = s3.create_client(config)
    for stream, file_info in file_data.items():
        if file_info['file_name'].exists():
            s3.upload_file(
                str(file_info['file_name']),
                s3_client,
                config.get('s3_bucket'),
                file_info['target_key'],
                encryption_type=config.get('encryption_type'),
                encryption_key=config.get('encryption_key'))
            LOGGER.debug("{} file {} uploaded to {}".format(stream, file_info['target_key'], config.get('s3_bucket')))

            # NOTE: Remove the local file(s)
            file_info['file_name'].unlink()


def persist_lines(messages, config):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    naming_convention_default = '{stream}-{timestamp}.jsonl'
    naming_convention = config.get('naming_convention') or naming_convention_default
    compression = None

    if f"{config.get('compression')}".lower() == 'gzip':
        compression = 'gzip'
        naming_convention_default = f"{naming_convention_default}.gz"
        naming_convention = f"{naming_convention}.gz"

    elif f"{config.get('compression')}".lower() == 'lzma':
        compression = 'lzma'
        naming_convention_default = f"{naming_convention_default}.xz"
        naming_convention = f"{naming_convention}.xz"

    elif f"{config.get('compression')}".lower() not in {'', 'none'}:
        raise NotImplementedError(
            "Compression type '{}' is not supported. "
            "Expected: 'none', 'gzip', or 'lzma'"
            .format(f"{config.get('compression')}".lower()))

    # NOTE: Use the system specific temp directory if no custom temp_dir provided
    temp_dir = Path(config.get('temp_dir', tempfile.gettempdir())).expanduser()

    # NOTE: Create temp_dir if not exists
    temp_dir.mkdir(parents=True, exist_ok=True)

    file_data = {}
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    for message in messages:
        try:
            o = json.loads(message)
        except json.decoder.JSONDecodeError:
            LOGGER.error('Unable to parse:\n{}'.format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(message))
            stream = o['stream']
            if stream not in schemas:
                raise Exception('A record for stream {} was encountered before a corresponding schema'.format(stream))

            # NOTE: Validate record
            try:
                validators[stream].validate(float_to_decimal(o['record']))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    LOGGER.error(
                        "Data validation failed and cannot load to destination. RECORD: {}\n"
                        "'multipleOf' validations that allows long precisions are not supported"
                        " (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema."
                        .format(o['record']))
                    raise ex

            file_data[stream]['file_data'].append(json.dumps(o['record']) + '\n')

            # NOTE: write temporary file
            #       Use 64Mb default memory buffer
            if sys.getsizeof(file_data[stream]['file_data']) > config.get('memory_buffer', 64e6):
                save_file(file_data[stream], compression)

            state = None
        elif message_type == 'STATE':
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(message))
            stream = o['stream']
            schemas[stream] = float_to_decimal(o['schema'])
            validators[stream] = Draft4Validator(schemas[stream], format_checker=FormatChecker())

            if 'key_properties' not in o:
                raise Exception('key_properties field is required')
            key_properties[stream] = o['key_properties']
            LOGGER.debug('Setting schema for {}'.format(stream))

            # NOTE: get the s3 file key
            if stream not in file_data:
                file_data[stream] = {
                    'target_key': get_target_key(
                        o,
                        naming_convention=naming_convention,
                        timestamp=now,
                        prefix=config.get('s3_key_prefix', '')),
                    'file_name': temp_dir / naming_convention_default.format(stream=stream, timestamp=now),
                    'file_data': []}

        elif message_type == 'ACTIVATE_VERSION':
            LOGGER.debug('ACTIVATE_VERSION {}'.format(message))
        else:
            LOGGER.warning('Unknown message type {} in message {}'.format(o['type'], o))

    for _, file_info in file_data.items():
        save_file(file_info, compression)

    return state, file_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    with open(args.config) as input_json:
        config = json.load(input_json)

    missing_params = {'s3_bucket'} - set(config.keys())
    if missing_params:
        raise Exception('Config is missing required keys: {}'.format(missing_params))

    with io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8') as input_messages:
        state, file_data = persist_lines(input_messages, config)

    # NOTE: Upload created files to S3
    if not config.get('dry', False):
        upload_files(file_data, config)

    emit_state(state)
    LOGGER.debug('Exiting normally')


if __name__ == '__main__':
    main()
