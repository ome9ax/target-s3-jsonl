#!/usr/bin/env python3

import argparse
import gzip
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
        time=datetime.now().strftime('%H%M%S')
    )

    # replace dynamic tokens
    # TODO: replace dynamic tokens such as {date(<format>)} with the date formatted as requested in <format>

    if prefix:
        filename = key.split('/')[-1]
        key = key.replace(filename, f'{prefix}{filename}')
    return key


def upload_files(config, filenames):
    s3_client = s3.create_client(config)
    for temp_filename, target_key in filenames:
        s3.upload_file(str(temp_filename),
                       s3_client,
                       config.get('s3_bucket'),
                       target_key,
                       encryption_type=config.get('encryption_type'),
                       encryption_key=config.get('encryption_key'))
        LOGGER.info("File {} uploaded to {}".format(target_key, config.get('s3_bucket')))

        # Remove the local file(s)
        temp_filename.unlink()


def persist_lines(messages, config):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    naming_convention_default = '{stream}-{timestamp}.jsonl'
    naming_convention = config.get('naming_convention') if config.get('naming_convention') is not None else naming_convention_default
    compression = None

    if f"{config.get('compression')}".lower() == 'gzip':
        compression = 'gzip'
        naming_convention_default = f"{naming_convention_default}.gz"
        naming_convention = f"{naming_convention}.gz"

    elif f"{config.get('compression')}".lower() not in {'', 'none'}:
        raise NotImplementedError(
            "Compression type '{}' is not supported. "
            "Expected: 'none' or 'gzip'"
            .format(f"{config.get('compression')}".lower())
        )

    # Use the system specific temp directory if no custom temp_dir provided
    temp_dir = Path(config.get('temp_dir', tempfile.gettempdir())).expanduser()

    # Create temp_dir if not exists
    if temp_dir:
        temp_dir.mkdir(parents=True, exist_ok=True)

    filenames = []
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    for message in messages:
        try:
            o = json.loads(message)
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(message))
            if o['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Validate record
            try:
                validators[o['stream']].validate(float_to_decimal(o['record']))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    LOGGER.error("Data validation failed and cannot load to destination. RECORD: {}\n"
                                 "'multipleOf' validations that allows long precisions are not supported"
                                 " (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema."
                    .format(o['record']))
                    raise ex

            temp_filename = naming_convention_default.format(stream=o['stream'], timestamp=now)
            temp_filename = temp_dir / temp_filename

            # write temporary file
            # TODO: bufferise 16Mb file_bytes = sys.getsizeof(message) > 16_000_000
            if compression == 'gzip':
                with open(temp_filename, 'ab') as output_file:
                    with gzip.open(output_file, 'wt', encoding='utf-8') as output_data:
                        output_data.writelines(json.dumps(o['record']) + '\n')
            else:
                with open(temp_filename, 'a', encoding='utf-8') as output_file:
                    output_file.write(json.dumps(o['record']) + '\n')

            # queue the file for later s3 upload
            target_key = get_target_key(o,
                                        naming_convention=naming_convention,
                                        timestamp=now,
                                        prefix=config.get('s3_key_prefix', ''))
            if not (temp_filename, target_key) in filenames:
                filenames.append((temp_filename, target_key))

            state = None
        elif message_type == 'STATE':
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(message))
            stream = o['stream']
            schema = float_to_decimal(o['schema'])
            schemas[stream] = schema
            validators[stream] = Draft4Validator(schema, format_checker=FormatChecker())
            if 'key_properties' not in o:
                raise Exception('key_properties field is required')
            key_properties[stream] = o['key_properties']
        else:
            LOGGER.warning('Unknown message type {} in message {}'.format(o['type'], o))

    # Upload created files to S3
    upload_files(config, filenames)

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    with open(args.config) as input_json:
        config = json.load(input_json)

    missing_params = {'s3_bucket'} - set(config.keys())
    if missing_params:
        raise Exception('Config is missing required keys: {}'.format(missing_params))

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(input_messages, config)

    emit_state(state)
    LOGGER.debug('Exiting normally')


if __name__ == '__main__':
    main()
