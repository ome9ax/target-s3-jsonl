#!/usr/bin/env python3

__version__ = '1.0.1'

import argparse
import gzip
import lzma
import json
from pathlib import Path
import sys
import tempfile
import datetime

from jsonschema import Draft4Validator, FormatChecker
from decimal import Decimal

from target_s3_jsonl import s3
from target_s3_jsonl.logger import get_logger

LOGGER = get_logger()


def add_metadata_columns_to_schema(schema_message):
    '''Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    '''
    schema_message['schema']['properties'].update(
        _sdc_batched_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_deleted_at={'type': ['null', 'string']},
        _sdc_extracted_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_primary_key={'type': ['null', 'string']},
        _sdc_received_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_sequence={'type': ['integer']},
        _sdc_table_version={'type': ['null', 'string']})

    return schema_message


def add_metadata_values_to_record(record_message, schema_message, timestamp):
    '''Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    '''
    now = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc).replace(tzinfo=None).isoformat()
    record_message['record'].update(
        _sdc_batched_at=now,
        _sdc_deleted_at=record_message.get('record', {}).get('_sdc_deleted_at'),
        _sdc_extracted_at=record_message.get('time_extracted'),
        _sdc_primary_key=schema_message.get('key_properties'),
        _sdc_received_at=now,
        _sdc_sequence=int(timestamp * 1e3),
        _sdc_table_version=record_message.get('version'))

    return record_message['record']


def remove_metadata_values_from_record(record_message):
    '''Removes every metadata _sdc column from a given record message
    '''
    for key in {
        '_sdc_batched_at',
        '_sdc_deleted_at',
        '_sdc_extracted_at',
        '_sdc_primary_key',
        '_sdc_received_at',
        '_sdc_sequence',
        '_sdc_table_version'
    }:

        record_message['record'].pop(key, None)

    return record_message['record']


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug('Emitting state {}'.format(line))
        sys.stdout.write('{}\n'.format(line))
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


def get_target_key(message, naming_convention=None, timestamp=None, prefix=None, timezone=None):
    '''Creates and returns an S3 key for the message'''
    if not naming_convention:
        naming_convention = '{stream}-{timestamp}.json'

    # replace simple tokens
    key = naming_convention.format(
        stream=message['stream'],
        timestamp=timestamp if timestamp is not None else datetime.datetime.now(timezone).strftime('%Y%m%dT%H%M%S'),
        date=datetime.datetime.now(timezone).strftime('%Y%m%d'),
        time=datetime.datetime.now(timezone).strftime('%H%M%S'))

    # NOTE: Replace dynamic tokens
    # TODO: replace dynamic tokens such as {date(<format>)} with the date formatted as requested in <format>

    return str(Path(key).parent / f'{prefix}{Path(key).name}') if prefix else key


def save_file(file_data, open_func):
    if any(file_data['file_data']):
        with open_func(file_data['file_name'], 'at', encoding='utf-8') as output_file:
            output_file.writelines(file_data['file_data'])

        del file_data['file_data'][:]
        LOGGER.debug("'{}' file saved using open_func '{}'".format(file_data['file_name'], open_func.__name__))


def upload_files(file_data, config):
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


def persist_lines(messages, config):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    naming_convention_default = '{stream}-{timestamp}.json'
    naming_convention = config.get('naming_convention') or naming_convention_default
    open_func = open
    timezone = datetime.timezone(datetime.timedelta(hours=config.get('timezone_offset'))) if config.get('timezone_offset') is not None else None

    if f"{config.get('compression')}".lower() == 'gzip':
        open_func = gzip.open
        naming_convention_default = f"{naming_convention_default}.gz"
        naming_convention = f"{naming_convention}.gz"

    elif f"{config.get('compression')}".lower() == 'lzma':
        open_func = lzma.open
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
    now = datetime.datetime.now(timezone)
    now_formatted = now.strftime('%Y%m%dT%H%M%S')

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
            record_to_load = o['record']
            try:
                validators[stream].validate(float_to_decimal(record_to_load))
            except Exception as ex:
                # NOTE: let anything but 'InvalidOperation' raised Exception slip by
                # And actual references of the validator logic can be find
                # at https://github.com/Julian/jsonschema/blob/main/jsonschema/_validators.py
                # logic covered in the 'jsonschema' package
                if type(ex).__name__ == "InvalidOperation":  # pragma: no cover
                    LOGGER.error(
                        "Data validation failed and cannot load to destination. RECORD: {}\n"
                        "'multipleOf' validations that allows long precisions are not supported"
                        " (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema."
                        .format(record_to_load))
                    raise ex

            if config.get('add_metadata_columns'):
                record_to_load = add_metadata_values_to_record(o, {}, now.timestamp())
            else:
                record_to_load = remove_metadata_values_from_record(o)

            file_data[stream]['file_data'].append(json.dumps(record_to_load) + '\n')

            # NOTE: write the lines into the temporary file when received data over 64Mb default memory buffer
            if sys.getsizeof(file_data[stream]['file_data']) > config.get('memory_buffer', 64e6):
                save_file(file_data[stream], open_func)

            state = None
        elif message_type == 'STATE':
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(message))
            stream = o['stream']

            if config.get('add_metadata_columns'):
                schemas[stream] = add_metadata_columns_to_schema(o)
            else:
                schemas[stream] = float_to_decimal(o['schema'])

            validators[stream] = Draft4Validator(schemas[stream], format_checker=FormatChecker())

            if 'key_properties' not in o:
                raise Exception('key_properties field is required')
            key_properties[stream] = o['key_properties']
            LOGGER.debug('Setting schema for {}'.format(stream))

            # NOTE: get the s3 file key. Persistent array data storage.
            if stream not in file_data:
                file_data[stream] = {
                    'target_key': get_target_key(
                        o,
                        naming_convention=naming_convention,
                        timestamp=now_formatted,
                        prefix=config.get('s3_key_prefix', ''),
                        timezone=timezone),
                    'file_name': temp_dir / naming_convention_default.format(stream=stream, timestamp=now_formatted),
                    'file_data': []}

        elif message_type == 'ACTIVATE_VERSION':
            LOGGER.debug('ACTIVATE_VERSION {}'.format(message))
        else:
            LOGGER.warning('Unknown message type "{}" in message "{}"'.format(o['type'], o))

    for _, file_info in file_data.items():
        save_file(file_info, open_func)

    return state, file_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    with open(args.config) as input_file:
        config = json.load(input_file)

    missing_params = {'s3_bucket'} - set(config.keys())
    if missing_params:
        raise Exception('Config is missing required keys: {}'.format(missing_params))

    state, file_data = persist_lines(sys.stdin, config)

    # NOTE: Upload created files to S3
    if not config.get('local', False):
        upload_files(file_data, config)

    emit_state(state)
    LOGGER.debug('Exiting normally')


if __name__ == '__main__':  # pragma: no cover
    main()
