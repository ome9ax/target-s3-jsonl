#!/usr/bin/env python3

__version__ = '1.2.2'

import argparse
import gzip
import lzma
import json
from pathlib import Path
import sys
import tempfile
import datetime
from uuid import uuid4

from jsonschema import Draft4Validator, FormatChecker
from adjust_precision_for_schema import adjust_decimal_precision_for_schema

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
    utcnow = timestamp.astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat()
    record_message['record'].update(
        _sdc_batched_at=utcnow,
        _sdc_deleted_at=record_message.get('record', {}).get('_sdc_deleted_at'),
        _sdc_extracted_at=record_message.get('time_extracted'),
        _sdc_primary_key=schema_message.get('key_properties'),
        _sdc_received_at=utcnow,
        _sdc_sequence=int(timestamp.timestamp() * 1e3),
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


def get_target_key(stream, config, timestamp=None, prefix=None):
    '''Creates and returns an S3 key for the stream'''

    # NOTE: Replace dynamic tokens
    key = config.get('naming_convention').format(
        stream=stream,
        timestamp=timestamp,
        date=timestamp,
        uuid=uuid4())

    return str(Path(key).parent / f'{prefix}{Path(key).name}') if prefix else key


def save_jsonl_file(file_data, open_func):
    if any(file_data['file_data']):
        with open_func(file_data['file_name'], 'at', encoding='utf-8') as output_file:
            output_file.writelines(file_data['file_data'])

        del file_data['file_data'][:]
        LOGGER.debug("'{}' file saved using open_func '{}'".format(file_data['file_name'], open_func.__name__))


def persist_lines(messages, config, save_records=save_jsonl_file):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    timezone = datetime.timezone(datetime.timedelta(hours=config.get('timezone_offset'))) if config.get('timezone_offset') is not None else None

    # NOTE: Use the system specific temp directory if no custom temp_dir provided
    temp_dir = Path(config.get('temp_dir', tempfile.gettempdir())).expanduser()

    # NOTE: Create temp_dir if not exists
    temp_dir.mkdir(parents=True, exist_ok=True)

    file_data = {}
    now = datetime.datetime.now(timezone)

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

            record_to_load = o['record']
            # NOTE: Validate record
            validators[stream].validate(record_to_load)

            if config.get('add_metadata_columns'):
                record_to_load = add_metadata_values_to_record(o, {}, now)
            else:
                record_to_load = remove_metadata_values_from_record(o)

            file_data[stream]['file_data'].append(json.dumps(record_to_load) + '\n')

            # NOTE: write the lines into the temporary file when received data over 64Mb default memory buffer
            if sys.getsizeof(file_data[stream]['file_data']) > config.get('memory_buffer'):
                save_records(file_data[stream], config.get('open_func'))

            state = None
        elif message_type == 'STATE':
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(message))
            stream = o['stream']
            schemas[stream] = o['schema']

            schemas[stream] = add_metadata_columns_to_schema(o) if config.get('add_metadata_columns') else o

            adjust_decimal_precision_for_schema(schemas[stream])

            # NOTE: prevent exception *** jsonschema.exceptions.UnknownType: Unknown type 'SCHEMA' for validator.
            #       'type' is a key word for jsonschema validator which is different from `{'type': 'SCHEMA'}` as the message type.
            schemas[stream].pop('type')
            validators[stream] = Draft4Validator(schemas[stream], format_checker=FormatChecker())

            if 'key_properties' not in o:
                raise Exception('key_properties field is required')
            key_properties[stream] = o['key_properties']
            LOGGER.debug('Setting schema for {}'.format(stream))

            # NOTE: get the s3 file key. Persistent array data storage.
            if stream not in file_data:
                file_data[stream] = {
                    'target_key': get_target_key(
                        stream=stream,
                        config=config,
                        timestamp=now,
                        prefix=config.get('s3_key_prefix', '')),
                    'file_name': temp_dir / config['naming_convention_default'].format(stream=stream, timestamp=now),
                    'file_data': []}

        elif message_type == 'ACTIVATE_VERSION':
            LOGGER.debug('ACTIVATE_VERSION {}'.format(message))
        else:
            LOGGER.warning('Unknown message type "{}" in message "{}"'.format(o['type'], o))

    for _, file_info in file_data.items():
        save_records(file_info, config.get('open_func'))

    return state, file_data


def config_file(config_path):
    datetime_format = {
        'timestamp_format': '%Y%m%dT%H%M%S',
        'date_format': '%Y%m%d'
    }

    naming_convention_default = '{stream}-{timestamp}.json' \
        .replace('{timestamp}', '{timestamp:' + datetime_format['timestamp_format'] + '}') \
        .replace('{date}', '{date:' + datetime_format['date_format'] + '}')

    config = {
        'compression': 'none',
        'naming_convention': naming_convention_default,
        'memory_buffer': 64e6
    }

    with open(config_path) as input_file:
        config.update(json.load(input_file))

    missing_params = {'s3_bucket'} - set(config.keys())
    if missing_params:
        raise Exception(f'Config is missing required settings: {missing_params}')

    unknown_params = set(config.keys()) - {
        'add_metadata_columns',
        'aws_access_key_id',
        'aws_secret_access_key',
        'aws_session_token',
        'aws_endpoint_url',
        'aws_profile',
        'role_arn',
        's3_bucket',
        's3_key_prefix',
        'encryption_type',
        'encryption_key',
        'compression',
        'naming_convention',
        'timezone_offset',
        'temp_dir',
        'local',
        'memory_buffer'
    }

    if unknown_params:
        raise Exception(f'Config unknown settings: {unknown_params}')

    config['naming_convention_default'] = naming_convention_default
    config['naming_convention'] = config['naming_convention'] \
        .replace('{timestamp}', '{timestamp:' + datetime_format['timestamp_format'] + '}') \
        .replace('{date}', '{date:' + datetime_format['date_format'] + '}')

    if f"{config.get('compression')}".lower() == 'gzip':
        config['open_func'] = gzip.open
        config['naming_convention_default'] = config['naming_convention_default'] + '.gz'
        config['naming_convention'] = config['naming_convention'] + '.gz'

    elif f"{config.get('compression')}".lower() == 'lzma':
        config['open_func'] = lzma.open
        config['naming_convention_default'] = config['naming_convention_default'] + '.xz'
        config['naming_convention'] = config['naming_convention'] + '.xz'

    elif f"{config.get('compression')}".lower() == 'none':
        config['open_func'] = open

    else:
        raise NotImplementedError(
            "Compression type '{}' is not supported. "
            "Expected: 'none', 'gzip', or 'lzma'"
            .format(f"{config.get('compression')}".lower()))

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    config = config_file(args.config)

    state, file_data = persist_lines(sys.stdin, config)

    # NOTE: Upload created files to S3
    upload_files(file_data, config)

    emit_state(state)
    LOGGER.debug('Exiting normally')


if __name__ == '__main__':  # pragma: no cover
    main()
