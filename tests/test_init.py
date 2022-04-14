'''Tests for the target_s3_jsonl.main module'''
# Standard library imports
from datetime import datetime as dt, timezone

# Third party imports
from pytest import fixture, raises
import boto3
from moto import mock_s3

from target import (
    sys,
    Path,
    datetime,
    argparse,
    json,
    save_jsonl_file,
)

# Package imports
from target_s3_jsonl import (
    upload_files,
    get_s3_config,
    main,
)


@fixture
def patch_datetime(monkeypatch):

    class mydatetime:
        @classmethod
        def utcnow(cls):
            return dt.fromtimestamp(1628663978.321056, tz=timezone.utc).replace(tzinfo=None)

        @classmethod
        def now(cls, x=timezone.utc, tz=None):
            return cls.utcnow()

        @classmethod
        def utcfromtimestamp(cls, x):
            return cls.utcnow()

        @classmethod
        def fromtimestamp(cls, x, format):
            return cls.utcnow()

        @classmethod
        def strptime(cls, x, format):
            return dt.strptime(x, format)

    monkeypatch.setattr(datetime, 'datetime', mydatetime)


@fixture
def patch_argument_parser(monkeypatch):

    class argument_parser:

        def __init__(self):
            self.config = str(Path('tests', 'resources', 'config.json'))

        def add_argument(self, x, y, help='Dummy config file', required=False):
            pass

        def parse_args(self):
            return self

    monkeypatch.setattr(argparse, 'ArgumentParser', argument_parser)


@fixture
def config():
    '''Use custom configuration set'''

    return {
        'file_type': 'jsonl',
        'add_metadata_columns': False,
        'aws_access_key_id': 'ACCESS-KEY',
        'aws_secret_access_key': 'SECRET',
        's3_bucket': 'BUCKET',
        'work_dir': 'tests/output',
        'memory_buffer': 2000000,
        'compression': 'none',
        'timezone_offset': 0,
        'path_template': '{stream}-{date_time:%Y%m%dT%H%M%S}.jsonl',
        'path_template_default': '{stream}-{date_time:%Y%m%dT%H%M%S}.json',
        'open_func': open
    }


@fixture
def input_multi_stream_data():
    '''Use custom parameters set'''

    with open(Path('tests', 'resources', 'messages-with-three-streams.json'), 'r', encoding='utf-8') as input_file:
        return [item for item in input_file]


@fixture
def state():
    '''Use expected state'''

    return {
        'currently_syncing': None,
        'bookmarks': {
            'tap_dummy_test-test_table_one': {'initial_full_table_complete': True},
            'tap_dummy_test-test_table_two': {'initial_full_table_complete': True},
            'tap_dummy_test-test_table_three': {'initial_full_table_complete': True}}}


@fixture
def file_metadata():
    '''Use expected metadata'''

    return {
        'tap_dummy_test-test_table_one': {
            'target_key': 'tap_dummy_test-test_table_one-20210811T063938.json',
            'file_name': Path('tests/output/tap_dummy_test-test_table_one-20210811T063938.json'),
            'file_data': []},
        'tap_dummy_test-test_table_two': {
            'target_key': 'tap_dummy_test-test_table_two-20210811T063938.json',
            'file_name': Path('tests/output/tap_dummy_test-test_table_two-20210811T063938.json'),
            'file_data': []},
        'tap_dummy_test-test_table_three': {
            'target_key': 'tap_dummy_test-test_table_three-20210811T063938.json',
            'file_name': Path('tests/output/tap_dummy_test-test_table_three-20210811T063938.json'),
            'file_data': [
                '{"c_pk": 1, "c_varchar": "1", "c_int": 1, "c_time": "04:00:00"}\n',
                '{"c_pk": 2, "c_varchar": "2", "c_int": 2, "c_time": "07:15:00"}\n',
                '{"c_pk": 3, "c_varchar": "3", "c_int": 3, "c_time": "23:00:03"}\n']}}


def clear_dir(dir_path):
    for path in dir_path.iterdir():
        path.unlink()
    dir_path.rmdir()


def test_get_s3_config(config):
    '''TEST : extract and enrich the configuration'''

    config.pop('file_type')
    assert get_s3_config(str(Path('tests', 'resources', 'config.json'))) == config

    assert get_s3_config(str(Path('tests', 'resources', 'config_naked.json'))) == {
        's3_bucket': 'BUCKET',
        'path_template': '{stream}-{date_time:%Y%m%dT%H%M%S}.json',
        'memory_buffer': 64e6,
        'compression': 'none',
        'path_template_default': '{stream}-{date_time:%Y%m%dT%H%M%S}.json',
        'open_func': open
    }

    with raises(Exception):
        get_s3_config(str(Path('tests', 'resources', 'config_no_bucket.json')))


@mock_s3
def test_upload_files(config, file_metadata):
    '''TEST : simple upload_files call'''

    Path(config['work_dir']).mkdir(parents=True, exist_ok=True)
    for _, file_info in file_metadata.items():
        save_jsonl_file(file_info, {'open_func': open})

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['s3_bucket'])

    upload_files(file_metadata, config)

    assert not file_metadata['tap_dummy_test-test_table_three']['file_name'].exists()

    clear_dir(Path(config['work_dir']))


@mock_s3
def test_main(monkeypatch, capsys, patch_datetime, patch_argument_parser, input_multi_stream_data, config, state, file_metadata):
    '''TEST : simple main call'''

    monkeypatch.setattr(sys, 'stdin', input_multi_stream_data)

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['s3_bucket'])

    main()

    captured = capsys.readouterr()
    assert captured.out == json.dumps(state) + '\n'

    for _, file_info in file_metadata.items():
        assert not file_info['file_name'].exists()

    class argument_parser:

        def __init__(self):
            self.config = str(Path('tests', 'resources', 'config_local.json'))

        def add_argument(self, x, y, help='Dummy config file', required=False):
            pass

        def parse_args(self):
            return self

    monkeypatch.setattr(argparse, 'ArgumentParser', argument_parser)

    main()

    captured = capsys.readouterr()
    assert captured.out == json.dumps(state) + '\n'

    for _, file_info in file_metadata.items():
        assert file_info['file_name'].exists()

    clear_dir(Path(config['work_dir']))
