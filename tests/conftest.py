from io import BufferedReader, TextIOWrapper
import sys
import datetime
from datetime import datetime as dt, timezone, tzinfo
from pathlib import Path
import argparse
import json

from pytest import fixture


def clear_dir(dir_path):
    for path in dir_path.iterdir():
        path.unlink()
    dir_path.rmdir()


@fixture
def patch_datetime(monkeypatch):

    class mydatetime(dt):
        @classmethod
        def now(cls, tz: tzinfo = None):
            # NOTE: timestamp dt.fromtimestamp(1628663978.321056, tz=timezone.utc)
            d: dt = dt.strptime('2022-04-29 07:39:38.321056+01:00', '%Y-%m-%d %H:%M:%S.%f%z')
            return d.astimezone(tz) if tz else d

        @classmethod
        def utcnow(cls):
            return cls.now(timezone.utc).replace(tzinfo=None)

    monkeypatch.setattr(datetime, 'datetime', mydatetime)


@fixture  # (scope='session')
def temp_path(tmpdir_factory):

    return tmpdir_factory.mktemp('root_dir')


@fixture
def config_raw(temp_path):
    '''Use custom configuration set'''

    return {
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
        'date_time': datetime.datetime.now(),
        'work_path': Path(config_raw['work_dir']),
        'open_func': open
    }


@fixture
def file_metadata(temp_path):
    '''Use expected metadata'''

    return {
        'tap_dummy_test-test_table_one': {
            'part': 1,
            'path': {
                1: {'relative_path': 'tap_dummy_test-test_table_one-2022-04-29 06:39:38.321056+00:00.json',
                    'absolute_path': Path(f'{temp_path}/tests/output/tap_dummy_test-test_table_one-2022-04-29 06:39:38.321056+00:00.json')}},
            'file_data': []},
        'tap_dummy_test-test_table_two': {
            'part': 1,
            'path': {
                1: {'relative_path': 'tap_dummy_test-test_table_two-2022-04-29 06:39:38.321056+00:00.json',
                    'absolute_path': Path(f'{temp_path}/tests/output/tap_dummy_test-test_table_two-2022-04-29 06:39:38.321056+00:00.json')}},
            'file_data': []},
        'tap_dummy_test-test_table_three': {
            'part': 1,
            'path': {
                1: {'relative_path': 'tap_dummy_test-test_table_three-2022-04-29 06:39:38.321056+00:00.json',
                    'absolute_path': Path(f'{temp_path}/tests/output/tap_dummy_test-test_table_three-2022-04-29 06:39:38.321056+00:00.json')}},
            'file_data': [
                {"c_pk": 1, "c_varchar": "1", "c_int": 1, "c_time": "04:00:00"},
                {"c_pk": 2, "c_varchar": "2", "c_int": 2, "c_time": "07:15:00"},
                {"c_pk": 3, "c_varchar": "3", "c_int": 3, "c_time": "23:00:03"}]}}


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
def input_multi_stream_data():
    '''messages-with-three-streams.json'''

    return Path('tests', 'resources', 'messages-with-three-streams.json').read_text(encoding='utf-8')[:-1].split('\n')


@fixture
def patch_argument_parser(monkeypatch, temp_path, config_raw):

    temp_file = temp_path.join('config.json')
    temp_file.write_text(json.dumps(config_raw), encoding='utf-8')

    class argument_parser:

        def __init__(self):
            self.config = str(temp_file)

        def add_argument(self, x, y, help='Dummy config file', required=False):
            pass

        def parse_args(self):
            return self

    monkeypatch.setattr(argparse, 'ArgumentParser', argument_parser)


@fixture  # (scope='module')
def patch_sys_stdin(monkeypatch):

    # Get a file-like object in binary mode
    input_file = Path('tests', 'resources', 'messages-with-three-streams.json').open('rb')
    # Wrap it in a buffered reader with a 4096 byte buffer
    # This can also be used to read later from the buffer independently without consuming the IOReader
    buffered = BufferedReader(input_file, buffer_size=4096)
    # Could then first_bytes = buffered.peek(2048)
    # Wrap the buffered reader in a text io wrapper that can decode to unicode
    decoded = TextIOWrapper(buffered, encoding='utf-8')

    monkeypatch.setattr(sys, 'stdin', decoded)
