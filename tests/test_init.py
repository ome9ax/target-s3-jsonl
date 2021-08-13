'''Tests for the target_s3_jsonl.main module'''
# Standard library imports
from copy import deepcopy
from datetime import datetime as dt, timezone

# Third party imports
from pytest import fixture, raises

# Package imports
from target_s3_jsonl import (
    sys,
    Decimal,
    datetime,
    argparse,
    gzip,
    lzma,
    json,
    Path,
    s3,
    add_metadata_columns_to_schema,
    add_metadata_values_to_record,
    remove_metadata_values_from_record,
    emit_state,
    float_to_decimal,
    get_target_key,
    save_file,
    upload_files,
    persist_lines,
    main,
)


@fixture
def patch_datetime(monkeypatch):

    class mydatetime:
        @classmethod
        def utcnow(cls):
            return dt.fromtimestamp(1628663978.321056, tz=timezone.utc).replace(tzinfo=None)

        @classmethod
        def now(cls, x=timezone.utc):
            return cls.utcnow()

        @classmethod
        def utcfromtimestamp(cls, x):
            return cls.utcnow()

        @classmethod
        def fromtimestamp(cls, x):
            return cls.utcnow()

    monkeypatch.setattr(datetime, 'datetime', mydatetime)


@fixture
def config():
    '''Use custom parameters set'''

    with open(Path('tests', 'resources', 'config.json'), 'r', encoding='utf-8') as config_file:
        return json.load(config_file)


@fixture
def input_data():
    '''Use custom parameters set'''

    with open(Path('tests', 'resources', 'messages-with-three-streams.json'), 'r', encoding='utf-8') as input_file:
        return [item for item in input_file]


@fixture
def invalid_row_data():
    '''Use custom parameters set'''

    with open(Path('tests', 'resources', 'invalid-json.json'), 'r', encoding='utf-8') as input_file:
        return [item for item in input_file]


@fixture
def invalid_order_data():
    '''Use custom parameters set'''

    with open(Path('tests', 'resources', 'invalid-message-order.json'), 'r', encoding='utf-8') as input_file:
        return [item for item in input_file]


@fixture
def state():
    '''Use custom parameters set'''

    return {
        'currently_syncing': None,
        'bookmarks': {
            'tap_dummy_test-test_table_one': {'initial_full_table_complete': True},
            'tap_dummy_test-test_table_two': {'initial_full_table_complete': True},
            'tap_dummy_test-test_table_three': {'initial_full_table_complete': True}}}


@fixture
def file_metadata():
    '''Use custom parameters set'''

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


def test_emit_state(capsys, state):
    '''TEST : simple emit_state call'''

    emit_state(state)
    captured = capsys.readouterr()
    assert captured.out == json.dumps(state) + '\n'

    emit_state(None)
    captured = capsys.readouterr()
    assert captured.out == ''


def test_add_metadata_columns_to_schema():
    '''TEST : simple add_metadata_columns_to_schema call'''

    assert add_metadata_columns_to_schema({
        "type": "SCHEMA", "stream": "tap_dummy_test-test_table_one",
        "schema": {
            "properties": {
                "c_pk": {
                    "inclusion": "automatic", "minimum": -2147483648, "maximum": 2147483647,
                    "type": ["null", "integer"]},
                "c_varchar": {
                    "inclusion": "available", "maxLength": 16, "type": ["null", "string"]},
                "c_int": {
                    "inclusion": "available", "minimum": -2147483648, "maximum": 2147483647,
                    "type": ["null", "integer"]}},
            "type": "object"},
        "key_properties": ["c_pk"]}) == {
            'type': 'SCHEMA', 'stream': 'tap_dummy_test-test_table_one',
            'schema': {
                'properties': {
                    'c_pk': {
                        'inclusion': 'automatic', 'minimum': -2147483648, 'maximum': 2147483647,
                        'type': ['null', 'integer']},
                    'c_varchar': {
                        'inclusion': 'available', 'maxLength': 16, 'type': ['null', 'string']},
                    'c_int': {
                        'inclusion': 'available', 'minimum': -2147483648, 'maximum': 2147483647,
                        'type': ['null', 'integer']},
                    '_sdc_batched_at': {
                        'type': ['null', 'string'], 'format': 'date-time'},
                    '_sdc_deleted_at': {'type': ['null', 'string']},
                    '_sdc_extracted_at': {'type': ['null', 'string'], 'format': 'date-time'},
                    '_sdc_primary_key': {'type': ['null', 'string']},
                    '_sdc_received_at': {'type': ['null', 'string'], 'format': 'date-time'},
                    '_sdc_sequence': {'type': ['integer']},
                    '_sdc_table_version': {'type': ['null', 'string']}},
                'type': 'object'},
            'key_properties': ['c_pk']}


def test_add_metadata_values_to_record():
    '''TEST : simple add_metadata_values_to_record call'''

    assert add_metadata_values_to_record({
        "type": "RECORD", "stream": "tap_dummy_test-test_table_one",
        "record": {
            "c_pk": 1, "c_varchar": "1", "c_int": 1, "c_float": 1.99},
        "version": 1, "time_extracted": "2019-01-31T15:51:47.465408Z"}, {}, 1628713605.321056) == {
            'c_pk': 1, 'c_varchar': '1', 'c_int': 1, 'c_float': 1.99,
            '_sdc_batched_at': '2021-08-11T20:26:45.321056',
            '_sdc_deleted_at': None,
            '_sdc_extracted_at': '2019-01-31T15:51:47.465408Z',
            '_sdc_primary_key': None,
            '_sdc_received_at': '2021-08-11T20:26:45.321056',
            '_sdc_sequence': 1628713605321,
            '_sdc_table_version': 1}


def test_remove_metadata_values_from_record():
    '''TEST : simple remove_metadata_values_from_record call'''

    assert remove_metadata_values_from_record({
        "type": "RECORD", "stream": "tap_dummy_test-test_table_one",
        "record": {
            "c_pk": 1, "c_varchar": "1", "c_int": 1, "c_float": 1.99,
            '_sdc_batched_at': '2021-08-11T21:16:22.420939',
            '_sdc_deleted_at': None,
            '_sdc_extracted_at': '2019-01-31T15:51:47.465408Z',
            '_sdc_primary_key': None,
            '_sdc_received_at': '2021-08-11T21:16:22.420939',
            '_sdc_sequence': 1628712982421,
            '_sdc_table_version': 1},
        "version": 1, "time_extracted": "2019-01-31T15:51:47.465408Z"}) == {
            'c_pk': 1, 'c_varchar': '1', 'c_int': 1, 'c_float': 1.99}


def test_float_to_decimal():
    '''TEST : simple float_to_decimal call'''

    assert float_to_decimal({
        "type": "RECORD", "stream": "tap_dummy_test-test_table_one",
        "record": {
            "c_pk": 1, "c_varchar": "1", "c_int": 1, "c_float": 1.99},
        "version": 1, "time_extracted": "2019-01-31T15:51:47.465408Z"}) == {
            "type": "RECORD", "stream": "tap_dummy_test-test_table_one",
            "record": {
                "c_pk": 1, "c_varchar": "1", "c_int": 1, "c_float": Decimal('1.99')},
            "version": 1, "time_extracted": "2019-01-31T15:51:47.465408Z"}


def test_get_target_key():
    '''TEST : simple get_target_key call'''

    assert get_target_key({'stream': 'dummy_stream'}, timestamp='99') == 'dummy_stream-99.json'
    assert get_target_key({'stream': 'dummy_stream'}, naming_convention='xxx-{stream}-{timestamp}.jsonl', timestamp='99') == 'xxx-dummy_stream-99.jsonl'


def test_save_file(config, file_metadata):
    '''TEST : simple save_file call'''
    Path(config['temp_dir']).mkdir(parents=True, exist_ok=True)

    # NOTE: test compression saved file
    for open_func, extension in {open: '', gzip.open: '.gz', lzma.open: '.xz'}.items():
        file_metadata_copy = deepcopy(file_metadata)
        for _, file_info in file_metadata_copy.items():
            file_info['file_name'] = file_info['file_name'].parent / f"{file_info['file_name'].name}{extension}"
            save_file(file_info, open_func)

        assert not file_metadata_copy['tap_dummy_test-test_table_one']['file_name'].exists()
        assert not file_metadata_copy['tap_dummy_test-test_table_two']['file_name'].exists()
        assert file_metadata_copy['tap_dummy_test-test_table_three']['file_name'].exists()

        with open_func(file_metadata_copy['tap_dummy_test-test_table_three']['file_name'], 'rt', encoding='utf-8') as input_file:
            assert [item for item in input_file] == file_metadata['tap_dummy_test-test_table_three']['file_data']

        del file_metadata_copy

    clear_dir(Path(config['temp_dir']))


def test_upload_files(monkeypatch, config, file_metadata):
    '''TEST : simple upload_files call'''

    monkeypatch.setattr(s3, 'create_client', lambda config: None)

    monkeypatch.setattr(
        s3, 'upload_file', lambda filename, s3_client, bucket, s3_key,
        encryption_type=None, encryption_key=None: None)

    Path(config['temp_dir']).mkdir(parents=True, exist_ok=True)
    for _, file_info in file_metadata.items():
        save_file(file_info, open)

    upload_files(file_metadata, config)

    assert not file_metadata['tap_dummy_test-test_table_three']['file_name'].exists()

    clear_dir(Path(config['temp_dir']))


def test_persist_lines(config, input_data, invalid_row_data, invalid_order_data, state, file_metadata):
    '''TEST : simple persist_lines call'''
    output_state, output_file_metadata = persist_lines(input_data, config)
    file_paths = set(path for path in Path(config['temp_dir']).iterdir())

    assert output_state == state

    assert len(file_paths) == 3

    assert len(set(str(values['file_name']) for _, values in output_file_metadata.items()) - set(str(path) for path in file_paths)) == 0

    with open(output_file_metadata['tap_dummy_test-test_table_three']['file_name'], 'r', encoding='utf-8') as input_file:
        assert [item for item in input_file] == file_metadata['tap_dummy_test-test_table_three']['file_data']


    for compression, extension in {'gzip': '.gz', 'lzma': '.xz', 'none': ''}.items():
        clear_dir(Path(config['temp_dir']))
        config_copy = deepcopy(config)
        config_copy['compression'] = compression
        output_state, output_file_metadata = persist_lines(input_data, config_copy)
        file_paths = set(path for path in Path(config['temp_dir']).iterdir())

        assert len(file_paths) == 3

        assert len(set(str(values['file_name']) for _, values in output_file_metadata.items()) - set(str(path) for path in file_paths)) == 0

    clear_dir(Path(config['temp_dir']))

    with raises(NotImplementedError):
        config_copy = deepcopy(config)
        config_copy['compression'] = 'dummy'
        output_state, output_file_metadata = persist_lines(input_data, config_copy)

    with raises(json.decoder.JSONDecodeError):
        output_state, output_file_metadata = persist_lines(invalid_row_data, config)

    with raises(Exception):
        output_state, output_file_metadata = persist_lines(invalid_order_data, config)


def test_main(monkeypatch, capsys, patch_datetime, input_data, config, state, file_metadata):
    '''TEST : simple persist_lines call'''

    class argument_parser:

        def __init__(self):
            self.config = str(Path('tests', 'resources', 'config.json'))

        def add_argument(self, x, y, help='Dummy config file', required=False):
            pass

        def parse_args(self):
            return self

    monkeypatch.setattr(argparse, 'ArgumentParser', argument_parser)

    monkeypatch.setattr(sys, 'stdin', input_data)

    main()
    captured = capsys.readouterr()
    assert captured.out == json.dumps(state) + '\n'

    for _, file_info in file_metadata.items():
        assert file_info['file_name'].exists()

    clear_dir(Path(config['temp_dir']))

    with raises(Exception):

        class argument_parser:

            def __init__(self):
                self.config = str(Path('tests', 'resources', 'config_no_bucket.json'))

            def add_argument(self, x, y, help='Dummy config file', required=False):
                pass

            def parse_args(self):
                return self

        monkeypatch.setattr(argparse, 'ArgumentParser', argument_parser)

        main()

    class argument_parser:

        def __init__(self):
            self.config = str(Path('tests', 'resources', 'config_local_false.json'))

        def add_argument(self, x, y, help='Dummy config file', required=False):
            pass

        def parse_args(self):
            return self

    monkeypatch.setattr(argparse, 'ArgumentParser', argument_parser)

    monkeypatch.setattr(s3, 'create_client', lambda config: None)

    monkeypatch.setattr(
        s3, 'upload_file', lambda filename, s3_client, bucket, s3_key,
        encryption_type=None, encryption_key=None: None)

    main()
    captured = capsys.readouterr()
    assert captured.out == json.dumps(state) + '\n'
