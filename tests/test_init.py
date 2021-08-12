'''Tests for the target_s3_jsonl.main module'''
# Standard library imports
from datetime import datetime as dt

# Third party imports
from pytest import fixture

# Package imports
from target_s3_jsonl import (
    Decimal,
    datetime,
    Path,
    json,
    s3,
    emit_state,
    float_to_decimal,
    get_target_key,
    save_file,
    upload_files,
    persist_lines,
    add_metadata_columns_to_schema,
    add_metadata_values_to_record,
    remove_metadata_values_from_record
)


with open(Path('tests', 'resources', 'messages-with-three-streams.json'), 'r', encoding='utf-8') as input_file, \
    open(Path('tests', 'resources', 'invalid-json.json'), 'r', encoding='utf-8') as invalid_row_file, \
        open(Path('tests', 'resources', 'invalid-message-order.json'), 'r', encoding='utf-8') as invalid_order_file:
    INPUT_DATA = [item for item in input_file]
    INVALID_ROW_DATA = [item for item in invalid_row_file]
    INVALID_ORDER_DATA = [item for item in invalid_order_file]


@fixture
def patch_datetime_now(monkeypatch):

    class mydatetime:
        @classmethod
        def now(cls, x):
            return dt(2021, 8, 11, 21, 26, 45, 321056).replace(tzinfo=None)

    monkeypatch.setattr(datetime, 'datetime', mydatetime)


@fixture
def config():
    '''Use custom parameters set'''

    with open(Path('tests', 'resources', 'config.json'), 'r', encoding='utf-8') as config_file:
        return json.load(config_file)


@fixture
def input_data():
    '''Use custom parameters set'''

    return INPUT_DATA


@fixture
def invalid_row_data():
    '''Use custom parameters set'''

    return INVALID_ROW_DATA


@fixture
def invalid_order_data():
    '''Use custom parameters set'''

    return INVALID_ORDER_DATA


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
            'target_key': 'tap_dummy_test-test_table_one-20210811T063938.jsonl',
            'file_name': Path('tests/output/tap_dummy_test-test_table_one-20210811T063938.jsonl'),
            'file_data': []},
        'tap_dummy_test-test_table_two': {
            'target_key': 'tap_dummy_test-test_table_two-20210811T063938.jsonl',
            'file_name': Path('tests/output/tap_dummy_test-test_table_two-20210811T063938.jsonl'),
            'file_data': []},
        'tap_dummy_test-test_table_three': {
            'target_key': 'tap_dummy_test-test_table_three-20210811T063938.jsonl',
            'file_name': Path('tests/output/tap_dummy_test-test_table_three-20210811T063938.jsonl'),
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


def test_add_metadata_values_to_record(patch_datetime_now):
    '''TEST : simple add_metadata_values_to_record call'''

    assert add_metadata_values_to_record({
        "type": "RECORD", "stream": "tap_dummy_test-test_table_one",
        "record": {
            "c_pk": 1, "c_varchar": "1", "c_int": 1, "c_float": 1.99},
        "version": 1, "time_extracted": "2019-01-31T15:51:47.465408Z"}, {}) == {
            'c_pk': 1, 'c_varchar': '1', 'c_int': 1, 'c_float': 1.99,
            '_sdc_batched_at': '2021-08-11T21:26:45.321056',
            '_sdc_deleted_at': None,
            '_sdc_extracted_at': '2019-01-31T15:51:47.465408Z',
            '_sdc_primary_key': None,
            '_sdc_received_at': '2021-08-11T21:26:45.321056',
            '_sdc_sequence': 1628713605000,
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

    assert get_target_key({'stream': 'dummy_stream'}, naming_convention='{stream}-{timestamp}.jsonl', timestamp='99') == 'dummy_stream-99.jsonl'


def test_save_file(config, file_metadata):
    '''TEST : simple save_file call'''
    Path(config['temp_dir']).mkdir(parents=True, exist_ok=True)
    for _, file_info in file_metadata.items():
        save_file(file_info, 'none')

    assert not file_metadata['tap_dummy_test-test_table_one']['file_name'].exists()
    assert not file_metadata['tap_dummy_test-test_table_two']['file_name'].exists()
    assert file_metadata['tap_dummy_test-test_table_three']['file_name'].exists()

    clear_dir(Path(config['temp_dir']))


def test_upload_files(monkeypatch, config, file_metadata):
    '''TEST : simple upload_files call'''

    monkeypatch.setattr(s3, 'create_client', lambda config: None)

    monkeypatch.setattr(
        s3, 'upload_file', lambda filename, s3_client, bucket, s3_key,
        encryption_type=None, encryption_key=None: None)

    Path(config['temp_dir']).mkdir(parents=True, exist_ok=True)
    for _, file_info in file_metadata.items():
        save_file(file_info, 'none')

    upload_files(file_metadata, config)

    assert not file_metadata['tap_dummy_test-test_table_three']['file_name'].exists()

    clear_dir(Path(config['temp_dir']))


def test_persist_lines(config, input_data, state, file_metadata):
    '''TEST : simple persist_lines call'''
    output_state, output_file_metadata = persist_lines(input_data, config)
    file_paths = set(path for path in Path(config['temp_dir']).iterdir())

    assert len(file_paths) == 3

    assert output_state == state

    assert len(set(str(values['file_name']) for _, values in output_file_metadata.items()) - set(str(path) for path in file_paths)) == 0

    with open(output_file_metadata['tap_dummy_test-test_table_three']['file_name'], 'r', encoding='utf-8') as input_file:
        assert [item for item in input_file] == file_metadata['tap_dummy_test-test_table_three']['file_data']

    clear_dir(Path(config['temp_dir']))
