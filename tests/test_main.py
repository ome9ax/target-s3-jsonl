'''Tests for the target_s3_jsonl.main module'''
# Standard library imports

# Third party imports
from pytest import fixture, mark

# Package imports
from target_s3_jsonl import Path, json, get_target_key, save_file, persist_lines


with open(Path('tests', 'resources', 'config.json'), 'r', encoding='utf-8') as config_file, \
    open(Path('tests', 'resources', 'messages-with-three-streams.json'), 'r', encoding='utf-8') as input_file, \
    open(Path('tests', 'resources', 'invalid-json.json'), 'r', encoding='utf-8') as invalid_row_file, \
        open(Path('tests', 'resources', 'invalid-message-order.json'), 'r', encoding='utf-8') as invalid_order_file:
    CONFIG = json.load(config_file)
    INPUT_DATA = [item for item in input_file]
    INVALID_ROW_DATA = [item for item in invalid_row_file]
    INVALID_ORDER_DATA = [item for item in invalid_order_file]


@fixture
def config():
    '''Use custom parameters set'''

    return CONFIG


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
                '{"c_pk": 3, "c_varchar": "3", "c_int": 3, "c_time": "23:00:03", "_sdc_deleted_at": "2019-02-10T15:51:50.215998Z"}\n']}
        }


def test_get_target_key():
    '''TEST : simple persist_lines call'''
    assert get_target_key({'stream': 'dummy_stream'}, naming_convention='{stream}-{timestamp}.jsonl', timestamp='99') == 'dummy_stream-99.jsonl'


def test_save_file(config, file_metadata):
    Path(config['temp_dir']).mkdir(parents=True, exist_ok=True)
    save_file(file_metadata['tap_dummy_test-test_table_three'], 'none')

    assert file_metadata['tap_dummy_test-test_table_three']['file_name'].exists()

    clear_dir(Path(config['temp_dir']))


def test_persist_lines(config, input_data, file_metadata):
    '''TEST : simple persist_lines call'''
    state, output_file_metadata = persist_lines(input_data, config)
    file_paths = set(path for path in Path(config['temp_dir']).iterdir())

    assert len(file_paths) == 3

    assert len(set(str(values['file_name']) for _, values in output_file_metadata.items())
        - set(str(path) for path in file_paths)) == 0

    with open(output_file_metadata['tap_dummy_test-test_table_three']['file_name'], 'r', encoding='utf-8') as input_file:
        input_data = [item for item in input_file]

    assert input_data == file_metadata['tap_dummy_test-test_table_three']['file_data']
    # breakpoint()

    clear_dir(Path(config['temp_dir']))


def clear_dir(dir_path):
    for path in dir_path.iterdir():
        path.unlink()
    dir_path.rmdir()
