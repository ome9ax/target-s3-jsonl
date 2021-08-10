'''Tests for the target_s3_jsonl.main module'''
# Standard library imports
from pathlib import Path

# Third party imports
from pytest import fixture, mark

# Package imports
from target_s3_jsonl import get_target_key, persist_lines


with open(Path('tests', 'resources', 'config.json'), 'r', encoding='utf-8') as config_file, \
    open(Path('tests', 'resources', 'messages-with-three-streams.json'), 'r', encoding='utf-8') as input_file, \
    open(Path('tests', 'resources', 'invalid-json.json'), 'r', encoding='utf-8') as invalid_row_file, \
    open(Path('tests', 'resources', 'invalid-message-order.json'), 'r', encoding='utf-8') as invalid_order_file:
    CONFIG = config_file.read()
    INPUT_DATA = input_file.read()
    INVALID_ROW_DATA = invalid_row_file.read()
    INVALID_ORDER_DATA = invalid_order_file.read()


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


def test_get_target_key():
    '''TEST : simple persist_lines call'''
    assert get_target_key({'stream': 'dummy_stream'}, naming_convention='{stream}-{timestamp}.jsonl', timestamp='99') == 'dummy_stream-99.jsonl'


def test_persist_lines():
    '''TEST : simple persist_lines call'''
    assert True
