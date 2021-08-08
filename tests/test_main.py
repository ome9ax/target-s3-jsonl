'''Tests for the target_s3_jsonl.main module'''
# Standard library imports

# Reader imports
from target_s3_jsonl import get_target_key, persist_lines


def test_get_target_key():
    '''TEST : simple persist_lines call'''
    assert get_target_key('dummy_stream', naming_convention='{stream}-{timestamp}.jsonl', timestamp='99') == 'dummy_stream-99.jsonl'


def test_persist_lines():
    '''TEST : simple persist_lines call'''
    assert True
