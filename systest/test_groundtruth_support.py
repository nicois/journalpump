# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""Unit tests for the semantic JSON comparator (Task 32 review item 13).

These exercise ``groundtruth_support.semantic_equal`` directly — no
binary, no journal fixture — pinning the type-awareness the parsed-object
``==`` did not have (``true == 1``), exact large-integer comparison, and
the non-JSON byte fallback.
"""

import pytest

from systest import groundtruth_support as gt


def test_assert_semantic_equal_is_silent_on_bytes():
    # Byte-identical: no warning, no failure.
    gt.assert_semantic_equal(b'{"a": 1}', b'{"a": 1}', "line 0")
    gt.assert_semantic_equal("plain non-json", "plain non-json", "line 1")


def test_assert_semantic_equal_warns_on_byte_diff():
    with pytest.warns(UserWarning, match="byte diff"):
        gt.assert_semantic_equal(b'{"a": 1, "b": 2}', b'{"b":2,"a":1}', "line 0")


def test_assert_semantic_equal_fails_on_semantic_diff():
    with pytest.raises(AssertionError, match="differs semantically"):
        gt.assert_semantic_equal(b'{"a": 1}', b'{"a": 2}', "line 0")


def test_bool_is_not_a_number():
    assert not gt.semantic_equal(b'{"a": true}', b'{"a": 1}')
    assert not gt.semantic_equal(b'{"a": false}', b'{"a": 0}')
    assert gt.semantic_equal(b'{"a": true}', b'{"a": true}')


def test_key_order_and_whitespace_are_free():
    assert gt.semantic_equal(b'{"a": 1, "b": 2}', b'{"b":2,"a":1}')
    assert gt.semantic_equal(b"[1, 2]", b"[ 1 , 2 ]")


def test_large_integers_keep_precision():
    big = b'{"n": 9007199254740993}'  # 2^53 + 1
    assert gt.semantic_equal(big, big)
    assert not gt.semantic_equal(big, b'{"n": 9007199254740992}')


def test_numeric_spellings_compare_numerically():
    assert gt.semantic_equal(b'{"n": 6}', b'{"n": 6.0}')
    assert not gt.semantic_equal(b'{"n": 6}', b'{"n": 7}')


def test_type_mismatch_is_not_equal():
    assert not gt.semantic_equal(b'{"a": "1"}', b'{"a": 1}')
    assert not gt.semantic_equal(b'{"a": null}', b'{"a": 0}')
    assert not gt.semantic_equal(b'[1]', b'{"0": 1}')


def test_nested_structures():
    assert not gt.semantic_equal(b'{"a": [true]}', b'{"a": [1]}')
    assert gt.semantic_equal(b'{"a": {"b": [1, null]}}', b'{"a": {"b": [1, null]}}')
    assert not gt.semantic_equal(b'{"a": {"b": [1, null]}}', b'{"a": {"b": [1]}}')


def test_non_json_falls_back_to_bytes():
    assert gt.semantic_equal(b"not json", b"not json")
    assert not gt.semantic_equal(b"not json", b"other")


def test_nonstandard_constants_do_not_match_json():
    # json.loads accepts NaN/Infinity by default; the comparator rejects
    # them, so they never silently match a real JSON value.
    assert not gt.semantic_equal(b'{"a": NaN}', b'{"a": 1}')
    assert not gt.semantic_equal(b'{"a": Infinity}', b'{"a": 1}')
