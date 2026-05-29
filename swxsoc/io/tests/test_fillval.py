"""
Unit tests for `swxsoc.io.fillval` helpers.

These tests exercise the pure-numpy NaN/mask/FILLVAL conversions in isolation
from spacepy.pycdf.
"""

import numpy as np
import pytest

from swxsoc.io import fillval as fv


# ----------------------------------------------------------------------
# Sentinel helpers
# ----------------------------------------------------------------------


def test_tt2000_fillval_int64():
    assert fv.tt2000_fillval_int64() == -9223372036854775808
    assert fv.tt2000_fillval_int64() == int(np.iinfo(np.int64).min)


def test_epoch_fillval_float():
    assert fv.epoch_fillval_float() == -1.0e31


# ----------------------------------------------------------------------
# Dtype checks
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "arr,expected",
    [
        (np.array([1.0, 2.0]), True),
        (np.array([1.0 + 2j]), True),
        (np.array([1, 2], dtype=np.int32), False),
        (np.array([b"a", b"b"]), False),
        (np.array(["a", "b"]), False),
    ],
)
def test_is_float_dtype(arr, expected):
    assert fv.is_float_dtype(arr) is expected


@pytest.mark.parametrize(
    "arr,expected",
    [
        (np.array([b"a", b"b"]), True),
        (np.array(["a", "b"]), True),
        (np.array([1.0]), False),
        (np.array([1], dtype=np.int8), False),
    ],
)
def test_is_string_dtype(arr, expected):
    assert fv.is_string_dtype(arr) is expected


# ----------------------------------------------------------------------
# compute_fill_mask
# ----------------------------------------------------------------------


def test_compute_fill_mask_float_nan_and_fillval():
    arr = np.array([1.0, np.nan, -1e31, 2.0])
    mask = fv.compute_fill_mask(arr, -1e31)
    np.testing.assert_array_equal(mask, [False, True, True, False])


def test_compute_fill_mask_float_no_fillval():
    arr = np.array([1.0, np.nan, 2.0])
    mask = fv.compute_fill_mask(arr, None)
    np.testing.assert_array_equal(mask, [False, True, False])


def test_compute_fill_mask_int():
    arr = np.array([0, -128, 5, -128], dtype=np.int8)
    mask = fv.compute_fill_mask(arr, np.int8(-128))
    np.testing.assert_array_equal(mask, [False, True, False, True])


def test_compute_fill_mask_int_no_fillval():
    arr = np.array([0, 1, 2], dtype=np.int8)
    mask = fv.compute_fill_mask(arr, None)
    np.testing.assert_array_equal(mask, [False, False, False])


def test_compute_fill_mask_string_bytes_vs_unicode():
    arr_bytes = np.array([b"a", b" ", b"c"])
    arr_str = np.array(["a", " ", "c"])
    # Mismatched kinds should still match the spec sentinel.
    np.testing.assert_array_equal(
        fv.compute_fill_mask(arr_bytes, " "), [False, True, False]
    )
    np.testing.assert_array_equal(
        fv.compute_fill_mask(arr_str, b" "), [False, True, False]
    )


def test_compute_fill_mask_string_does_not_treat_nan_as_fill():
    # On the read side, the literal bytes b"nan" must NOT map to a mask bit.
    arr = np.array([b"nan", b" ", b"x"])
    mask = fv.compute_fill_mask(arr, b" ")
    np.testing.assert_array_equal(mask, [False, True, False])


# ----------------------------------------------------------------------
# apply_fill_on_write
# ----------------------------------------------------------------------


def test_apply_fill_on_write_float_nan_replaced():
    arr = np.array([1.0, np.nan, 3.0])
    out = fv.apply_fill_on_write(arr, None, -1e31)
    np.testing.assert_array_equal(out, [1.0, -1e31, 3.0])


def test_apply_fill_on_write_float_mask_and_nan_combined():
    arr = np.array([1.0, np.nan, 3.0, 4.0])
    mask = np.array([False, False, False, True])
    out = fv.apply_fill_on_write(arr, mask, -1e31)
    np.testing.assert_array_equal(out, [1.0, -1e31, 3.0, -1e31])


def test_apply_fill_on_write_int_mask_only_dtype_preserved():
    arr = np.array([1, 2, 3], dtype=np.int16)
    mask = np.array([False, True, False])
    out = fv.apply_fill_on_write(arr, mask, np.int16(-32768))
    assert out.dtype == np.int16
    np.testing.assert_array_equal(out, [1, -32768, 3])


def test_apply_fill_on_write_int_no_mask_unchanged():
    arr = np.array([1, 2, 3], dtype=np.int16)
    out = fv.apply_fill_on_write(arr, None, np.int16(-32768))
    np.testing.assert_array_equal(out, [1, 2, 3])
    # Independent copy.
    assert out is not arr


def test_apply_fill_on_write_string_nan_convenience():
    # numpy coerces np.nan in an S array to the literal bytes b"nan"
    arr = np.array(["a", "b", "c", "d"], dtype="S3")
    arr[1] = np.nan  # becomes b"nan"
    out = fv.apply_fill_on_write(arr, None, b" ")
    assert out[1] == b" "
    assert out[0] == b"a"


def test_apply_fill_on_write_string_mask():
    arr = np.array([b"a", b"b", b"c"])
    mask = np.array([False, True, False])
    out = fv.apply_fill_on_write(arr, mask, b" ")
    np.testing.assert_array_equal(out, [b"a", b" ", b"c"])


def test_apply_fill_on_write_no_fillval_is_noop_copy():
    arr = np.array([1.0, np.nan, 3.0])
    out = fv.apply_fill_on_write(arr, np.array([False, False, True]), None)
    np.testing.assert_array_equal(out, arr, strict=False)
    assert np.isnan(out[1])
    assert out is not arr


def test_apply_fill_on_write_does_not_mutate_input():
    arr = np.array([1.0, np.nan, 3.0])
    snapshot = arr.copy()
    fv.apply_fill_on_write(arr, None, -1e31)
    np.testing.assert_array_equal(arr, snapshot, strict=False)
    assert np.isnan(arr[1])


# ----------------------------------------------------------------------
# apply_fillval_to_nan
# ----------------------------------------------------------------------


def test_apply_fillval_to_nan_float():
    arr = np.array([1.0, -1e31, 3.0])
    out = fv.apply_fillval_to_nan(arr, -1e31)
    assert np.isnan(out[1])
    assert out[0] == 1.0
    assert out[2] == 3.0


def test_apply_fillval_to_nan_int_is_noop():
    arr = np.array([1, -128, 3], dtype=np.int8)
    out = fv.apply_fillval_to_nan(arr, np.int8(-128))
    np.testing.assert_array_equal(out, arr)
    assert out.dtype == np.int8


def test_apply_fillval_to_nan_none_fillval_is_noop():
    arr = np.array([1.0, 2.0])
    out = fv.apply_fillval_to_nan(arr, None)
    np.testing.assert_array_equal(out, arr)
