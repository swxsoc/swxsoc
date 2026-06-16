"""
Helpers for converting between in-memory NaN/mask representations and
on-disk CDF FILLVAL sentinels.

This module is intentionally CDF-agnostic; it operates on plain numpy arrays
and scalar FILLVAL values so it can be unit-tested in isolation from
:mod:`spacepy.pycdf`.

Conventions
-----------
- Floats round-trip both ``np.nan`` and a parallel boolean ``mask``.
- Integers preserve dtype; the FILLVAL sentinel stays in ``.data`` and the
  mask marks fill positions.
- Strings (``S`` / ``U`` dtypes) use the ISTP single-space sentinel
  (``b" "`` / ``" "``).  As a write-time convenience numpy's coercion of
  ``np.nan`` to the literal bytes ``b"nan"`` (and ``b"NaN"``) is also treated
  as fill.  The reader is strict: only the spec sentinel maps to a mask bit.
- The ISTP FILLVAL values for specific CDF types (including the Epoch
  variants) are owned by :func:`swxsoc.io.fillval.get_fillval`.
"""

from typing import Any

import numpy as np

from swxsoc.util import const

__all__ = [
    "is_float_dtype",
    "is_string_dtype",
    "compute_fill_mask",
    "apply_fill_on_write",
    "apply_fillval_to_nan",
]

# ----------------------------------------------------------------------
# ISTP FILLVAL Sentinel Values
# ----------------------------------------------------------------------


def get_fillval(cdf_type: int):
    """
    Function to return the ISTP FILLVAL sentinel value for a given CDF type.

    Parameters
    ----------
    cdf_type : int
        The CDF type code (numeric) for which to retrieve the FILLVAL.

    Returns
    -------
    scalar
        The FILLVAL sentinel value corresponding to the provided CDF type.
    """

    # Fill value, indexed by the CDF type (numeric)
    fillvals = {}
    # Integers
    for i in (1, 2, 4, 8):
        fillvals[getattr(const, "CDF_INT{}".format(i)).value] = -(2 ** (8 * i - 1))
        if i == 8:
            continue
        fillvals[getattr(const, "CDF_UINT{}".format(i)).value] = 2 ** (8 * i) - 1
    fillvals[const.CDF_EPOCH16.value] = (-1e31, -1e31)
    fillvals[const.CDF_REAL8.value] = -1e31
    fillvals[const.CDF_REAL4.value] = -1e31
    fillvals[const.CDF_CHAR.value] = " "
    fillvals[const.CDF_UCHAR.value] = " "
    # Equivalent pairs
    for cdf_t, equiv in (
        (const.CDF_TIME_TT2000, const.CDF_INT8),
        (const.CDF_EPOCH, const.CDF_REAL8),
        (const.CDF_BYTE, const.CDF_INT1),
        (const.CDF_FLOAT, const.CDF_REAL4),
        (const.CDF_DOUBLE, const.CDF_REAL8),
    ):
        fillvals[cdf_t.value] = fillvals[equiv.value]
    value = fillvals[cdf_type]
    return value


# ----------------------------------------------------------------------
# Dtype checks
# ----------------------------------------------------------------------


def is_float_dtype(arr: np.ndarray) -> bool:
    """
    Return True if ``arr`` has a numpy floating-point dtype (real or complex).
    """
    arr = np.asarray(arr)
    return np.issubdtype(arr.dtype, np.floating) or np.issubdtype(
        arr.dtype, np.complexfloating
    )


def is_string_dtype(arr: np.ndarray) -> bool:
    """
    Return True if ``arr`` has a numpy byte-string (``S``) or unicode (``U``)
    dtype.
    """
    arr = np.asarray(arr)
    return arr.dtype.kind in ("S", "U")


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------


def _coerce_string_value(value: Any, dtype_kind: str) -> Any:
    """
    Normalize ``value`` to ``bytes`` or ``str`` matching ``dtype_kind``.
    """
    if isinstance(value, np.generic):
        value = value.item()
    if dtype_kind == "S":
        if isinstance(value, str):
            return value.encode("utf-8")
        return value
    # dtype_kind == "U"
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _string_eq(arr: np.ndarray, value: Any) -> np.ndarray:
    """
    Element-wise equality of a string array against a scalar value, with
    byte/unicode normalization so comparisons work regardless of the source
    kind.
    """
    if value is None:
        return np.zeros(arr.shape, dtype=bool)
    coerced = _coerce_string_value(value, arr.dtype.kind)
    return arr == coerced


# ----------------------------------------------------------------------
# Read path: FILLVAL -> mask/NaN
# ----------------------------------------------------------------------


def compute_fill_mask(arr: np.ndarray, fillval: Any) -> np.ndarray:
    """
    Compute a boolean mask of fill positions in ``arr``.

    For floats, both ``NaN`` values and exact equality to ``fillval`` count as
    fill.  For integers and strings, only exact equality to ``fillval`` counts.
    The literal bytes ``b"nan"`` are never treated as fill on read.

    Parameters
    ----------
    arr : array-like
        The raw data array as read from the CDF.
    fillval : scalar or None
        The variable's ``FILLVAL`` attribute.  If ``None``, only ``NaN`` is
        detected for float arrays; integer and string arrays yield an
        all-False mask.

    Returns
    -------
    numpy.ndarray
        Boolean array, same shape as ``arr``.
    """
    arr = np.asarray(arr)

    if is_float_dtype(arr):
        # If Float, create mask from combination of NaN and fillval matches.
        mask = np.isnan(arr)
        if fillval is not None:
            mask = mask | (arr == fillval)
        return mask

    if is_string_dtype(arr):
        # If String, create mask from fillval matches only.  Don't treat b"nan" as fill on read.
        return _string_eq(arr, fillval)

    # Integer / other dtypes
    # Don't check for NaN here since Numpy does not support NaN for integer dtypes
    if fillval is None:
        return np.zeros(arr.shape, dtype=bool)

    return arr == fillval


def apply_fillval_to_nan(arr: np.ndarray, fillval: Any) -> np.ndarray:
    """
    For float arrays, return a copy of ``arr`` with ``fillval`` positions
    replaced by ``np.nan``.

    Non-float arrays and ``fillval is None`` are no-ops: a copy of ``arr`` is
    returned unchanged.
    """
    arr = np.asarray(arr)
    if fillval is None or not is_float_dtype(arr):
        return arr.copy()
    return np.where(arr == fillval, np.nan, arr)


# ----------------------------------------------------------------------
# Write path: NaN/mask -> FILLVAL
# ----------------------------------------------------------------------


def apply_fill_on_write(arr: np.ndarray, mask: np.ndarray, fillval: Any) -> np.ndarray:
    """
    Return a copy of ``arr`` with fill positions replaced by ``fillval``.

    Fill positions are the union of:

    - ``mask`` (when provided),
    - ``NaN`` values (float arrays only),
    - the literal bytes ``b"nan"`` / ``b"NaN"`` (string arrays only;
      write-side convenience for users who pass ``np.nan`` into an ``S`` or
      ``U`` array, which numpy coerces to the bytes ``b"nan"``).

    Parameters
    ----------
    arr : array-like
        Source data.  Not mutated.
    mask : array-like of bool, or None
        Explicit mask of fill positions.  Broadcast-compatible with ``arr``.
    fillval : scalar or None
        The variable's ``FILLVAL`` attribute.  If ``None``, ``arr`` is
        returned as a copy with no replacements.

    Returns
    -------
    numpy.ndarray
        A new array with fill positions replaced.
    """
    arr = np.asarray(arr)
    if fillval is None:
        return arr.copy()

    fill_positions = np.zeros(arr.shape, dtype=bool)
    if mask is not None:
        fill_positions = fill_positions | np.asarray(mask, dtype=bool)

    if is_float_dtype(arr):
        fill_positions = fill_positions | np.isnan(arr)
    elif is_string_dtype(arr):
        # numpy coerces np.nan in an S/U array to the literal bytes b"nan".
        # Treat that (and the title-case variant) as fill on write.
        fill_positions = (
            fill_positions | _string_eq(arr, b"nan") | _string_eq(arr, b"NaN")
        )

    if not fill_positions.any():
        return arr.copy()

    out = arr.copy()
    if is_string_dtype(arr):
        out[fill_positions] = _coerce_string_value(fillval, arr.dtype.kind)
    else:
        out[fill_positions] = fillval
    return out
