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
- ISTP fill sentinels for the CDF Epoch types are exposed as scalar helpers.
"""

import numpy as np

__all__ = [
    "is_float_dtype",
    "is_string_dtype",
    "compute_fill_mask",
    "apply_fill_on_write",
    "apply_fillval_to_nan",
    "tt2000_fillval_int64",
    "epoch_fillval_float",
]


# ----------------------------------------------------------------------
# ISTP fill sentinels for Epoch types
# ----------------------------------------------------------------------


def tt2000_fillval_int64() -> int:
    """
    Return the ISTP FILLVAL sentinel for ``CDF_TIME_TT2000`` variables.

    Returns
    -------
    int
        ``-9223372036854775808`` (``numpy.int64`` min).
    """
    return int(np.iinfo(np.int64).min)


def epoch_fillval_float() -> float:
    """
    Return the ISTP FILLVAL sentinel for ``CDF_EPOCH`` and ``CDF_EPOCH16``
    variables.

    Returns
    -------
    float
        ``-1.0e31``.
    """
    return -1.0e31


# ----------------------------------------------------------------------
# Dtype checks
# ----------------------------------------------------------------------


def is_float_dtype(arr) -> bool:
    """
    Return True if ``arr`` has a numpy floating-point dtype (real or complex).
    """
    arr = np.asarray(arr)
    return np.issubdtype(arr.dtype, np.floating) or np.issubdtype(
        arr.dtype, np.complexfloating
    )


def is_string_dtype(arr) -> bool:
    """
    Return True if ``arr`` has a numpy byte-string (``S``) or unicode (``U``)
    dtype.
    """
    arr = np.asarray(arr)
    return arr.dtype.kind in ("S", "U")


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------


def _coerce_string_value(value, dtype_kind: str):
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


def _string_eq(arr: np.ndarray, value) -> np.ndarray:
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
# Read path: FILLVAL -> mask
# ----------------------------------------------------------------------


def compute_fill_mask(arr, fillval) -> np.ndarray:
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
        mask = np.isnan(arr)
        if fillval is not None:
            mask = mask | (arr == fillval)
        return mask
    if is_string_dtype(arr):
        return _string_eq(arr, fillval)
    # Integer / other dtypes
    if fillval is None:
        return np.zeros(arr.shape, dtype=bool)
    return arr == fillval


# ----------------------------------------------------------------------
# Write path: NaN/mask -> FILLVAL
# ----------------------------------------------------------------------


def apply_fill_on_write(arr, mask, fillval) -> np.ndarray:
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


# ----------------------------------------------------------------------
# Read path: FILLVAL -> NaN (float only)
# ----------------------------------------------------------------------


def apply_fillval_to_nan(arr, fillval) -> np.ndarray:
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
