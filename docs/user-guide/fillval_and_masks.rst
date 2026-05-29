.. _fillval_and_masks:

**********************************
FILLVAL, NaN, and Mask Round-trips
**********************************

Overview
========

``swxsoc`` round-trips missing data between in-memory containers and CDF files using the ISTP FILLVAL convention.
Masks are the canonical in-memory representation of missing data.
For convenience, floating-point NaN values are also treated as missing on the write path.
Integer and string dtypes are never promoted to floats; missing positions are tracked exclusively through a boolean mask.

Per-dtype behavior
==================

.. list-table::
   :header-rows: 1
   :widths: 15 30 30 25

   * - Dtype
     - In-memory fill source
     - On disk
     - On read result
   * - Float
     - ``NaN`` or ``.mask`` bit set
     - ``FILLVAL`` (e.g. ``-1.0e31``)
     - ``Masked`` quantity; underlying value is ``NaN``; ``.mask`` set
   * - Integer
     - ``.mask`` bit set, or value equal to ``FILLVAL`` sentinel
     - ``FILLVAL`` sentinel (e.g. ``-32768`` for ``int16``)
     - ``NDData``/``NDCube`` with ``.mask`` set; dtype preserved
   * - String (S/U)
     - ``.mask`` bit set, or value equal to ``b"nan"`` / ``b"NaN"``
     - Single space ``b" "``
     - ``NDData`` with ``.mask`` set; data shows ``b" "`` at masked positions
   * - Time (Epoch)
     - ``Time`` column with native astropy masking (``t[i] = np.ma.masked``)
     - Raw ``TT2000`` sentinel ``-9223372036854775808`` (or ``-1.0e31`` for ``EPOCH``)
     - ``Time`` column with ``.mask`` set at sentinel positions

Floats
======

Float measurements may contain ``NaN`` directly, or they can be wrapped in :class:`~astropy.utils.masked.Masked` to carry an explicit boolean mask.
Both are written to disk as the variable's ``FILLVAL``.
On read, the variable comes back as a ``Masked`` quantity whose underlying numeric data has ``NaN`` at masked positions::

   import numpy as np
   from astropy.units import Quantity
   from astropy.utils.masked import Masked

   values = Quantity(np.array([1.0, np.nan, 3.0], dtype=np.float32), unit="m")
   # After write/read round-trip, ``loaded`` is a Masked Quantity with
   # mask=[False, True, False] and underlying value [1.0, nan, 3.0].

Integers
========

Integer dtypes are never promoted to float.
Missing positions must be carried as a boolean ``.mask`` on an :class:`~astropy.nddata.NDData` (or :class:`~ndcube.NDCube`)::

   import numpy as np
   from astropy.nddata import NDData

   nd = NDData(
       data=np.array([10, 20, 30, 40], dtype=np.int16),
       mask=np.array([False, True, False, True]),
   )

On disk, masked positions are written as the variable's ``FILLVAL`` sentinel (chosen from the schema).
On read, the dtype is preserved and the mask is restored.
If a sentinel value appears in the on-disk data without an explicit mask, it is treated as fill on read.

Strings
=======

String round-trips are intentionally asymmetric:

* On write, any position equal to the literal bytes ``b"nan"`` or ``b"NaN"`` is treated as fill (this is how numpy coerces ``np.nan`` into ``S``/``U`` arrays) and is emitted to the CDF as a single space ``b" "``.
* On read, only the spec sentinel ``b" "`` is mapped to a mask bit.
  The literal string ``"nan"`` is never reinterpreted on read.

A consequence: a legitimate string value of ``"nan"`` will be coerced to fill on write.
If you need to preserve the literal four-byte string ``"nan"``, do not use it as a data value.

Time / Epoch
============

Time columns use astropy's native masking on :class:`~astropy.time.Time`::

   from astropy.time import Time
   import numpy as np

   t = Time(np.arange(5), format="unix")
   t[2] = np.ma.masked

When a mask is present, the writer bypasses the datetime conversion path and writes raw ``TT2000`` nanoseconds (``int64``) so the ISTP sentinel ``-9223372036854775808`` is preserved exactly.
On read, the time column comes back as a ``Time`` whose ``.mask`` is set at sentinel positions; ``.masked`` is ``True``.

Working with masks
==================

After loading a CDF, you can inspect masks uniformly across container types::

   loaded = swxdata.timeseries["measurement"]   # Masked Quantity
   mask = loaded.mask
   raw = loaded.unmasked.value                  # underlying numpy array

   loaded_support = swxdata.support["my_int"]   # NDData
   mask = loaded_support.mask
   raw = loaded_support.data

   loaded_spectra = swxdata.spectra["my_cube"]  # NDCube
   mask = loaded_spectra.mask

   loaded_time = swxdata.timeseries["time"]     # Time (native mask)
   mask = loaded_time.mask

Caveats
=======

* Integer dtypes are never promoted to float; rely on the mask to identify missing positions.
* The string write path treats ``b"nan"`` / ``b"NaN"`` as fill regardless of any mask; the read path is strict and uses only ``b" "``.
* For time columns, masked positions are written as raw ``TT2000`` nanoseconds; the historical datetime write path is unchanged when no mask is present.
