.. _swxdata_examples:

****************************************
Examples: Building a SWXData Data Object
****************************************

This page has short, self-contained examples of each piece of a :py:class:`~swxsoc.swxdata.SWXData` object: ``timeseries``, ``spectra``, ``support``, and ``meta``.
For an explanation of which piece your data belongs in, see :ref:`reading_writing_cdf`.

.. note::

   **Required install:** ``pip install swxsoc[cdf]``

All examples on this page use the ``hermes`` mission configuration:

.. doctest::

    >>> import os
    >>> import swxsoc
    >>> os.environ["SWXSOC_MISSION"] = "hermes"
    >>> swxsoc.reconfigure()

timeseries: Time-Varying Scalar Measurements
============================================

.. _swxdata_ts_example:

A single TimeSeries
-------------------

Create a `~astropy.timeseries.TimeSeries` from a start time and a fixed cadence:

.. doctest::

    >>> import numpy as np
    >>> import astropy.units as u
    >>> from astropy.timeseries import TimeSeries
    >>> example_ts = TimeSeries(
    ...     time_start="2016-03-22T12:30:31",
    ...     time_delta=3 * u.s,
    ...     data={"Bx": u.Quantity(value=[1, 2, 3, 4], unit="nT", dtype=np.uint16)},
    ... )

You can also create the time array directly with `~astropy.time.Time`:

.. doctest::

    >>> from astropy.time import Time, TimeDelta
    >>> times = Time("2010-01-01 00:00:00", scale="utc") + TimeDelta(np.arange(100) * u.s)
    >>> ts_2 = TimeSeries(
    ...     time=times,
    ...     data={
    ...         "diff_e_flux": u.Quantity(
    ...             value=np.arange(100) * 1e-3,
    ...             unit="1/(cm**2 * s * eV * steradian)",
    ...             dtype=np.float32,
    ...         )
    ...     },
    ... )

Multiple TimeSeries (multi-epoch)
---------------------------------

When measurements are sampled on different time axes, pass a `dict` of ``TimeSeries``.
Each key names the time axis (epoch) for its ``TimeSeries``.
Keys must use underscores, not hyphens.

.. doctest::

    >>> # Collected at one-second cadence
    >>> primary_epoch = Time("2010-01-01 00:00:00", scale="utc") + TimeDelta(np.arange(100) * u.s)
    >>> # Collected at 10-second cadence
    >>> secondary_epoch = Time("2010-01-01 00:00:00", scale="utc") + TimeDelta(np.arange(10) * (10 * u.s))
    >>> multi_epoch_ts = {
    ...     "Epoch": TimeSeries(
    ...         time=primary_epoch,
    ...         data={
    ...             "diff_e_flux": u.Quantity(
    ...                 value=np.arange(100) * 1e-3,
    ...                 unit="1/(cm**2 * s * eV * steradian)",
    ...                 dtype=np.float32,
    ...             )
    ...         },
    ...     ),
    ...     "Epoch_state": TimeSeries(
    ...         time=secondary_epoch,
    ...         data={
    ...             "temperature": u.Quantity(
    ...                 value=np.arange(10),
    ...                 unit="deg_C",
    ...                 dtype=np.float32,
    ...             )
    ...         },
    ...     ),
    ... }

When the same column name appears in more than one ``TimeSeries``, ``swxsoc`` prefixes later copies with their epoch key when it writes the CDF file, for example ``Epoch_state_Lat``.
The original names are restored when you load the file.
See section 6 of the :ref:`cdf_format_guide` for details.

.. _swxdata_spectra_example:

spectra: Higher-Dimensional Data with a WCS
===========================================

``spectra`` is an `~ndcube.NDCollection` of named `~ndcube.NDCube` objects.
Each ``NDCube`` holds a data array, a `~astropy.wcs.WCS` that describes its coordinate axes, optional metadata, and a unit.
The first array axis must be time, with the same length as the ``TimeSeries``.
For more about ``NDCube``, see the `ndcube documentation <https://docs.sunpy.org/projects/ndcube/en/stable/>`_.

A four-dimensional ion spectrum
-------------------------------

This example stores ion counts with the array shape ``[time, energy, pitch angle, species]``.
The data values are counts, and each axis other than time is a physical coordinate or an index for a species.

Describe the axes with a `~astropy.wcs.WCS`.
**WCS axes are numbered in the reverse order of NumPy axes**: WCS axis 1 is the last NumPy axis, and time is the last WCS axis.
Set ``ctype``, ``cname``, ``cunit``, ``crpix``, ``crval``, and ``cdelt`` for every axis:

.. doctest::

    >>> from astropy.wcs import WCS
    >>> from ndcube import NDCube, NDCollection
    >>> n_time, n_energy, n_pitch, n_species = 4, 8, 6, 3
    >>> ion_wcs = WCS(naxis=4)
    >>> ion_wcs.wcs.ctype = ["SPECIES", "PANG", "ENER", "TIME"]
    >>> ion_wcs.wcs.cname = ["species", "pitch_angle", "energy", "time"]
    >>> ion_wcs.wcs.cunit = ["", "deg", "keV", "s"]
    >>> ion_wcs.wcs.crpix = [1, 1, 1, 1] # The first pixel is at index 1 in each axis
    >>> ion_wcs.wcs.crval = [1, 0, 10, 0] # The first pixel has these values in each axis
    >>> ion_wcs.wcs.cdelt = [1, 36, 10, 3] # The value changes by this amount per pixel in each axis

The ``cunit`` of the species axis is blank because species is a dimensionless index.

Create the ``NDCube`` with a ``DEPEND_i`` entry in ``meta`` for each non-time axis.
``DEPEND_i`` names the ``support`` variable whose length matches NumPy axis ``i``, so ``DEPEND_1`` is energy, ``DEPEND_2`` is pitch angle, and ``DEPEND_3`` is species.
This numbering follows the NumPy axis order, so it runs in the opposite direction to the WCS axis numbers above.

.. doctest::

    >>> ion_counts = NDCube(
    ...     data=np.random.randint(0, 100, size=(n_time, n_energy, n_pitch, n_species)).astype(np.uint16),
    ...     wcs=ion_wcs,
    ...     unit="ct",
    ...     meta={
    ...         "CATDESC": "Ion counts",
    ...         "VAR_TYPE": "data",
    ...         "DEPEND_0": "Epoch",
    ...         "DEPEND_1": "energy",
    ...         "DEPEND_2": "pitch_angle",
    ...         "DEPEND_3": "species_mass",
    ...     },
    ... )

The axis values do not change over time, so store them in ``support``:

.. doctest::

    >>> example_support = {
    ...     "energy": u.Quantity(value=np.arange(10, 90, 10), unit="keV", dtype=np.float32),
    ...     "pitch_angle": u.Quantity(value=np.linspace(0, 180, n_pitch), unit="deg", dtype=np.float32),
    ...     "species_mass": u.Quantity(value=[1, 4, 16], unit="u", dtype=np.float32),
    ... }
    >>> example_support["energy"].meta = {"CATDESC": "Ion energy bin center", "VAR_TYPE": "support_data"}
    >>> example_support["pitch_angle"].meta = {"CATDESC": "Pitch angle bin center", "VAR_TYPE": "support_data"}
    >>> example_support["species_mass"].meta = {"CATDESC": "Species mass", "VAR_TYPE": "support_data"}

Collect the ``NDCube`` in an ``NDCollection`` and check its shape:

.. doctest::

    >>> example_spectra = NDCollection([("ion_counts", ion_counts)])
    >>> example_spectra["ion_counts"].data.shape
    (4, 8, 6, 3)

Every ``spectra`` variable must follow these rules:

- The first NumPy axis is time, and its length matches the ``TimeSeries``.
- The WCS has one axis for each NumPy axis, and time is the last WCS axis.
- Each ``DEPEND_i`` (for ``i`` 1 or higher) names a ``support`` variable whose length matches NumPy axis ``i``.
- Each ``NDCube`` has ``VAR_TYPE`` set to ``data``.

Other shapes
------------

Use the same pattern for other layouts.
To store ``[time, energy, pitch angle]``, use ``WCS(naxis=3)`` with the ``PANG`` and ``ENER`` axes and the ``TIME`` axis last, and set ``DEPEND_1`` to ``energy`` and ``DEPEND_2`` to ``pitch_angle``.
To store ``[time, energy, species]``, use ``WCS(naxis=3)`` with the ``SPECIES`` and ``ENER`` axes and the ``TIME`` axis last, and set ``DEPEND_1`` to ``energy`` and ``DEPEND_2`` to ``species_mass``.

To store each species as a separate ``[time, energy]`` structure, add one ``NDCube`` per species to the ``NDCollection``.
Each cube uses a 2-D WCS for energy and time, and the same ``energy`` support variable:

.. doctest::

    >>> energy_time_wcs = WCS(naxis=2)
    >>> energy_time_wcs.wcs.ctype = ["ENER", "TIME"]
    >>> energy_time_wcs.wcs.cname = ["energy", "time"]
    >>> energy_time_wcs.wcs.cunit = ["keV", "s"]
    >>> energy_time_wcs.wcs.crpix = [1, 1]
    >>> energy_time_wcs.wcs.crval = [10, 0]
    >>> energy_time_wcs.wcs.cdelt = [10, 3]
    >>> per_species = NDCollection(
    ...     [
    ...         (
    ...             f"ion_counts_{name}",
    ...             NDCube(
    ...                 data=np.random.randint(0, 100, size=(n_time, n_energy)).astype(np.uint16),
    ...                 wcs=energy_time_wcs,
    ...                 unit="ct",
    ...                 meta={
    ...                     "CATDESC": f"Ion counts for {name}",
    ...                     "VAR_TYPE": "data",
    ...                     "DEPEND_0": "Epoch",
    ...                     "DEPEND_1": "energy",
    ...                 },
    ...             ),
    ...         )
    ...         for name in ["H", "He", "O"]
    ...     ]
    ... )
    >>> per_species["ion_counts_He"].data.shape
    (4, 8)

.. _swxdata_support_example:

support: Time-Invariant Data and Data without a WCS
===================================================

``support`` is a `dict` of `~astropy.units.Quantity` or `~astropy.nddata.NDData` objects.
A ``Quantity`` is convenient for scalars and 1-D arrays, and ``NDData`` is convenient for multi-dimensional arrays.
For more about ``NDData``, see the `astropy NDData documentation <https://docs.astropy.org/en/stable/nddata/>`_.

Time-invariant data
-------------------

Variables with a ``VAR_TYPE`` of ``support_data`` or ``metadata`` are written as time-invariant (non-record-varying) variables:

.. doctest::

    >>> from astropy.nddata import NDData
    >>> const_param = u.Quantity(value=[1e-3], unit="keV", dtype=np.float32)
    >>> const_param.meta = {"CATDESC": "Constant Parameter", "VAR_TYPE": "support_data"}
    >>> data_mask = NDData(
    ...     data=np.eye(100, 100, dtype=np.uint16),
    ...     meta={"CATDESC": "Data Mask", "VAR_TYPE": "support_data"},
    ... )
    >>> example_support.update({"const_param": const_param, "data_mask": data_mask})

The ``meta`` of each ``Quantity`` or ``NDData`` becomes the variable attributes of that variable in the CDF file.

Time-varying data without a WCS
-------------------------------

If your time-varying data has more than one value per time step, and the index along an axis is an ID or category rather than a physical coordinate, put it in ``support`` as ``NDData`` and set ``VAR_TYPE`` to ``data``.
For example, this stores the dose rate from 2 dosimeters over 4 time steps, with shape ``[4, 2]``:

.. doctest::

    >>> dosimeter_ids = NDData(
    ...     data=np.array([1, 2], dtype=np.uint16),
    ...     meta={"CATDESC": "Dosimeter ID", "VAR_TYPE": "support_data"},
    ... )
    >>> dose_rate = NDData(
    ...     data=np.random.random(size=(4, 2)).astype(np.float32),
    ...     meta={
    ...         "CATDESC": "Dose rate per dosimeter",
    ...         "VAR_TYPE": "data",
    ...         "DEPEND_0": "Epoch",
    ...         "DEPEND_1": "dosimeter_ids", # This is an ID axis, not a physical coordinate
    ...     },
    ... )
    >>> example_support["dosimeter_ids"] = dosimeter_ids
    >>> example_support["dose_rate"] = dose_rate

``dose_rate`` is written to the CDF file as a record-varying variable that depends on ``Epoch``.
The same pattern extends to more ID axes, for example a constellation of 32 satellites with 2 dosimeters each has shape ``[time, 32, 2]``.
When you load the file with :py:func:`~swxsoc.swxdata.SWXData.load`, multi-dimensional record-varying variables are returned in ``spectra``.

.. _swxdata_meta_example:

meta: Global Metadata Attributes
================================

``meta`` is a `dict` of global attributes.
It must include ``Descriptor``, ``Data_level``, and ``Data_version``.
:py:func:`~swxsoc.swxdata.SWXData.global_attribute_template` fills in those three for you:

.. doctest::

    >>> from swxsoc.swxdata import SWXData
    >>> input_attrs = SWXData.global_attribute_template("eea", "l1", "1.0.0")

You can then add any other global attributes:

.. doctest::

    >>> input_attrs.update(
    ...     {
    ...         "DOI": "https://doi.org/<PREFIX>/<SUFFIX>",
    ...         "Data_product_descriptor": "odpd",
    ...         "HTTP_LINK": [
    ...             "https://istp-metadata.readthedocs.io/en/latest/",
    ...             "https://spdf.gsfc.nasa.gov/istp-metadata-editor/",
    ...         ],
    ...         "Instrument_mode": "default",
    ...         "Instrument_type": "Particles (space)",
    ...         "LINK_TEXT": ["ISTP Guidelines", "ISTP Metadata Editor"],
    ...         "LINK_TITLE": ["ISTP Guidelines", "ISTP Metadata Editor"],
    ...         "MODS": ["v1.0.0 - Original version."],
    ...         "PI_affiliation": "HERMES",
    ...         "PI_name": "HERMES SOC",
    ...         "TEXT": "Example HERMES EEA data file.",
    ...     }
    ... )

See :ref:`schema_information_guide` for a list of every global attribute.

Putting It All Together
=======================

Combine the pieces into a ``SWXData`` object and save it as a CDF file:

.. doctest::

    >>> import tempfile
    >>> example_ts["Bx"].meta = {"CATDESC": "X component of the magnetic field"}
    >>> example_sw_data = SWXData(
    ...     timeseries=example_ts,
    ...     support=example_support,
    ...     spectra=example_spectra,
    ...     meta=input_attrs,
    ... )
    >>> with tempfile.TemporaryDirectory() as tmp_dir:
    ...     cdf_file_path = example_sw_data.save(output_path=tmp_dir)

A multi-epoch ``SWXData`` object is created the same way, with a `dict` of ``TimeSeries``:

.. doctest::

    >>> multi_epoch_ts["Epoch"]["diff_e_flux"].meta = {"CATDESC": "Differential electron flux"}
    >>> multi_epoch_ts["Epoch_state"]["temperature"].meta = {"CATDESC": "Instrument temperature"}
    >>> multi_epoch_data = SWXData(timeseries=multi_epoch_ts, meta=input_attrs)
    >>> multi_epoch_data.timeseries["Epoch_state"]["temperature"].meta["CATDESC"]
    'Instrument temperature'

For a longer example that also adds data and metadata after the object is created, see :ref:`tutorial1`.
