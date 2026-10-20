.. _swxdata_examples:

****************************************
Examples: Building a SWXData Data Object
****************************************

This page has short, self-contained examples of each piece of a :py:class:`~swxsoc.swxdata.SWXData` object: ``timeseries``, ``spectra``, ``support``, and ``meta``.
For an explanation of which piece your data belongs in, see :ref:`reading_writing_cdf`.

.. note::

   **Required install:** ``pip install swxsoc[cdf]``

All examples on this page use the ``hermes`` mission configuration:

    >>> import os
    >>> import swxsoc
    >>> os.environ["SWXSOC_MISSION"] = "hermes"
    >>> swxsoc.reconfigure()

timeseries: Time-Varying Scalar Measurements
============================================

A single TimeSeries
-------------------

Create a `~astropy.timeseries.TimeSeries` from a start time and a fixed cadence:

    >>> import numpy as np
    >>> import astropy.units as u
    >>> from astropy.timeseries import TimeSeries
    >>> example_ts = TimeSeries(
    ...     time_start="2016-03-22T12:30:31",
    ...     time_delta=3 * u.s,
    ...     data={"Bx": u.Quantity(value=[1, 2, 3, 4], unit="nT", dtype=np.uint16)},
    ... )

You can also create the time array directly with `~astropy.time.Time`:

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

spectra: Higher-Dimensional Data with a WCS
===========================================

``spectra`` is an `~ndcube.NDCollection` of named `~ndcube.NDCube` objects.
Each ``NDCube`` holds a data array, a `~astropy.wcs.WCS` that describes its coordinate axes, optional metadata, and a unit.
The first array axis must be time, with the same length as the ``TimeSeries``.
For more about ``NDCube``, see the `ndcube documentation <https://docs.sunpy.org/projects/ndcube/en/stable/>`_.

    >>> from astropy.wcs import WCS
    >>> from ndcube import NDCube, NDCollection
    >>> example_spectra = NDCollection(
    ...     [
    ...         (
    ...             "example_spectra",
    ...             NDCube(
    ...                 data=np.random.random(size=(4, 10)),
    ...                 wcs=WCS(naxis=2),
    ...                 meta={"CATDESC": "Example Spectra Variable"},
    ...                 unit="eV",
    ...             ),
    ...         )
    ...     ]
    ... )

support: Time-Invariant Data and Data without a WCS
===================================================

``support`` is a `dict` of `~astropy.units.Quantity` or `~astropy.nddata.NDData` objects.
A ``Quantity`` is convenient for scalars and 1-D arrays, and ``NDData`` is convenient for multi-dimensional arrays.
For more about ``NDData``, see the `astropy NDData documentation <https://docs.astropy.org/en/stable/nddata/>`_.

Time-invariant data
-------------------

Variables with a ``VAR_TYPE`` of ``support_data`` or ``metadata`` are written as time-invariant (non-record-varying) variables:

    >>> from astropy.nddata import NDData
    >>> const_param = u.Quantity(value=[1e-3], unit="keV", dtype=np.float32)
    >>> const_param.meta = {"CATDESC": "Constant Parameter", "VAR_TYPE": "support_data"}
    >>> data_mask = NDData(
    ...     data=np.eye(100, 100, dtype=np.uint16),
    ...     meta={"CATDESC": "Data Mask", "VAR_TYPE": "support_data"},
    ... )
    >>> example_support = {"const_param": const_param, "data_mask": data_mask}

The ``meta`` of each ``Quantity`` or ``NDData`` becomes the variable attributes of that variable in the CDF file.

Time-varying data without a WCS
-------------------------------

If your time-varying data has more than one value per time step but no WCS, put it in ``support`` as ``NDData`` and set ``VAR_TYPE`` to ``data``.
This example stores a particle count array with 4 time steps and 8 energy channels:

    >>> counts = NDData(
    ...     data=np.random.randint(0, 100, size=(4, 8), dtype=np.uint16),
    ...     meta={
    ...         "CATDESC": "Counts per energy channel",
    ...         "VAR_TYPE": "data",
    ...         "DEPEND_0": "Epoch",
    ...     },
    ... )
    >>> example_support["counts"] = counts

``counts`` is written to the CDF file as a record-varying variable that depends on ``Epoch``.
When you load the file with :py:func:`~swxsoc.swxdata.SWXData.load`, multi-dimensional record-varying variables are returned in ``spectra``.

meta: Global Metadata Attributes
================================

``meta`` is a `dict` of global attributes.
It must include ``Descriptor``, ``Data_level``, and ``Data_version``.
:py:func:`~swxsoc.swxdata.SWXData.global_attribute_template` fills in those three for you:

    >>> from swxsoc.swxdata import SWXData
    >>> input_attrs = SWXData.global_attribute_template("eea", "l1", "1.0.0")

You can then add any other global attributes:

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

    >>> multi_epoch_ts["Epoch"]["diff_e_flux"].meta = {"CATDESC": "Differential electron flux"}
    >>> multi_epoch_ts["Epoch_state"]["temperature"].meta = {"CATDESC": "Instrument temperature"}
    >>> multi_epoch_data = SWXData(timeseries=multi_epoch_ts, meta=input_attrs)
    >>> multi_epoch_data.timeseries["Epoch_state"]["temperature"].meta["CATDESC"]
    'Instrument temperature'

For a longer example that also adds data and metadata after the object is created, see :ref:`tutorial1`.
