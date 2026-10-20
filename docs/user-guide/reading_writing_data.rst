.. _reading_writing_cdf:

*****************************************
Reading and Writing CDF Data with SWXData
*****************************************

The :py:class:`~swxsoc.swxdata.SWXData` class turns your measurements and metadata into archive-ready CDF files, and reads existing CDF files back into Python.
This page shows how to build a ``SWXData`` object, supply the required metadata, add data, plot it, and save it as a CDF file.

.. note::

   **Required install:** ``pip install swxsoc[cdf]``

   The ``cdf`` extra installs `SpacePy <https://spacepy.github.io/>`_, which includes the NASA CDF library.
   You do not need to install the CDF library separately.

Overview
========

``SWXData`` simplifies data management, improves data discovery, and helps your files follow CDF standards.
Data is stored in standard Python objects: `~astropy.timeseries.TimeSeries`, `~astropy.units.Quantity`, `~astropy.nddata.NDData`, and `~ndcube.NDCollection`.
Metadata is stored in dictionaries, with one `dict` of global attributes for the whole file and one `dict` of variable attributes for each variable.
``SWXData`` derives as many of the ISTP-required attributes as it can, so you only supply the ones it cannot work out for itself.

Related pages:

- :ref:`cdf_format_guide` explains the CDF file format and the ISTP metadata standards.
- :ref:`schema_information_guide` lists which metadata attributes are required, derived, or optional.
- :ref:`swxdata_examples` has more examples of each piece of a ``SWXData`` object.
- :ref:`tutorial1` is a complete script that creates a CDF file from start to finish.
- :ref:`cdf_validation` shows how to check a CDF file before you submit it to an archive.
- :ref:`fillval_and_masks` explains how missing data and masks are written and read.

Quick Start
===========

Select a mission
----------------

``SWXData`` uses the active mission configuration to check instrument names and data levels, and to build file names.
The examples on this page use the ``hermes`` mission, selected with the ``SWXSOC_MISSION`` environment variable:

    >>> import os
    >>> import swxsoc
    >>> os.environ["SWXSOC_MISSION"] = "hermes"
    >>> swxsoc.reconfigure()

To define your own mission and instruments, see :ref:`customization`.

Create a SWXData object
-----------------------

The smallest valid ``SWXData`` object needs one `~astropy.timeseries.TimeSeries` with at least one measurement, and the three required global attributes:

    >>> import numpy as np
    >>> import astropy.units as u
    >>> from astropy.timeseries import TimeSeries
    >>> from swxsoc.swxdata import SWXData
    >>> ts = TimeSeries(
    ...     time_start="2016-03-22T12:30:31",
    ...     time_delta=3 * u.s,
    ...     data={"Bx": u.Quantity([1, 2, 3, 4], "nT", dtype=np.int16)},
    ... )
    >>> meta = SWXData.global_attribute_template(instr_name="nemisis", data_level="l1", version="1.0.0")
    >>> sw_data = SWXData(timeseries=ts, meta=meta)
    >>> sw_data.timeseries["Bx"].meta["CATDESC"] = "X component of the magnetic field"

Note the use of `astropy.time` and `astropy.units`.
They attach a time scale and a physical unit to every value, and ``SWXData`` requires them.

Set the ``dtype`` of each measurement to the smallest type that holds your data, in this case 16-bit integers.
If you do not, NumPy usually defaults to ``float64`` and the CDF file will be larger than needed.
The ``dtype`` values that map to CDF types are ``int8``, ``int16``, ``int32``, ``int64``, ``uint8``, ``uint16``, ``uint32``, ``float32``, and ``float64``.
CDF has no ``float16`` or ``uint64`` type.

For more information about data type mapping for CDF files see :ref:`data_type_mapping`.

The SWXData Data Structure
==========================

A ``SWXData`` object is built from four arguments:

.. list-table::
   :header-rows: 1
   :widths: 12 12 46 30

   * - Argument
     - Required?
     - What goes in it
     - Python type
   * - ``timeseries``
     - Yes
     - A time column plus time-varying scalar measurements, one value per time step.
       Use a `dict` of ``TimeSeries`` when measurements are sampled on different time axes (multiple epochs).
     - `~astropy.timeseries.TimeSeries`, or `dict` of `str` to ``TimeSeries``
   * - ``support``
     - No
     - Data that does not vary with time, such as constants, calibration tables, and masks.
       Also any multi-dimensional data that cannot be described by a World Coordinate System (WCS).
     - `dict` of `str` to `~astropy.units.Quantity` or `~astropy.nddata.NDData`
   * - ``spectra``
     - No
     - Time-varying vector, spectral, or higher-dimensional data whose axes can be described by a WCS.
     - `~ndcube.NDCollection` of `~ndcube.NDCube`
   * - ``meta``
     - Yes
     - Global metadata attributes for the file.
       Must include the :ref:`required global attributes <rw_required_global_attrs>`.
     - `dict`

Rules that ``SWXData`` enforces when you create the object:

- Every ``timeseries`` column, other than ``time``, must be a one-dimensional `~astropy.units.Quantity` with a unit.
- Every ``support`` value must be a `~astropy.units.Quantity` or `~astropy.nddata.NDData`.
- ``spectra`` must be an `~ndcube.NDCollection`.
- When ``timeseries`` is a `dict` with more than one entry, the keys must use underscores, not hyphens (for example ``REACH_134``, not ``REACH-134``).
  CDF variable names cannot contain hyphens.

You can also create a ``SWXData`` object by loading an existing CDF file, see :ref:`rw_reading_cdf`.

Choosing where to put your data
-------------------------------

Use the shape of your data to decide which argument it belongs in:

- **One value per time step** (for example temperature, count rate, or one magnetic field component): add a column to ``timeseries``.
- **Many values per time step, with axes you can describe with a WCS** (for example an energy spectrum with known energy bins): add an `~ndcube.NDCube` to ``spectra``.
- **Many values per time step, with no WCS**: add an `~astropy.nddata.NDData` to ``support``.
  Set its ``VAR_TYPE`` attribute to ``"data"`` so that it is written as a time-varying (record-varying) variable.
- **Values that do not change with time** (for example calibration constants or lookup tables): add a `~astropy.units.Quantity` or `~astropy.nddata.NDData` to ``support``.

**Fields instruments** (for example magnetometers and electric field instruments):

- Put each scalar component, such as ``Bx``, ``By``, and ``Bz``, in its own ``timeseries`` column.
- To store a vector as a single variable, such as an ``(N, 3)`` array, put it in ``spectra`` as an `~ndcube.NDCube` with a meaningful WCS.
  If you cannot describe it with a WCS, put it in ``support`` as an `~astropy.nddata.NDData`.

**Particle instruments** (for example electron and ion spectrometers and energetic particle telescopes):

- Put scalar quantities, such as total count rate or density, in ``timeseries`` columns.
- Put energy- or angle-resolved distributions in ``spectra`` as an `~ndcube.NDCube` if the energy and angle axes can be described by a WCS.
  Otherwise, put them in ``support`` as an `~astropy.nddata.NDData`.

See :ref:`swxdata_examples` for code examples of each of these.

Required Metadata
=================

Metadata attributes describe your data so that other people and archive tools can use it.
``SWXData`` derives many attributes for you, such as ``Logical_file_id``, ``FILLVAL``, and ``UNITS``.
The tables in the :ref:`schema_information_guide` list every attribute, and which ones are required or derived.

.. _rw_required_global_attrs:

Required global attributes
--------------------------

``SWXData`` cannot be created unless the global metadata includes these three attributes:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Attribute
     - Format
     - Example
   * - ``Descriptor``
     - ``SHORTNAME>Long Name`` of the instrument
     - ``EEA>Electron Electrostatic Analyzer``
   * - ``Data_level``
     - ``SHORTNAME>Long Name`` of the data level
     - ``L1>Level 1``
   * - ``Data_version``
     - Version string in the form ``X.Y.Z``
     - ``1.0.0``

You can pass global attributes with the ``meta`` argument, or in the ``meta`` of a single ``TimeSeries``.
Other ISTP-required attributes, such as ``PI_name`` and ``TEXT``, should also be filled in before you submit a file to an archive.

Use :py:func:`~swxsoc.swxdata.SWXData.global_attribute_template` to get a `dict` of the global attributes you need to supply.
Passing an instrument name, data level, and version fills in the three required attributes for you.
Instrument names and data levels come from the active mission configuration.

Required variable attributes
----------------------------

Every variable needs a ``CATDESC`` (catalog description) attribute, a short human-readable description of the variable.
Use :py:func:`~swxsoc.swxdata.SWXData.measurement_attribute_template` to get a `dict` of the variable attributes you need to supply.

For details on both template functions, and on which attributes ``SWXData`` derives, see :ref:`schema_information_guide`.

Updating metadata
-----------------

``SWXData`` is mutable, so you can edit metadata after the object is created.
Global attributes are in the :py:attr:`~swxsoc.swxdata.SWXData.meta` `dict`:

    >>> sw_data.meta["PI_name"] = "Dr. Jane Doe"

Variable attributes are in the ``meta`` `dict` of each variable:

    >>> sw_data.timeseries["Bx"].meta.update({"CATDESC": "X component of the magnetic field, GSE"})

When ``timeseries`` is a `dict` of multiple epochs, select the epoch first, for example ``sw_data.timeseries["Epoch"]["Bx"].meta``.

Be careful when you edit derived attributes, because you might make the file non-compliant.

.. _rw_reading_cdf:

Reading a CDF File
==================

Load an existing CDF file into a ``SWXData`` object with :py:func:`~swxsoc.swxdata.SWXData.load`, passing a `~pathlib.Path` to the file.
This example loads a sample file that is included with ``swxsoc``:

    >>> from pathlib import Path
    >>> sample_file = (
    ...     Path(swxsoc.__file__).parent
    ...     / "data"
    ...     / "sample"
    ...     / "hermes_nms_default_l1_20160322T123031_v0.0.1.cdf"
    ... )
    >>> loaded_data = SWXData.load(sample_file)
    >>> loaded_data.meta["Data_version"]
    '0.0.1'

You can then add measurements, update metadata, and save the result to a new CDF file.

Adding Data
===========

You can add data to an existing ``SWXData`` object.
New ``timeseries`` measurements must have the same number of time stamps as the existing ones.

Add a time-varying scalar measurement with :py:func:`~swxsoc.swxdata.SWXData.add_measurement`:

    >>> sw_data.add_measurement(
    ...     measure_name="By",
    ...     data=u.Quantity([4, 3, 2, 1], "nT", dtype=np.int16),
    ...     meta={"CATDESC": "Y component of the magnetic field, GSE"},
    ... )

Add data that does not vary with time with :py:func:`~swxsoc.swxdata.SWXData.add_support`:

    >>> sw_data.add_support(
    ...     name="calibration_const",
    ...     data=u.Quantity([1e-1], "keV", dtype=np.float32),
    ...     meta={"CATDESC": "Calibration factor", "VAR_TYPE": "support_data"},
    ... )

Add higher-dimensional time-varying data with :py:func:`~swxsoc.swxdata.SWXData.add_spectra`, passing an `~ndcube.NDCube`:

    >>> from astropy.wcs import WCS
    >>> from ndcube import NDCube
    >>> sw_data.add_spectra(
    ...     name="example_spectra",
    ...     data=NDCube(data=np.random.random(size=(4, 10)), wcs=WCS(naxis=2), unit="eV"),
    ...     meta={"CATDESC": "Example spectra variable"},
    ... )

Remove any variable by name with :py:func:`~swxsoc.swxdata.SWXData.remove`.

Visualizing Data
================

:py:func:`~swxsoc.swxdata.SWXData.plot` gives a quick look at the data.
By default, each measurement is plotted in its own panel.
Plotting requires ``matplotlib``, which is installed with the ``cdf`` extra.

.. plot::
    :include-source:

    >>> import numpy as np
    >>> import matplotlib.pyplot as plt
    >>> import astropy.units as u
    >>> from astropy.timeseries import TimeSeries
    >>> from swxsoc.swxdata import SWXData
    >>> bx = np.concatenate([[0], np.random.choice(a=[-1, 0, 1], size=1000)]).cumsum(0)
    >>> by = np.concatenate([[0], np.random.choice(a=[-1, 0, 1], size=1000)]).cumsum(0)
    >>> bz = np.concatenate([[0], np.random.choice(a=[-1, 0, 1], size=1000)]).cumsum(0)
    >>> plot_ts = TimeSeries(time_start="2016-03-22T12:30:31", time_delta=3 * u.s, data={"Bx": u.Quantity(bx, "nT", dtype=np.int16)})
    >>> plot_meta = SWXData.global_attribute_template("nemisis", "l1", "1.0.0")
    >>> plot_data = SWXData(timeseries=plot_ts, meta=plot_meta)
    >>> plot_data.add_measurement(measure_name="By", data=u.Quantity(by, "nT", dtype=np.int16))
    >>> plot_data.add_measurement(measure_name="Bz", data=u.Quantity(bz, "nT", dtype=np.int16))
    >>> fig = plt.figure()
    >>> plot_data.plot() # doctest: +SKIP
    >>> plt.show() # doctest: +SKIP

Writing a CDF File
==================

Write a CDF file with :py:func:`~swxsoc.swxdata.SWXData.save`.
``output_path`` can be a directory, in which case the file name is built from the ``Logical_file_id`` attribute, or a full file path.
If you do not pass ``output_path``, the file is written to the current directory.
Set ``overwrite=True`` to replace an existing file with the same name.
``save`` returns the path to the new file.

    >>> import tempfile
    >>> with tempfile.TemporaryDirectory() as tmp_dir:
    ...     cdf_file_path = sw_data.save(output_path=tmp_dir)

Missing data, ``NaN`` values, and masks are written as the CDF ``FILLVAL``, see :ref:`fillval_and_masks`.

.. note::

    **Multi-epoch data:** When a ``SWXData`` object has more than one ``TimeSeries``, variable names can collide in the CDF file's flat list of variables.
    ``swxsoc`` prefixes a name only when there is a collision.
    For example, if ``Lat`` appears in two time series, the first is written as ``Lat`` and the second as ``REACH_134_Lat``.
    The original structure is restored when you load the file.
    See section 6 of the :ref:`cdf_format_guide` for details.

Before you share or archive the file, validate it as described in :ref:`cdf_validation`.
