.. _quick_reference:

***************
Quick Reference
***************

Short answers to common ``swxsoc`` tasks, with a link to the full guide for each.
The examples on this page run in order, so later answers reuse objects from earlier ones.

Creating CDF Files
==================

What do I need to install to work with CDF files?
-------------------------------------------------

Install the ``cdf`` extra:

.. code-block:: bash

    pip install "swxsoc[cdf]"

It includes SpacePy, which bundles the NASA CDF library, so you do not need to install the CDF library separately.
See :ref:`install_extras` for the other optional extras.

How do I select my mission?
---------------------------

Set the ``SWXSOC_MISSION`` environment variable, then reload the configuration:

    >>> import os
    >>> import swxsoc
    >>> os.environ["SWXSOC_MISSION"] = "hermes"
    >>> swxsoc.reconfigure()
    >>> swxsoc.config["mission"]["mission_name"]
    'hermes'

You can also set ``selected_mission`` in your :file:`config.yml` file.
See :ref:`customization`.

What is the smallest SWXData object I can create?
-------------------------------------------------

One `~astropy.timeseries.TimeSeries` with at least one measurement, plus the three required global attributes ``Descriptor``, ``Data_level``, and ``Data_version``:

    >>> import numpy as np
    >>> import astropy.units as u
    >>> from astropy.timeseries import TimeSeries
    >>> from swxsoc.swxdata import SWXData
    >>> ts = TimeSeries(
    ...     time_start="2016-03-22T12:30:31",
    ...     time_delta=3 * u.s,
    ...     data={"Bx": u.Quantity([1, 2, 3, 4], "nT", dtype=np.int16)},
    ... )
    >>> meta = SWXData.global_attribute_template("nemisis", "l1", "1.0.0")
    >>> sw_data = SWXData(timeseries=ts, meta=meta)
    >>> sw_data.timeseries["Bx"].meta["CATDESC"] = "X component of the magnetic field"

See :ref:`reading_writing_cdf`.

Which metadata attributes do I have to fill in?
-----------------------------------------------

Use the template functions to get a `dict` of the attributes you need to supply:

    >>> dict(SWXData.global_attribute_template("nemisis", "l1", "1.0.0"))
    {'Data_level': 'L1>Level 1', 'Data_version': '1.0.0', 'Descriptor': 'NEMISIS>Noise Eliminating Magnetometer Instrument in a Small Integrated System', 'Discipline': None, 'Instrument_type': None, 'Mission_group': None, 'PI_affiliation': None, 'PI_name': None, 'Project': None, 'Source_name': None, 'TEXT': None}
    >>> dict(SWXData.measurement_attribute_template())
    {'CATDESC': None}

``swxsoc`` derives all other required attributes for you.
See :ref:`schema_information_guide`.

Where do I put data that is not one value per time step?
--------------------------------------------------------

- Time-varying data whose axes can be described by a World Coordinate System (WCS): an `~ndcube.NDCube` in ``spectra``.
- Time-varying data with no WCS: an `~astropy.nddata.NDData` in ``support`` with ``VAR_TYPE`` set to ``"data"``.
- Data that does not change with time: a `~astropy.units.Quantity` or `~astropy.nddata.NDData` in ``support``.

See :ref:`swxdata_examples`.

How do I write a CDF file?
--------------------------

Call :py:func:`~swxsoc.swxdata.SWXData.save`, which returns the path to the new file:

    >>> import tempfile
    >>> tmp_dir = tempfile.TemporaryDirectory()
    >>> cdf_path = sw_data.save(output_path=tmp_dir.name)
    >>> cdf_path.name
    'hermes_nem_l1_20160322T123031_v1.0.0.cdf'

See :ref:`reading_writing_cdf`.

How do I check that my CDF file is valid?
-----------------------------------------

Call :py:func:`~swxsoc.util.validation.validate`, which returns a `list` of problems:

    >>> from swxsoc.util.validation import validate
    >>> validation_errors = validate(cdf_path)
    >>> "Required attribute (PI_name) not present in global attributes." in validation_errors
    True

An empty list means no problems were found.
See :ref:`cdf_validation`.

How do I read an existing CDF file?
-----------------------------------

Call :py:func:`~swxsoc.swxdata.SWXData.load` with the path to the file:

    >>> loaded_data = SWXData.load(cdf_path)
    >>> loaded_data.timeseries["Bx"].meta["CATDESC"]
    'X component of the magnetic field'
    >>> tmp_dir.cleanup()

See :ref:`rw_reading_cdf`.

How do I mark missing or bad data?
----------------------------------

Use a masked array or ``NaN`` values, which are written to the CDF file as the ``FILLVAL`` attribute value.
See :ref:`fillval_and_masks`.

Where are the CDF file naming and metadata conventions?
-------------------------------------------------------

See :ref:`cdf_format_guide`.

Running a Pipeline
==================

Where do I put my config.yml file?
----------------------------------

Run ``swxsoc.print_config(swxsoc.config)`` to see the active configuration and the files it was read from, or set the ``SWXSOC_CONFIGDIR`` environment variable.
See :ref:`customization`.

Which environment variables does swxsoc read?
---------------------------------------------

See :ref:`customization_env_vars`.

How do I change how much swxsoc logs?
-------------------------------------

Set the level of the ``swxsoc`` logger, for example ``swxsoc.log.setLevel("DEBUG")``.
See :ref:`logger`.

How do I send pipeline notifications to Slack or Mattermost?
------------------------------------------------------------

Set the ``COMMS_PLATFORM`` environment variable and call :py:func:`~swxsoc.comm.factory.get_comms_client`.
See :ref:`comms_clients`.

How do I record measurements for a Grafana dashboard?
-----------------------------------------------------

Use :py:func:`~swxsoc.db.timeseries.record_timeseries` to write them to the mission's timeseries database.
See :ref:`recording_to_timeseries_database`.

How do I add an annotation to a Grafana dashboard?
--------------------------------------------------

Use :py:func:`~swxsoc.util.grafana.create_annotation`.
See :ref:`grafana_annotation_management`.

How do I track which files have been processed?
-----------------------------------------------

Use `~swxsoc.db.tracker.MetaTracker`, which needs the ``tracker`` extra.
See :ref:`metatracker_guide`.

How do I search for and download mission data?
----------------------------------------------

Use `~swxsoc.net.client.SWXSOCClient` with ``sunpy.net.Fido``.
See :ref:`retrieving_data`.
