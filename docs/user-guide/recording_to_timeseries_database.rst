.. _recording_to_timeseries_database:

==================================================
Record Measurements to Mission Timeseries Database
==================================================

This module provides functionality to record measurements to the mission's timeseries database, which can then be visualized in a Grafana Dashboard.
Measurements can be anything such as housekeeping data or science data as long as each measurement is associated with a time stamp.

.. note::
    Looking to track file-level provenance and processing status instead of
    scalar measurements? See :ref:`metatracker_guide` for `~swxsoc.db.tracker.MetaTracker`.

.. warning::
    This functionality requires AWS credentials with permission to write to the mission's timeseries database.
    If this functionality is called by the swxsoc cloud pipeline then those credentials are already available.

Writing data
============
The primary interface to write data into the timeseries database is the `~swxsoc.db.timeseries.record_timeseries` function.
Its primary input is a `~astropy.timeseries.TimeSeries` object.
The column names will be used as the label for the measurement.

Example Usage
-------------
.. doctest::

    >>> from astropy import units as u
    >>> from astropy.timeseries import TimeSeries
    >>> ts = TimeSeries(time_start='2016-03-22T12:30:31',
    ...                 time_delta=3 * u.s,
    ...                 n_samples=5)
    >>> ts['volt1'] = [1., 4., 5., 6., 4.] * u.volt
    >>> from swxsoc.db.timeseries import record_timeseries
    >>> record_timeseries(ts, instrument_name='test')  # doctest: +SKIP

Note that if duplicate measurements are sent then the last writer wins.