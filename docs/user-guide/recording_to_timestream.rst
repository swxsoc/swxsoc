.. _recording_to_timestream:

=====================================
Record Measurements to AWS Timestream
=====================================

This module provides functionality to record measurements to an `AWS Timestream <https://docs.aws.amazon.com/timestream/>`_ database which can then be visualized in a Grafana Dashboard.
Measurements can be anything such as housekeeping data or science data as long as each measurement is associated with a time stamp.

.. warning::
    This functionality requires AWS credentials with permission to write to the AWS timestream database.
    If this functionality is called by the swxsox cloud pipeline then those credentials are already available.

Writing data
============
The primary interface to write data into the Timestream datase is the `~swxsoc.util.util.record_timeseries` function.
Its primary input is a `~astropy.timeseries.TimeSeries` object.
The column names will be used as the label for the measurement.

Example Usage
-------------
.. code-block::

    from astropy import units as u
    from astropy.timeseries import TimeSeries
    ts = TimeSeries(time_start='2016-03-22T12:30:31',
                    time_delta=3 * u.s,
                    n_samples=5)
    ts['volt1'] = [1., 4., 5., 6., 4.] * u.volt

    from swxsoc.util.util import record_timeseries

    record_timeseries(ts, instrument_name='test')

Note that if duplicate measurements are sent then the last writer wins.