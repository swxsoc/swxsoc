.. _recording_to_timestream:

=============================================
Record Measures and Dimensions in Timestream
=============================================

This module provides functionality to record measures with dimensions in an AWS Timestream database. You can log various metrics and associate them with specific instruments and dimensions for effective data analysis.

Prerequisites
=============
Before using the `~swxsoc.util.util.record_dimension_timestream` function, ensure that the following prerequisites are met:

- You have access to an AWS account with Timestream enabled and configured.

.. note::
    If you are using the `~swxsoc.util.util.record_dimension_timestream` function within the cloud processing pipeline, you do not need to worry about this prerequisite.


Using the `record_dimension_timestream` Function
==================================================
To record dimensions in Timestream, you need to call the `~swxsoc.util.util.record_dimension_timestream` function with the appropriate parameters. Below is an example of how to use this function.

Example Usage
-------------
.. code-block:: python

    from swxsoc.util.util import record_dimension_timestream

    # First measurement: Temperature Reading
    instrument_name = "eea"
    dimensions = [
        {"Name": "Location", "Value": "Mars"},
        {"Name": "SensorType", "Value": "Temperature"},
        {"Name": "Unit", "Value": "Celsius"},
    ]
    measure_name = "temperature_reading"
    measure_value = 25.2
    measure_value_type = "DOUBLE";

    # Record the temperature reading
    record_dimension_timestream(
        dimensions=dimensions,
        instrument_name=instrument_name,
        measure_name=measure_name,
        measure_value=measure_value,
        measure_value_type=measure_value_type,
    )

    >>> INFO: Using timestamp: 1727258906620 [swxsoc.util.util]
    >>> INFO: Successfully wrote record {'Time': '1727258906620', 'Dimensions': [{'Name': 'Location', 'Value': 'Mars'}, {'Name': 'SensorType', 'Value': 'Temperature'}, {'Name': 'Unit', 'Value': 'Celsius'}, {'Name': 'InstrumentName', 'Value': 'eea'}], 'MeasureName': 'temperature_reading', 'MeasureValue': '25.2', 'MeasureValueType': 'DOUBLE'} to Timestream: dev-sdc_aws_logs/dev-measures_table [swxsoc.util.util]



Parameters
==========
The `~swxsoc.util.util.record_dimension_timestream` function accepts the following parameters:

- **dimensions** (list): A list of dimensions to record. Each dimension should be a dictionary with `Name` and `Value` keys.
- **instrument_name** (str, optional): The name of the instrument being logged. Defaults to `None`.
- **measure_name** (str): The name of the measure being recorded. Defaults to `"timestamp"`.
- **measure_value** (any, optional): The value of the measure being recorded. If not provided, defaults to the current UTC timestamp.
- **measure_value_type** (str): The type of the measure value (e.g., `"DOUBLE"`, `"BIGINT"`). Defaults to `"DOUBLE"`.
- **timestamp** (str, optional): The timestamp for the record in milliseconds. Defaults to the current time in milliseconds if not provided.
