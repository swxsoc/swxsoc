"""Tests util.py that interact with timestream"""

import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID
from moto.timestreamwrite.models import timestreamwrite_backends

from astropy import units as u
from astropy.timeseries import TimeSeries

from swxsoc.util import util

# Create a timestream database with boto3
client = boto3.client("timestream-write", region_name="us-east-1")
database_name = "dev-swxsoc_sdc_aws_logs"
table_name = "dev-swxsoc_measures_table"
client.create_database(DatabaseName=database_name)

# Create a table in the database
client.create_table(
    DatabaseName=database_name,
    TableName=table_name,
    RetentionProperties={
        "MemoryStoreRetentionPeriodInHours": 24,
        "MagneticStoreRetentionPeriodInDays": 7,
    },
)


@mock_aws
def test_record_timeseries_quantity_1col():

    ts = TimeSeries(time_start="2016-03-22T12:30:31", time_delta=3 * u.s, n_samples=5)
    ts["temp4"] = [1.0, 4.0, 5.0, 6.0, 4.0] * u.deg_C
    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    assert len(records) == len(ts["temp4"])
    # TODO assert measureName == temp4_deg_C
    # TODO assert measureValues == ts["temp4"].value
    # TODO assert len(measureValues) == 1  only one column of data
    # TODO assert timestamp[0] == 2016-03-22T12:30:31.000


@mock_aws
def test_record_timeseries_quantity_multicol():
    """Test with more than one column and a column without a quantity."""
    ts = TimeSeries(time_start="2016-03-22T12:30:31", time_delta=3 * u.s, n_samples=5)
    ts["temp4"] = [1.0, 4.0, 5.0, 6.0, 4.0] * u.deg_C
    ts["rail5v"] = [5.1, 5.2, 4.9, 4.8, 5.0] * u.volt
    ts["status"] = [0, 1, 1, 1, 2]
    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    assert len(records) == len(ts["temp4"]) * 3
    # TODO assert measureName0 == temp4_deg_C
    # TODO assert measureName1 == rail5v_volt
    # TODO assert measureName2 == status
    # TODO assert measureValues0 == ts["temp4"].value
    # TODO assert len(measureValues) == 3  
    # TODO assert timestamp[0] == 2016-03-22T12:30:31.000


@mock_aws
def test_record_dimension_timestream():
    # Create a timestream database with boto3
    client = boto3.client("timestream-write", region_name="us-east-1")
    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"
    client.create_database(DatabaseName=database_name)

    # Create a table in the database
    client.create_table(
        DatabaseName=database_name,
        TableName=table_name,
        RetentionProperties={
            "MemoryStoreRetentionPeriodInHours": 24,
            "MagneticStoreRetentionPeriodInDays": 7,
        },
    )

    instrument_name = "eea"
    dimensions = [
        {"Name": "Location", "Value": "Mars"},
        {"Name": "SensorType", "Value": "Temperature"},
        {"Name": "Unit", "Value": "Celsius"},
    ]
    measure_name = "temperature_reading"
    measure_value = 25.2
    measure_value_type = "DOUBLE"

    # Call the function to record dimensions in Timestream
    util._record_dimension_timestream(
        dimensions=dimensions,
        instrument_name=instrument_name,
        measure_name=measure_name,
        measure_value=measure_value,
        measure_value_type=measure_value_type,
    )

    instrument_name = "spani"
    dimensions = [
        {"Name": "Location", "Value": "Earth"},
        {"Name": "Activity", "Value": "Running"},
        {"Name": "Unit", "Value": "Seconds"},
    ]

    # Timestamp in milliseconds
    timestamp = "1628460000000"

    # Call the function to record the time as a measure in Timestream
    util._record_dimension_timestream(
        dimensions=dimensions,
        instrument_name=instrument_name,
        timestamp=timestamp,
    )

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    assert len(records) == 2


@mock_aws
def test_invalid_record_dimension_timestream():
    # Create a timestream database with boto3
    client = boto3.client("timestream-write", region_name="us-east-1")
    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"
    client.create_database(DatabaseName=database_name)

    # Create a table in the database
    client.create_table(
        DatabaseName=database_name,
        TableName=table_name,
        RetentionProperties={
            "MemoryStoreRetentionPeriodInHours": 24,
            "MagneticStoreRetentionPeriodInDays": 7,
        },
    )

    instrument_name = "invalid_instrument"
    dimensions = [
        {"Name": "Location", "Value": "Mars"},
        {"Name": "SensorType", "Value": "Temperature"},
        {"Name": "Unit", "Value": "Celsius"},
    ]

    # Call the function to record dimensions in Timestream
    util._record_dimension_timestream(
        dimensions=dimensions,
    )

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    record = backend.databases[database_name].tables[table_name].records[0]

    record_dimensions = record["Dimensions"]

    # Check if InstrumentName is in the record dimensions
    assert "InstrumentName" not in record_dimensions


@mock_aws
def test_invalid_instrument_record_dimension_timestream():
    # Create a timestream database with boto3
    client = boto3.client("timestream-write", region_name="us-east-1")
    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"
    client.create_database(DatabaseName=database_name)

    # Create a table in the database
    client.create_table(
        DatabaseName=database_name,
        TableName=table_name,
        RetentionProperties={
            "MemoryStoreRetentionPeriodInHours": 24,
            "MagneticStoreRetentionPeriodInDays": 7,
        },
    )

    dimensions = "invalid"

    # Call the function to record dimensions in Timestream
    util._record_dimension_timestream(dimensions=dimensions)

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    # Assert that no records were created
    assert len(records) == 0
