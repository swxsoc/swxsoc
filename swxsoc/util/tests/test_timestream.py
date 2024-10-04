"""Tests util.py that interact with timestream"""

import os
import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID
from moto.timestreamwrite.models import timestreamwrite_backends
import pytest

from astropy import units as u
from astropy.timeseries import TimeSeries


from swxsoc.util import util


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def mocked_timestream(aws_credentials):
    """
    Return a mocked S3 client
    """
    with mock_aws():
        """Fixture to mock Timestream database and table."""
        client = boto3.client("timestream-write", region_name="us-east-1")
        database_name = "dev-swxsoc_sdc_aws_logs"
        table_name = "dev-swxsoc_measures_table"
        client.create_database(DatabaseName=database_name)

        client.create_table(
            DatabaseName=database_name,
            TableName=table_name,
            RetentionProperties={
                "MemoryStoreRetentionPeriodInHours": 24,
                "MagneticStoreRetentionPeriodInDays": 7,
            },
        )
        yield client


def test_record_timeseries_quantity_1col(mocked_timestream):
    timeseries_name = "test_measurements"
    ts = TimeSeries(
        time_start="2016-03-22T12:30:31",
        time_delta=3 * u.s,
        n_samples=5,
        meta={"name": timeseries_name},
    )
    ts["temp4"] = [1.0, 4.0, 5.0, 6.0, 4.0] * u.deg_C
    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    # Assert that there should be 5 records, one for each timestamp
    assert len(records) == len(ts["temp4"])

    for i, record in enumerate(records):
        # Assert the time is correct
        time = str(int(ts.time[i].to_datetime().timestamp() * 1000))
        assert record["Time"] == time
        assert record["MeasureName"] == timeseries_name
        # Check the MeasureValues
        measure_values = record["MeasureValues"]
        assert len(measure_values) == 1  # Only one column of data

        # Assert the measure name, value, and type
        temp4_measure = next(
            (mv for mv in measure_values if mv["Name"] == "temp4_deg_C"), None
        )
        assert temp4_measure is not None, "temp4_deg_C not found in MeasureValues"
        assert temp4_measure["Value"] == str(
            ts["temp4"].value[i]
        ), "MeasureValue does not match"
        assert temp4_measure["Type"] == "DOUBLE", "MeasureValueType does not match"


def test_record_timeseries_quantity_multicol(mocked_timestream):
    timeseries_name = "test_measurements"
    ts = TimeSeries(time_start="2016-03-22T12:30:31", time_delta=3 * u.s, n_samples=5)
    ts["temp4"] = [1.0, 4.0, 5.0, 6.0, 4.0] * u.deg_C
    ts["rail5v"] = [5.1, 5.2, 4.9, 4.8, 5.0] * u.volt
    ts["status"] = [0, 1, 1, 1, 2]
    util.record_timeseries(ts, ts_name=timeseries_name, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    # There should be 5 records, one for each timestamp
    assert len(records) == len(ts["temp4"])

    for i, record in enumerate(records):
        # Assert the time is correct
        time = str(int(ts.time[i].to_datetime().timestamp() * 1000))
        assert record["Time"] == time

        # Check the MeasureValues
        measure_values = record["MeasureValues"]
        assert len(measure_values) == 3  # We expect 3 columns of data

        # Assert for each measure
        temp4_measure = next(
            (mv for mv in measure_values if mv["Name"] == "temp4_deg_C"), None
        )
        rail5v_measure = next(
            (mv for mv in measure_values if mv["Name"] == "rail5v_V"), None
        )
        status_measure = next(
            (mv for mv in measure_values if mv["Name"] == "status"), None
        )

        assert temp4_measure is not None, "temp4_deg_C not found in MeasureValues"
        assert temp4_measure["Value"] == str(
            ts["temp4"].value[i]
        ), "temp4 MeasureValue does not match"
        assert (
            temp4_measure["Type"] == "DOUBLE"
        ), "temp4 MeasureValueType does not match"

        assert rail5v_measure is not None, "rail5v_V not found in MeasureValues"
        assert rail5v_measure["Value"] == str(
            ts["rail5v"].value[i]
        ), "rail5v MeasureValue does not match"
        assert (
            rail5v_measure["Type"] == "DOUBLE"
        ), "rail5v MeasureValueType does not match"

        assert status_measure is not None, "status not found in MeasureValues"
        assert status_measure["Value"] == str(
            ts["status"].value[i]
        ), "status MeasureValue does not match"
        assert (
            status_measure["Type"] == "VARCHAR"
        ), "status MeasureValueType does not match"


def test_record_dimension_timestream(mocked_timestream):
    instrument_name = "eea"
    dimensions = [
        {"Name": "Location", "Value": "Mars"},
        {"Name": "SensorType", "Value": "Temperature"},
        {"Name": "Unit", "Value": "Celsius"},
    ]
    measure_name = "temperature_reading"
    measure_value = 25.2
    measure_value_type = "DOUBLE"

    util._record_dimension_timestream(
        dimensions=dimensions,
        instrument_name=instrument_name,
        measure_name=measure_name,
        measure_value=measure_value,
        measure_value_type=measure_value_type,
    )

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = (
        backend.databases["dev-swxsoc_sdc_aws_logs"]
        .tables["dev-swxsoc_measures_table"]
        .records
    )

    assert len(records) == 1


def test_invalid_record_dimension_timestream(mocked_timestream):
    instrument_name = "invalid_instrument"
    dimensions = [
        {"Name": "Location", "Value": "Mars"},
        {"Name": "SensorType", "Value": "Temperature"},
        {"Name": "Unit", "Value": "Celsius"},
    ]

    util._record_dimension_timestream(
        dimensions=dimensions,
    )

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    record = (
        backend.databases["dev-swxsoc_sdc_aws_logs"]
        .tables["dev-swxsoc_measures_table"]
        .records[0]
    )

    record_dimensions = record["Dimensions"]

    assert "InstrumentName" not in record_dimensions


def test_invalid_instrument_record_dimension_timestream(mocked_timestream):
    dimensions = "invalid"

    util._record_dimension_timestream(dimensions=dimensions)

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = (
        backend.databases["dev-swxsoc_sdc_aws_logs"]
        .tables["dev-swxsoc_measures_table"]
        .records
    )

    assert len(records) == 0
