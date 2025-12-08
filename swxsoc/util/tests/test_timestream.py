"""Tests util.py that interact with timestream"""

import os

import boto3
import numpy as np
import pytest
from astropy import units as u
from astropy.timeseries import TimeSeries
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID
from moto.timestreamwrite.models import timestreamwrite_backends

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
        assert temp4_measure["Value"] == str(ts["temp4"].value[i]), (
            "MeasureValue does not match"
        )
        assert temp4_measure["Type"] == "DOUBLE", "MeasureValueType does not match"


def test_record_timeseries_quantity_1col_array(mocked_timestream):
    timeseries_name = "test_measurements"
    ts = TimeSeries(
        time_start="2016-03-22T12:30:31",
        time_delta=3 * u.s,
        n_samples=5,
        meta={"name": timeseries_name},
    )
    ts["temp4_arr"] = np.arange(6 * 5).reshape((5, 6))
    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    # Assert that there should be 5 records, one for each timestamp
    assert len(records) == len(ts["temp4_arr"])

    for i, record in enumerate(records):
        # Assert the time is correct
        time = str(int(ts.time[i].to_datetime().timestamp() * 1000))
        assert record["Time"] == time
        assert record["MeasureName"] == timeseries_name
        # Check the MeasureValues
        measure_values = record["MeasureValues"]
        assert (
            len(measure_values) == ts["temp4_arr"].shape[1]
        )  # Only one column of data

        # Assert the measure name, value, and type
        # Loop through each column in the array
        for j in range(ts["temp4_arr"].shape[1]):
            measure_name = f"temp4_arr_val{j}"
            temp_measure = next(
                (mv for mv in measure_values if mv["Name"] == measure_name), None
            )
            assert temp_measure is not None, (
                f"{measure_name} not found in MeasureValues"
            )
            assert temp_measure["Value"] == str(float(ts["temp4_arr"].value[i, j])), (
                "MeasureValue does not match"
            )
            assert temp_measure["Type"] == "DOUBLE", "MeasureValueType does not match"


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
        assert temp4_measure["Value"] == str(ts["temp4"].value[i]), (
            "temp4 MeasureValue does not match"
        )
        assert temp4_measure["Type"] == "DOUBLE", (
            "temp4 MeasureValueType does not match"
        )

        assert rail5v_measure is not None, "rail5v_V not found in MeasureValues"
        assert rail5v_measure["Value"] == str(ts["rail5v"].value[i]), (
            "rail5v MeasureValue does not match"
        )
        assert rail5v_measure["Type"] == "DOUBLE", (
            "rail5v MeasureValueType does not match"
        )

        assert status_measure is not None, "status not found in MeasureValues"
        assert status_measure["Value"] == str(ts["status"].value[i]), (
            "status MeasureValue does not match"
        )
        assert status_measure["Type"] == "DOUBLE", (
            "status MeasureValueType does not match"
        )


def test_record_timeseries_with_nan_values(mocked_timestream):
    """Test that NaN values are skipped and not written to Timestream."""
    timeseries_name = "test_measurements_with_nan"
    ts = TimeSeries(
        time_start="2016-03-22T12:30:31",
        time_delta=3 * u.s,
        n_samples=5,
        meta={"name": timeseries_name},
    )
    # Create data with some NaN values
    ts["temp4"] = [1.0, np.nan, 5.0, np.nan, 4.0] * u.deg_C
    ts["rail5v"] = [5.1, 5.2, np.nan, 4.8, 5.0] * u.volt

    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    # Assert that there should be 5 records, one for each timestamp
    assert len(records) == 5

    for i, record in enumerate(records):
        measure_values = record["MeasureValues"]

        # Check first record - should have both measures
        if i == 0:
            assert len(measure_values) == 2
            temp4_measure = next(
                (mv for mv in measure_values if mv["Name"] == "temp4_deg_C"), None
            )
            rail5v_measure = next(
                (mv for mv in measure_values if mv["Name"] == "rail5v_V"), None
            )
            assert temp4_measure is not None
            assert rail5v_measure is not None

        # Check second record - temp4 is NaN, should only have rail5v
        elif i == 1:
            assert len(measure_values) == 1
            rail5v_measure = next(
                (mv for mv in measure_values if mv["Name"] == "rail5v_V"), None
            )
            temp4_measure = next(
                (mv for mv in measure_values if mv["Name"] == "temp4_deg_C"), None
            )
            assert rail5v_measure is not None
            assert temp4_measure is None  # Should be skipped

        # Check third record - rail5v is NaN, should only have temp4
        elif i == 2:
            assert len(measure_values) == 1
            temp4_measure = next(
                (mv for mv in measure_values if mv["Name"] == "temp4_deg_C"), None
            )
            rail5v_measure = next(
                (mv for mv in measure_values if mv["Name"] == "rail5v_V"), None
            )
            assert temp4_measure is not None
            assert rail5v_measure is None  # Should be skipped

        # Check fourth record - temp4 is NaN, should only have rail5v
        elif i == 3:
            assert len(measure_values) == 1
            rail5v_measure = next(
                (mv for mv in measure_values if mv["Name"] == "rail5v_V"), None
            )
            temp4_measure = next(
                (mv for mv in measure_values if mv["Name"] == "temp4_deg_C"), None
            )
            assert rail5v_measure is not None
            assert temp4_measure is None  # Should be skipped


def test_record_timeseries_with_nan_in_arrays(mocked_timestream):
    """Test that NaN values in arrays are skipped and not written to Timestream."""
    timeseries_name = "test_array_with_nan"
    ts = TimeSeries(
        time_start="2016-03-22T12:30:31",
        time_delta=3 * u.s,
        n_samples=3,
        meta={"name": timeseries_name},
    )
    # Create array data with NaN values
    ts["data_arr"] = np.array(
        [[1.0, 2.0, np.nan, 4.0], [5.0, np.nan, 7.0, 8.0], [9.0, 10.0, 11.0, np.nan]]
    )

    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    # Assert that there should be 3 records, one for each timestamp
    assert len(records) == 3

    # First record should have 3 values (index 2 is NaN)
    assert len(records[0]["MeasureValues"]) == 3
    measure_names = [mv["Name"] for mv in records[0]["MeasureValues"]]
    assert "data_arr_val0" in measure_names
    assert "data_arr_val1" in measure_names
    assert "data_arr_val2" not in measure_names  # NaN, should be skipped
    assert "data_arr_val3" in measure_names

    # Second record should have 3 values (index 1 is NaN)
    assert len(records[1]["MeasureValues"]) == 3
    measure_names = [mv["Name"] for mv in records[1]["MeasureValues"]]
    assert "data_arr_val0" in measure_names
    assert "data_arr_val1" not in measure_names  # NaN, should be skipped
    assert "data_arr_val2" in measure_names
    assert "data_arr_val3" in measure_names


def test_record_timeseries_with_boolean_values(mocked_timestream):
    """Test that boolean values are correctly stored as BOOLEAN type."""
    timeseries_name = "test_boolean_measurements"
    ts = TimeSeries(
        time_start="2016-03-22T12:30:31",
        time_delta=3 * u.s,
        n_samples=5,
        meta={"name": timeseries_name},
    )
    ts["temp4"] = [1.0, 4.0, 5.0, 6.0, 4.0] * u.deg_C
    ts["heater_on"] = [True, False, True, True, False]
    ts["safety_flag"] = [False, False, True, False, False]

    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    # Assert that there should be 5 records, one for each timestamp
    assert len(records) == 5

    for i, record in enumerate(records):
        measure_values = record["MeasureValues"]

        # Should have 3 measures: temp4, heater_on, safety_flag
        assert len(measure_values) == 3

        # Check temp4 is DOUBLE
        temp4_measure = next(
            (mv for mv in measure_values if mv["Name"] == "temp4_deg_C"), None
        )
        assert temp4_measure is not None
        assert temp4_measure["Type"] == "DOUBLE"

        # Check heater_on is BOOLEAN with correct value
        heater_measure = next(
            (mv for mv in measure_values if mv["Name"] == "heater_on"), None
        )
        assert heater_measure is not None
        assert heater_measure["Type"] == "BOOLEAN"
        assert heater_measure["Value"] == str(ts["heater_on"][i]).lower()

        # Check safety_flag is BOOLEAN with correct value
        safety_measure = next(
            (mv for mv in measure_values if mv["Name"] == "safety_flag"), None
        )
        assert safety_measure is not None
        assert safety_measure["Type"] == "BOOLEAN"
        assert safety_measure["Value"] == str(ts["safety_flag"][i]).lower()


def test_record_timeseries_with_numpy_boolean(mocked_timestream):
    """Test that numpy boolean values are correctly stored as BOOLEAN type."""
    timeseries_name = "test_numpy_boolean"
    ts = TimeSeries(
        time_start="2016-03-22T12:30:31",
        time_delta=3 * u.s,
        n_samples=3,
        meta={"name": timeseries_name},
    )
    # Use numpy bool_ type
    ts["flag"] = np.array([True, False, True], dtype=np.bool_)

    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    assert len(records) == 3

    for i, record in enumerate(records):
        measure_values = record["MeasureValues"]
        flag_measure = next((mv for mv in measure_values if mv["Name"] == "flag"), None)

        assert flag_measure is not None
        assert flag_measure["Type"] == "BOOLEAN"
        # Check value is lowercase string
        expected_value = "true" if ts["flag"][i] else "false"
        assert flag_measure["Value"] == expected_value


def test_record_timeseries_with_mixed_types(mocked_timestream):
    """Test that mixed data types (boolean, numeric, string) are handled correctly."""
    timeseries_name = "test_mixed_types"
    ts = TimeSeries(
        time_start="2016-03-22T12:30:31",
        time_delta=3 * u.s,
        n_samples=3,
        meta={"name": timeseries_name},
    )
    ts["temp"] = [1.5, 2.5, 3.5] * u.deg_C
    ts["enabled"] = [True, False, True]
    ts["mode"] = ["nominal", "safe", "nominal"]
    ts["count"] = [10, 20, 30]

    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    assert len(records) == 3

    for i, record in enumerate(records):
        measure_values = record["MeasureValues"]
        assert len(measure_values) == 4

        # Check each type
        temp_measure = next(
            (mv for mv in measure_values if mv["Name"] == "temp_deg_C"), None
        )
        assert temp_measure["Type"] == "DOUBLE"

        enabled_measure = next(
            (mv for mv in measure_values if mv["Name"] == "enabled"), None
        )
        assert enabled_measure["Type"] == "BOOLEAN"

        mode_measure = next((mv for mv in measure_values if mv["Name"] == "mode"), None)
        assert mode_measure["Type"] == "VARCHAR"

        count_measure = next(
            (mv for mv in measure_values if mv["Name"] == "count"), None
        )
        assert count_measure["Type"] == "DOUBLE"


def test_record_timeseries_all_nan_column(mocked_timestream):
    """Test handling of a column with all NaN values."""
    timeseries_name = "test_all_nan"
    ts = TimeSeries(
        time_start="2016-03-22T12:30:31",
        time_delta=3 * u.s,
        n_samples=3,
        meta={"name": timeseries_name},
    )
    ts["temp"] = [1.0, 2.0, 3.0] * u.deg_C
    ts["bad_sensor"] = [np.nan, np.nan, np.nan] * u.volt

    util.record_timeseries(ts, instrument_name="test")

    database_name = "dev-swxsoc_sdc_aws_logs"
    table_name = "dev-swxsoc_measures_table"

    backend = timestreamwrite_backends[ACCOUNT_ID]["us-east-1"]
    records = backend.databases[database_name].tables[table_name].records

    assert len(records) == 3

    # Each record should only have temp, bad_sensor should be completely skipped
    for record in records:
        measure_values = record["MeasureValues"]
        assert len(measure_values) == 1
        assert measure_values[0]["Name"] == "temp_deg_C"

        # Ensure bad_sensor is not present
        bad_sensor_measure = next(
            (mv for mv in measure_values if "bad_sensor" in mv["Name"]), None
        )
        assert bad_sensor_measure is None


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
