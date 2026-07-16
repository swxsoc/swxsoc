"""
AWS Timestream recording functions for SWXSOC data.

This module provides functions to record timeseries data and individual
measurements to AWS Timestream for viewing on dashboards like Grafana.
"""

import numbers
import os
import time
import traceback
from datetime import datetime, timezone

import astropy.units as u
import boto3
import numpy as np
from astropy.timeseries import TimeSeries

import swxsoc
from swxsoc.util.util import is_production_environment

__all__ = [
    "record_timeseries",
    "_record_dimension_timestream",
    "create_timestream_client_session",
    "log_to_timestream",
]


def _get_timestream_names() -> tuple[str, str, str]:
    """
    Return Timestream database, table, and mission names for the current environment.

    The mission name is read from ``swxsoc.config["mission"]["mission_name"]``.
    If the configured mission is ``demo`` (the library default), the name
    ``swxsoc`` is used instead so that Timestream resources remain
    mission-agnostic. For any other configured mission the real name is used
    directly (e.g. ``hermes_sdc_aws_logs``).

    Non-production environments are prefixed with ``dev-`` to match existing
    deployment conventions.

    Returns
    -------
    database_name : str
        The Timestream database name, prefixed with ``dev-`` in non-production environments.
    table_name : str
        The Timestream table name, prefixed with ``dev-`` in non-production environments.
    mission_name : str
        The mission name used as a Timestream record dimension.
    """
    mission_name = swxsoc.config["mission"]["mission_name"]
    if mission_name == "demo":
        mission_name = "swxsoc"

    database_name = f"{mission_name}_sdc_aws_logs"
    table_name = f"{mission_name}_measures_table"

    if not is_production_environment():
        database_name = f"dev-{database_name}"
        table_name = f"dev-{table_name}"

    return database_name, table_name, mission_name


def create_timestream_client_session(region: str = "us-east-1"):
    """
    Create a boto3 Timestream write client session.

    Parameters
    ----------
    region : str, optional
        The AWS region to create the client in. Defaults to ``"us-east-1"``.

    Returns
    -------
    boto3.client
        The boto3 Timestream write client.
    """
    try:
        return boto3.client("timestream-write", region_name=region)
    except Exception as e:
        swxsoc.log.error({"status": "ERROR", "message": e})
        raise e


def log_to_timestream(
    timestream_client,
    action_type: str,
    file_key: str,
    new_file_key: str = None,
    source_bucket: str = None,
    destination_bucket: str = None,
) -> None:
    """
    Log an S3 pipeline action (e.g. upload, move, delete) to Timestream for audit purposes.

    This is distinct from :func:`record_timeseries`: ``log_to_timestream`` records
    discrete S3 file-movement *actions* to the ``sdc_aws_s3_bucket_log_table``
    (audit trail), while ``record_timeseries`` records science *measurements* to
    the ``{mission}_measures_table``.

    Parameters
    ----------
    timestream_client : boto3.client
        The Timestream write client.
    action_type : str
        The type of action performed (e.g. ``"PUT"``, ``"DELETE"``).
    file_key : str
        The name of the file the action was performed on.
    new_file_key : str, optional
        The new name of the file, if renamed/moved.
    source_bucket : str, optional
        The name of the source bucket. Either this or ``destination_bucket`` is required.
    destination_bucket : str, optional
        The name of the destination bucket. Either this or ``source_bucket`` is required.

    Returns
    -------
    None

    Notes
    -----
    Database and table names read the ``SWXSOC_MISSION`` environment variable
    directly (not ``swxsoc.config``) and special-case ``hermes``/unset to the
    un-prefixed ``sdc_aws_logs`` / ``sdc_aws_s3_bucket_log_table`` names, to
    preserve the exact historical Timestream names used by existing
    dashboards. Dev/prod detection is consolidated onto
    :func:`swxsoc.util.util.is_production_environment`.
    """
    swxsoc.log.debug("Logging to Timestream")
    current_time = str(int(time.time() * 1000))
    try:
        if not source_bucket and not destination_bucket:
            raise ValueError("A Source or Destination Buckets is required")

        # Check environment variable for SWXSOC_MISSION
        mission_name = os.getenv("SWXSOC_MISSION")
        if not mission_name or mission_name == "hermes":
            database_name = "sdc_aws_logs"
            table_name = "sdc_aws_s3_bucket_log_table"
        else:
            database_name = f"{mission_name}_sdc_aws_logs"
            table_name = f"{mission_name}_sdc_aws_s3_bucket_log_table"

        if not is_production_environment():
            database_name = f"dev-{database_name}"
            table_name = f"dev-{table_name}"

        # Write to Timestream
        timestream_client.write_records(
            DatabaseName=database_name,
            TableName=table_name,
            Records=[
                {
                    "Time": current_time,
                    "Dimensions": [
                        {"Name": "action_type", "Value": action_type},
                        {
                            "Name": "source_bucket",
                            "Value": source_bucket or "N/A",
                        },
                        {
                            "Name": "destination_bucket",
                            "Value": destination_bucket or "N/A",
                        },
                        {"Name": "file_key", "Value": file_key},
                        {
                            "Name": "new_file_key",
                            "Value": new_file_key or "N/A",
                        },
                    ],
                    "MeasureName": "timestamp",
                    "MeasureValue": str(datetime.now(timezone.utc).timestamp()),
                    "MeasureValueType": "DOUBLE",
                },
            ],
        )

        swxsoc.log.debug(f"File {file_key} Successfully Logged to Timestream")

    except Exception as e:
        swxsoc.log.error({"status": "ERROR", "message": e})
        raise e


def record_timeseries(
    ts: TimeSeries, ts_name: str = None, instrument_name: str = ""
) -> None:
    """
    Record a timeseries of measurements to AWS Timestream for viewing on a dashboard like Grafana.

    This function requires AWS credentials with permission to write to the AWS Timestream database.

    Parameters
    ----------
    ts : TimeSeries
        A timeseries with column data to record. Note that times are assumed to be in UTC.
    ts_name : str, optional
        The name of the timeseries to record. If None or empty string, defaults to ts.meta['name']
        or 'measurement_group'.
    instrument_name : str, optional
        The instrument name. If not provided or empty, uses ts.meta['INSTRUME'].

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If instrument_name is invalid or not in the configured mission instrument names.

    Notes
    -----
    Records are written in batches of 100 to comply with Timestream API limits.
    Database and table names are derived from the configured mission name
    (e.g. ``hermes_sdc_aws_logs``). When the default ``demo`` mission is
    active, ``swxsoc`` is used instead so that Timestream resources remain
    mission-agnostic. Names are automatically prefixed with 'dev-' when not
    in PRODUCTION environment.

    NaN values are skipped entirely and not written to Timestream. When a NaN is encountered in the
    timeseries data, that specific measure value is omitted from the record. The function logs the
    total count of NaN values skipped across all columns and time points.

    Data type inference follows a hierarchical approach to determine the appropriate Timestream type:

    - **BOOLEAN**: Values of type `bool` or `np.bool_` are stored as BOOLEAN type with lowercase
      string representation ("true" or "false") as required by Timestream.
    - **DOUBLE**: Numeric values (instances of `numbers.Number`) are stored as DOUBLE type.
    - **VARCHAR**: All other values default to VARCHAR type for text/string storage.

    The boolean check is performed first since `bool` is a subclass of `int` in Python. This ensures
    boolean flags are correctly identified and not mistakenly stored as numeric DOUBLE values.
    """
    timestream_client = create_timestream_client_session()

    # Validate Instrument name
    instrument_name = (
        instrument_name.lower()
        if "INSTRUME" not in ts.meta
        else ts.meta["INSTRUME"].lower()
    )
    if instrument_name == "" or instrument_name is None:
        error = f"Invalid instrument name: {instrument_name}. Must be one of {swxsoc.config['mission']['inst_names']}."
        swxsoc.log.error(error)
        raise ValueError(error)

    # Validate Timeseries name
    if ts_name is None or ts_name == "":
        ts_name = ts.meta.get("name", "measurement_group")

    # Get the Database, Table, and Mission names based on configured defaults and environment
    database_name, table_name, mission_name = _get_timestream_names()

    dimensions = [
        {"Name": "mission", "Value": mission_name},
        {"Name": "source", "Value": os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")},
        {"Name": "instrument", "Value": instrument_name},
    ]

    # Create a list to hold all records to be written
    records = []
    total_nan_count = 0

    # Loop over each time point in the timeseries, creating a record for each
    for i, time_point in enumerate(ts.time):
        measure_record = {
            "Time": str(int(time_point.to_datetime().timestamp() * 1000)),
            "Dimensions": dimensions,
            "MeasureName": ts_name,
            "MeasureValueType": "MULTI",
            "MeasureValues": [],
        }

        for this_col in ts.colnames:
            if this_col == "time":  # skip the time column
                continue

            if len(ts[this_col].shape) == 1:  # usual case, a single value in the column
                # Handle both Quantity and regular values
                if isinstance(ts[this_col], u.Quantity):
                    measure_unit = ts[this_col].unit
                    value = ts[this_col].value[i]
                else:
                    measure_unit = ""
                    value = ts[this_col][i]

                # Skip adding NaN values to the record
                if isinstance(value, numbers.Number) and np.isnan(value):
                    total_nan_count += 1
                    continue

                # Determine the appropriate Timestream data type
                if isinstance(value, (bool, np.bool_)):
                    measure_type = "BOOLEAN"
                    measure_value = str(
                        value
                    ).lower()  # Timestream expects "true" or "false"
                elif isinstance(value, numbers.Number):
                    measure_type = "DOUBLE"
                    measure_value = str(value)
                else:
                    measure_type = "VARCHAR"
                    measure_value = str(value)

                measure_record["MeasureValues"].append(
                    {
                        "Name": (
                            f"{this_col}_{measure_unit}" if measure_unit else this_col
                        ),
                        "Value": measure_value,
                        "Type": measure_type,
                    }
                )
            else:  # the values in the timeseries are arrays
                values = ts[this_col][i]
                if isinstance(values, u.Quantity):
                    values = values.value  # remove the unit
                values = values.flatten()

                # Loop over each value in the array and add to MeasureValues
                for i, value in enumerate(values):
                    # Skip adding NaN values to the record
                    if isinstance(value, numbers.Number) and np.isnan(value):
                        total_nan_count += 1
                        continue

                    # Determine the appropriate Timestream data type for array values
                    if isinstance(value, (bool, np.bool_)):
                        measure_type = "BOOLEAN"
                        measure_value = str(
                            value
                        ).lower()  # Timestream expects "true" or "false"
                    elif isinstance(value, numbers.Number):
                        measure_type = "DOUBLE"
                        measure_value = str(float(value))
                    else:
                        measure_type = "VARCHAR"
                        measure_value = str(value)

                    measure_record["MeasureValues"].append(
                        {
                            "Name": f"{this_col}_val{i}",
                            "Value": measure_value,
                            "Type": measure_type,
                        }
                    )

        # Only add the record if there are MeasureValues to write
        if measure_record["MeasureValues"]:
            records.append(measure_record)
        else:
            swxsoc.log.debug(
                f"Skipping record at time {time_point} for {ts_name} due to all NaN values."
            )

    # Log total NaN values skipped
    if total_nan_count > 0:
        swxsoc.log.info(f"Skipped {total_nan_count} NaN values in {ts_name}")

    # Process records in batches of 100 to avoid exceeding the Timestream API limit
    batch_size = 100
    for start in range(0, len(records), batch_size):
        chunk = records[start : start + batch_size]  # noqa: E203
        try:
            result = timestream_client.write_records(
                DatabaseName=database_name,
                TableName=table_name,
                Records=chunk,
            )
            swxsoc.log.info(
                f"Successfully wrote {len(chunk)} {ts_name} records to Timestream: {database_name}/{table_name}, "
                f"writeRecords Status: {result['ResponseMetadata']['HTTPStatusCode']}"
            )
        except timestream_client.exceptions.RejectedRecordsException as err:
            swxsoc.log.error(f"Failed to write records to Timestream: {err}")
            for rr in err.response["RejectedRecords"]:
                swxsoc.log.info(f"Rejected Index {rr['RecordIndex']}: {rr['Reason']}")
                if "ExistingVersion" in rr:
                    swxsoc.log.info(
                        f"Rejected record existing version: {rr['ExistingVersion']}"
                    )
        except Exception as err:
            swxsoc.log.error(f"Failed to write to Timestream: {err}")
            # Log Stack trace for debugging
            swxsoc.log.error(traceback.format_exc())


def _record_dimension_timestream(
    dimensions: list,
    instrument_name: str = None,
    measure_name: str = "timestamp",
    measure_value: any = None,
    measure_value_type: str = "DOUBLE",
    timestamp: str = None,
) -> None:
    """
    Record a single measurement to an `AWS timestream <https://docs.aws.amazon.com/timestream/>`_ for viewing on a dashboard such as Grafana.

    .. warning::
        This function requires AWS credentials with permission to write to the AWS timestream database.

    :param dimensions: A list of dimensions to record. Each dimension should be a dictionary with 'Name' and 'Value' keys.
    :type dimensions: list[dict]
    :param instrument_name: Optional. Name of the instrument to add as a dimension. Defaults to None.
    :type instrument_name: str, optional
    :param measure_name: The name of the measure being recorded. Defaults to "timestamp".
    :type measure_name: str
    :param measure_value: The value of the measure being recorded. Defaults to the current UTC timestamp if not provided.
    :type measure_value: any, optional
    :param measure_value_type: The type of the measure value (e.g., "DOUBLE", "BIGINT"). Defaults to "DOUBLE".
    :type measure_value_type: str
    :param timestamp: The timestamp for the record in milliseconds. Defaults to the current time if not provided.
    :type timestamp: str, optional
    :return: None
    """
    timestream_client = create_timestream_client_session()

    # Use current time in milliseconds if no timestamp is provided
    if not timestamp:
        timestamp = int(time.time() * 1000)

    # Default measure_value to current UTC timestamp if not provided
    utc_now = datetime.now(timezone.utc)
    if measure_value is None:
        measure_value = str(utc_now.timestamp())

    swxsoc.log.info(f"Using timestamp: {timestamp}")

    # Lowercase instrument name for consistency if provided
    if instrument_name:
        instrument_name = instrument_name.lower()

    # Add instrument_name as a dimension if provided
    if instrument_name and instrument_name in swxsoc.config["mission"]["inst_names"]:
        dimensions.append({"Name": "InstrumentName", "Value": instrument_name})
    else:
        swxsoc.log.info(
            "No valid instrument name provided. Skipping instrument dimension."
        )

    try:
        # Resolve database and table names from mission-agnostic defaults.
        database_name, table_name, _ = _get_timestream_names()

        record = {
            "Time": str(timestamp),
            "Dimensions": dimensions,
            "MeasureName": measure_name,
            "MeasureValue": str(measure_value),
            "MeasureValueType": measure_value_type,
        }

        # Write records to Timestream
        timestream_client.write_records(
            DatabaseName=database_name,
            TableName=table_name,
            Records=[record],
        )
        swxsoc.log.info(
            f"Successfully wrote record {record} to Timestream: {database_name}/{table_name}"
        )

    except Exception as e:
        swxsoc.log.error(f"Failed to write to Timestream: {e}")
