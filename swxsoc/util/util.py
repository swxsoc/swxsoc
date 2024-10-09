"""
This module provides general utility functions.
"""

import os
from datetime import datetime, timezone
import time

from astropy.time import Time
from astropy.timeseries import TimeSeries
import astropy.units as u

import boto3

import swxsoc


__all__ = [
    "create_science_filename",
    "parse_science_filename",
    "record_timeseries",
    "_record_dimension_timestream",
    "VALID_DATA_LEVELS",
]

TIME_FORMAT_L0 = "%Y%j-%H%M%S"
TIME_FORMAT = "%Y%m%dT%H%M%S"
VALID_DATA_LEVELS = ["l0", "l1", "ql", "l2", "l3", "l4"]
FILENAME_EXTENSION = ".cdf"


def create_science_filename(
    instrument: str,
    time: str,
    level: str,
    version: str,
    mode: str = "",
    descriptor: str = "",
    test: bool = False,
):
    """Return a compliant filename. The format is defined as

    {mission}_{inst}_{mode}_{level}{test}_{descriptor}_{time}_v{version}.cdf

    This format is only appropriate for data level >= 1.

    Parameters
    ----------
    instrument : `str`
        The instrument name. Must be one of the following "eea", "nemesis", "merit", "spani"
    time : `str` (in isot format) or ~astropy.time
        The time
    level : `str`
        The data level. Must be one of the following "l0", "l1", "l2", "l3", "l4", "ql"
    version : `str`
        The file version which must be given as X.Y.Z
    descriptor : `str`
        An optional file descriptor.
    mode : `str`
        An optional instrument mode.
    test : bool
        Selects whether the file is a test file.

    Returns
    -------
    filename : `str`
        A CDF file name including the given parameters that matches the mission's file naming conventions

    Raises
    ------
    ValueError: If the instrument is not recognized as one of the mission's instruments
    ValueError: If the data level is not recognized as one of the mission's valid data levels
    ValueError: If the data version does not match the mission's data version formatting conventions
    ValueError: If the data product descriptor or instrument mode do not match the mission's formatting conventions
    """
    test_str = ""

    if isinstance(time, str):
        time_str = Time(time, format="isot").strftime(TIME_FORMAT)
    else:
        time_str = time.strftime(TIME_FORMAT)

    if instrument not in swxsoc.config["mission"]["inst_names"]:
        raise ValueError(
            f"Instrument, {instrument}, is not recognized. Must be one of {swxsoc.config['mission']['inst_names']}."
        )
    if level not in VALID_DATA_LEVELS[1:]:
        raise ValueError(
            f"Level, {level}, is not recognized. Must be one of {VALID_DATA_LEVELS[1:]}."
        )
    # check that version is in the right format with three parts
    if len(version.split(".")) != 3:
        raise ValueError(
            f"Version, {version}, is not formatted correctly. Should be X.Y.Z"
        )
    # check that version has integers in each part
    for item in version.split("."):
        try:
            int_value = int(item)
        except ValueError:
            raise ValueError(f"Version, {version}, is not all integers.")

    if test is True:
        test_str = "test"

    # the parse_science_filename function depends on _ not being present elsewhere
    if ("_" in mode) or ("_" in descriptor):
        raise ValueError(
            "The underscore symbol _ is not allowed in mode or descriptor."
        )

    filename = f"{swxsoc.config['mission']['mission_name']}_{swxsoc.config['mission']['inst_to_shortname'][instrument]}_{mode}_{level}{test_str}_{descriptor}_{time_str}_v{version}"
    filename = filename.replace("__", "_")  # reformat if mode or descriptor not given

    return filename + swxsoc.config["mission"]["file_extension"]


def parse_science_filename(filepath: str) -> dict:
    """
    Parses a science filename into its consitutient properties (instrument, mode, test, time, level, version, descriptor).

    Parameters
    ----------
    filepath: `str`
        Fully specificied filepath of an input file

    Returns
    -------
    result : `dict`
        A dictionary with each property.

    Raises
    ------
    ValueError: If the file's mission name is not "swxsoc"
    ValueError: If the file's instreument name is not one of the mission's instruments
    ValueError: If the data level >0 for packet files
    ValueError: If not a CDF File
    """

    result = {
        "instrument": None,
        "mode": None,
        "test": False,
        "time": None,
        "level": None,
        "version": None,
        "descriptor": None,
    }

    filename = os.path.basename(filepath)
    file_name, file_ext = os.path.splitext(filename)

    filename_components = file_name.split("_")

    if filename_components[0] != swxsoc.config["mission"]["mission_name"]:
        raise ValueError(f"File {filename} not recognized. Not a valid mission name.")

    if file_ext == ".bin":
        if filename_components[1] not in swxsoc.config["mission"]["inst_targetnames"]:
            raise ValueError(
                f"File {filename} not recognized. Not a valid target name."
            )

        offset = 1 if len(filename_components) > 5 else 0

        if offset:
            result["mode"] = filename_components[2]

        if filename_components[2 + offset] != VALID_DATA_LEVELS[0]:
            raise ValueError(
                f"Data level {filename_components[2 + offset]} is not correct for this file extension."
            )
        else:
            result["level"] = filename_components[2 + offset]
        #  reverse the dictionary to look up instrument name from the short name
        from_shortname = {
            v: k for k, v in swxsoc.config["mission"]["inst_to_targetname"].items()
        }

        result["time"] = Time.strptime(filename_components[3 + offset], TIME_FORMAT_L0)

    elif file_ext == swxsoc.config["mission"]["file_extension"]:
        if filename_components[1] not in swxsoc.config["mission"]["inst_shortnames"]:
            raise ValueError(
                "File {filename} not recognized. Not a valid instrument name."
            )

        #  reverse the dictionary to look up instrument name from the short name
        from_shortname = {
            v: k for k, v in swxsoc.config["mission"]["inst_to_shortname"].items()
        }

        result["time"] = Time.strptime(filename_components[-2], TIME_FORMAT)

        # mode and descriptor are optional so need to figure out if one or both or none is included
        if filename_components[2][0:2] not in VALID_DATA_LEVELS:
            # if the first component is not data level then it is mode and the following is data level
            result["mode"] = filename_components[2]
            result["level"] = filename_components[3].replace("test", "")
            if "test" in filename_components[3]:
                result["test"] = True
            if len(filename_components) == 7:
                result["descriptor"] = filename_components[4]
        else:
            result["level"] = filename_components[2].replace("test", "")
            if "test" in filename_components[2]:
                result["test"] = True
            if len(filename_components) == 6:
                result["descriptor"] = filename_components[3]
    else:
        raise ValueError(f"File extension {file_ext} not recognized.")

    result["instrument"] = from_shortname[filename_components[1]]
    result["version"] = filename_components[-1][1:]  # remove the v

    return result


def record_timeseries(
    ts: TimeSeries, ts_name: str = None, instrument_name: str = ""
) -> None:
    """
    Record a timeseries of measurements to AWS Timestream for viewing on a dashboard like Grafana.

    This function requires AWS credentials with permission to write to the AWS Timestream database.

    :param ts: A timeseries with column data to record.
    :type ts: TimeSeries
    :param ts_name: The name of the timeseries to record.
    :type ts_name: str
    :param instrument_name: Optional. If not provided, uses ts.meta['INSTRUME']
    :type instrument_name: str
    :return: None
    """
    timestream_client = boto3.client("timestream-write", region_name="us-east-1")

    # Get mission name from environment or default to 'hermes'
    mission_name = swxsoc.config["mission"]["mission_name"]
    instrument_name = (
        instrument_name.lower()
        if "INSTRUME" not in ts.meta
        else ts.meta["INSTRUME"].lower()
    )

    if ts_name is None or ts_name == "":
        ts_name = ts.meta.get("name", "measurement_group")

    database_name = f"{mission_name}_sdc_aws_logs"
    table_name = f"{mission_name}_measures_table"

    if os.getenv("LAMBDA_ENVIRONMENT") != "PRODUCTION":
        database_name = f"dev-{database_name}"
        table_name = f"dev-{table_name}"

    dimensions = [
        {"Name": "mission", "Value": mission_name},
        {"Name": "source", "Value": os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")},
    ]

    if instrument_name != "":
        dimensions.append({"Name": "instrument", "Value": instrument_name})

    records = []
    for i, time_point in enumerate(ts.time):
        measure_record = {
            "Time": str(int(time_point.to_datetime().timestamp() * 1000)),
            "Dimensions": dimensions,
            "MeasureName": ts_name,
            "MeasureValueType": "MULTI",
            "MeasureValues": [],
        }

        for this_col in ts.colnames:
            if this_col == "time":
                continue

            # Handle both Quantity and regular values
            if isinstance(ts[this_col], u.Quantity):
                measure_unit = ts[this_col].unit
                value = ts[this_col].value[i]
            else:
                measure_unit = ""
                value = ts[this_col][i]

            measure_record["MeasureValues"].append(
                {
                    "Name": f"{this_col}_{measure_unit}" if measure_unit else this_col,
                    "Value": str(value),
                    "Type": "DOUBLE" if isinstance(value, (int, float)) else "VARCHAR",
                }
            )

        records.append(measure_record)

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
    timestream_client = boto3.client("timestream-write", region_name="us-east-1")

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
        # Get mission name from environment or default to 'hermes'
        mission_name = swxsoc.config["mission"]["mission_name"]

        # Define database and table names based on mission and environment
        database_name = f"{mission_name}_sdc_aws_logs"
        table_name = f"{mission_name}_measures_table"

        if os.getenv("LAMBDA_ENVIRONMENT") != "PRODUCTION":
            database_name = f"dev-{database_name}"
            table_name = f"dev-{table_name}"

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
