"""
This module provides general utility functions.
"""

import os
from pathlib import Path
import boto3
from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from astropy.time import Time

import swxsoc


__all__ = [
    "create_science_filename",
    "parse_science_filename",
    "get_latest_dependent_file",
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


def filter_science_file(
    science_file_dict: dict,
    instrument: str = None,
    mode: str = None,
    test: bool = None,
    level: str = None,
    version: str = None,
    descriptor: str = None,
) -> bool:
    """
    Filters the science file based on provided criteria.

    Parameters
    ----------
    science_file_dict : dict
        A dictionary containing science file properties.
    instrument : str, optional
        The instrument name to filter by.
    mode : str, optional
        The mode to filter by.
    test : bool, optional
        The test flag to filter by.
    level : str, optional
        The data level to filter by.
    version : str, optional
        The version to filter by.
    descriptor : str, optional
        The descriptor to filter by.

    Returns
    -------
    bool
        True if the science file matches the criteria, False otherwise.
    """
    criteria = {
        "instrument": instrument,
        "mode": mode,
        "test": test,
        "level": level,
        "version": version,
        "descriptor": descriptor,
    }
    for key, value in criteria.items():
        if value is not None and science_file_dict.get(key) != value:
            return False
    return True


def list_files_in_s3(bucket_name: str, prefix: str) -> list:
    """
    Lists files in an S3 bucket with a specified prefix.

    Parameters
    ----------
    bucket_name : str
        The name of the S3 bucket.
    prefix : str
        The prefix to filter the files.

    Returns
    -------
    list
        A list of file keys.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    return [
        obj["Key"] for page in pages if "Contents" in page for obj in page["Contents"]
    ]


def download_file_from_s3(bucket_name: str, key: str, local_dir: str) -> Path:
    """
    Downloads a file from S3 to a local directory.

    Parameters
    ----------
    bucket_name : str
        The name of the S3 bucket.
    key : str
        The key of the file to download.
    local_dir : str
        The local directory to save the file.

    Returns
    -------
    Path
        The path to the downloaded file.
    """
    s3 = boto3.client("s3")
    local_file = Path(local_dir) / Path(key).name
    s3.download_file(bucket_name, key, str(local_file))
    return local_file


def list_files_in_spdf() -> list:
    """Stub function for listing files in SPDF."""
    return []


def download_file_from_spdf(file_path: str, local_dir: str) -> Path:
    """Stub function for downloading file from SPDF."""
    return Path(file_path)


def generate_prefixes(level: str, start_time: str, end_time: str) -> list:
    """
    Generates a list of prefixes based on the level and time range.

    Parameters
    ----------
    level : str
        The data level.
    start_time : str
        The start time in ISO format.
    end_time : str
        The end time in ISO format.

    Returns
    -------
    list
        A list of prefixes.
    """
    current_time = start_time
    prefixes = []

    while current_time <= end_time:
        prefix = f"{level}/{current_time.year}/{current_time.month:02d}/"
        prefixes.append(prefix)
        current_time += relativedelta(months=1)

    return prefixes


def get_latest_dependent_file(
    instrument: str,
    level: str,
    start_time: str,
    end_time: str,
    mode: str = None,
    test: bool = None,
    version: str = None,
    descriptor: str = None,
    use_s3: bool = True,
) -> Path:
    """
    Retrieves the latest dependent file based on the provided criteria.

    Parameters
    ----------
    instrument : str
        The instrument name.
    level : str
        The data level.
    start_time : str
        The start time in ISO format.
    end_time : str
        The end time in ISO format.
    mode : str, optional
        The mode to filter by.
    test : bool, optional
        The test flag to filter by.
    version : str, optional
        The version to filter by.
    descriptor : str, optional
        The descriptor to filter by.
    use_s3 : bool, optional
        Whether to get file from S3 or fallback to SPDF (default is True).

    Returns
    -------
    Path
        The path to the latest dependent file, or None if no matching file is found.
    """
    start_time = parse_date(start_time)
    end_time = parse_date(end_time)

    bucket_prefix = "" if os.getenv("LAMBDA_ENVIRONMENT") == "PRODUCTION" else "dev-"
    mission_name = swxsoc.config["mission"]["mission_name"]
    bucket_name = (
        f"{bucket_prefix}{mission_name}-{instrument}"
        if instrument in swxsoc.config["mission"]["inst_names"]
        else f"{bucket_prefix}{mission_name}-ancillary"
    )

    prefixes = generate_prefixes(level, start_time, end_time)
    all_files = []

    try:
        for prefix in prefixes:
            all_files.extend(list_files_in_s3(bucket_name, prefix))
    except Exception:
        use_s3 = False

    all_files.sort(key=lambda x: parse_science_filename(x)["time"], reverse=True)

    for file in all_files:
        try:
            science_file_dict = parse_science_filename(file)
            file_time = science_file_dict["time"]

            if start_time <= file_time <= end_time:
                if filter_science_file(
                    science_file_dict,
                    instrument,
                    mode,
                    test,
                    version,
                    descriptor,
                ):
                    if not use_s3:
                        return download_file_from_spdf(file, "/tmp")
                    else:
                        return download_file_from_s3(bucket_name, file, "/tmp")
        except ValueError:
            continue

    return None
