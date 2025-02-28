"""
This module provides general utility functions.
"""

import os
from datetime import datetime, timezone
import time
import re

from astropy.time import Time
import astropy.units as u
from astropy.timeseries import TimeSeries
import requests
from datetime import datetime
from typing import List, Dict, Optional, Union
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from botocore import UNSIGNED
from botocore.client import Config
from datetime import datetime
from dateutil.relativedelta import relativedelta
from parfive import Downloader
import sunpy.util.net
import sunpy.time
import sunpy.net.attrs as a
from sunpy.net.attr import AttrWalker, AttrAnd, AttrOr, SimpleAttr
from sunpy.net.base_client import BaseClient, QueryResponseTable, convert_row_to_table


import swxsoc


__all__ = [
    "create_science_filename",
    "parse_science_filename",
    "SWXSOCClient",
    "VALID_DATA_LEVELS",
    "SearchTime",
    "Level",
    "Instrument",
    "DevelopmentBucket",
    "record_timeseries",
    "get_dashboard_id",
    "get_panel_id",
    "query_annotations",
    "create_annotation",
    "remove_annotation_by_id",
    "_record_dimension_timestream",
    "VALID_DATA_LEVELS",
]

TIME_FORMAT_L0 = "%Y%j-%H%M%S"
TIME_FORMAT = "%Y%m%dT%H%M%S"
VALID_DATA_LEVELS = ["l0", "l1", "ql", "l2", "l3", "l4"]


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
    #  reverse the dictionary to look up instrument name from the short name
    from_shortname = {
        v: k for k, v in swxsoc.config["mission"]["inst_to_shortname"].items()
    }

    if file_ext == swxsoc.config["mission"]["file_extension"]:
        filename_components = file_name.split("_")

        if filename_components[0] != swxsoc.config["mission"]["mission_name"]:
            raise ValueError(
                f"File {filename} not recognized. Not a valid mission name."
            )

        if filename_components[1] not in swxsoc.config["mission"]["inst_shortnames"]:
            raise ValueError(
                "File {filename} not recognized. Not a valid instrument name."
            )

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
        result["instrument"] = from_shortname[filename_components[1]]
        result["version"] = filename_components[-1][1:]  # remove the v

    else:
        mission_config = swxsoc.config["mission"]
        mission_name = mission_config["mission_name"].lower()
        inst_names = [
            name.lower()
            for name in mission_config["inst_names"] + mission_config["inst_shortnames"]
        ]

        swxsoc.log.debug(f"Configured mission name: {mission_name}")
        swxsoc.log.debug(f"Configured instrument names: {inst_names}")

        # Compile regex patterns
        mission_pattern = re.compile(
            rf"(?:^|[_\-.]){mission_name}(?:[_\-.]|$)", re.IGNORECASE
        )
        inst_pattern = re.compile(
            r"(?:^|[_\-.])(" + "|".join(inst_names) + r")(?:[_\-.]|$)", re.IGNORECASE
        )
        time_patterns = [
            re.compile(
                r"\d{8}[-_ T]?\d{6}"
            ),  # YYYYMMDD-HHMMSS, YYYYMMDD_HHMMSS, YYYYMMDD HHMMSS
            re.compile(
                r"\d{4}-\d{2}-\d{2}[-_ T]\d{2}:\d{2}:\d{2}"
            ),  # ISO 8601 with variants
            re.compile(r"\d{7}-\d{6}"),  # Legacy L0 format (YYYYJJJ-HHMMSS)
        ]

        # Match mission name
        mission_matches = mission_pattern.findall(filename)

        # Debug mission matches

        if not mission_matches:
            raise ValueError(
                f"File {filename} not recognized. Not a valid mission name '{mission_name}'."
            )
        elif len(mission_matches) > 1:
            raise ValueError(
                f"File {filename} not recognized. Multiple mission names found: {mission_matches}."
            )

        mission_name_found = mission_matches[0].lower()
        swxsoc.log.debug(f"Mission name: {mission_name_found}")

        # Match instrument name
        inst_matches = inst_pattern.findall(filename.lower())
        if not inst_matches:
            raise ValueError(
                f"File {filename} not recognized. No valid instrument name found."
            )
        elif len(inst_matches) > 1:
            raise ValueError(
                f"File {filename} not recognized. Multiple instrument names found: {inst_matches}."
            )

        instrument_name_found = inst_matches[0].lower()
        try:
            instrument_name_found = from_shortname[instrument_name_found]
        except KeyError:
            instrument_name_found = instrument_name_found

        swxsoc.log.debug(f"Instrument name: {instrument_name_found}")
        TIME_FORMAT_L0 = "%Y%j-%H%M%S"

        # Match time
        parsed_time = None
        for pattern in time_patterns:
            time_matches = pattern.findall(filename)
            if time_matches:
                try:
                    parsed_time = sunpy.time.parse_time(time_matches[0])
                    swxsoc.log.debug(f"Parsed time: {parsed_time}")
                except ValueError as e:
                    swxsoc.log.debug(f"Error parsing time {time_matches[0]}: {e}")

                try:
                    parsed_time = datetime.strptime(time_matches[0], TIME_FORMAT_L0)
                    swxsoc.log.debug(f"Parsed time: {parsed_time}")
                except ValueError as e:
                    swxsoc.log.debug(f"Error parsing time {time_matches[0]}: {e}")
                if parsed_time is not None:
                    break

        if parsed_time is None:
            raise ValueError(
                f"File {filename} does not contain a recognizable time format."
            )

        swxsoc.log.debug(f"Time: {parsed_time}")
        # Turn the parsed time into a Time object

        result["mission_name_found"] = mission_name_found
        result["instrument"] = instrument_name_found
        result["time"] = Time(parsed_time)
        result["level"] = "l0"

    return result


# ================================================================================================
#                                  SWXSOC FIDO CLIENT
# ================================================================================================

# Initialize the attribute walker
walker = AttrWalker()


# Map sunpy attributes to SWXSOC attributes for easy access
class SearchTime(a.Time):
    """
    Attribute for specifying the time range for the search.

    Attributes
    ----------
    start : `str`
        The start time in ISO format.
    end : `str`
        The end time in ISO format.
    """


class Level(a.Level):
    """
    Attribute for specifying the data level for the search.

    Attributes
    ----------
    value : str
        The data level value.
    """


class Instrument(a.Instrument):
    """
    Attribute for specifying the instrument for the search.

    Attributes
    ----------
    value : str
        The instrument value.
    """


class DevelopmentBucket(SimpleAttr):
    """
    Attribute for specifying whether to search in the DevelopmentBucket for testing purposes.

    Attributes
    ----------
    value : bool
        Whether to use the DevelopmentBucket. Defaults to False.
    """


@walker.add_creator(AttrOr)
def create_or(wlk, tree):
    """
    Creates an 'AttrOr' object from the provided tree of attributes.

    Parameters
    ----------
    wlk : AttrWalker
        The AttrWalker instance used for creating the attributes.
    tree : AttrOr
        The 'AttrOr' tree structure.

    Returns
    -------
    list
        A list of created attributes.
    """
    results = []
    for sub in tree.attrs:
        results.append(wlk.create(sub))
    return results


@walker.add_creator(AttrAnd)
def create_and(wlk, tree):
    """
    Creates an 'AttrAnd' object from the provided tree of attributes.

    Parameters
    ----------
    wlk : AttrWalker
        The AttrWalker instance used for creating the attributes.
    tree : AttrAnd
        The 'AttrAnd' tree structure.

    Returns
    -------
    list
        A list containing a single dictionary of attributes.
    """
    result = {}
    for sub in tree.attrs:
        wlk.apply(sub, result)
    return [result]


@walker.add_applier(SearchTime)
def apply_time(wlk, attr, params):
    """
    Applies 'a.Time' attribute to the parameters.

    Parameters
    ----------
    wlk : AttrWalker
        The AttrWalker instance used for applying the attributes.
    attr : a.Time
        The 'a.Time' attribute to be applied.
    params : dict
        The parameters dictionary to be updated.
    """
    params.update({"startTime": attr.start.isot, "endTime": attr.end.isot})


@walker.add_applier(Level)
def apply_level(wlk, attr, params):
    """
    Applies 'a.Level' attribute to the parameters.

    Parameters
    ----------
    wlk : AttrWalker
        The AttrWalker instance used for applying the attributes.
    attr : a.Level
        The 'a.Level' attribute to be applied.
    params : dict
        The parameters dictionary to be updated.
    """
    params.update({"level": attr.value.lower()})


@walker.add_applier(Instrument)
def apply_instrument(wlk, attr, params):
    """
    Applies 'a.Instrument' attribute to the parameters.

    Parameters
    ----------
    wlk : AttrWalker
        The AttrWalker instance used for applying the attributes.
    attr : a.Instrument
        The 'a.Instrument' attribute to be applied.
    params : dict
        The parameters dictionary to be updated.
    """
    params.update({"instrument": attr.value.upper()})


@walker.add_applier(DevelopmentBucket)
def apply_development_bucket(wlk, attr, params):
    """
    Applies 'DevelopmentBucket' attribute to the parameters.

    Parameters
    ----------
    wlk : AttrWalker
        The AttrWalker instance used for applying the attributes.
    attr : DevelopmentBucket
        The 'DevelopmentBucket' attribute to be applied.
    params : dict
        The parameters dictionary to be updated.
    """
    params.update({"use_development_bucket": attr.value})


class SWXSOCClient(BaseClient):
    """
    Client for interacting with SWXSOC data. This client provides search and fetch functionality for SWXSOC data and is based on the sunpy BaseClient for FIDO.

    For more information on the sunpy BaseClient, see: https://docs.sunpy.org/en/stable/generated/api/sunpy.net.base_client.BaseClient.html

    """

    size_column = "size"

    def search(self, query=None):
        """
        Searches for data based on the given query.

        Parameters
        ----------
        query : AttrAnd
            The query object specifying search criteria.

        Returns
        -------
        QueryResponseTable
            A table containing the search results.
        """
        if query is None:
            query = AttrAnd([])

        queries = walker.create(query)
        swxsoc.log.info(f"Searching with {queries}")

        results = []
        for query_parameters in queries:
            results.extend(self._make_search(query_parameters))

        if results == []:
            return QueryResponseTable(names=[], rows=[], client=self)

        names = [
            "instrument",
            "mode",
            "test",
            "time",
            "level",
            "version",
            "descriptor",
            "key",
            "size",
            "bucket",
            "etag",
            "storage_class",
            "last_modified",
        ]
        return QueryResponseTable(names=names, rows=results, client=self)

    @convert_row_to_table
    def fetch(self, query_results, *, path, downloader, **kwargs):
        """
        Fetches the files based on query results and queues them up to be downloaded to the specified path by your downloader.

        Note: The downloader must be an instance of parfive.Downloader

        Parameters
        ----------
        query_results : list
            The results of the search query.
        path : str
            The directory path where files should be saved.
        downloader : Downloader
            The parfive downloader instance used for fetching files.
        """

        if not isinstance(downloader, Downloader):
            raise ValueError("Downloader must be an instance of parfive.Downloader")

        for row in query_results:
            swxsoc.log.info(f"Fetching {row['key']}")
            if path is None or path == ".":
                path = os.getcwd()

            if os.path.exists(path) and not os.path.isdir(path):
                raise ValueError(f"Path {path} is not a directory")

            filepath = self._make_filename(path, row)

            presigned_url = self.generate_presigned_url(row["bucket"], row["key"])
            url = (
                presigned_url
                if presigned_url is not None
                else f'https://{row["bucket"]}.s3.amazonaws.com/{row["key"]}'
            )

            downloader.enqueue_file(url, filename=filepath)

    @classmethod
    def _make_filename(cls, path, row):
        """
        Creates a filename based on the provided path and row data.

        Parameters
        ----------
        path : str
            The directory path.
        row : dict
            The row data containing the file key.

        Returns
        -------
        str
            The full file path.
        """
        return os.path.join(path, row["key"].split("/")[-1])

    @staticmethod
    def generate_presigned_url(bucket_name, object_key, expiration=3600):
        """
        Generates a presigned URL for accessing an object in S3. If credentials are not available
        or access is denied, attempts an unsigned request for public access.

        Parameters
        ----------
        bucket_name : str
            The name of the S3 bucket.
        object_key : str
            The key of the S3 object.
        expiration : int, optional
            The expiration time in seconds for the presigned URL. Default is 3600 seconds.

        Returns
        -------
        str or None
            The presigned URL if successful, or a direct unsigned URL if public access is allowed.
            Otherwise, returns None.
        """
        try:
            # Attempt to generate a presigned URL with credentials
            s3_client = boto3.client("s3")

            # Try to list one object to check if credentials are available
            s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)

            response = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": object_key},
                ExpiresIn=expiration,
            )
            return response

        except NoCredentialsError:
            swxsoc.log.warning("Credentials not available. Trying unsigned access.")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "AccessDenied":
                swxsoc.log.warning(
                    f"Access denied to {bucket_name}/{object_key}. Trying unsigned access."
                )
            else:
                swxsoc.log.warning(f"Error generating presigned URL: {e}")
                return None

        # If credentials are missing or access is denied, try unsigned access
        try:
            # Attempt to access the object with an unsigned request (public access)
            swxsoc.log.info(f"Attempting unsigned access to {bucket_name}/{object_key}")
            url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}"
            return url
        except ClientError as unsigned_error:
            print(f"Unsigned access failed: {unsigned_error}")
            return None

    @classmethod
    def _can_handle_query(cls, *query):
        """
        Determines if the client can handle the given query based on its attributes.

        Parameters
        ----------
        query : tuple
            The query attributes to check.

        Returns
        -------
        bool
            True if the client can handle the query, otherwise False.
        """
        query_attrs = set(type(x) for x in query)
        supported_attrs = {SearchTime, Level, Instrument, DevelopmentBucket}
        return supported_attrs.issuperset(query_attrs)

    @classmethod
    def _make_search(cls, query):
        """
        Performs a search based on the provided query parameters.

        Parameters
        ----------
        query : dict
            The query parameters including instrument, levels, time range, and development bucket flag.

        Returns
        -------
        list
            A list of rows containing the search results.
        """
        instrument = query.get("instrument")
        levels = query.get("level")
        start_time = query.get("startTime")
        end_time = query.get("endTime")
        use_development_bucket = query.get("use_development_bucket")

        if levels is not None and not isinstance(levels, list):
            levels = [levels]

        if levels is not None and len(levels) > 0:
            for level in levels:
                if level not in VALID_DATA_LEVELS:
                    raise ValueError(f"Invalid data level: {level}")
        else:
            levels = VALID_DATA_LEVELS

        if start_time is None:
            start_time = "2000-01-01"

        if end_time is None:
            end_time = datetime.now().isoformat()

        instrument_buckets = {
            f"{swxsoc.config['mission']['inst_to_targetname'][inst]}": (
                f"{'dev-' if use_development_bucket else ''}"
                f"{swxsoc.config['mission']['mission_name']}-{inst}"
            )
            for inst in swxsoc.config["mission"]["inst_names"]
        }

        swxsoc.log.debug(f"Mapping of instruments to S3 buckets: {instrument_buckets}")

        if instrument is None or instrument not in instrument_buckets:
            swxsoc.log.info(
                f"No instrument specified or invalid instrument. Searching all instruments."
            )
            instrument_bucket_to_search = instrument_buckets.values()
        else:
            swxsoc.log.info(f"Searching for instrument: {instrument}")
            instrument_bucket_to_search = [instrument_buckets[instrument]]

        swxsoc.log.debug(f"Searching in buckets: {instrument_bucket_to_search}")

        files_in_s3 = cls.list_files_in_s3(instrument_bucket_to_search)

        if levels is not None or start_time is not None or end_time is not None:
            swxsoc.log.info(
                f"Searching for files with level {levels} between {start_time} and {end_time}"
            )

            prefixes = cls.generate_prefixes(levels, start_time, end_time)

            files_in_s3 = [
                f
                for f in files_in_s3
                if any(f["Key"].startswith(prefix) for prefix in prefixes)
            ]
        else:
            swxsoc.log.info(f"Searching for all files")

        swxsoc.log.info(f"Found {len(files_in_s3)} files in S3")

        rows = []
        for s3_object in files_in_s3:
            swxsoc.log.debug(f"Processing S3 object: {s3_object}")

            try:
                info = parse_science_filename(s3_object["Key"])
            except ValueError:
                info = {}

            row = [
                info.get("instrument", "unknown"),
                info.get("mode", "unknown"),
                info.get("test", False),
                info.get("time", "unknown"),
                info.get("level", "unknown"),
                info.get("version", "unknown"),
                info.get("descriptor", "unknown"),
                s3_object["Key"],
                s3_object["Size"] * u.byte,
                s3_object["Bucket"],
                s3_object["ETag"],
                s3_object["StorageClass"],
                s3_object["LastModified"],
            ]
            rows.append(row)

        return rows

    @staticmethod
    def list_files_in_s3(bucket_names: list) -> list:
        """
        Lists all files in the specified S3 buckets. If access is denied, it retries with an unsigned request.

        Parameters
        ----------
        bucket_names : list
            A list of S3 bucket names.

        Returns
        -------
        list
            A list of dictionaries containing metadata about each S3 object.
        """
        content = []
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        for bucket_name in bucket_names:
            try:
                # Try with authenticated client
                pages = paginator.paginate(Bucket=bucket_name)
                for page in pages:
                    for obj in page.get("Contents", []):
                        metadata = {
                            "Key": obj["Key"],
                            "LastModified": sunpy.time.parse_time(obj["LastModified"]),
                            "Size": obj["Size"],
                            "ETag": obj["ETag"],
                            "StorageClass": obj.get("StorageClass", "STANDARD"),
                            "Bucket": bucket_name,
                        }
                        content.append(metadata)
            except (ClientError, NoCredentialsError) as e:
                swxsoc.log.warning(f"Error accessing bucket {bucket_name}: {e}")
                if isinstance(e, NoCredentialsError):
                    error_code = "NoCredentialsError"
                elif isinstance(e, ClientError):
                    error_code = e.response["Error"]["Code"]
                # Retry?
                if error_code == "AccessDenied" or error_code == "NoCredentialsError":
                    swxsoc.log.warning(
                        f"Access denied to bucket {bucket_name}. Trying unsigned request."
                    )
                    # Retry with an unsigned (anonymous) client
                    try:
                        unsigned_s3 = boto3.client(
                            "s3", config=Config(signature_version=UNSIGNED)
                        )
                        unsigned_paginator = unsigned_s3.get_paginator(
                            "list_objects_v2"
                        )
                        pages = unsigned_paginator.paginate(Bucket=bucket_name)
                        for page in pages:
                            for obj in page.get("Contents", []):
                                metadata = {
                                    "Key": obj["Key"],
                                    "LastModified": sunpy.time.parse_time(
                                        obj["LastModified"]
                                    ),
                                    "Size": obj["Size"],
                                    "ETag": obj["ETag"],
                                    "StorageClass": obj.get("StorageClass", "STANDARD"),
                                    "Bucket": bucket_name,
                                }
                                content.append(metadata)
                    except ClientError as retry_error:
                        raise Exception(
                            f"Unsigned request failed for bucket {bucket_name} (Ensure you have the correct IAM permissions, or are on the VPN)"
                        )
                else:
                    raise Exception(f"Error accessing bucket {bucket_name}: {e}")

        return content

    @staticmethod
    def generate_prefixes(levels: list, start_time: str, end_time: str) -> list:
        """
        Generates a list of prefixes based on the level and time range.

        Parameters
        ----------
        levels : list
            A list of data levels.
        start_time : str
            The start time in ISO format.
        end_time : str
            The end time in ISO format.

        Returns
        -------
        list
            A list of prefixes.
        """
        current_time = datetime.fromisoformat(start_time)
        end_time = datetime.fromisoformat(end_time)
        prefixes = []

        while current_time <= end_time:
            for level in levels:
                prefix = f"{level}/{current_time.year}/{current_time.month:02d}/"
                prefixes.append(prefix)
            current_time += relativedelta(months=1)
            swxsoc.log.debug(f"Generated prefix: {prefix}")

        return prefixes


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


def _to_milliseconds(dt: datetime) -> int:
    """
    Converts a datetime object to milliseconds since epoch.

    Args:
        dt (datetime): Datetime object to convert.

    Returns:
        int: Milliseconds since epoch.
    """
    if isinstance(dt, Time):
        # Convert astropy Time object to a standard datetime object in UTC
        dt = dt.to_datetime(timezone=None)  # Convert to naive datetime in UTC
        return int(dt.timestamp() * 1000)

    return int(dt.timestamp() * 1000)


def get_dashboard_id(
    dashboard_name: str, mission_dashboard: Optional[str] = None
) -> Optional[int]:
    """
    Retrieves the dashboard UID by its name. Issues a warning if multiple dashboards with the same name are found.

    Args:
        dashboard_name (str): Name of the dashboard to retrieve.

    Returns:
        Optional[int]: The UID of the dashboard, or None if not found.
    """
    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        response = requests.get(
            f"{BASE_URL}/api/search", headers=HEADERS, params={"query": dashboard_name}
        )
        response.raise_for_status()
        dashboards = response.json()
    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(f"Failed to retrieve dashboards: {e}")
        return None
    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(f"Failed to retrieve panels for dashboard: {e}")
        return None

    matching_dashboards = [
        dashboard
        for dashboard in dashboards
        if "title" in dashboard and dashboard["title"] == dashboard_name
    ]

    if len(matching_dashboards) == 0:
        swxsoc.log.warning(
            f"Dashboard with title '{dashboard_name}' not found. Annotation will be created without a dashboard."
        )

    if len(matching_dashboards) > 1:
        swxsoc.log.warning(
            f"Multiple dashboards with title '{dashboard_name}' found. "
            f"Using the first matching dashboard UID ({matching_dashboards[0]['uid']}). Consider using unique dashboard titles."
        )

    return matching_dashboards[0]["uid"] if matching_dashboards else None


def get_panel_id(
    dashboard_id: int, panel_name: str, mission_dashboard: Optional[str] = None
) -> Optional[int]:
    """
    Retrieves the panel ID by dashboard UID and panel name. Issues a warning if multiple panels with the same name are found.

    Args:
        dashboard_id (int): UID of the dashboard.
        panel_name (str): Name of the panel to retrieve.

    Returns:
        Optional[int]: The ID of the panel, or None if not found.
    """
    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        response = requests.get(
            f"{BASE_URL}/api/dashboards/uid/{dashboard_id}", headers=HEADERS
        )
        response.raise_for_status()
        panels = response.json().get("dashboard", {}).get("panels", [])

    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(
            f"Failed to retrieve panels for dashboard ID {dashboard_id}: {e}"
        )
        return None

    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(
            f"Failed to retrieve panels for dashboard ID {dashboard_id}: {e}"
        )
        return None

    matching_panels = [panel for panel in panels if panel["title"] == panel_name]

    if len(matching_panels) == 0:
        swxsoc.log.warning(
            f"Panel with title '{panel_name}' not found in dashboard ID {dashboard_id}. Annotation will be created without a panel."
        )

    if len(matching_panels) > 1:
        swxsoc.log.warning(
            f"Multiple panels with title '{panel_name}' found in dashboard ID {dashboard_id}. "
            f"Using the first matching panel ID ({matching_panels[0]['id']}). Consider using unique panel titles."
        )

    return matching_panels[0]["id"] if matching_panels else None


def query_annotations(
    start_time: datetime,
    end_time: Optional[datetime] = None,
    tags: Optional[List[str]] = None,
    limit: Optional[int] = 100,
    dashboard_id: Optional[int] = None,
    panel_id: Optional[int] = None,
    dashboard_name: Optional[str] = None,
    panel_name: Optional[str] = None,
    mission_dashboard: Optional[str] = None,
) -> List[Dict[str, Union[str, int]]]:
    """
    Queries annotations within a specific timeframe with optional filters for tags, dashboard, and panel names.

    Args:
        start_time (datetime): Start time of the query.
        end_time (Optional[datetime]): End time of the query; defaults to start_time if None.
        tags (Optional[List[str]]): List of tags to filter the annotations.
        limit (Optional[int]): Maximum number of annotations to retrieve.
        dashboard_id (Optional[int]): UID of the dashboard to filter annotations.
        panel_id (Optional[int]): ID of the panel to filter annotations.
        dashboard_name (Optional[str]): Name of the dashboard to look up UID if `dashboard_id` is not provided.
        panel_name (Optional[str]): Name of the panel to look up ID if `panel_id` is not provided.

    Returns:
        List[Dict[str, Union[str, int]]]: List of annotations matching the query criteria.
    """
    # Look up dashboard and panel IDs if names are provided
    if dashboard_name and not dashboard_id:
        dashboard_id = get_dashboard_id(dashboard_name, mission_dashboard)
    if dashboard_id and panel_name and not panel_id:
        panel_id = get_panel_id(dashboard_id, panel_name, mission_dashboard)

    if not end_time:
        end_time = start_time

    params = {
        "from": _to_milliseconds(start_time),
        "to": _to_milliseconds(end_time),
        "limit": limit,
    }
    if tags:
        params["tags"] = tags
    if dashboard_id:
        params["dashboardUID"] = dashboard_id
    if panel_id:
        params["panelId"] = panel_id

    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        response = requests.get(
            f"{BASE_URL}/api/annotations", headers=HEADERS, params=params
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(f"Failed to query annotations: {e}")
        return []
    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(
            f"Failed to retrieve panels for dashboard ID {dashboard_id}: {e}"
        )
        return []


def create_annotation(
    start_time: datetime,
    text: str,
    tags: List[str],
    end_time: Optional[datetime] = None,
    dashboard_id: Optional[int] = None,
    panel_id: Optional[int] = None,
    dashboard_name: Optional[str] = None,
    panel_name: Optional[str] = None,
    mission_dashboard: Optional[str] = None,
    overwrite: bool = False,
) -> Dict[str, Union[str, int]]:
    """
    Creates a new annotation for a specified event or time period, with optional filtering by dashboard and panel names.

    Args:
        start_time (datetime): Start time of the annotation.
        text (str): Annotation text to display.
        tags (List[str]): List of tags for categorizing the annotation.
        end_time (Optional[datetime]): End time of the annotation, if applicable.
        dashboard_id (Optional[int]): UID of the dashboard to associate the annotation.
        panel_id (Optional[int]): ID of the panel to associate the annotation.
        dashboard_name (Optional[str]): Name of the dashboard to look up UID if `dashboard_id` is not provided.
        panel_name (Optional[str]): Name of the panel to look up ID if `panel_id` is not provided.

    Returns:
        Dict[str, Union[str, int]]: The created annotation data.
    """
    # Look up dashboard and panel IDs if names are provided
    if dashboard_name and not dashboard_id:
        dashboard_id = get_dashboard_id(dashboard_name, mission_dashboard)
    if dashboard_id and panel_name and not panel_id:
        panel_id = get_panel_id(dashboard_id, panel_name, mission_dashboard)

    # Overwrite functionality: query and remove existing identical annotations
    if overwrite:
        swxsoc.log.info("Overwriting existing annotations.")
        existing_annotations = query_annotations(
            start_time=start_time,
            end_time=end_time or start_time,
            tags=tags,
            dashboard_id=dashboard_id,
            panel_id=panel_id,
            mission_dashboard=mission_dashboard,
        )

        for annotation in existing_annotations:
            if annotation.get("text") == text:
                annotation_id = annotation.get("id")
                if annotation_id:
                    removed = remove_annotation_by_id(annotation_id, mission_dashboard)
                    if removed:
                        swxsoc.log.info(
                            f"Removed existing annotation with ID {annotation_id}."
                        )
    payload = {
        "time": _to_milliseconds(start_time),
        "text": text,
        "tags": tags,
    }
    if end_time:
        payload["timeEnd"] = _to_milliseconds(end_time)
    if dashboard_id:
        payload["dashboardUID"] = dashboard_id
    if panel_id:
        payload["panelId"] = panel_id

    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        response = requests.post(
            f"{BASE_URL}/api/annotations", headers=HEADERS, json=payload
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(f"Failed to create annotation: {e}")
        return {}
    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(
            f"Failed to retrieve panels for dashboard ID {dashboard_id}: {e}"
        )
        return {}


def remove_annotation_by_id(
    annotation_id: int, mission_dashboard: Optional[str] = None
) -> bool:
    """
    Deletes an annotation by its ID.

    Args:
        annotation_id (int): The ID of the annotation to delete.

    Returns:
        bool: True if the annotation was successfully deleted, False otherwise.
    """
    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        full_url = f"{BASE_URL}/api/annotations/{annotation_id}"
        response = requests.delete(full_url, headers=HEADERS)
        response.raise_for_status()
        return (
            response.status_code == 200
        )  # Returns True if annotation was deleted successfully (204 No Content)
    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(
            f"Failed to remove annotation with ID {annotation_id}: {e} [swxsoc.util.util]"
        )
        return False
    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(f"Failed to connect to the server: {e}")
        return False
