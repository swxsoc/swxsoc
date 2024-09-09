"""
This module provides general utility functions.
"""

import os

# Set the environment variable
os.environ["SWXSOC_MISSION"] = "padre"

from astropy.time import Time as AstropyTime
import astropy.units as u
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime
from dateutil.relativedelta import relativedelta
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
    "Time",
    "Level",
    "Instrument",
    "DevelopmentBucket",
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

        result["time"] = AstropyTime.strptime(
            filename_components[3 + offset], TIME_FORMAT_L0
        )

    elif file_ext == swxsoc.config["mission"]["file_extension"]:
        if filename_components[1] not in swxsoc.config["mission"]["inst_shortnames"]:
            raise ValueError(
                "File {filename} not recognized. Not a valid instrument name."
            )

        #  reverse the dictionary to look up instrument name from the short name
        from_shortname = {
            v: k for k, v in swxsoc.config["mission"]["inst_to_shortname"].items()
        }

        result["time"] = AstropyTime.strptime(filename_components[-2], TIME_FORMAT)

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


# ================================================================================================
#                                  SWXSOC FIDO CLIENT
# ================================================================================================

# Initialize the attribute walker
walker = AttrWalker()


# Map sunpy attributes to SWXSOC attributes for easy access
class Time(a.Time):
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


@walker.add_applier(Time)
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
    Client for interacting with SWXSOC data.

    Attributes
    ----------
    size_column : str
        The name of the column representing the size of files.
    """

    size_column = "size"

    def search(self, query):
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
        queries = walker.create(query)
        swxsoc.log.info(f"Searching with {queries}")

        results = []
        for query_parameters in queries:
            results.extend(self._make_search(query_parameters))

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
        Fetches the files based on query results and saves them to the specified path.

        Parameters
        ----------
        query_results : list
            The results of the search query.
        path : str
            The directory path where files should be saved.
        downloader : Downloader
            The downloader instance used for fetching files.
        """
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
        Generates a presigned URL for accessing an object in S3.

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
            The presigned URL if successful, otherwise None.
        """
        try:
            s3_client = boto3.client("s3")
            response = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": object_key},
                ExpiresIn=expiration,
            )
        except NoCredentialsError:
            print("Credentials not available")
            return None

        return response

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
        supported_attrs = {a.Time, a.Level}
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

            info = parse_science_filename(s3_object["Key"])
            row = [
                info.get("instrument", None),
                info.get("mode", None),
                info.get("test", False),
                info.get("time", None),
                info.get("level", None),
                info.get("version", None),
                info.get("descriptor", None),
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
        Lists all files in the specified S3 buckets.

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
