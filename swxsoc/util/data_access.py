"""
Data access client for SWxSOC data using the FIDO pattern.
"""

import os
import urllib.request
from datetime import datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urljoin

import astropy.units as u
import boto3
import sunpy.net.attrs as a
import sunpy.time
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError, NoCredentialsError
from dateutil.relativedelta import relativedelta
from parfive import Downloader
from sunpy.net.attr import AttrAnd, AttrOr, AttrWalker, SimpleAttr
from sunpy.net.base_client import BaseClient, QueryResponseTable, convert_row_to_table

import swxsoc
from swxsoc.util.util import parse_science_filename

__all__ = [
    "S3DataClient",
    "HTTPDataClient",
    "SearchTime",
    "Level",
    "Instrument",
    "DataType",
    "DevelopmentBucket",
]


class AbsDataClient(BaseClient):
    """
    Client for interacting with SWXSOC data. This client provides search and fetch functionality for SWXSOC data and is based on the sunpy BaseClient for FIDO.

    For more information on the sunpy BaseClient, see: https://docs.sunpy.org/en/stable/generated/api/sunpy.net.base_client.BaseClient.html

    """

    def search(self, query: AttrAnd) -> QueryResponseTable:
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

    def _make_search(self, query: dict) -> list:
        """
        Abstract method to be implemented by subclasses for performing the actual search.

        Parameters
        ----------
        query : dict
            The query parameters to search for.

        Returns
        -------
        list
            A list of results matching the query.
        """
        raise NotImplementedError("Subclasses must implement this method.")

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
            raise TypeError("Downloader must be an instance of parfive.Downloader")

        if path is None or path == ".":
            path = os.getcwd()

        if not os.path.exists(path) or not os.path.isdir(path):
            raise FileNotFoundError(f"Path {path} is not a directory")
        self._make_fetch(query_results, path=path, downloader=downloader, **kwargs)

    def _make_fetch(self, query_results, *, path, downloader, **kwargs):
        """
        Abstract method to be implemented by subclasses for performing the actual fetch.

        Parameters
        ----------
        query_results : list
            The results of the search query.
        path : str
            The directory path where files should be saved.
        downloader : Downloader
            The parfive downloader instance used for fetching files.
        """
        raise NotImplementedError("Subclasses must implement this method.")

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
        supported_attrs = {SearchTime, Level, Instrument, DataType, DevelopmentBucket}
        return supported_attrs.issuperset(query_attrs)


class S3DataClient(AbsDataClient):
    """
    Data source for searching and fetching from S3 buckets.
    """

    def _make_search(self, query: dict) -> list:
        """
        Abstract method to be implemented by subclasses for performing the actual search.

        Parameters
        ----------
        query : dict
            The query parameters to search for.

        Returns
        -------
        list
            A list of results matching the query.
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
                if level not in swxsoc.config["mission"]["valid_data_levels"]:
                    raise ValueError(f"Invalid data level: {level}")
        else:
            levels = swxsoc.config["mission"]["valid_data_levels"]

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

        files_in_s3 = S3DataClient.list_files_in_s3(instrument_bucket_to_search)

        if levels is not None or start_time is not None or end_time is not None:
            swxsoc.log.info(
                f"Searching for files with level {levels} between {start_time} and {end_time}"
            )

            prefixes = S3DataClient.generate_prefixes(levels, start_time, end_time)

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

    def _make_fetch(self, query_results, *, path, downloader, **kwargs):
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
        for row in query_results:
            swxsoc.log.info(f"Fetching {row['key']}")
            filepath = S3DataClient._make_filename(path, row)
            presigned_url = S3DataClient.generate_presigned_url(
                row["bucket"], row["key"]
            )
            url = (
                presigned_url
                if presigned_url is not None
                else f'https://{row["bucket"]}.s3.amazonaws.com/{row["key"]}'
            )
            downloader.enqueue_file(url, filename=filepath)

    @staticmethod
    def list_files_in_s3(bucket_names: list) -> list:
        # Move the logic from SWXSOCClient.list_files_in_s3 here
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
                        raise ClientError(
                            f"Unsigned request failed for bucket {bucket_name} (Ensure you have the correct IAM permissions, or are on the VPN)"
                        )
                else:
                    raise ClientError(f"Error accessing bucket {bucket_name}: {e}")

        return content

    @staticmethod
    def generate_prefixes(levels: list, start_time: str, end_time: str) -> list:
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

    @staticmethod
    def generate_presigned_url(bucket_name, object_key, expiration=3600):
        # Move the logic from SWXSOCClient.generate_presigned_url here
        try:
            s3_client = boto3.client("s3")
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
        try:
            swxsoc.log.info(f"Attempting unsigned access to {bucket_name}/{object_key}")
            url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}"
            return url
        except ClientError as unsigned_error:
            swxsoc.log.error(f"Unsigned access failed: {unsigned_error}")
            return None

    @staticmethod
    def _make_filename(path, row):
        return os.path.join(path, row["key"].split("/")[-1])


class HTTPDataClient(AbsDataClient):
    """
    Data source for searching and fetching from HTTP file servers.
    """

    def __init__(self, base_url="https://umbra.nascom.nasa.gov/padre/"):
        super().__init__()
        self.base_url = base_url

    def _make_search(self, query: dict) -> list:
        """
        Abstract method to be implemented by subclasses for performing the actual search.

        Parameters
        ----------
        query : dict
            The query parameters to search for.

        Returns
        -------
        list
            A list of results matching the query.
        """
        # Extract query parameters
        instrument = query.get("instrument")
        level = query.get("level")
        data_type = query.get("data_type")  # Extract data_type
        start_time = query.get("startTime")
        end_time = query.get("endTime")

        # Get search paths with data_type
        search_paths = self._get_search_paths(
            instrument, level, data_type, start_time, end_time
        )
        swxsoc.log.info(f"Search paths: {search_paths}")

        # Search each path
        all_files = []
        for path in search_paths:
            url = urljoin(self.base_url, path)
            swxsoc.log.info(f"Searching HTTP directory: {url}")
            files = self._crawl_directory(url, max_depth=3)
            all_files.extend(files)

        # Process and return results
        rows = []
        for file_url in all_files:
            swxsoc.log.info(f"Processing file URL: {file_url}")
            try:
                info = parse_science_filename(file_url)
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
                file_url,  # Key
                None,  # Size will be determined later
                None,  # Bucket not applicable for HTTP
                None,  # ETag not applicable for HTTP
                None,  # StorageClass not applicable for HTTP
                None,  # LastModified not applicable for HTTP
            ]
            rows.append(row)

        return rows

    def _get_search_paths(
        self,
        instrument=None,
        level=None,
        data_type=None,
        start_time=None,
        end_time=None,
    ):
        """Generate HTTP paths to search based on query parameters."""
        paths = []

        # Handle instrument paths
        if instrument:
            instrument_paths = [f"padre-{instrument.lower()}"]
        else:
            # If no instrument specified, search all known instruments
            instrument_paths = ["padre-meddea", "padre-sharp"]

        # Handle level paths
        if level:
            level_paths = [level.lower()]
        else:
            level_paths = ["l0", "l1"]  # Use config values

        # Handle data type paths
        if data_type:
            data_type_paths = [data_type.lower()]
        else:
            # Default data types to search - could come from config
            data_type_paths = ["housekeeping", "spectrum", "photon"]

        # Generate time-based paths if time range specified
        if start_time and end_time:
            time_paths = self._generate_time_paths(start_time, end_time)

            # Combine all path components
            for instrument_path in instrument_paths:
                for level_path in level_paths:
                    for data_type_path in data_type_paths:
                        for time_path in time_paths:
                            paths.append(
                                f"{instrument_path}/{level_path}/{data_type_path}/{time_path}/"
                            )
        else:
            # Without time constraints, include data type in the paths
            for instrument_path in instrument_paths:
                for level_path in level_paths:
                    for data_type_path in data_type_paths:
                        paths.append(f"{instrument_path}/{level_path}/{data_type_path}")

        return paths

    def _generate_time_paths(self, start_time, end_time):
        """
        Generate all year/month/day path components between start_time and end_time.

        Parameters
        ----------
        start_time : str
            Start time in ISO format (e.g., '2025-05-04')
        end_time : str
            End time in ISO format (e.g., '2025-07-07')

        Returns
        -------
        list
            List of path strings in format 'YYYY/MM/DD'
        """
        # Parse the ISO format times
        start_date = datetime.fromisoformat(start_time.split("T")[0])
        end_date = datetime.fromisoformat(end_time.split("T")[0])

        # Initialize empty list for paths
        time_paths = []

        # Iterate through each day in the range
        current_date = start_date
        while current_date <= end_date:
            # Format as YYYY/MM/DD
            path = (
                f"{current_date.year}/{current_date.month:02d}/{current_date.day:02d}"
            )
            time_paths.append(path)

            # Move to next day
            current_date += timedelta(days=1)

        swxsoc.log.debug(
            f"Generated {len(time_paths)} time paths from {start_time} to {end_time}"
        )
        return time_paths

    def _crawl_directory(self, url, max_depth=3, file_extension=".fits", base_url=None):
        """Directory crawler using only standard library."""

        # Track the original base URL on first call
        if base_url is None:
            base_url = url

        class LinkParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.links = []

            def handle_starttag(self, tag, attrs):
                if tag == "a":
                    for attr, value in attrs:
                        if attr == "href":
                            self.links.append(value)

        if max_depth <= 0:
            return []

        files = []
        try:
            with urllib.request.urlopen(url) as response:
                html = response.read().decode("utf-8")

            parser = LinkParser()
            parser.feed(html)

            for href in parser.links:
                # Skip parent directory links and query parameters
                if not href or href.startswith("?") or href == "../":
                    continue

                full_url = urljoin(url, href)

                # Don't crawl up: make sure we're still below our starting point
                if not full_url.startswith(base_url) or len(full_url) < len(base_url):
                    continue

                if href.endswith("/") and max_depth > 1:
                    subdir_files = self._crawl_directory(
                        full_url, max_depth - 1, file_extension, base_url
                    )
                    files.extend(subdir_files)
                elif href.lower().endswith(file_extension.lower()):
                    files.append(full_url)

            return files
        except Exception as e:
            swxsoc.log.warning(f"Error processing {url}: {e}")
            return []

    def _make_fetch(self, query_results, *, path, downloader, **kwargs):
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
            raise TypeError("Downloader must be an instance of parfive.Downloader")

        if path is None or path == ".":
            path = os.getcwd()

        if not os.path.exists(path) or not os.path.isdir(path):
            raise FileNotFoundError(f"Path {path} is not a directory")
        for row in query_results:
            url = row["key"]
            swxsoc.log.info(f"Fetching {url}")

            # Create local filepath
            filepath = HTTPDataClient._make_filename(path, row)

            # Queue the download - HTTP URLs are already complete
            downloader.enqueue_file(url, filename=filepath)

    @staticmethod
    def _make_filename(path, row):
        """
        Extract filename from HTTP URL and join with download path.

        Parameters
        ----------
        path : str
            Directory path where file will be saved
        row : dict
            Result row containing key (URL)

        Returns
        -------
        str
            Full path where the file should be saved
        """
        # Extract filename from URL (last part of the path)
        url = row["key"]
        filename = url.split("/")[-1]
        return os.path.join(path, filename)


# Initialize the attribute walker
walker = AttrWalker()


# Map sunpy attributes to SWxSOC attributes for easy access
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


class DataType(SimpleAttr):
    """
    Attribute for specifying the data type for the search.

    Attributes
    ----------
    value : str
        The data type value.
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


@walker.add_applier(DataType)
def apply_data_type(wlk, attr, params):
    """
    Applies 'DataType' attribute to the parameters.

    Parameters
    ----------
    wlk : AttrWalker
        The AttrWalker instance used for applying the attributes.
    attr : DataType
        The 'DataType' attribute to be applied.
    params : dict
        The parameters dictionary to be updated.
    """
    params.update({"data_type": attr.value.lower()})


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
