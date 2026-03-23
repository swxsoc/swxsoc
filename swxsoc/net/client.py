"""
SWXSOC FIDO Client for searching and fetching data from AWS S3.

This module provides the SWXSOCClient class, which implements the sunpy
BaseClient interface for querying SWXSOC data archives.
"""

import os
from datetime import datetime
from pathlib import Path

import astropy.units as u
import boto3
import sunpy.time
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError, NoCredentialsError
from dateutil.relativedelta import relativedelta
from parfive import Downloader
from sunpy.net.attr import AttrAnd
from sunpy.net.base_client import BaseClient, QueryResponseTable, convert_row_to_table

import swxsoc
from swxsoc.net.attr import DevelopmentBucket, Instrument, Level, SearchTime, walker

__all__ = ["SWXSOCClient"]


class SWXSOCClient(BaseClient):
    """
    Client for searching for SWXSOC data on AWS.
    This client provides search and fetch functionality for SWXSOC data and is based on the sunpy BaseClient for FIDO.

    For more information on the sunpy BaseClient, see: https://docs.sunpy.org/en/stable/generated/api/sunpy.net.base_client.BaseClient.html

    Note that AWS buckets may require access keys.

    Examples
    --------
    >>> from swxsoc.net.attr import AttrAnd, SearchTime, Level, Descriptor, Instrument
    >>> from swxsoc.net.client import SWXSOCClient
    >>> from astropy.time import Time
    >>> client = SWXSOCClient()
    >>> query = AttrAnd([SearchTime(start=Time("2025-07-10T00:00:00"), end=Time("2025-07-11T00:00:00")),
    ...    Instrument("meddea"),
    ...    Level("l0"),
    ...    Descriptor("housekeeping")])
    >>> results = client.search(query)  # doctest: +SKIP
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
                else f"https://{row['bucket']}.s3.amazonaws.com/{row['key']}"
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
        from swxsoc.util.util import parse_science_filename

        instrument = query.get("instrument")
        levels = query.get("level")
        start_time = query.get("startTime")
        end_time = query.get("endTime")
        descriptor = query.get("descriptor")
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
                "No instrument specified or invalid instrument. Searching all instruments."
            )
            instrument_bucket_to_search = instrument_buckets.values()
        else:
            swxsoc.log.info(f"Searching for instrument: {instrument}")
            instrument_bucket_to_search = [instrument_buckets[instrument]]

        swxsoc.log.info(f"Searching in buckets: {instrument_bucket_to_search}")

        files_in_s3 = cls.list_files_in_s3(instrument_bucket_to_search)

        if levels is not None or start_time is not None or end_time is not None:
            swxsoc.log.info(
                f"Searching for files with level {levels} between {start_time} and {end_time}"
            )
            if descriptor:
                swxsoc.log.info(f"Searching for files with descriptor: {descriptor}")

            prefixes = cls.generate_prefixes(levels, start_time, end_time, descriptor)

            matched_files = []
            for this_s3_file in files_in_s3:
                for this_prefix_list in prefixes:
                    if all(
                        this_token in str(Path(this_s3_file["Key"]).parent)
                        for this_token in this_prefix_list
                    ):
                        matched_files.append(this_s3_file)
        else:
            swxsoc.log.info("Searching for all files")
        # remove duplicates
        unique_matched_files = []
        seen = []
        for this_file in matched_files:
            if this_file["Key"] not in seen:
                seen.append(this_file["Key"])
                unique_matched_files.append(this_file)
        matched_files = unique_matched_files

        swxsoc.log.info(
            f"Found {len(matched_files)} files in S3 matching search criteria"
        )

        rows = []

        for s3_object in matched_files:
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
                    except ClientError:
                        raise Exception(
                            f"Unsigned request failed for bucket {bucket_name} (Ensure you have the correct IAM permissions, or are on the VPN)"
                        )
                else:
                    raise Exception(f"Error accessing bucket {bucket_name}: {e}")

        return content

    @staticmethod
    def generate_prefixes(
        levels: list, start_time: str, end_time: str, descriptor: str
    ) -> list:
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
        descriptor : str
            The file descriptor

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
                these_tokens = [
                    f"{current_time.year}",
                    f"{current_time.month:02d}",
                    level,
                ]
                if descriptor:
                    these_tokens.append(descriptor)
                prefixes.append(these_tokens)
            current_time += relativedelta(months=1)
        swxsoc.log.debug(f"Generated prefix: {prefixes}")

        return prefixes
