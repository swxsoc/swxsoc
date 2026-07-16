"""
S3 helper functions for the SWxSOC data pipeline.

These helpers are used by the SDC AWS Lambda pipelines to move science files
between the incoming and instrument-specific S3 buckets.
"""

import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import boto3

import swxsoc


def create_s3_client_session():
    """
    Create a boto3 S3 client session.

    Returns
    -------
    boto3.client
        The boto3 S3 client.
    """
    try:
        return boto3.client("s3")
    except Exception as e:
        swxsoc.log.error({"status": "ERROR", "message": e}, exc_info=True)
        raise e


def parse_file_key(file_path: str) -> str:
    """
    Extract the file name from a full S3 object path.

    Parameters
    ----------
    file_path : str
        The full path (or S3 event key) of the file.

    Returns
    -------
    str
        The file name.
    """
    return Path(file_path).name


def create_s3_file_key(science_file_parser: Callable, old_file_key: str) -> str:
    """
    Build the destination S3 key for a science file within its instrument bucket.

    The key follows the ``{mission}/{instrument}/{level}/{descriptor}/{yyyy}/{mm}/{dd}/``
    layout, where ``descriptor`` is derived from the parsed file's descriptor
    (mapped to a friendlier name when known) or the parsed file's data level.

    Parameters
    ----------
    science_file_parser : Callable
        A parser function (e.g. ``swxsoc.util.util.parse_science_filename``)
        that returns a dict describing the file (``instrument``, ``level``,
        ``time``, and optionally ``descriptor``).
    old_file_key : str
        The existing S3 key (or local path) of the file to be relocated.

    Returns
    -------
    str
        The new S3 key for the file within its instrument bucket.
    """
    try:
        # Skip parsing and put any files whose filename contains "latest" into their own directory
        file_name = Path(old_file_key).name
        if "latest" in file_name:
            return f"latest/{file_name}"

        science_file = science_file_parser(old_file_key)
        time_value = science_file["time"].value

        if isinstance(time_value, datetime):
            timestamp = time_value
        else:
            timestamp = datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%S.%f")

        year = timestamp.year
        month = f"{timestamp.month:02d}"
        day = f"{timestamp.day:02d}"
        level = science_file["level"]
        descriptor = science_file.get("descriptor")

        if not descriptor:
            descriptor = "unknown"

        # Short names to long names mapping for descriptors
        descriptor_mapping = {
            "spec": "spectrum",
            "eventlist": "eventlist",
            "hk": "housekeeping",
        }

        if descriptor in descriptor_mapping:
            descriptor = descriptor_mapping[descriptor]

        # Get first valid_data_level from config
        valid_data_levels = swxsoc.config.get("mission").get(
            "valid_data_levels", ["l0", "l1", "ql"]
        )

        if level == valid_data_levels[0]:
            new_file_key = f"{level}/{year}/{month}/{day}/{old_file_key}"
        else:
            new_file_key = f"{level}/{descriptor}/{year}/{month}/{day}/{old_file_key}"

        return new_file_key

    except Exception as e:
        swxsoc.log.error(
            {"status": "ERROR", "message": str(e), "file_key": old_file_key}
        )
        raise


def list_files_in_bucket(s3_client, bucket_name: str) -> list:
    """
    List all object keys in an S3 bucket.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client to use.
    bucket_name : str
        The name of the bucket to list.

    Returns
    -------
    list
        The keys of all objects in the bucket.
    """
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            files.append(obj["Key"])

    return files


def check_file_existence_in_target_buckets(
    s3_client, file_key: str, source_bucket: str, target_buckets: list
) -> bool:
    """
    Check whether a file already exists in any of the given target buckets.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client to use.
    file_key : str
        The key of the file to check for.
    source_bucket : str
        The bucket the file originated from (excluded from the check).
    target_buckets : list
        The list of bucket names to check.

    Returns
    -------
    bool
        ``True`` if the file exists in any target bucket other than the source, else ``False``.
    """
    for target_bucket in target_buckets:
        if object_exists(s3_client, target_bucket, file_key):
            print(f"File {file_key} from {source_bucket} exists in {target_bucket}")
            return True
        else:
            print(
                f"File {file_key} from {source_bucket} does not exist in {target_bucket}"
            )

    return False


def object_exists(s3_client, bucket: str, file_key: str) -> bool:
    """
    Check whether an object exists in an S3 bucket.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client to use.
    bucket : str
        The name of the bucket.
    file_key : str
        The key of the object.

    Returns
    -------
    bool
        ``True`` if the object exists, ``False`` otherwise.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=file_key)
        return True
    except s3_client.exceptions.ClientError:
        return False


def download_file_from_s3(
    s3_client, source_bucket: str, file_key: str, parsed_file_key: str
) -> Path:
    """
    Download a file from S3 to ``/tmp``.

    NOTE: This function downloads the file to the ``/tmp`` directory.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client to use.
    source_bucket : str
        The name of the bucket to download from.
    file_key : str
        The key of the object to download.
    parsed_file_key : str
        The file name to use when saving to ``/tmp``.

    Returns
    -------
    pathlib.Path
        The path of the downloaded file.
    """
    swxsoc.log.info(f"Downloading file {parsed_file_key} from {source_bucket}")

    # Assume the file will be downloaded to /tmp
    download_path = Path("/tmp") / parsed_file_key
    swxsoc.log.debug(f"Downloading {file_key} from {source_bucket} to {download_path}")

    # Download the file from the source bucket to the specified path
    s3_client.download_file(source_bucket, file_key, str(download_path))

    return download_path


def upload_file_to_s3(
    s3_client, filename: str, destination_bucket: str, file_key: str
) -> Path:
    """
    Upload a file from ``/tmp`` to an S3 bucket.

    NOTE: This function assumes that the file to be uploaded is located in ``/tmp``.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client to use.
    filename : str
        The file name (relative to ``/tmp``) to upload.
    destination_bucket : str
        The name of the destination bucket.
    file_key : str
        The destination key for the object.

    Returns
    -------
    pathlib.Path
        The local path of the file that was uploaded.
    """
    swxsoc.log.info(f"Uploading file {file_key} to {destination_bucket}")

    # Assume the file is located in /tmp
    upload_path = Path("/tmp") / filename
    swxsoc.log.debug(
        f"Uploading {upload_path} to bucket {destination_bucket} as {file_key}"
    )

    # Upload file to the destination bucket with the specified key
    s3_client.upload_file(str(upload_path), destination_bucket, file_key)
    return upload_path


def copy_file_in_s3(
    s3_client,
    source_bucket: str,
    destination_bucket: str,
    file_key: str,
    new_file_key: str,
    delete_source_file: bool = True,
) -> None:
    """
    Copy (and optionally move) an object between S3 buckets.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client to use.
    source_bucket : str
        The name of the source bucket.
    destination_bucket : str
        The name of the destination bucket.
    file_key : str
        The key of the object in the source bucket.
    new_file_key : str
        The key to give the object in the destination bucket.
    delete_source_file : bool, optional
        Whether to delete the source object after copying (move semantics).
        Defaults to ``True``.

    Returns
    -------
    None
    """
    # Create copy source object
    copy_source = {"Bucket": source_bucket, "Key": file_key}
    swxsoc.log.debug(
        f"Copying {file_key} from {source_bucket} to {destination_bucket} as {new_file_key}"
    )
    # Copy file from source bucket to destination bucket
    s3_client.copy_object(
        CopySource=copy_source,
        Bucket=destination_bucket,
        Key=new_file_key,
    )
    swxsoc.log.debug(
        f"Source file {file_key} copied from {source_bucket} to {destination_bucket}"
    )

    # Verify the file was copied successfully
    success = object_exists(s3_client, destination_bucket, new_file_key)
    swxsoc.log.debug(
        f"File copy verification for {new_file_key} in {destination_bucket}: {success}"
    )

    # Delete source file if requested (move operation)
    if delete_source_file and success:
        s3_client.delete_object(Bucket=source_bucket, Key=file_key)
        swxsoc.log.debug(f"Source file {file_key} deleted from {source_bucket}")


def get_science_file(
    instrument_bucket_name: str,
    file_key: str,
    parsed_file_key: str,
    dry_run: bool = False,
) -> Path:
    """
    Resolve the local path of a science file, downloading it from S3 if needed.

    Resolution order:

    1. If ``dry_run`` is set, or the ``USE_INSTRUMENT_TEST_DATA`` environment
       variable is ``"True"``, assume the file is already available locally
       under the instrument package's test data and return the expected path
       without downloading.
    2. If the ``SDC_AWS_FILE_PATH`` environment variable is set, use it directly.
    3. Otherwise, download the file from ``instrument_bucket_name`` via S3.

    Parameters
    ----------
    instrument_bucket_name : str
        The name of the S3 bucket to download from.
    file_key : str
        The key of the object in the bucket.
    parsed_file_key : str
        The file name to use when saving to ``/tmp``.
    dry_run : bool, optional
        Indicates whether the operation is a dry run. Defaults to ``False``.

    Returns
    -------
    pathlib.Path
        The local path of the science file.
    """
    if dry_run:
        swxsoc.log.info("Dry Run - File will not be downloaded")
        return Path(file_key)

    if os.getenv("USE_INSTRUMENT_TEST_DATA") == "True":
        swxsoc.log.info("Using test data from instrument package")
        return Path(file_key)

    file_path = os.getenv("SDC_AWS_FILE_PATH")
    if file_path:
        swxsoc.log.info(
            f"Using file path specified in environment variables: {file_path}"
        )
        return Path(file_path)

    # Initialize S3 Client
    s3_client = create_s3_client_session()

    # Verify object eists in S3 Bucket
    if object_exists(
        s3_client=s3_client,
        bucket=instrument_bucket_name,
        file_key=file_key,
    ):
        # Download the file from S3 to /tmp
        return download_file_from_s3(
            s3_client, instrument_bucket_name, file_key, parsed_file_key
        )

    raise FileNotFoundError(
        f"File {file_key} not found in bucket {instrument_bucket_name}"
    )


def push_science_file(
    science_filename_parser: Callable,
    destination_bucket: str,
    calibrated_filename: str,
    dry_run: bool = False,
) -> str:
    """
    Upload a science file to the destination bucket, unless this is a dry run.
    Generates the file key for the new file using the given parser.

    Parameters
    ----------
    science_filename_parser : Callable
        The parser function used to generate the destination S3 key.
    destination_bucket : str
        The name of the destination S3 bucket.
    calibrated_filename : str
        The local path of the new file to be uploaded.
    dry_run : bool, optional
        Indicates whether the operation is a dry run. Defaults to ``False``.

    Returns
    -------
    str
        The key of the newly uploaded (or would-be uploaded) file.
    """
    # Generate file key for new file
    new_file_key = create_s3_file_key(science_filename_parser, calibrated_filename)

    if dry_run:
        swxsoc.log.info("Dry Run - File will not be uploaded")
        return new_file_key

    if os.getenv("USE_INSTRUMENT_TEST_DATA") == "True":
        swxsoc.log.info("Using test data from instrument package")
        return new_file_key

    if os.getenv("SDC_AWS_FILE_PATH"):
        swxsoc.log.info(
            "SDC_AWS_FILE_PATH is set. File Processed Locally - File will not be uploaded to S3"
        )
        return new_file_key

    # Initialize S3 Client
    s3_client = create_s3_client_session()

    # Upload the file to the destination bucket
    upload_file_to_s3(
        s3_client=s3_client,
        destination_bucket=destination_bucket,
        filename=calibrated_filename,
        file_key=new_file_key,
    )

    return new_file_key
