"""Tests for swxsoc.io.s3 (S3 helper functions)"""

import os
import tempfile
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from swxsoc.io import s3
from swxsoc.io.s3 import (
    check_file_existence_in_target_buckets,
    copy_file_in_s3,
    create_s3_client_session,
    create_s3_file_key,
    download_file_from_s3,
    get_science_file,
    list_files_in_bucket,
    object_exists,
    parse_file_key,
    push_science_file,
    upload_file_to_s3,
)
from swxsoc.util.util import parse_science_filename


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


def test_create_s3_client_session():
    client = create_s3_client_session()
    assert client.meta.service_model.service_name == "s3"


def test_parse_file_key():
    assert parse_file_key("some/nested/path/file.cdf") == "file.cdf"
    assert parse_file_key("file.cdf") == "file.cdf"


def test_create_s3_file_key():
    """
    Test function that tests if the create_s3_file_key function
    returns the correct file key.
    """

    # Test L0 file
    test_valid_file_key = "hermes_EEA_l0_2022335-200137_v01.bin"

    valid_key = create_s3_file_key(
        parse_science_filename, old_file_key=test_valid_file_key
    )

    assert valid_key == "l0/2022/12/01/hermes_EEA_l0_2022335-200137_v01.bin"

    # Test CDF file
    test_valid_file_key = "hermes_eea_ql_eventlist_20230205T000006_v1.0.01.cdf"

    valid_key = create_s3_file_key(
        parse_science_filename, old_file_key=test_valid_file_key
    )

    assert (
        valid_key
        == "ql/eventlist/2023/02/05/hermes_eea_ql_eventlist_20230205T000006_v1.0.01.cdf"
    )

    # Test CDF file
    test_valid_file_key = "hermes_eea_l1_hk_20230205T000006_v1.0.01.cdf"

    valid_key = create_s3_file_key(
        parse_science_filename, old_file_key=test_valid_file_key
    )

    assert (
        valid_key
        == "l1/housekeeping/2023/02/05/hermes_eea_l1_hk_20230205T000006_v1.0.01.cdf"
    )

    def test_parser(filename):
        return {"level": "l0"}

    # Test that the function raises a KeyError if the parser does not return a level
    try:
        create_s3_file_key(test_parser, old_file_key=test_valid_file_key)
    except KeyError as e:
        assert e is not None

    # Test unvalid file key
    test_invalid_file_key = "hermes_EEA_l0_2022335-200137_v01"

    try:
        create_s3_file_key(parse_science_filename, old_file_key=test_invalid_file_key)
    except ValueError as e:
        assert e is not None

    # Test file key containing "latest"
    test_latest_file_key = "hermes_EEA_l0_latest_2022335-200137_v01.bin"
    valid_key = create_s3_file_key(
        parse_science_filename, old_file_key=test_latest_file_key
    )
    assert valid_key == "latest/hermes_EEA_l0_latest_2022335-200137_v01.bin"


def test_list_files_in_bucket(s3_client):
    s3_client.create_bucket(Bucket="test-bucket")
    s3_client.put_object(Bucket="test-bucket", Key="a.txt", Body=b"a")
    s3_client.put_object(Bucket="test-bucket", Key="b.txt", Body=b"b")

    files = list_files_in_bucket(s3_client, "test-bucket")

    assert sorted(files) == ["a.txt", "b.txt"]


def test_object_exists(s3_client):
    s3_client.create_bucket(Bucket="test-bucket")
    s3_client.put_object(Bucket="test-bucket", Key="a.txt", Body=b"a")

    assert object_exists(s3_client, "test-bucket", "a.txt") is True
    assert object_exists(s3_client, "test-bucket", "missing.txt") is False


def test_check_file_existence_in_target_buckets(s3_client):
    s3_client.create_bucket(Bucket="source-bucket")
    s3_client.create_bucket(Bucket="target-bucket")
    s3_client.put_object(Bucket="target-bucket", Key="a.txt", Body=b"a")

    assert check_file_existence_in_target_buckets(
        s3_client, "a.txt", "source-bucket", ["source-bucket", "target-bucket"]
    )
    assert not check_file_existence_in_target_buckets(
        s3_client, "missing.txt", "source-bucket", ["source-bucket", "target-bucket"]
    )


def test_download_and_upload_file_to_s3(tmp_path, s3_client):
    s3_client.create_bucket(Bucket="test-bucket")
    local_file = tmp_path / "a.txt"
    local_file.write_text("hello")

    upload_file_to_s3(
        s3_client,
        filename=str(local_file),
        destination_bucket="test-bucket",
        file_key="a.txt",
    )
    assert object_exists(s3_client, "test-bucket", "a.txt")

    downloaded_path = download_file_from_s3(
        s3_client, "test-bucket", "a.txt", "downloaded.txt"
    )
    assert downloaded_path.exists()
    assert downloaded_path.read_text() == "hello"


def test_copy_file_in_s3_move(s3_client):
    s3_client.create_bucket(Bucket="source-bucket")
    s3_client.create_bucket(Bucket="destination-bucket")
    s3_client.put_object(Bucket="source-bucket", Key="a.txt", Body=b"a")

    copy_file_in_s3(s3_client, "source-bucket", "destination-bucket", "a.txt", "b.txt")

    assert object_exists(s3_client, "destination-bucket", "b.txt")
    assert not object_exists(s3_client, "source-bucket", "a.txt")


def test_copy_file_in_s3_keep_source(s3_client):
    s3_client.create_bucket(Bucket="source-bucket")
    s3_client.create_bucket(Bucket="destination-bucket")
    s3_client.put_object(Bucket="source-bucket", Key="a.txt", Body=b"a")

    copy_file_in_s3(
        s3_client,
        "source-bucket",
        "destination-bucket",
        "a.txt",
        "b.txt",
        delete_source_file=False,
    )

    assert object_exists(s3_client, "destination-bucket", "b.txt")
    assert object_exists(s3_client, "source-bucket", "a.txt")


def test_get_science_file_dry_run():
    path = get_science_file("some-bucket", "file_key.cdf", "parsed.cdf", dry_run=True)
    assert str(path) == "file_key.cdf"


def test_get_science_file_test_data(monkeypatch):
    monkeypatch.setenv("USE_INSTRUMENT_TEST_DATA", "True")
    path = get_science_file("some-bucket", "file_key.cdf", "parsed.cdf")
    assert str(path) == "file_key.cdf"


def test_get_science_file_local_path(monkeypatch, tmp_path):
    local_file = tmp_path / "local.cdf"
    local_file.write_text("data")
    monkeypatch.delenv("USE_INSTRUMENT_TEST_DATA", raising=False)
    monkeypatch.setenv("SDC_AWS_FILE_PATH", str(local_file))

    path = get_science_file("some-bucket", "file_key.cdf", "parsed.cdf")
    assert path == tmp_path / local_file


def test_get_science_file_downloads_from_s3(monkeypatch, s3_client, tmp_path):
    monkeypatch.delenv("USE_INSTRUMENT_TEST_DATA", raising=False)
    monkeypatch.delenv("SDC_AWS_FILE_PATH", raising=False)
    monkeypatch.setattr(s3, "create_s3_client_session", lambda: s3_client)

    s3_client.create_bucket(Bucket="instrument-bucket")
    s3_client.put_object(Bucket="instrument-bucket", Key="file_key.cdf", Body=b"data")

    path = get_science_file("instrument-bucket", "file_key.cdf", "parsed.cdf")

    assert path == Path(tempfile.gettempdir()) / "parsed.cdf"
    assert path.read_text() == "data"


def test_get_science_file_not_found(monkeypatch, s3_client):
    monkeypatch.delenv("USE_INSTRUMENT_TEST_DATA", raising=False)
    monkeypatch.delenv("SDC_AWS_FILE_PATH", raising=False)
    monkeypatch.setattr(s3, "create_s3_client_session", lambda: s3_client)

    s3_client.create_bucket(Bucket="instrument-bucket")

    with pytest.raises(FileNotFoundError):
        get_science_file("instrument-bucket", "file_key.cdf", "parsed.cdf")


def test_push_science_file_dry_run():
    filename = "hermes_eea_l1_hk_20230205T000006_v1.0.01.cdf"
    new_key = push_science_file(
        parse_science_filename, "test-bucket", filename, dry_run=True
    )
    assert (
        new_key
        == "l1/housekeeping/2023/02/05/hermes_eea_l1_hk_20230205T000006_v1.0.01.cdf"
    )


def test_push_science_file_uploads(monkeypatch, s3_client):
    s3_client.create_bucket(Bucket="test-bucket")
    monkeypatch.delenv("USE_INSTRUMENT_TEST_DATA", raising=False)
    monkeypatch.delenv("SDC_AWS_FILE_PATH", raising=False)
    monkeypatch.setattr(s3, "create_s3_client_session", lambda: s3_client)

    filename = "hermes_eea_l1_hk_20230205T000006_v1.0.01.cdf"
    local_file = Path(tempfile.gettempdir()) / filename
    local_file.write_text("data")

    new_key = push_science_file(parse_science_filename, "test-bucket", filename)

    assert (
        new_key
        == "l1/housekeeping/2023/02/05/hermes_eea_l1_hk_20230205T000006_v1.0.01.cdf"
    )
    assert object_exists(s3_client, "test-bucket", new_key)
