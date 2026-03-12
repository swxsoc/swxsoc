"""Tests for swxsoc.net.client (FIDO client)"""

import boto3
import parfive
import pytest
from astropy.time import Time
from moto import mock_aws
from sunpy.net.attr import AttrAnd

from swxsoc.net.attr import Descriptor, DevelopmentBucket, Instrument, Level, SearchTime
from swxsoc.net.client import SWXSOCClient

time = "2024-04-06T12:06:21"
time_formatted = "20240406T120621"
time_unix_ms = "1712405181000"


@mock_aws
def test_search_all_attr():
    # HERMES mission is set by default via autouse fixture in conftest.py

    conn = boto3.resource("s3", region_name="us-east-1")

    bucket_name = "hermes-eea"
    conn.create_bucket(Bucket=bucket_name)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key="l0/2024/04/hermes_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=bucket_name,
        Key=f"l1/2024/04/hermes_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )

    fido_client = SWXSOCClient()

    # Test search with a query for specific instrument, level, and time
    query = AttrAnd(
        [
            SearchTime("2024-01-01", "2025-01-01"),
            DevelopmentBucket(False),
            Level("l0"),
            Instrument("eea"),
        ]
    )
    results = fido_client.search(query)

    for result in results:
        assert result["instrument"] == "eea"
        assert result["level"] == "l0"
        assert result["version"] is None
        assert result["time"] == Time("2024-04-03T12:46:03")

    # Test search with a query for specific instrument, level, and time
    query = AttrAnd(
        [
            SearchTime("2024-01-01", "2025-01-01"),
            DevelopmentBucket(False),
            Level("l1"),
            Instrument("eea"),
        ]
    )
    results = fido_client.search(query)
    for result in results:
        assert result["instrument"] == "eea"
        assert result["level"] == "l1"
        assert result["version"] == "1.2.3"
        assert result["time"] == Time("2024-04-06T12:06:21")


@mock_aws
def test_search_time_attr():
    # HERMES mission is set by default via autouse fixture in conftest.py

    conn = boto3.resource("s3", region_name="us-east-1")

    buckets = ["hermes-eea", "hermes-nemisis", "hermes-merit", "hermes-spani"]

    for bucket in buckets:
        conn.create_bucket(Bucket=bucket)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=buckets[0],
        Key="l0/2024/04/hermes_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=buckets[0],
        Key=f"l1/2024/04/hermes_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )
    s3.put_object(
        Bucket=buckets[1],
        Key="l0/2024/04/hermes_NEM_l0_2024094-124603_v01.bin",
        Body=b"test data 3",
    )
    s3.put_object(
        Bucket=buckets[1],
        Key=f"l3/2024/04/hermes_nem_l3_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 4",
    )
    s3.put_object(
        Bucket=buckets[2],
        Key="l0/2024/04/hermes_MERIT_l0_2024094-124603_v01.bin",
        Body=b"test data 5",
    )
    s3.put_object(
        Bucket=buckets[2],
        Key=f"l3/2024/04/hermes_mrt_l3_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 6",
    )
    s3.put_object(
        Bucket=buckets[3],
        Key="l0/2024/04/hermes_SPANI_l0_2024094-124603_v01.bin",
        Body=b"test data 7",
    )
    s3.put_object(
        Bucket=buckets[3],
        Key=f"l3/2024/04/hermes_spn_l3_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 8",
    )

    fido_client = SWXSOCClient()

    # Test search with a query for specific time
    query = AttrAnd([SearchTime("2024-01-01", "2025-01-01")])
    results = fido_client.search(query)

    for result in results:
        assert result["time"] >= Time("2024-01-01")
        assert result["time"] <= Time("2025-01-01")

    # Test search with a query for out of range time
    query = AttrAnd([SearchTime("2025-01-01", "2026-01-01")])
    results = fido_client.search(query)
    assert len(results) == 0


@mock_aws
def test_search_instrument_attr():
    # HERMES mission is set by default via autouse fixture in conftest.py

    conn = boto3.resource("s3", region_name="us-east-1")

    buckets = ["hermes-eea", "hermes-nemisis", "hermes-merit", "hermes-spani"]

    for bucket in buckets:
        conn.create_bucket(Bucket=bucket)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=buckets[0],
        Key="l0/2024/04/hermes_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=buckets[0],
        Key=f"l1/2024/04/hermes_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )

    fido_client = SWXSOCClient()

    # Test search with a query for specific instrument
    query = AttrAnd([Instrument("eea")])
    results = fido_client.search(query)

    for result in results:
        assert result["instrument"] == "eea"

    # Test search with a query for out of range instrument
    query = AttrAnd([Instrument("not_instrument")])
    results = fido_client.search(query)

    # Should search all instruments
    assert len(results) == 2


@mock_aws
def test_search_level_attr():
    # HERMES mission is set by default via autouse fixture in conftest.py

    conn = boto3.resource("s3", region_name="us-east-1")

    buckets = ["hermes-eea", "hermes-nemisis", "hermes-merit", "hermes-spani"]

    for bucket in buckets:
        conn.create_bucket(Bucket=bucket)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=buckets[0],
        Key="l0/2024/04/hermes_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=buckets[0],
        Key=f"l1/2024/04/hermes_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )
    s3.put_object(
        Bucket=buckets[1],
        Key="l0/2024/04/hermes_NEM_l0_2024094-124603_v01.bin",
        Body=b"test data 3",
    )

    fido_client = SWXSOCClient()

    # Test search with a query for specific level
    query = AttrAnd([Level("l0")])
    results = fido_client.search(query)

    for result in results:
        assert result["level"] == "l0"

    assert len(results) == 2

    # Test search with a query for existing level but not in the bucket
    query = AttrAnd([Level("l2")])
    results = fido_client.search(query)

    assert len(results) == 0

    # Test search with a query for out of range should raise an error
    query = AttrAnd([Level("l5")])
    with pytest.raises(ValueError):
        results = fido_client.search(query)


@mock_aws
def test_search_development_bucket():
    # HERMES mission is set by default via autouse fixture in conftest.py

    conn = boto3.resource("s3", region_name="us-east-1")

    dev_buckets = [
        "dev-hermes-eea",
        "dev-hermes-nemisis",
        "dev-hermes-merit",
        "dev-hermes-spani",
    ]
    buckets = ["hermes-eea", "hermes-nemisis", "hermes-merit", "hermes-spani"]

    for bucket in dev_buckets:
        conn.create_bucket(Bucket=bucket)

    for bucket in buckets:
        conn.create_bucket(Bucket=bucket)

    s3 = boto3.client("s3")
    for bucket in dev_buckets:
        s3.put_object(
            Bucket=bucket,
            Key="l0/2024/04/hermes_EEA_l0_2024094-124603_v01.bin",
            Body=b"test data 1",
        )

    fido_client = SWXSOCClient()

    # Test search with a query for in development bucket
    query = AttrAnd([DevelopmentBucket(True)])
    results = fido_client.search(query)

    assert len(results) == 1

    # Test search with a query for not in development bucket
    query = AttrAnd([DevelopmentBucket(False)])
    results = fido_client.search(query)

    assert len(results) == 0


@mock_aws
def test_fetch():
    # HERMES mission is set by default via autouse fixture in conftest.py

    conn = boto3.resource("s3", region_name="us-east-1")

    bucket_name = "hermes-eea"
    conn.create_bucket(Bucket=bucket_name)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key="l0/2024/04/hermes_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=bucket_name,
        Key=f"l1/2024/04/hermes_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )

    fido_client = SWXSOCClient()

    # Test search with a query for specific instrument, level, and time
    query = AttrAnd(
        [
            SearchTime("2024-01-01", "2025-01-01"),
            DevelopmentBucket(False),
            Level("l0"),
            Instrument("eea"),
        ]
    )
    results = fido_client.search(query)

    # Initalizing parfive downloader
    downloader = parfive.Downloader(progress=False, overwrite=True)

    fido_client.fetch(results, path=".", downloader=downloader)

    assert downloader.queued_downloads == 1

    # Test search with a query for specific instrument, and time
    query = AttrAnd(
        [
            SearchTime("2024-01-01", "2025-01-01"),
            DevelopmentBucket(False),
            Instrument("eea"),
        ]
    )
    results = fido_client.search(query)

    # Initalizing parfive downloader
    downloader = parfive.Downloader(progress=False, overwrite=True)

    fido_client.fetch(results, path=".", downloader=downloader)

    assert downloader.queued_downloads == 2

    s3.put_object(
        Bucket=bucket_name,
        Key=f"l1/housekeeping/2024/04/hermes_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )
    s3.put_object(
        Bucket=bucket_name,
        Key=f"l1/spectrum/2024/04/hermes_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )
    query = AttrAnd(
        [
            SearchTime("2024-01-01", "2025-01-01"),
            DevelopmentBucket(False),
            Instrument("eea"),
            Descriptor("housekeeping"),
        ]
    )
    results = fido_client.search(query)
    assert len(results) == 1
    query = AttrAnd(
        [
            SearchTime("2024-01-01", "2025-01-01"),
            DevelopmentBucket(False),
            Instrument("eea"),
            Descriptor("spectrum"),
        ]
    )
    results = fido_client.search(query)
    assert len(results) == 1
    query = AttrAnd(
        [
            DevelopmentBucket(False),
            Instrument("eea"),
            Level("l1"),
        ]
    )
    results = fido_client.search(query)
    assert len(results) == 3
