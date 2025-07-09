"""Tests for data_access.py (SWXSOCClient and FIDO attribute classes)"""

import os
import tempfile
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler

import boto3
import parfive
import pytest
from astropy.time import Time
from moto import mock_aws

import swxsoc
from swxsoc.util.data_access import (
    AttrAnd,
    DataType,
    DevelopmentBucket,
    HTTPDataClient,
    Instrument,
    Level,
    S3DataClient,
    SearchTime,
)

time = "2024-04-06T12:06:21"
time_formatted = "20240406T120621"


@mock_aws
def test_search_all_attr():
    conn = boto3.resource("s3", region_name="us-east-1")

    bucket_name = "swxsoc-eea"
    conn.create_bucket(Bucket=bucket_name)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key="l0/2024/04/swxsoc_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=bucket_name,
        Key=f"l1/2024/04/swxsoc_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )

    fido_client = S3DataClient()

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
    conn = boto3.resource("s3", region_name="us-east-1")

    buckets = ["swxsoc-eea", "swxsoc-nemisis", "swxsoc-merit", "swxsoc-spani"]

    for bucket in buckets:
        conn.create_bucket(Bucket=bucket)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=buckets[0],
        Key="l0/2024/04/swxsoc_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=buckets[0],
        Key=f"l1/2024/04/swxsoc_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )
    s3.put_object(
        Bucket=buckets[1],
        Key="l0/2024/04/swxsoc_NEM_l0_2024094-124603_v01.bin",
        Body=b"test data 3",
    )
    s3.put_object(
        Bucket=buckets[1],
        Key=f"l3/2024/04/swxsoc_nem_l3_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 4",
    )
    s3.put_object(
        Bucket=buckets[2],
        Key="l0/2024/04/swxsoc_MERIT_l0_2024094-124603_v01.bin",
        Body=b"test data 5",
    )
    s3.put_object(
        Bucket=buckets[2],
        Key=f"l3/2024/04/swxsoc_mrt_l3_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 6",
    )
    s3.put_object(
        Bucket=buckets[3],
        Key="l0/2024/04/swxsoc_SPANI_l0_2024094-124603_v01.bin",
        Body=b"test data 7",
    )
    s3.put_object(
        Bucket=buckets[3],
        Key=f"l3/2024/04/swxsoc_spn_l3_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 8",
    )

    fido_client = S3DataClient()

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
    conn = boto3.resource("s3", region_name="us-east-1")

    buckets = ["swxsoc-eea", "swxsoc-nemisis", "swxsoc-merit", "swxsoc-spani"]

    for bucket in buckets:
        conn.create_bucket(Bucket=bucket)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=buckets[0],
        Key="l0/2024/04/swxsoc_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=buckets[0],
        Key=f"l1/2024/04/swxsoc_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )

    fido_client = S3DataClient()

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
    conn = boto3.resource("s3", region_name="us-east-1")

    buckets = ["swxsoc-eea", "swxsoc-nemisis", "swxsoc-merit", "swxsoc-spani"]

    for bucket in buckets:
        conn.create_bucket(Bucket=bucket)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=buckets[0],
        Key="l0/2024/04/swxsoc_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=buckets[0],
        Key=f"l1/2024/04/swxsoc_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )
    s3.put_object(
        Bucket=buckets[1],
        Key="l0/2024/04/swxsoc_NEM_l0_2024094-124603_v01.bin",
        Body=b"test data 3",
    )

    fido_client = S3DataClient()

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
    conn = boto3.resource("s3", region_name="us-east-1")

    dev_buckets = [
        "dev-swxsoc-eea",
        "dev-swxsoc-nemisis",
        "dev-swxsoc-merit",
        "dev-swxsoc-spani",
    ]
    buckets = ["swxsoc-eea", "swxsoc-nemisis", "swxsoc-merit", "swxsoc-spani"]

    for bucket in dev_buckets:
        conn.create_bucket(Bucket=bucket)

    for bucket in buckets:
        conn.create_bucket(Bucket=bucket)

    s3 = boto3.client("s3")
    for bucket in dev_buckets:
        s3.put_object(
            Bucket=bucket,
            Key="l0/2024/04/swxsoc_EEA_l0_2024094-124603_v01.bin",
            Body=b"test data 1",
        )

    fido_client = S3DataClient()

    # Test search with a query for in development bucket
    query = AttrAnd([DevelopmentBucket(True)])
    results = fido_client.search(query)

    assert len(results) == 4

    # Test search with a query for not in development bucket
    query = AttrAnd([DevelopmentBucket(False)])
    results = fido_client.search(query)

    assert len(results) == 0


@mock_aws
def test_fetch():
    conn = boto3.resource("s3", region_name="us-east-1")

    bucket_name = "swxsoc-eea"
    conn.create_bucket(Bucket=bucket_name)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key="l0/2024/04/swxsoc_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )
    s3.put_object(
        Bucket=bucket_name,
        Key=f"l1/2024/04/swxsoc_eea_l1_{time_formatted}_v1.2.3.cdf",
        Body=b"test data 2",
    )

    fido_client = S3DataClient()

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

    assert len(results) == 1

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


# --- Helper: Temporary HTTP server serving a directory ---
@pytest.fixture
def http_file_server():
    # Set Config to use the padre mission
    os.environ["SWXSOC_MISSION"] = "padre"
    swxsoc._reconfigure()
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files and directories
        os.makedirs(
            os.path.join(tmpdir, "padre-meddea/l1/housekeeping/2025/05/04"),
            exist_ok=True,
        )
        file1 = os.path.join(
            tmpdir,
            "padre-meddea/l1/housekeeping/2025/05/04/padre_meddea_l1_housekeeping_20250504T000000_v0.1.0.fits",
        )
        with open(file1, "w") as f:
            f.write("dummy fits data 1")
        file2 = os.path.join(
            tmpdir,
            "padre-meddea/l1/housekeeping/2025/05/04/padre_meddea_l1_housekeeping_20250504T000000_v0.2.0.fits",
        )
        with open(file2, "w") as f:
            f.write("dummy fits data 2")

        # Start HTTP server in a thread
        class QuietHandler(SimpleHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

        server = HTTPServer(("localhost", 0), QuietHandler)
        port = server.server_port
        thread = threading.Thread(target=server.serve_forever)
        cwd = os.getcwd()
        os.chdir(tmpdir)
        thread.start()
        try:
            yield f"http://localhost:{port}/"
        finally:
            server.shutdown()
            thread.join()
            os.chdir(cwd)
            del os.environ["SWXSOC_MISSION"]
            swxsoc._reconfigure()


# --- Parameterized tests for HTTPDataSource.search ---
@pytest.mark.parametrize(
    "instrument,level,data_type,expected_count",
    [
        ("meddea", "l1", "housekeeping", 2),
        ("meddea", "l1", None, 2),
        (None, "l1", "housekeeping", 2),
        ("meddea", None, "housekeeping", 2),
        ("meddea", "l0", "housekeeping", 0),
    ],
)
def test_httpdatasource_search(
    http_file_server, instrument, level, data_type, expected_count
):
    ds = HTTPDataClient(base_url=http_file_server)
    query = {}
    if instrument:
        query["instrument"] = instrument
    if level:
        query["level"] = level
    if data_type:
        query["data_type"] = data_type
    # Add a time range that matches the files
    query["startTime"] = "2025-05-04"
    query["endTime"] = "2025-05-04"
    results = ds._make_search(query)
    assert len(results) == expected_count
    for row in results:
        assert row[0] == (instrument or "meddea")
        assert row[4] == (level or "l1")
        if data_type:
            assert row[6] == data_type
        assert row[7].endswith(".fits")


# --- Test SWXSOCClient with HTTPDataSource ---
def test_swxsocclient_http_search(http_file_server):
    client = HTTPDataClient(base_url=http_file_server)
    query = AttrAnd(
        [
            SearchTime("2025-05-04", "2025-05-04"),
            Level("l1"),
            Instrument("meddea"),
            DataType("housekeeping"),
        ]
    )
    results = client.search(query)
    assert len(results) == 2
    for row in results:
        assert row["instrument"] == "meddea"
        assert row["level"] == "l1"
        assert row["key"].endswith(".fits")


# --- Test fetch queues downloads ---
def test_swxsocclient_http_fetch(http_file_server):
    import parfive

    client = HTTPDataClient(base_url=http_file_server)
    query = AttrAnd(
        [
            SearchTime("2025-05-04", "2025-05-04"),
            Level("l1"),
            Instrument("meddea"),
            DataType("housekeeping"),
        ]
    )
    results = client.search(query)
    downloader = parfive.Downloader(progress=False, overwrite=True)
    client.fetch(results, path=".", downloader=downloader)
    assert downloader.queued_downloads == 2


@mock_aws
def test_fetch_exceptions_s3():
    """Test fetch exceptions for S3 data source."""
    conn = boto3.resource("s3", region_name="us-east-1")
    bucket_name = "swxsoc-eea"
    conn.create_bucket(Bucket=bucket_name)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key="l0/2024/04/swxsoc_EEA_l0_2024094-124603_v01.bin",
        Body=b"test data 1",
    )

    fido_client = S3DataClient()
    query = AttrAnd([Instrument("eea"), Level("l0")])
    results = fido_client.search(query)

    # Test with incorrect downloader
    with pytest.raises(TypeError):
        fido_client.fetch(results, path=".", downloader="not a downloader")

    # Test with non-existent path
    non_existent_path = "/path/does/not/exist/definitely/not"
    with pytest.raises(FileNotFoundError):
        downloader = parfive.Downloader(progress=False, overwrite=True)
        fido_client.fetch(results, path=non_existent_path, downloader=downloader)


def test_fetch_exceptions_http(http_file_server):
    """Test fetch exceptions for HTTP data source."""
    client = HTTPDataClient(base_url=http_file_server)
    query = AttrAnd(
        [
            SearchTime("2025-05-04", "2025-05-04"),
            Level("l1"),
            Instrument("meddea"),
            DataType("housekeeping"),
        ]
    )
    results = client.search(query)

    # Test with incorrect downloader
    with pytest.raises(TypeError):
        client.fetch(results, path=".", downloader="not a downloader")

    # Test with non-existent path
    non_existent_path = "/path/does/not/exist/definitely/not"
    with pytest.raises(FileNotFoundError):
        downloader = parfive.Downloader(progress=False, overwrite=True)
        client.fetch(results, path=non_existent_path, downloader=downloader)
