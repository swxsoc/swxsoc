"""Tests for util.py"""

import os
import pytest
import yaml
from moto import mock_aws

import boto3
from pathlib import Path
import parfive

from astropy import units as u
from astropy.timeseries import TimeSeries
from astropy.time import Time

import swxsoc
from swxsoc.util import util

time = "2024-04-06T12:06:21"
time_formatted = "20240406T120621"

# YAML content as a dictionary
config_content = {
    "general": {"time_format": "%Y-%m-%d %H:%M:%S"},
    "selected_mission": "mission",
    "missions_data": {
        "mission": {
            "file_extension": ".txt",
            "instruments": [
                {
                    "name": "instrument1",
                    "shortname": "ins1",
                    "fullname": "Instrument 1",
                    "targetname": "INS1",
                },
                {
                    "name": "instrument2",
                    "shortname": "ins2",
                    "fullname": "Instrument 2",
                    "targetname": "INS2",
                },
            ],
        }
    },
    "logger": {
        "log_level": "INFO",
        "use_color": True,
        "log_warnings": True,
        "log_exceptions": True,
        "log_to_file": True,
        "log_file_path": "swxsoc.log",
        "log_file_level": "INFO",
        "log_file_format": "%(asctime)s, %(origin)s, %(levelname)s, %(message)s",
    },
}

# Path to the temporary file
tmp_file_path = Path("swxsoc/tests/config.yml")


# fmt: off
@pytest.mark.parametrize("instrument,time,level,version,result", [
    ("eea", time, "l1", "1.2.3", f"swxsoc_eea_l1_{time_formatted}_v1.2.3.cdf"),
    ("merit", time, "l2", "2.4.5", f"swxsoc_mrt_l2_{time_formatted}_v2.4.5.cdf"),
    ("nemisis", time, "l2", "1.3.5", f"swxsoc_nem_l2_{time_formatted}_v1.3.5.cdf"),
    ("spani", time, "l3", "2.4.5", f"swxsoc_spn_l3_{time_formatted}_v2.4.5.cdf"),
]
)
def test_science_filename_output_a(instrument, time, level, version, result):
    """Test simple cases with expected output"""
    assert (
        util.create_science_filename(instrument, time, level=level, version=version)
        == result
    )
# fmt: on


def test_science_filename_output_b():
    """Test more complex cases of expected output"""
    # mode
    assert (
        util.create_science_filename(
            "spani", time, level="l3", mode="2s", version="2.4.5"
        )
        == f"swxsoc_spn_2s_l3_{time_formatted}_v2.4.5.cdf"
    )
    # test
    assert (
        util.create_science_filename(
            "spani", time, level="l1", version="2.4.5", test=True
        )
        == f"swxsoc_spn_l1test_{time_formatted}_v2.4.5.cdf"
    )
    # all options
    assert (
        util.create_science_filename(
            "spani",
            time,
            level="l3",
            mode="2s",
            descriptor="burst",
            version="2.4.5",
            test=True,
        )
        == f"swxsoc_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"
    )
    # Time object instead of str
    assert (
        util.create_science_filename(
            "spani",
            Time(time),
            level="l3",
            mode="2s",
            descriptor="burst",
            version="2.4.5",
            test=True,
        )
        == f"swxsoc_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"
    )
    # Time object but created differently
    assert (
        util.create_science_filename(
            "spani",
            Time(2460407.004409722, format="jd"),
            level="l3",
            mode="2s",
            descriptor="burst",
            version="2.4.5",
            test=True,
        )
        == f"swxsoc_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"
    )


def test_parse_science_filename_output():
    """Test for known outputs"""
    # all parameters
    input = {
        "instrument": "spani",
        "mode": "2s",
        "level": "l3",
        "test": False,
        "descriptor": "burst",
        "version": "2.4.5",
        "time": Time("2024-04-06T12:06:21"),
    }

    f = util.create_science_filename(
        input["instrument"],
        input["time"],
        input["level"],
        input["version"],
        test=input["test"],
        descriptor=input["descriptor"],
        mode=input["mode"],
    )
    assert util.parse_science_filename(f) == input

    # test only
    input = {
        "instrument": "nemisis",
        "level": "l3",
        "test": True,
        "version": "2.4.5",
        "time": Time("2024-04-06T12:06:21"),
        "mode": None,
        "descriptor": None,
    }

    f = util.create_science_filename(
        input["instrument"],
        input["time"],
        input["level"],
        input["version"],
        test=input["test"],
    )
    assert util.parse_science_filename(f) == input

    # descriptor only
    input = {
        "instrument": "spani",
        "mode": None,
        "level": "l3",
        "test": False,
        "descriptor": "burst",
        "version": "2.4.5",
        "time": Time("2024-04-06T12:06:21"),
    }

    f = util.create_science_filename(
        input["instrument"],
        input["time"],
        input["level"],
        input["version"],
        descriptor=input["descriptor"],
    )
    assert util.parse_science_filename(f) == input

    # mode only
    input = {
        "instrument": "nemisis",
        "mode": "2s",
        "level": "l2",
        "test": False,
        "descriptor": None,
        "version": "2.7.9",
        "time": Time("2024-04-06T12:06:21"),
    }

    f = util.create_science_filename(
        input["instrument"],
        input["time"],
        input["level"],
        input["version"],
        mode=input["mode"],
    )
    assert util.parse_science_filename(f) == input


@pytest.mark.parametrize(
    "filename",
    [
        ("swxsoc_SPANI_VA_l0_2026215ERROR124603_v21.bin"),  # Bad time Value
        ("swxsoc_FAKE_VA_l0_2026215-124603_v21.bin"),  # Bad Instrument Value
    ],
)
def test_parse_science_filename_errors_l0(filename):
    """Test for errors in l0 and above files"""
    with pytest.raises(ValueError):
        # wrong time name
        f = ""
        util.parse_science_filename(filename)


def test_parse_science_filename_errors_l1():
    """Test for errors in l1 and above files"""
    with pytest.raises(ValueError):
        # wrong mission name
        f = "veeger_spn_2s_l3test_burst_20240406_120621_v2.4.5"
        util.parse_science_filename(f)

        # wrong instrument name
        f = "swxsoc_www_2s_l3test_burst_20240406_120621_v2.4.5"
        util.parse_science_filename(f)


good_time = "2025-06-02T12:04:01"
good_instrument = "eea"
good_level = "l1"
good_version = "1.3.4"

# fmt: off


@pytest.mark.parametrize(
    "instrument,time,level,version",
    [
        (good_instrument, good_time, good_level, "1.3"),  # bad version specifications
        (good_instrument, good_time, good_level, "1"),
        (good_instrument, good_time, good_level, "1.5.6.7"),
        (good_instrument, good_time, good_level, "1.."),
        (good_instrument, good_time, good_level, "a.5.6"),
        (good_instrument, good_time, "la", good_version),  # wrong level specifications
        (good_instrument, good_time, "squirrel", good_version),
        (good_instrument, good_time, "0l", good_version),
        ("potato", good_time, good_level, good_version),  # wrong instrument names
        ("eeb", good_time, good_level, good_version),
        ("fpi", good_time, good_level, good_version),
        (good_instrument, "2023-13-04T12:06:21", good_level, good_version),  # non-existent time
        (good_instrument, "2023/13/04 12:06:21", good_level, good_version),  # not isot format
        (good_instrument, "2023/13/04 12:06:21", good_level, good_version),  # not isot format
        (good_instrument, "12345345", good_level, good_version),  # not valid input for time
    ]
)
def test_science_filename_errors_l1_a(instrument, time, level, version):
    """"""
    with pytest.raises(ValueError) as e:
        util.create_science_filename(
            instrument, time, level=level, version=version
        )
# fmt: on


def test_science_filename_errors_l1_b():
    with pytest.raises(ValueError):
        # _ character in mode
        util.create_science_filename(
            "eeb", time="12345345", level=good_level, version=good_version, mode="o_o"
        )
    with pytest.raises(ValueError):
        # _ character in descriptor
        util.create_science_filename(
            "eeb",
            time="12345345",
            level=good_level,
            version=good_version,
            descriptor="blue_green",
        )


# fmt: off
@pytest.mark.parametrize("filename,instrument,time", [
    ("swxsoc_NEM_l0_2024094-124603_v01.bin", "nemisis", "2024-04-03T12:46:03"),
    ("swxsoc_EEA_l0_2026337-124603_v11.bin", "eea", "2026-12-03T12:46:03"),
    ("swxsoc_MERIT_l0_2026215-124603_v21.bin", "merit", "2026-08-03T12:46:03"),
    ("swxsoc_SPANI_l0_2026337-065422_v11.bin", "spani", "2026-12-03T06:54:22"),
    ("swxsoc_MERIT_VC_l0_2026215-124603_v21.bin", "merit", "2026-08-03T12:46:03"),
    ("swxsoc_SPANI_VA_l0_2026215-124603_v21.bin", "spani", "2026-08-03T12:46:03"),
    ("SPANI_VA_l0_2026215-124603_v21.bin", "spani", "2026-08-03T12:46:03"),
    ("spani_VA_l0_2026215-124603_v21.bin", "spani", "2026-08-03T12:46:03")
])
def test_parse_l0_filenames(filename, instrument, time):
    """Testing parsing of MOC-generated level 0 files."""
    # Set SWXSOC_MISSION to 'swxsoc' mission
    mission_name = "swxsoc"
    os.environ["SWXSOC_MISSION"] = mission_name
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == "l0"
    assert result['version'] is None
    assert result['time'] == Time(time)
    assert result['mission'] == mission_name
# fmt: on


# fmt: off
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("hermes_NEM_l0_2024094-124603_v01.bin", "nemisis", "2024-04-03T12:46:03", "l0", None, None),
    ("hermes_EEA_l0_2026337-124603_v11.bin", "eea", "2026-12-03T12:46:03", "l0", None, None),
])
def test_parse_env_var_configured(filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"

    swxsoc._reconfigure()
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'] == Time(time)
    assert result['mode'] == mode
# fmt: on

# fmt: off


@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("padre_MEDDEA_l0_2025131-192102_v3.bin", "meddea", "2025-05-11 19:21:02", "raw", None, None),
    ("padre_MEDDEA_apid13_2025131-192102.bin", "meddea", "2025-05-11 19:21:02", "raw", None, None),
    ("padreSP11_250331134058.dat", "sharp", "2025-03-31 13:40:58", "raw", None, None),
    ("padreSP11_250331134058.idx", "sharp", "2025-03-31 13:40:58", "raw", None, None),
    ("padreMDA0_000107034739.dat", "meddea", "2000-01-07 03:47:39", "raw", None, None),
    ("padreMDA0_000107034739.idx", "meddea", "2000-01-07 03:47:39", "raw", None, None),
    ("padreMDU8_000107034739.dat", "meddea", "2000-01-07 03:47:39", "raw", None, None),
    ("padreMDU8_000107034739.idx", "meddea", "2000-01-07 03:47:39", "raw", None, None),
    ("padre_meddea_l0test_light_20250131T192102_v0.3.0.bin", "meddea", "2025-01-31T19:21:02.000", "raw", None, None),
    ("padre_sharp_ql_20230430T000000_v0.0.1.fits", "sharp", "2023-04-30T00:00:00.000", "ql", "0.0.1", None),


])
def test_parse_padre_science_files(filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "padre"

    swxsoc._reconfigure()
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'] == Time(time)
    assert result['mode'] == mode
# fmt: on


# fmt: off
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("hermes_NEM_l0_2024094-124603_v01.bin", "nemisis", "2024-04-03T12:46:03", "l0", None, None),
    ("hermes_EEA_l0_2026337-124603_v11.bin", "eea", "2026-12-03T12:46:03", "l0", None, None),
    ("hermes_MERIT_l0_2026215-124603_v21.bin", "merit", "2026-08-03T12:46:03", "l0", None, None),
    ("hermes_SPANI_l0_2026337-065422_v11.bin", "spani", "2026-12-03T06:54:22", "l0", None, None),
    (f"hermes_eea_l1_{time_formatted}_v1.2.3.cdf", "eea", "2024-04-06T12:06:21", "l1", "1.2.3", None),
    (f"hermes_mrt_l2_{time_formatted}_v1.2.5.cdf", "merit", "2024-04-06T12:06:21", "l2", "1.2.5", None),
])
def test_parse_env_var_configured(filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"

    swxsoc._reconfigure()
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'] == Time(time)
    assert result['mode'] == mode
# fmt: on


# fmt: off
@pytest.mark.parametrize("instrument,time,level,version,result", [
    ("eea", time, "l1", "1.2.3", f"hermes_eea_l1_{time_formatted}_v1.2.3.cdf"),
    ("merit", time, "l2", "2.4.5", f"hermes_mrt_l2_{time_formatted}_v2.4.5.cdf"),
    ("nemisis", time, "l2", "1.3.5", f"hermes_nem_l2_{time_formatted}_v1.3.5.cdf"),
    ("spani", time, "l3", "2.4.5", f"hermes_spn_l3_{time_formatted}_v2.4.5.cdf"),
]
)
def test_create_env_var_configured(instrument, time, level, version, result):
    """Test simple cases with expected output"""
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    # Import the 'util' submodule from 'swxsoc.util'
    swxsoc._reconfigure()
    assert (
        util.create_science_filename(instrument, time, level=level, version=version)
        == result
    )
# fmt: on


# fmt: off
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("mission_INS1_l0_2024094-124603_v01.bin", "instrument1", "2024-04-03T12:46:03", "raw", None, None),
    ("mission_INS1_l0_2026337-124603_v11.bin", "instrument1", "2026-12-03T12:46:03", "raw", None, None),
    ("mission_INS2_l0_2026215-124603_v21.bin", "instrument2", "2026-08-03T12:46:03", "raw", None, None),
    ("mission_INS2_l0_2026337-065422_v11.bin", "instrument2", "2026-12-03T06:54:22", "raw", None, None),
    (f"mission_ins1_l1_{time_formatted}_v1.2.3.txt", "instrument1", "2024-04-06T12:06:21", "l1", "1.2.3", None),
    (f"mission_ins2_l2_{time_formatted}_v1.2.5.txt", "instrument2", "2024-04-06T12:06:21", "l2", "1.2.5", None),
])
def test_parse_configdir_configured(filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    # If the file exists, delete it
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)

    # Write the dictionary to a YAML file
    with open(tmp_file_path, 'w') as file:
        yaml.dump(config_content, file, default_flow_style=False)

    # Set SWXSOC_CONFIGDIR
    os.environ["SWXSOC_CONFIGDIR"] = str(tmp_file_path.parent)

    # Remove SWXSOC_MISSION environment variable if it exists
    if "SWXSOC_MISSION" in os.environ:
        del os.environ["SWXSOC_MISSION"]

    # Import the 'util' submodule from 'swxsoc.util'
    swxsoc._reconfigure()

    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'] == Time(time)
    assert result['mode'] == mode

    del os.environ["SWXSOC_CONFIGDIR"]
    swxsoc._reconfigure()
# fmt: on


# fmt: off
@pytest.mark.parametrize("instrument,time,level,version,result", [
    ("instrument1", time, "l1", "1.2.3", f"mission_ins1_l1_{time_formatted}_v1.2.3.txt"),
    ("instrument1", time, "l2", "2.4.5", f"mission_ins1_l2_{time_formatted}_v2.4.5.txt"),
    ("instrument2", time, "l2", "1.3.5", f"mission_ins2_l2_{time_formatted}_v1.3.5.txt"),
    ("instrument2", time, "l3", "2.4.5", f"mission_ins2_l3_{time_formatted}_v2.4.5.txt"),
]
)
def test_create_configdir_configured(instrument, time, level, version, result):
    """Test simple cases with expected output"""
    # If the file exists, delete it
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)

    # Write the dictionary to a YAML file
    with open(tmp_file_path, 'w') as file:
        yaml.dump(config_content, file, default_flow_style=False)

    # Set SWXSOC_CONFIGDIR
    os.environ["SWXSOC_CONFIGDIR"] = str(tmp_file_path.parent)

    # Remove SWXSOC_MISSION environment variable if it exists
    if "SWXSOC_MISSION" in os.environ:
        del os.environ["SWXSOC_MISSION"]

    # Import the 'util' submodule from 'swxsoc.util'
    swxsoc._reconfigure()

    assert (
        util.create_science_filename(instrument, time, level=level, version=version)
        == result
    )

    del os.environ["SWXSOC_CONFIGDIR"]
    swxsoc._reconfigure()
# fmt: on


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

    fido_client = util.SWXSOCClient()

    # Test search with a query for specific instrument, level, and time
    query = util.AttrAnd(
        [
            util.SearchTime("2024-01-01", "2025-01-01"),
            util.DevelopmentBucket(False),
            util.Level("l0"),
            util.Instrument("eea"),
        ]
    )
    results = fido_client.search(query)

    for result in results:
        assert result["instrument"] == "eea"
        assert result["level"] == "l0"
        assert result["version"] is None
        assert result["time"] == Time("2024-04-03T12:46:03")

    # Test search with a query for specific instrument, level, and time
    query = util.AttrAnd(
        [
            util.SearchTime("2024-01-01", "2025-01-01"),
            util.DevelopmentBucket(False),
            util.Level("l1"),
            util.Instrument("eea"),
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

    fido_client = util.SWXSOCClient()

    # Test search with a query for specific time
    query = util.AttrAnd([util.SearchTime("2024-01-01", "2025-01-01")])
    results = fido_client.search(query)

    for result in results:
        assert result["time"] >= Time("2024-01-01")
        assert result["time"] <= Time("2025-01-01")

    # Test search with a query for out of range time
    query = util.AttrAnd([util.SearchTime("2025-01-01", "2026-01-01")])
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

    fido_client = util.SWXSOCClient()

    # Test search with a query for specific instrument
    query = util.AttrAnd([util.Instrument("eea")])
    results = fido_client.search(query)

    for result in results:
        assert result["instrument"] == "eea"

    # Test search with a query for out of range instrument
    query = util.AttrAnd([util.Instrument("not_instrument")])
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

    fido_client = util.SWXSOCClient()

    # Test search with a query for specific level
    query = util.AttrAnd([util.Level("l0")])
    results = fido_client.search(query)

    for result in results:
        assert result["level"] == "l0"

    assert len(results) == 2

    # Test search with a query for existing level but not in the bucket
    query = util.AttrAnd([util.Level("l2")])
    results = fido_client.search(query)

    assert len(results) == 0

    # Test search with a query for out of range should raise an error
    query = util.AttrAnd([util.Level("l5")])
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

    fido_client = util.SWXSOCClient()

    # Test search with a query for in development bucket
    query = util.AttrAnd([util.DevelopmentBucket(True)])
    results = fido_client.search(query)

    assert len(results) == 4

    # Test search with a query for not in development bucket
    query = util.AttrAnd([util.DevelopmentBucket(False)])
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

    fido_client = util.SWXSOCClient()

    # Test search with a query for specific instrument, level, and time
    query = util.AttrAnd(
        [
            util.SearchTime("2024-01-01", "2025-01-01"),
            util.DevelopmentBucket(False),
            util.Level("l0"),
            util.Instrument("eea"),
        ]
    )
    results = fido_client.search(query)

    # Initalizing parfive downloader
    downloader = parfive.Downloader(progress=False, overwrite=True)

    fido_client.fetch(results, path=".", downloader=downloader)

    assert downloader.queued_downloads == 1

    # Test search with a query for specific instrument, and time
    query = util.AttrAnd(
        [
            util.SearchTime("2024-01-01", "2025-01-01"),
            util.DevelopmentBucket(False),
            util.Instrument("eea"),
        ]
    )
    results = fido_client.search(query)

    # Initalizing parfive downloader
    downloader = parfive.Downloader(progress=False, overwrite=True)

    fido_client.fetch(results, path=".", downloader=downloader)

    assert downloader.queued_downloads == 2
