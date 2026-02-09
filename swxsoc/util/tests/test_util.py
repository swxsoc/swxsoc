"""Tests for util.py"""

import os
import pytest
import yaml
from pathlib import Path
import parfive

from astropy.time import Time
import boto3
from moto import mock_aws

import swxsoc
from swxsoc.util import util

time = "2024-04-06T12:06:21"
time_formatted = "20240406T120621"
time_unix_ms = "1712405181000"

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
@pytest.mark.parametrize("instrument,mode,level,test,descriptor,time_input,version,result", [
    # Simple cases with string time
    ("eea", None, "l1", False, None, time, "1.2.3", f"hermes_eea_l1_{time_formatted}_v1.2.3.cdf"),
    ("merit", None, "l2", False, None, time, "2.4.5", f"hermes_mrt_l2_{time_formatted}_v2.4.5.cdf"),
    ("nemisis", None, "l2", False, None, time, "1.3.5", f"hermes_nem_l2_{time_formatted}_v1.3.5.cdf"),
    ("spani", None, "l3", False, None, time, "2.4.5", f"hermes_spn_l3_{time_formatted}_v2.4.5.cdf"),
    # Complex cases with optional parameters
    ("spani", "2s", "l3", False, None, time, "2.4.5", f"hermes_spn_2s_l3_{time_formatted}_v2.4.5.cdf"),  # mode
    ("spani", None, "l1", True, None, time, "2.4.5", f"hermes_spn_l1test_{time_formatted}_v2.4.5.cdf"),  # test
    ("spani", "2s", "l3", True, "burst", time, "2.4.5", f"hermes_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"),  # all options
    # Time object instead of str
    ("spani", "2s", "l3", True, "burst", Time(time), "2.4.5", f"hermes_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"),
    # Time object created with julian date
    ("spani", "2s", "l3", True, "burst", Time(2460407.004409722, format="jd"), "2.4.5", f"hermes_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"),
])
def test_create_science_filename_hermes(instrument, mode, level, test, descriptor, time_input, version, result):
    """Test create_science_filename with various parameter combinations"""
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
    # Build kwargs for optional parameters
    kwargs = {"level": level, "version": version}
    if mode is not None:
        kwargs["mode"] = mode
    if descriptor is not None:
        kwargs["descriptor"] = descriptor
    if test:
        kwargs["test"] = test
    
    # Test the result
    assert util.create_science_filename(instrument, time_input, **kwargs) == result
# fmt: on


# fmt: off
@pytest.mark.parametrize("instrument,mode,level,test,descriptor,time_input,version", [
    ("spani", "2s", "l3", False, "burst", Time(time), "2.4.5"),  # all parameters
    ("nemisis", None, "l3", True, None, Time(time), "2.4.5"),  # test only
    ("spani", None, "l3", False, "burst", Time(time), "2.4.5"),  # descriptor only
    ("nemisis", "2s", "l2", False, None, Time(time), "2.7.9"),  # mode only
])
def test_parse_science_filename_hermes(instrument, mode, level, test, descriptor, time_input, version):
    """Test parse_science_filename with various parameter combinations"""
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
    # Build expected dictionary
    expected = {
        "instrument": instrument,
        "mode": mode,
        "level": level,
        "test": test,
        "descriptor": descriptor,
        "version": version,
        "time": time_input,
    }

    # Build kwargs for create_science_filename
    kwargs = {"level": level, "version": version}
    if mode is not None:
        kwargs["mode"] = mode
    if descriptor is not None:
        kwargs["descriptor"] = descriptor
    if test:
        kwargs["test"] = test
    
    f = util.create_science_filename(instrument, time_input, **kwargs)
    assert util.parse_science_filename(f) == expected
# fmt: on


# fmt: off
good_time = "2025-06-02T12:04:01"
good_instrument = "eea"
good_level = "l1"
good_version = "1.3.4"
@pytest.mark.parametrize("instrument,time,level,version,mode,descriptor,expected_error", [
    # Version validation errors
    (good_instrument, good_time, good_level, "1.3", None, None, "Version.*is not formatted correctly"),
    (good_instrument, good_time, good_level, "1", None, None, "Version.*is not formatted correctly"),
    (good_instrument, good_time, good_level, "1.5.6.7", None, None, "Version.*is not formatted correctly"),
    (good_instrument, good_time, good_level, "1..", None, None, "Version.*is not all integers"),
    (good_instrument, good_time, good_level, "a.5.6", None, None, "Version.*is not all integers"),
    # Level validation errors
    (good_instrument, good_time, "la", good_version, None, None, "Level.*is not recognized"),
    (good_instrument, good_time, "squirrel", good_version, None, None, "Level.*is not recognized"),
    (good_instrument, good_time, "0l", good_version, None, None, "Level.*is not recognized"),
    # Instrument validation errors
    ("potato", good_time, good_level, good_version, None, None, "Instrument.*is not recognized"),
    ("eeb", good_time, good_level, good_version, None, None, "Instrument.*is not recognized"),
    ("fpi", good_time, good_level, good_version, None, None, "Instrument.*is not recognized"),
    # Time validation errors
    (good_instrument, "2023-13-04T12:06:21", good_level, good_version, None, None, "Input values did not match"),
    (good_instrument, "2023/13/04 12:06:21", good_level, good_version, None, None, "Input values did not match"),
    (good_instrument, "12345345", good_level, good_version, None, None, "Input values did not match"),
    # Underscore character validation
    (good_instrument, good_time, good_level, good_version, "o_o", None, "underscore symbol _ is not allowed"),
    (good_instrument, good_time, good_level, good_version, None, "blue_green", "underscore symbol _ is not allowed"),
])
def test_create_science_filename_errors(instrument, time, level, version, mode, descriptor, expected_error):
    """Test create_science_filename raises appropriate errors for invalid inputs"""
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
    kwargs = {"level": level, "version": version}
    if mode is not None:
        kwargs["mode"] = mode
    if descriptor is not None:
        kwargs["descriptor"] = descriptor
    
    with pytest.raises(ValueError, match=expected_error):
        util.create_science_filename(instrument, time, **kwargs)
# fmt: on


# fmt: off
@pytest.mark.parametrize(
    "filename,expected_error",
    [
        (
            "hermes_SPANI_VA_l0_2026215ERROR124603_v21.bin",
            "No recognizable time format",
        ),  # Bad time value
        (
            "hermes_FAKE_VA_l0_2026215-124603_v21.bin",
            "No valid instrument name found",
        ),  # Bad instrument value
        (
            "veeger_spn_2s_l3test_burst_20240406_120621_v2.4.5.cdf",
            "Not a valid mission name",
        ),  # Wrong mission name
        (
            "hermes_www_2s_l3test_burst_20240406_120621_v2.4.5.cdf",
            "Invalid instrument shortname",
        ),  # Wrong instrument name
    ],
)
def test_parse_science_filename_errors(filename, expected_error):
    """Test for errors in filename parsing"""
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()

    with pytest.raises(ValueError, match=expected_error):
        util.parse_science_filename(filename)
# fmt: on


# fmt: off
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("hermes_NEM_l0_2024094-124603_v01.bin", "nemisis", "2024-04-03T12:46:03", "l0", None, None),
    ("hermes_EEA_l0_2026337-124603_v11.bin", "eea", "2026-12-03T12:46:03", "l0", None, None),
    ("hermes_MERIT_l0_2026215-124603_v21.bin", "merit", "2026-08-03T12:46:03", "l0", None, None),
    ("hermes_SPANI_l0_2026337-065422_v11.bin", "spani", "2026-12-03T06:54:22", "l0", None, None),
    ("hermes_MERIT_VC_l0_2026215-124603_v21.bin", "merit", "2026-08-03T12:46:03", "l0", None, None),
    ("hermes_SPANI_VA_l0_2026215-124603_v21.bin", "spani", "2026-08-03T12:46:03", "l0", None, None),
    ("SPANI_VA_l0_2026215-124603_v21.bin", "spani", "2026-08-03T12:46:03", "l0", None, None),
    ("spani_VA_l0_2026215-124603_v21.bin", "spani", "2026-08-03T12:46:03", "l0", None, None),
])
def test_parse_l0_filenames_hermes(filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    # Set SWXSOC_MISSION to 'hermes' mission
    mission_name = "hermes"
    os.environ["SWXSOC_MISSION"] = mission_name
    swxsoc._reconfigure()
    
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'] == Time(time)
    assert result['mode'] == mode
    assert result['mission'] == mission_name
# fmt: on


# fmt: off
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("padre_MEDDEA_l0_2025131-192102_v3.bin", "meddea", "2025-05-11 19:21:02", "l0", None, None),
    ("padre_MEDDEA_apid13_2025131-192102.bin", "meddea", "2025-05-11 19:21:02", "raw", None, None),
    ("padreSP11_250331134058.dat", "sharp", "2025-03-31 13:40:58", "raw", None, None),
    ("padreSP11_250331134058.idx", "sharp", "2025-03-31 13:40:58", "raw", None, None),
    ("padreMDA0_000107034739.dat", "meddea", "2000-01-07 03:47:39", "raw", None, None),
    ("padreMDA0_000107034739.idx", "meddea", "2000-01-07 03:47:39", "raw", None, None),
    ("padreMDU8_000107034739.dat", "meddea", "2000-01-07 03:47:39", "raw", None, None),
    ("padreMDU8_000107034739.idx", "meddea", "2000-01-07 03:47:39", "raw", None, None),
    ("padre_meddea_l0test_light_20250131T192102_v0.3.0.bin", "meddea", "2025-01-31 19:21:02", "raw", None, None),
    ("padre_sharp_ql_20230430T000000_v0.0.1.fits", "sharp", "2023-04-30T00:00:00.000", "ql", "0.0.1", None),
    ("padre_get_EPS2_BP_INST0_CHARGER_XP_Data_1762019652327_1762198944391.csv", "craft", "2025-11-01T17:54:12.327", "raw", None, None),
    ("padre_get_EPS2_BP_INST0_CHARGER_YP_Data_1762019652327_1762198944391.csv", "craft", "2025-11-01T17:54:12.327", "raw", None, None),
    ("padre_get_EPS_9_Data_1762008094193_1762187403300.csv", "craft", "2025-11-01T14:41:34.193", "raw", None, None),
    ("padre_get_EPS_9_Data_1763282491281_1836308076540.csv", "craft", "2025-11-16T08:41:31.281", "raw", None, None),
])
def test_parse_padre_science_files(filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    # Set SWXSOC_MISSION to 'padre' mission
    os.environ["SWXSOC_MISSION"] = "padre"
    swxsoc._reconfigure()
    
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'].isot == Time(time).isot  # compare str otherwise breaks for unix time
    assert str(result['time']) == str(time)
    assert result['mode'] == mode
# fmt: on


# fmt: off
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("mission_INS1_l0_2024094-124603_v01.bin", "instrument1", "2024-04-03T12:46:03", "l0", None, None),
    ("mission_INS1_l0_2026337-124603_v11.bin", "instrument1", "2026-12-03T12:46:03", "l0", None, None),
    ("mission_INS2_l0_2026215-124603_v21.bin", "instrument2", "2026-08-03T12:46:03", "l0", None, None),
    ("mission_INS2_l0_2026337-065422_v11.bin", "instrument2", "2026-12-03T06:54:22", "l0", None, None),
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


def test_extract_time_warning(caplog):
    util._extract_time("padre_get_EPS_9_Data_1836308076540_1836308076540.csv")
    assert "Found future time" in caplog.text



@mock_aws
def test_search_all_attr():
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
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
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
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
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
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
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
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
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
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

    fido_client = util.SWXSOCClient()

    # Test search with a query for in development bucket
    query = util.AttrAnd([util.DevelopmentBucket(True)])
    results = fido_client.search(query)

    assert len(results) == 1

    # Test search with a query for not in development bucket
    query = util.AttrAnd([util.DevelopmentBucket(False)])
    results = fido_client.search(query)

    assert len(results) == 0


@mock_aws
def test_fetch():
    # Set SWXSOC_MISSION to 'hermes' mission
    os.environ["SWXSOC_MISSION"] = "hermes"
    swxsoc._reconfigure()
    
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
    query = util.AttrAnd(
        [
            util.SearchTime("2024-01-01", "2025-01-01"),
            util.DevelopmentBucket(False),
            util.Instrument("eea"),
            util.Descriptor("housekeeping"),
        ]
    )
    results = fido_client.search(query)
    assert len(results) == 1
    query = util.AttrAnd(
        [
            util.SearchTime("2024-01-01", "2025-01-01"),
            util.DevelopmentBucket(False),
            util.Instrument("eea"),
            util.Descriptor("spectrum"),
        ]
    )
    results = fido_client.search(query)
    assert len(results) == 1
    query = util.AttrAnd(
        [
            util.DevelopmentBucket(False),
            util.Instrument("eea"),
            util.Level("l1"),
        ]
    )
    results = fido_client.search(query)
    assert len(results) == 3
