"""Tests for util.py"""

import pytest

from astropy.time import Time
from swxsoc.util import util

time = "2024-04-06T12:06:21"
time_formatted = "20240406T120621"


# fmt: off
@pytest.mark.parametrize("mission,instrument,time,level,version,extension,result", [
    ("swxsoc", "eea", time, "l1", "1.2.3", ".cdf", f"swxsoc_eea_l1_{time_formatted}_v1.2.3.cdf"),
    ("swxsoc", "merit", time, "l2", "2.4.5", ".cdf", f"swxsoc_mrt_l2_{time_formatted}_v2.4.5.cdf"),
    ("swxsoc", "nemisis", time, "l2", "1.3.5", ".cdf", f"swxsoc_nms_l2_{time_formatted}_v1.3.5.cdf"),
    ("swxsoc", "spani", time, "l3", "2.4.5", ".cdf", f"swxsoc_spn_l3_{time_formatted}_v2.4.5.cdf"),
]
)
def test_science_filename_output_a(mission, instrument, time, level, version, extension, result):
    """Test simple cases with expected output"""
    assert (
        util.create_science_filename(mission, instrument, time, level, version, extension)
        == result
    )
# fmt: on


def test_science_filename_output_b():
    """Test more complex cases of expected output"""

    # mode
    assert (
        util.create_science_filename(
            "swxsoc",
            "spani",
            time,
            level="l3",
            mode="2s",
            version="2.4.5",
            extension=".cdf",
        )
        == f"swxsoc_spn_2s_l3_{time_formatted}_v2.4.5.cdf"
    )
    # test
    assert (
        util.create_science_filename(
            "swxsoc",
            "spani",
            time,
            level="l1",
            version="2.4.5",
            test=True,
            extension=".cdf",
        )
        == f"swxsoc_spn_l1test_{time_formatted}_v2.4.5.cdf"
    )
    # all options
    assert (
        util.create_science_filename(
            "swxsoc",
            "spani",
            time,
            level="l3",
            mode="2s",
            descriptor="burst",
            version="2.4.5",
            test=True,
            extension=".cdf",
        )
        == f"swxsoc_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"
    )
    # Time object instead of str
    assert (
        util.create_science_filename(
            "swxsoc",
            "spani",
            Time(time),
            level="l3",
            mode="2s",
            descriptor="burst",
            version="2.4.5",
            test=True,
            extension=".cdf",
        )
        == f"swxsoc_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"
    )
    # Time object but created differently
    assert (
        util.create_science_filename(
            "swxsoc",
            "spani",
            Time(2460407.004409722, format="jd"),
            level="l3",
            mode="2s",
            descriptor="burst",
            version="2.4.5",
            test=True,
            extension=".cdf",
        )
        == f"swxsoc_spn_2s_l3test_burst_{time_formatted}_v2.4.5.cdf"
    )


def test_parse_science_filename_output():
    """Test for known outputs"""
    # all parameters
    input = {
        "mission": "swxsoc",
        "instrument": "spani",
        "mode": "2s",
        "level": "l3",
        "test": False,
        "descriptor": "burst",
        "version": "2.4.5",
        "time": Time("2024-04-06T12:06:21"),
        "extension": ".cdf",
    }

    f = util.create_science_filename(**input)
    assert util.parse_science_filename(f) == input

    # test only
    input = {
        "mission": "swxsoc",
        "instrument": "nemisis",
        "level": "l3",
        "test": True,
        "version": "2.4.5",
        "time": Time("2024-04-06T12:06:21"),
        "mode": "",
        "descriptor": "",
        "extension": ".cdf",
    }

    f = util.create_science_filename(**input)
    assert util.parse_science_filename(f) == input

    # descriptor only
    input = {
        "mission": "swxsoc",
        "instrument": "spani",
        "mode": "",
        "level": "l3",
        "test": False,
        "descriptor": "burst",
        "version": "2.4.5",
        "time": Time("2024-04-06T12:06:21"),
        "extension": ".cdf",
    }

    f = util.create_science_filename(**input)
    assert util.parse_science_filename(f) == input

    # mode only
    input = {
        "mission": "swxsoc",
        "instrument": "nemisis",
        "mode": "2s",
        "level": "l2",
        "test": False,
        "descriptor": "",
        "version": "2.7.9",
        "time": Time("2024-04-06T12:06:21"),
        "extension": ".cdf",
    }

    f = util.create_science_filename(**input)
    assert util.parse_science_filename(f) == input


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
        (good_instrument, good_time, "l0", good_version),
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
            "swxsoc", instrument, time, level=level, version=version, extension=".cdf",
        )
# fmt: on


def test_science_filename_errors_l1_b():
    with pytest.raises(ValueError):
        # _ character in mode
        util.create_science_filename(
            "swxsoc",
            "eeb",
            time="12345345",
            level=good_level,
            version=good_version,
            extension=".cdf",
            mode="o_o",
        )
    with pytest.raises(ValueError):
        # _ character in descriptor
        util.create_science_filename(
            "swxsoc",
            "eeb",
            time="12345345",
            level=good_level,
            version=good_version,
            extension=".cdf",
            descriptor="blue_green",
        )


# fmt: off
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("swxsoc_MAG_l0_2024094-124603_v01.bin", "nemisis", "2024-04-03T12:46:03", "l0", "01", ""),
    ("swxsoc_EEA_l0_2026337-124603_v11.bin", "eea", "2026-12-03T12:46:03", "l0", "11", ""),
    ("swxsoc_MERIT_l0_2026215-124603_v21.bin", "merit", "2026-08-03T12:46:03", "l0", "21", ""),
    ("swxsoc_SPANI_l0_2026337-065422_v11.bin", "spani", "2026-12-03T06:54:22", "l0", "11", ""),
    ("swxsoc_MERIT_VC_l0_2026215-124603_v21.bin", "merit", "2026-08-03T12:46:03", "l0", "21", "VC"),
    ("swxsoc_SPANI_VA_l0_2026215-124603_v21.bin", "spani", "2026-08-03T12:46:03", "l0", "21", "VA")
])
def test_parse_l0_filenames(filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'] == Time(time)
    assert result['mode'] == mode
# fmt: on
