"""Tests for util.py"""

import pytest
from astropy.time import Time

from swxsoc.util import util

time = "2024-04-06T12:06:21"
time_formatted = "20240406T120621"
time_unix_ms = "1712405181000"


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
    # HERMES mission is set by default via autouse fixture in conftest.py
    
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
    # HERMES mission is set by default via autouse fixture in conftest.py
    
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
    # HERMES mission is set by default via autouse fixture in conftest.py
    
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
            "Invalid instrument shortname",
        ),  # Wrong mission name
        (
            "hermes_www_2s_l3test_burst_20240406_120621_v2.4.5.cdf",
            "Invalid instrument shortname",
        ),  # Wrong instrument name
        (
          "hermes_eea_l0_nemisis_20240406_120621.bin",
          "Multiple instrument names found",
          # Multiple Instruments listed in Raw filename  
        ),
    ],
)
def test_parse_science_filename_errors(filename, expected_error):
    """Test for errors in filename parsing"""
    # HERMES mission is set by default via autouse fixture in conftest.py

    with pytest.raises(ValueError, match=expected_error):
        util.parse_science_filename(filename)
# fmt: on


# fmt: off
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("hermes_NEM_l0_2024094-124603_v01.bin", "nemisis", "2024-04-03T12:46:03", "l0", None, None),
    ("hermes_EEA_l0_2025337-124603_v11.bin", "eea", "2025-12-03T12:46:03", "l0", None, None),
    ("hermes_MERIT_l0_2025215-124603_v21.bin", "merit", "2025-08-03T12:46:03", "l0", None, None),
    ("hermes_SPANI_l0_2025337-065422_v11.bin", "spani", "2025-12-03T06:54:22", "l0", None, None),
    ("hermes_MERIT_VC_l0_2025215-124603_v21.bin", "merit", "2025-08-03T12:46:03", "l0", None, None),
    ("hermes_SPANI_VA_l0_2025215-124603_v21.bin", "spani", "2025-08-03T12:46:03", "l0", None, None),
    ("SPANI_VA_l0_2025215-124603_v21.bin", "spani", "2025-08-03T12:46:03", "l0", None, None),
    ("spani_VA_l0_2025215-124603_v21.bin", "spani", "2025-08-03T12:46:03", "l0", None, None),
])
def test_parse_l0_filenames_hermes(filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    # HERMES mission is set by default via autouse fixture in conftest.py
    mission_name = "hermes"
    
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'] == Time(time)
    assert result['mode'] == mode
    assert result['mission'] == mission_name
# fmt: on


# fmt: off
@pytest.mark.parametrize("use_mission", ["padre"], indirect=True)
@pytest.mark.parametrize("filename,instrument,time,level,version,mode", [
    ("padre_MEDDEA_l0_2025131-192102_v3.bin", "meddea", "2025-05-11 19:21:02", "l0", None, None),
    ("padre_MEDDEA_apid13_2025131-192102.bin", "meddea", "2025-05-11 19:21:02", "raw", None, None),
    ("padreSP11_250331134058.dat", "sharp", "2025-03-31 13:40:58", "raw", None, None),
    ("padreSP11_250331134058.idx", "sharp", "2025-03-31 13:40:58", "raw", None, None),
    ("padreMDA0_240107034739.dat", "meddea", "2024-01-07 03:47:39", "raw", None, None),
    ("padreMDA0_240107034739.idx", "meddea", "2024-01-07 03:47:39", "raw", None, None),
    ("padreMDU8_240107034739.dat", "meddea", "2024-01-07 03:47:39", "raw", None, None),
    ("padreMDU8_240107034739.idx", "meddea", "2024-01-07 03:47:39", "raw", None, None),
    ("padre_meddea_l0test_light_20250131T192102_v0.3.0.bin", "meddea", "2025-01-31 19:21:02", "raw", None, None),
    ("padre_sharp_ql_20230430T000000_v0.0.1.fits", "sharp", "2023-04-30 00:00:00", "ql", "0.0.1", None),
    ("padre_get_EPS2_BP_INST0_CHARGER_XP_Data_1762019652327_1762198944391.csv", "craft", "2025-11-01T17:54:12.327", "raw", None, None),
    ("padre_get_EPS2_BP_INST0_CHARGER_YP_Data_1762019652327_1762198944391.csv", "craft", "2025-11-01T17:54:12.327", "raw", None, None),
    ("padre_get_EPS_9_Data_1762008094193_1762187403300.csv", "craft", "2025-11-01T14:41:34.193", "raw", None, None),
    ("padre_get_EPS_9_Data_1763282491281_1836308076540.csv", "craft", "2025-11-16T08:41:31.281", "raw", None, None),
    ("padre_craft_dirlist_1772908542.txt", "craft", "2026-03-07T18:35:42.000", "raw", None, None),
    ("padre_craft_dirlist_1772908542.csv", "craft", "2026-03-07T18:35:42.000", "raw", None, None),
])
def test_parse_padre_science_files(use_mission, filename, instrument, time, level, version, mode):
    """Testing parsing of MOC-generated level 0 files."""
    # PADRE mission is set via use_mission fixture
    
    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'].isot == Time(time).isot  # compare str otherwise breaks for unix time
    assert str(result['time']) == str(time)
    assert result['mode'] == mode
# fmt: on


# fmt: off
@pytest.mark.parametrize("use_mission", ["swxsoc_pipeline"], indirect=True)
@pytest.mark.parametrize("filename,instrument,time,level,version,mode,descriptor", [
    # Standard format CDF file with underscore in mission name
    ("swxsoc_pipeline_reach_all_l1c_20251201T000000_v2.0.0.cdf", "reach", "2025-12-01T00:00:00.000", "l1c", "2.0.0", "all", None),
    # CSV file matching file_rules for REACH (l1c, "%Y%m%dT%H%M%S" format)
    ("REACH-ALL_20251205T060517_20251205T060517.csv", "reach", "2025-12-05T06:05:17.000", "l1c", None, None, None),
    # JSON file matching file_rules for REACH (l1c, "%Y%m%dT%H%M%S" format)
    ("REACH-ALL_20251201T013010_20251205T060517.json", "reach", "2025-12-01T01:30:10.000", "l1c", None, None, None),
])
def test_parse_swxsoc_pipeline_science_files(use_mission, filename, instrument, time, level, version, mode, descriptor):
    """Testing parsing of SWxSOC Pipeline (REACH) filenames."""
    # swxsoc_pipeline mission is set via use_mission fixture

    result = util.parse_science_filename(filename)
    assert result['instrument'] == instrument
    assert result['level'] == level
    assert result['version'] == version
    assert result['time'].isot == Time(time).isot
    assert result['mode'] == mode
    assert result['descriptor'] == descriptor
# fmt: on


@pytest.mark.parametrize("use_mission", ["padre"], indirect=True)
@pytest.mark.parametrize(
    "filename,expected_error",
    [
        (
            "padre_get_EPS_9_Data_1546344000000_1546344000000.csv",
            "before mission minimum valid time",
        ),  # Too old: 2019-01-01
        (
            "padre_get_EPS_9_Data_1836302400000_1836302400000.csv",
            "after mission maximum valid time",
        ),  # Too new: 2028-03-10 Note: This test will fail after 2028-03-10
    ],
)
def test_extract_time_errors(use_mission, filename, expected_error):
    """Test that Time Parsing raises appropriate errors for out-of-range times"""
    import swxsoc

    mission_config = swxsoc.config["mission"]
    with pytest.raises(ValueError, match=expected_error):
        util._extract_time(filename, mission_config=mission_config)


@pytest.mark.parametrize("use_mission", ["padre"], indirect=True)
@pytest.mark.parametrize(
    "filename,expected_warning",
    [
        (
            "padre_get_EPS_9_Data_1836302400000_1836302400000.csv",
            "Found future time",
        ),  # Too new: 2028-03-10 Note: This test will fail after 2028-03-10
        (
            "padre_meddea_l0_1969-06-01T12:00:00_v0.0.1.bin",
            "Found suspiciously old time",
        ),  # Old: 1969-06-01 (before 1970)
    ],
)
def test_extract_time_warnings(use_mission, filename, expected_warning, caplog):
    """Test that Time Parsing raises appropriate warnings when mission_config is None"""
    # Pass None for mission_config to test warning behavior instead of error raising
    util._extract_time(filename, mission_config=None)
    assert expected_warning in caplog.text


def test_get_instrument_package_hermes(use_mission):
    # Mission: hermes
    assert util.get_instrument_package("eea") == "hermes_eea"
    assert util.get_instrument_package("EEA") == "hermes_eea"
    assert util.get_instrument_package("nemisis") == "hermes_nemisis"
    assert util.get_instrument_package("Nemisis") == "hermes_nemisis"
    with pytest.raises(ValueError):
        util.get_instrument_package("not_an_inst")


@pytest.mark.parametrize("use_mission", ["padre"], indirect=True)
def test_get_instrument_package_padre(use_mission):
    # Mission: padre
    assert util.get_instrument_package("meddea") == "padre_meddea"
    assert util.get_instrument_package("MEDDEA") == "padre_meddea"
    assert util.get_instrument_package("sharp") == "padre_sharp"
    assert util.get_instrument_package("SHARP") == "padre_sharp"
    with pytest.raises(ValueError):
        util.get_instrument_package("fake")


@pytest.mark.parametrize("use_mission", ["swxsoc_pipeline"], indirect=True)
def test_get_instrument_package_swxsoc_pipeline(use_mission):
    # Mission: swxsoc_pipeline
    assert util.get_instrument_package("reach") == "swxsoc_reach"
    assert util.get_instrument_package("REACH") == "swxsoc_reach"
    with pytest.raises(ValueError):
        util.get_instrument_package("unknown")
