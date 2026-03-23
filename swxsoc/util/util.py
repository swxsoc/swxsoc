"""
This module provides general utility functions.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import sunpy.time
from astropy.time import Time

import swxsoc
from swxsoc.util.exceptions import warn_user

# --- Backward compatibility: moved symbols re-exported from new locations ---
from swxsoc.db.timeseries import (
    _record_dimension_timestream as _record_dimension_timestream,
    record_timeseries as record_timeseries,
)
from swxsoc.net.attr import (
    Descriptor as Descriptor,
    DevelopmentBucket as DevelopmentBucket,
    Instrument as Instrument,
    Level as Level,
    SearchTime as SearchTime,
    walker as walker,
)
from swxsoc.net.client import SWXSOCClient as SWXSOCClient
from swxsoc.util.grafana import (
    create_annotation as create_annotation,
    get_dashboard_id as get_dashboard_id,
    get_panel_id as get_panel_id,
    query_annotations as query_annotations,
    remove_annotation_by_id as remove_annotation_by_id,
)

__all__ = [
    "create_science_filename",
    "parse_science_filename",
]

TIME_FORMAT = "%Y%m%dT%H%M%S"  # YYYYMMDDTHHMMSS

TIME_PATTERNS = {
    "unix_ms": re.compile(r"(?<!\d)\d{13}(?!\d)"),  # unix time stamps in milliseconds
    "unix_s": re.compile(r"(?<!\d)\d{10}(?!\d)"),  # unix time stamps in seconds
    "%Y-%m-%dT%H:%M:%S": re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),  # ISO 8601
    "%Y%m%d-%H%M%S": re.compile(r"\d{8}-\d{6}"),  # YYYYMMDD-HHMMSS
    "%Y%m%dT%H%M%S": re.compile(r"\d{8}T\d{6}"),  # YYYYMMDDTHHMMSS
    "%Y%m%d%H%M%S": re.compile(r"(?<!\d)\d{14}(?!\d)"),  # YYYYMMDDHHMMSS
    "%y%m%d%H%M%S": re.compile(r"(?<!\d)\d{12}(?!\d)"),  # YYMMDDHHMMSS
    "%Y%j-%H%M%S": re.compile(r"\d{7}-\d{6}"),  # YYYYJJJ-HHMMSS
    "%Y%j_%H%M%S": re.compile(r"\d{7}_\d{6}"),  # YYYYJJJ_HHMMSS
    "%Y%m%d": re.compile(r"(?<!\d)\d{8}(?!\d)"),  # YYYYMMDD
}


def create_science_filename(
    instrument: str,
    time: str,
    level: str,
    version: str,
    mode: str = "",
    descriptor: str = "",
    test: bool = False,
):
    """Return a compliant filename. The format is defined as

    {mission}_{inst}_{mode}_{level}{test}_{descriptor}_{time}_v{version}.cdf

    This format is only appropriate for data level >= 1.

    Parameters
    ----------
    instrument : `str`
        The instrument name. Must be one of the following "eea", "nemesis", "merit", "spani"
    time : `str` (in isot format) or ~astropy.time
        The time
    level : `str`
        The data level. Must be one of the following "l0", "l1", "l2", "l3", "l4", "ql"
    version : `str`
        The file version which must be given as X.Y.Z
    descriptor : `str`
        An optional file descriptor.
    mode : `str`
        An optional instrument mode.
    test : bool
        Selects whether the file is a test file.

    Returns
    -------
    filename : `str`
        A CDF file name including the given parameters that matches the mission's file naming conventions

    Raises
    ------
    ValueError: If the instrument is not recognized as one of the mission's instruments
    ValueError: If the data level is not recognized as one of the mission's valid data levels
    ValueError: If the data version does not match the mission's data version formatting conventions
    ValueError: If the data product descriptor or instrument mode do not match the mission's formatting conventions
    """
    test_str = ""
    mission_config = swxsoc.config["mission"]

    if isinstance(time, str):
        time_str = Time(time, format="isot").strftime(TIME_FORMAT)
    else:
        time_str = time.strftime(TIME_FORMAT)

    if instrument not in mission_config["inst_names"]:
        raise ValueError(
            f"Instrument, {instrument}, is not recognized. Must be one of {mission_config['inst_names']}."
        )
    if level not in mission_config["valid_data_levels"]:
        raise ValueError(
            f"Level, {level}, is not recognized. Must be one of {mission_config['valid_data_levels']}."
        )
    # check that version is in the right format with three parts
    if len(version.split(".")) != 3:
        raise ValueError(
            f"Version, {version}, is not formatted correctly. Should be X.Y.Z"
        )
    # check that version has integers in each part
    for item in version.split("."):
        try:
            int(item)
        except ValueError:
            raise ValueError(f"Version, {version}, is not all integers.")

    if test is True:
        test_str = "test"

    # the parse_science_filename function depends on _ not being present elsewhere
    if ("_" in mode) or ("_" in descriptor):
        raise ValueError(
            "The underscore symbol _ is not allowed in mode or descriptor."
        )

    # Parse Filename and Instrument Name out of the config
    mission_name = mission_config["mission_name"]
    instrument_shortname = mission_config["inst_to_shortname"].get(
        instrument, instrument
    )

    # Combine Parts into Filename
    filename = f"{mission_name}_{instrument_shortname}_{mode}_{level}{test_str}_{descriptor}_{time_str}_v{version}"
    filename = filename.replace("__", "_")  # reformat if mode or descriptor not given

    return filename + mission_config["file_extension"]


def _get_instrument_mapping(config: dict) -> dict:
    """
    Maps instrument shortnames to their full names and additional names.
    This is used for parsing filenames and ensuring consistency in naming.

    Parameters
    ----------
    config : dict
        The configuration dictionary containing mission and instrument details.

    Returns
    -------
    dict
        A dictionary mapping shortnames to full names and additional names.
    """
    return {
        **{s: m for m, s in config["inst_to_shortname"].items()},
        **{s: m for m, lst in config["inst_to_extra_inst_names"].items() for s in lst},
    }


def _parse_standard_format(filename: str, mission_config: dict) -> dict:
    """
    Parses the standard filename format and extracts relevant fields.
    Handles the following format:
    {mission}_{inst}_{mode}_{level}{test}_{descriptor}_{time}_v{version}.{extension}

    Parameters
    ----------
    filename : str
        The filename to parse (with or without path).
    mission_config : dict
        The configuration dictionary containing mission and instrument details.

    Returns
    -------
    dict
        A dictionary containing the parsed fields.

    Raises
    ------
    ValueError
        If the filename does not match the expected format or contains invalid values.
    """

    result = {}
    mission_name = mission_config["mission_name"]
    shortnames = mission_config["inst_shortnames"]

    # Split the filename into components
    filename = Path(filename).stem
    components = filename.split("_")

    # Handle mission names that contain underscores (e.g. "swxsoc_pipeline")
    # by joining the appropriate number of leading components
    mission_name_parts = mission_name.split("_")
    n_mission_parts = len(mission_name_parts)
    parsed_mission_name = "_".join(components[:n_mission_parts])

    if parsed_mission_name != mission_name:
        warn_user(
            f"Not a valid mission name: {parsed_mission_name}. Expected: {mission_name}. Reverting to parsing with assumption of configured mission name.",
        )
    else:
        # Strip mission name parts so remaining components start with instrument
        components = components[n_mission_parts:]

    if components[0] not in shortnames:
        raise ValueError(
            f"Invalid instrument shortname: {components[0]}. Expected one of {shortnames}"
        )

    # Parse Instrument Name
    inst_name = components[0]
    mapping = _get_instrument_mapping(mission_config)
    result["instrument"] = mapping.get(inst_name.lower(), inst_name)
    result["time"] = _extract_time(
        filename, expected_format=TIME_FORMAT, mission_config=mission_config
    )

    # Handle optional fields: mode, test, descriptor
    result["test"] = "test" in components[1] or "test" in components[2]
    if components[1][:2] not in mission_config["valid_data_levels"]:
        result["mode"] = components[1]
        result["level"] = components[2].replace("test", "")
        if len(components) == 6:
            result["descriptor"] = components[3]
    else:
        result["level"] = components[1].replace("test", "")
        if len(components) == 5:
            result["descriptor"] = components[2]

    result["version"] = components[-1].lstrip("v")
    return result


def _extract_instrument_name(filename: str, mission_config: dict) -> str:
    """
    Extracts the instrument name from the filename using regex patterns.

    Parameters
    ----------
    filename : str
        The filename from which to extract the instrument name.
    mission_config : dict
        The configuration dictionary containing mission and instrument details.

    Returns
    -------
    str
        The extracted instrument name.

    Raises
    ------
    ValueError
        If no valid instrument name is found in the filename.
    """

    all_inst_names = [
        name.lower()
        for name in (
            mission_config["inst_names"]
            + mission_config["inst_shortnames"]
            + [n for sublist in mission_config["extra_inst_names"] for n in sublist]
        )
    ]
    mission_name = mission_config["mission_name"].lower()
    pattern = re.compile(
        rf"(?:^|[_\-.]|{mission_name})("  # Group 1: Prefix
        + "|".join(
            re.escape(name) for name in all_inst_names
        )  # Group 2: Instrument name
        + r"(?:\d+)?)(?:[_\-.]|$|\d)",  # Group 3: Suffix,
        re.IGNORECASE,
    )
    matches = pattern.findall(filename.lower())
    if not matches:
        raise ValueError(f"No valid instrument name found in {filename}")
    if len(matches) > 1:
        raise ValueError(f"Multiple instrument names found: {matches}")
    return matches[0]


def _extract_data_level(filename: str, possible_levels: List[str]) -> str:
    """
    Extracts the data level from the filename using regex patterns. If no data level is found, then the first possible level is returned.

    Parameters
    ----------
    filename : str
        The filename from which to extract the data level.
    possible_levels : List[str]
        A list of possible data levels to search for.

    Returns
    -------
    str
        The extracted data level.
    """
    if len(possible_levels) == 1:
        # Exact match (e.g. 'raw')
        return possible_levels[0]

    # Grouped levels (L0-L3): Extract from filename
    # Search filename for 'l0', 'l1', etc.
    found_level = None
    for lvl in possible_levels:
        # Simple check: is 'l1' sandwiched by delimiters?
        if re.search(rf"[_\-.]{lvl}[_\-.]", filename, re.IGNORECASE):
            found_level = lvl
            break

    return found_level if found_level else possible_levels[0]


def _extract_time(
    filename: str,
    expected_format: Optional[str] = None,
    mission_config: Optional[dict] = None,
) -> Time:
    """
    Extracts time from the filename using regex patterns.
    Handles various formats including ISO 8601 and legacy L0 formats.

    Parameters
    ----------
    filename : str
        The filename from which to extract the time.
    expected_format : Optional[str]
        The expected time format to use for parsing.
    mission_config : Optional[dict]
        The configuration dictionary containing mission details.

    Returns
    -------
    Time
        The extracted time as an astropy Time object.

    Raises
    ------
    ValueError
        If no recognizable time format is found in the filename.
    ValueError
        If the extracted time is outside the valid range defined in the mission configuration.
    """
    time_parsers = [
        _try_parse_with_expected_format,
        _try_all_patterns,
    ]
    # Use Strategy Pattern to try different parsers
    for parser in time_parsers:
        result = parser(filename, expected_format)
        if result:
            return _validate_time(result, mission_config=mission_config)
    raise ValueError(f"No recognizable time format in {filename}")


def _try_parse_with_expected_format(
    filename: str, expected_format: str
) -> Optional[Time]:
    """
    Try to parse time using the expected format.

    Parameters
    ----------
    filename : str
        The filename from which to extract the time.
    expected_format : str
        The expected time format to use for parsing.

    Examples
    --------
    >>> _try_parse_with_expected_format("swxsoc_eea_l1_20230115T123045_v1.0.0.cdf", "%Y%m%dT%H%M%S")
    <Time object: scale='utc' format='datetime' value=2023-01-15 12:30:45>
    >>> _try_parse_with_expected_format("padre_get_EPS_9_Data_1673785845000.csv", "unix_ms")
    <Time object: scale='utc' format='isot' value=2023-01-15T12:30:45.000>
    """
    # Return early if no expected format is provided
    if not expected_format:
        return None

    # Get the regex pattern for the expected format
    pattern = TIME_PATTERNS.get(expected_format)
    if not pattern:
        swxsoc.log.warning(
            f"No regex pattern found for expected time format '{expected_format}'. "
            "Falling back to all patterns."
        )
        return None

    # Look for a match in the filename using the expected format
    match = pattern.search(filename)
    if not match:
        swxsoc.log.warning(
            f"No time string matching expected format '{expected_format}' found in {filename}."
        )
        return None

    time_str = match.group(0)
    return _parse_time_string(time_str, expected_format)


def _try_all_patterns(filename: str, *args, **kwargs) -> Optional[Time]:
    """
    Try to parse time using all known patterns.

    Parameters
    ----------
    filename : str
        The filename from which to extract the time.

    Returns
    -------
    Time
        The extracted time as an astropy Time object, or None if not found.

    Examples
    --------
    >>> _try_all_patterns("swxsoc_eea_l1_20230115T123045_v1.0.0.cdf")
    <Time object: scale='utc' format='datetime' value=2023-01-15 12:30:45>
    """
    for format_str, pattern in TIME_PATTERNS.items():
        match = pattern.search(filename)
        if match:
            time_str = match.group(0)
            parsed_time = _parse_time_string(time_str, format_str)
            if parsed_time:
                return parsed_time
    return None


def _parse_time_string(time_str: str, format_str: str) -> Optional[Time]:
    """
    Parse a time string with a specific format.

    Parameters
    ----------
    time_str : str
        The time string to parse.
    format_str : str
        The format string to use for parsing.

    Examples
    --------
    >>> _parse_time_string("2023-01-15 12:30:45", "%Y-%m-%d %H:%M:%S")
    <Time object: scale='utc' format='datetime' value=2023-01-15 12:30:45>
    >>> _parse_time_string("1673785845000", "unix_ms")
    <Time object: scale='utc' format='isot' value=2023-01-15T12:30:45.000>
    >>> _parse_time_string("invalid", "%Y-%m-%d")
    """
    # Special case for unix time
    if format_str in ("unix_ms", "unix_s"):
        return _parse_unix_timestamp(time_str, format_str)

    # Try datetime string formatters
    try:
        return Time(datetime.strptime(time_str, format_str))
    except ValueError:
        pass

    # Fall back to sunpy parser as last resort
    try:
        return Time(sunpy.time.parse_time(time_str))
    except Exception:
        return None


def _parse_unix_timestamp(time_str: str, format_str: str) -> Time:
    """
    Parse Unix timestamp in milliseconds or seconds.

    Parameters
    ----------
    time_str : str
        The Unix timestamp string.
    format_str : str
        The format identifier: ``"unix_ms"`` for milliseconds, or ``"unix_s"`` for seconds.

    Returns
    -------
    Time
        The parsed time as an astropy Time object.

    Examples
    --------
    >>> _parse_unix_timestamp("1673785845000", "unix_ms")
    <Time object: scale='utc' format='isot' value=2023-01-15T12:30:45.000>
    >>> _parse_unix_timestamp("1673785845", "unix_s")
    <Time object: scale='utc' format='isot' value=2023-01-15T12:30:45.000>
    """
    divisor = 1000.0 if format_str == "unix_ms" else 1.0
    t_unix = Time(int(time_str) / divisor, format="unix")
    t_unix.format = "isot"  # Need to set format to isot for consistency
    return t_unix


def _validate_time(extracted_time: Time, mission_config: Optional[dict] = None) -> Time:
    """
    Validate the extracted time against configured mission constraints.

    When mission_config is provided, raises ValueError for times outside the valid range.
    When mission_config is None, issues warnings for suspicious times but does not raise.

    Parameters
    ----------
    extracted_time : Time
        The extracted time to validate.
    mission_config : Optional[dict], optional
        The configuration dictionary containing mission details with 'min_valid_time'
        and 'max_valid_time' keys. If None, performs basic validation with warnings only.

    Returns
    -------
    Time
        The validated time (same as input).

    Raises
    ------
    ValueError
        If mission_config is provided and the extracted time is before the configured
        minimum valid time (mission_config['min_valid_time']).
    ValueError
        If mission_config is provided and the extracted time is after the configured
        maximum valid time (mission_config['max_valid_time']).
    """
    if mission_config is None:
        # Fallback to basic validation when no config provided
        if extracted_time > Time.now():
            swxsoc.log.warning(f"Found future time {extracted_time}.")
        if extracted_time < Time("1970-01-01"):
            swxsoc.log.warning(f"Found suspiciously old time {extracted_time}.")
        return extracted_time

    # Get configured time constraints
    min_valid_time = mission_config.get("min_valid_time")
    max_valid_time = mission_config.get("max_valid_time")

    # Validate minimum time
    if min_valid_time and extracted_time < min_valid_time:
        raise ValueError(
            f"Extracted time {extracted_time} is before mission minimum valid time {min_valid_time}."
        )

    # Validate maximum time
    if max_valid_time and extracted_time > max_valid_time:
        raise ValueError(
            f"Extracted time {extracted_time} is after mission maximum valid time {max_valid_time}."
        )

    return extracted_time


def parse_science_filename(filepath: str) -> dict:
    """
    Parses a science filename into its constituent properties.

    Parameters
    ----------
    filepath : str
        Fully qualified filepath of an input file.

    Returns
    -------
    dict
        Parsed fields such as instrument, mode, test, time, level, version, and descriptor.

    Raises
    ------
    ValueError
        If mission name or instrument is not recognized, or time format is invalid.
    """
    import swxsoc

    # setup defaults
    mission_config = swxsoc.config["mission"]
    filepath = Path(filepath)
    filename = filepath.name
    file_ext = filepath.suffix
    result = {
        "instrument": None,
        "mode": None,
        "test": False,
        "time": None,
        "level": None,
        "version": None,
        "descriptor": None,
    }

    # Case 1: The file is in a standard format used for archive/science files
    if file_ext == mission_config["file_extension"]:
        parsed = _parse_standard_format(filename, mission_config)
        result.update(parsed)
        return result

    # Extract instrument name for file rule matching
    try:
        inst_name_raw = _extract_instrument_name(filename, mission_config)
        mapping = _get_instrument_mapping(mission_config)
        inst_name = mapping.get(inst_name_raw.lower(), inst_name_raw)
        result["instrument"] = inst_name
    except ValueError as e:
        raise ValueError(f"Error extracting instrument name: {e}")

    # Check for specific File Rules
    matched_rule = None
    mission_rules = mission_config.get("inst_file_rules", {})
    inst_rules = mission_rules.get(inst_name, [])
    for rule in inst_rules:
        # Check Extension
        if file_ext.lower() == rule["extension"].lower():
            matched_rule = rule
            break

    # Case 2: The file is in a non-standard format, but matches a known rule
    if matched_rule:
        # Extract Data Level
        data_level = _extract_data_level(filename, matched_rule["levels"])
        # Get the expected time format based on rule definition
        expected_format = matched_rule.get("time_format")
        # Parse time using the expected format
        parsed_time = _extract_time(
            filename, expected_format=expected_format, mission_config=mission_config
        )
        result.update(
            {
                "mission": mission_config["mission_name"].lower(),
                "level": data_level,
                "time": parsed_time,
            }
        )

    # Case 3: The file does not match any known format
    else:
        parsed_time = _extract_time(filename, mission_config=mission_config)
        result.update(
            {
                "mission": mission_config["mission_name"].lower(),
                "instrument": inst_name,  # At least we got the instrument from the filename
                "time": parsed_time,
                "level": _extract_data_level(
                    filename, mission_config["valid_data_levels"]
                ),
            }
        )

    return result


def get_instrument_package(instrument_name: str) -> str:
    """
    Determines the package name of the correct instrument package to use for processing a file based on the instrument name.
    This is determined through two possibilities:
    1. The instrument name is directly mapped to a package in the instrument configuration under "instrument_package".
    2. The package is default determined by "{mission__name}_{instrument_name}"

    Parameters
    ----------
    instrument_name : str
        The name of the instrument to find the package for.

    Returns
    -------
    str
        The name of the package to use for processing files from the specified instrument.

    Raises
    ------
    ValueError
        If the instrument name is not recognized as one of the mission's instruments.
    """
    mission_config = swxsoc.config["mission"]

    # sanitize instrument name for matching (e.g. case insensitive)
    instrument_name = instrument_name.lower()

    # check if the instrument is available for the mission
    if instrument_name not in mission_config["inst_names"]:
        raise ValueError(
            f"Instrument, {instrument_name}, is not recognized. Must be one of {list(mission_config['inst_names'])}."
        )

    # get the instrument configuration
    inst_package = mission_config["inst_packages"].get(instrument_name)
    if inst_package:
        # if a package is explicitly defined for the instrument, use it
        return inst_package
    else:
        # otherwise, default to the convention of {mission_name}_{instrument_name}
        return f"{mission_config['mission_name'].lower()}_{instrument_name.lower()}"
