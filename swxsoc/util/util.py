"""
This module provides general utility functions.
"""

import os

from astropy.time import Time

import swxsoc


__all__ = ["create_science_filename", "parse_science_filename", "VALID_DATA_LEVELS"]

TIME_FORMAT_L0 = "%Y%j-%H%M%S"
TIME_FORMAT = "%Y%m%dT%H%M%S"
VALID_DATA_LEVELS = ["l0", "l1", "ql", "l2", "l3", "l4"]
FILENAME_EXTENSIONS = [".cdf", ".fits", ".bin"]


def create_science_filename(
    mission: str,
    instrument: str,
    time: str,
    level: str,
    version: str,
    extension: str,
    mode: str = "",
    descriptor: str = "",
    test: bool = False,
):
    """Return a compliant filename. The format is defined as

    {mission}_{inst}_{mode}_{level}{test}_{descriptor}_{time}_v{version}{extension}

    This format is only appropriate for data level >= 1.

    Parameters
    ----------
    mission: `str`
        The mission/project name.
    instrument : `str`
        The instrument name.
    time : `str` (in isot format) or ~astropy.time
        The time
    level : `str`
        The data level. Must be one of the following "l0", "l1", "l2", "l3", "l4", "ql"
    version : `str`
        The file version which must be given as X.Y.Z
    extension : `str`
        The file extension. Must be one of the following: ".cdf", ".fits", ".bin"
    descriptor : `str`
        An optional file descriptor. Must not contain the underscore symbol '_'.
    mode : `str`
        An optional instrument mode. Must not contain the underscore symbol '_'.
    test : bool
        Selects whether the file is a test file.

    Returns
    -------
    filename : `str`
        A file name including the given parameters that matches the file naming conventions

    Raises
    ------
    ValueError: If the instrument is not recognized as one of the mission's instruments
    ValueError: If the data level is not recognized as one of the valid data levels
    ValueError: If the data version does not match the data version formatting conventions
    ValueError: If the data product descriptor or instrument mode do not match the formatting conventions
    ValueError: If the file extension is not recognized
    """
    test_str = ""

    if isinstance(time, str):
        time_str = Time(time, format="isot").strftime(TIME_FORMAT)
    else:
        time_str = time.strftime(TIME_FORMAT)

    # check that the instrument is valid
    if instrument not in swxsoc.config["mission"]["inst_names"]:
        raise ValueError(
            f"Instrument, {instrument}, is not recognized. Must be one of {swxsoc.config['mission']['inst_names']}."
        )

    # check that level is valid
    if level not in VALID_DATA_LEVELS[1:]:
        raise ValueError(
            f"Level, {level}, is not recognized. Must be one of {VALID_DATA_LEVELS[1:]}."
        )

    # check that version is in the right format with three parts
    if len(version.split(".")) != 3:
        raise ValueError(
            f"Version, {version}, is not formatted correctly. Should be X.Y.Z"
        )

    # check that version has integers in each part
    for item in version.split("."):
        try:
            int_value = int(item)
        except ValueError:
            raise ValueError(f"Version, {version}, is not all integers.")

    # check that the extension is valid
    if extension not in FILENAME_EXTENSIONS:
        raise ValueError(
            f"Extension, {extension}, is not recognized. Must be one of {FILENAME_EXTENSIONS}."
        )

    if test is True:
        test_str = "test"

    # the parse_science_filename function depends on _ not being present elsewhere
    if ("_" in mode) or ("_" in descriptor):
        raise ValueError(
            "The underscore symbol _ is not allowed in mode or descriptor."
        )

    filename = f"{mission}_{swxsoc.config['mission']['inst_to_shortname'][instrument]}_{mode}_{level}{test_str}_{descriptor}_{time_str}_v{version}{extension}"
    filename = filename.replace("__", "_")  # reformat if mode or descriptor not given

    return filename


def parse_science_filename(filepath: str) -> dict:
    """
    Parses a science filename, given in the required file name format, into its constituent properties (mission, instrument, mode, test, time, level, version, descriptor, extension).

    The required file name format is defined as:

    {mission}_{inst}_{mode}_{level}{test}_{descriptor}_{time}_v{version}{extension}

    Parameters
    ----------
    filepath: `str`
        Fully specificied filepath of an input file

    Returns
    -------
    result : `dict`
        A dictionary with each property.

    Raises
    ------
    ValueError: If the file's mission name does not match the conigured mission name
    ValueError: If the file's instreument name is not one of the mission's instruments
    ValueError: If the data level >0 for packet files
    """

    result = {
        "mission": "",
        "instrument": "",
        "mode": "",
        "test": False,
        "time": "",
        "level": "",
        "version": "",
        "descriptor": "",
        "extension": "",
    }

    filename = os.path.basename(filepath)
    file_name, file_ext = os.path.splitext(filename)

    filename_components = file_name.split("_")

    # check that the mission is valid
    if filename_components[0] != swxsoc.config["mission"]["mission_name"]:
        raise ValueError(f"File {filename} not recognized. Not a valid mission name.")

    if file_ext == ".bin":

        # check that the instrument is valid
        if filename_components[1] not in swxsoc.config["mission"]["inst_targetnames"]:
            raise ValueError(
                f"File {filename} not recognized. Not a valid target name."
            )

        offset = 1 if len(filename_components) > 5 else 0

        if offset:
            result["mode"] = filename_components[2]

        if filename_components[2 + offset] != VALID_DATA_LEVELS[0]:
            raise ValueError(
                f"Data level {filename_components[2 + offset]} is not correct for this file extension."
            )
        else:
            result["level"] = filename_components[2 + offset]
        #  reverse the dictionary to look up instrument name from the short name
        from_shortname = {
            v: k for k, v in swxsoc.config["mission"]["inst_to_targetname"].items()
        }

        result["time"] = Time.strptime(filename_components[3 + offset], TIME_FORMAT_L0)

    elif file_ext == ".cdf":
        if filename_components[1] not in swxsoc.config["mission"]["inst_shortnames"]:
            raise ValueError(
                "File {filename} not recognized. Not a valid instrument name."
            )

        #  reverse the dictionary to look up instrument name from the short name
        from_shortname = {
            v: k for k, v in swxsoc.config["mission"]["inst_to_shortname"].items()
        }

        result["time"] = Time.strptime(filename_components[-2], TIME_FORMAT)

        # mode and descriptor are optional so need to figure out if one or both or none is included
        if filename_components[2][0:2] not in VALID_DATA_LEVELS:
            # if the first component is not data level then it is mode and the following is data level
            result["mode"] = filename_components[2]
            result["level"] = filename_components[3].replace("test", "")
            if "test" in filename_components[3]:
                result["test"] = True
            if len(filename_components) == 7:
                result["descriptor"] = filename_components[4]
        else:
            result["level"] = filename_components[2].replace("test", "")
            if "test" in filename_components[2]:
                result["test"] = True
            if len(filename_components) == 6:
                result["descriptor"] = filename_components[3]
    else:
        raise ValueError(f"File extension {file_ext} not recognized.")

    result["mission"] = filename_components[0]
    result["instrument"] = from_shortname[filename_components[1]]
    result["version"] = filename_components[-1][1:]  # remove the v
    result["extension"] = file_ext

    return result
