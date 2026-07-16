"""
This module provides configuration file functionality.

This code is based on that provided by SunPy see
    licenses/SUNPY.rst
"""

import os
import shutil
from pathlib import Path

import yaml
from astropy.time import Time

import swxsoc
from swxsoc.util.exceptions import warn_user

# This is to fix issue with AppDirs not writing to /tmp/ in AWS Lambda
if not os.getenv("LAMBDA_ENVIRONMENT"):
    from sunpy.extern.appdirs import AppDirs

__all__ = [
    "load_config",
    "copy_default_config",
    "print_config",
    "CONFIG_DIR",
    "TSD_REGION",
    "get_incoming_bucket",
    "get_instrument_bucket",
    "get_all_instrument_buckets",
]

# Default directories for Lambda Environment
CONFIG_DIR = "/tmp/.config"
CACHE_DIR = "/tmp/.cache"

# AWS region used for the Timestream client session.
TSD_REGION = os.getenv("AWS_REGION", "us-east-1")

# This is to fix issue with AppDirs not writing to /tmp/ in AWS Lambda
if not os.getenv("LAMBDA_ENVIRONMENT"):
    # This is to avoid creating a new config dir for each new dev version.
    # We use AppDirs to locate and create the config directory.
    dirs = AppDirs("swxsoc", "swxsoc")
    # Default one set by AppDirs
    CONFIG_DIR = dirs.user_config_dir
    CACHE_DIR = dirs.user_cache_dir


def load_config():
    """
    Load and read the configuration file.

    If a configuration file does not exist in the user's home directory,
    it will read in the defaults from the package's data directory.

    The selected mission can be overridden by setting the `SWXSOC_MISSION`
    environment variable. This environment variable will take precedence
    over the mission specified in the configuration file.

    Returns:
        dict: The loaded configuration data.
    """
    config_path = Path(_get_user_configdir()) / "config.yml"
    if not config_path.exists():
        config_path = Path(swxsoc.__file__).parent / "data" / "config.yml"

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    # Loaded either from env var or from config file
    selected_mission = os.getenv("SWXSOC_MISSION", config["selected_mission"])
    missions_data = config.get("missions_data", {})
    mission_data = missions_data.get(selected_mission, {})

    # The File Extension Used for Archive Files and Science File Formats
    file_extension = mission_data.get("file_extension", "")

    # Parse time values if they exist, otherwise set to None
    min_valid_time = mission_data.get("min_valid_time", None)
    max_valid_time = mission_data.get("max_valid_time", None)

    # Convert time values to Time objects, handling special "now" value
    if min_valid_time is not None:
        min_valid_time = Time.now() if min_valid_time == "now" else Time(min_valid_time)
    if max_valid_time is not None:
        max_valid_time = Time.now() if max_valid_time == "now" else Time(max_valid_time)

    # S3 bucket names cannot contain underscores; convert to dashes to match
    # the Terraform naming convention.
    bucket_mission_name = selected_mission.replace("_", "-")

    # Incomving Bucket Name can be overridden by the environment variable SWXSOC_INCOMING_BUCKET
    incoming_bucket_override = os.getenv("SWXSOC_INCOMING_BUCKET")

    config["mission"] = {
        "file_extension": (
            f".{file_extension}"
            if not file_extension.startswith(".")
            else file_extension
        ),
        "mission_name": selected_mission,
        "bucket_mission_name": bucket_mission_name,
        "min_valid_time": min_valid_time,
        "max_valid_time": max_valid_time,
        "valid_data_levels": mission_data.get(
            "valid_data_levels", ["raw", "l0", "l1", "ql", "l2", "l3", "l4"]
        ),
        "inst_names": [inst["name"] for inst in mission_data.get("instruments", [])],
        "inst_shortnames": [
            inst["shortname"] for inst in mission_data.get("instruments", [])
        ],
        "inst_fullnames": [
            inst["fullname"] for inst in mission_data.get("instruments", [])
        ],
        "inst_targetnames": [
            inst["targetname"] for inst in mission_data.get("instruments", [])
        ],
        "extra_inst_names": [
            inst["extra_inst_names"]
            for inst in mission_data.get("instruments", [])
            if "extra_inst_names" in inst
        ],
        "inst_packages": {
            inst["name"]: inst.get("instrument_package", None)
            for inst in mission_data.get("instruments", [])
        },
        "inst_file_rules": {
            inst["name"]: inst.get("file_rules", [])
            for inst in mission_data.get("instruments", [])
        },
        # AWS S3 bucket name for incoming files, can be overridden by the environment variable SWXSOC_INCOMING_BUCKET
        "incoming_bucket": (
            incoming_bucket_override
            if incoming_bucket_override is not None
            else f"{bucket_mission_name}-incoming"
        ),
        # Create a mapping of instrument names to their corresponding S3 bucket names
        "instr_to_bucket_name": {
            inst["name"]: f"{bucket_mission_name}-{inst['name']}"
            for inst in mission_data.get("instruments", [])
        },
    }

    config["mission"].update(
        {
            "inst_to_shortname": dict(
                zip(
                    config["mission"]["inst_names"],
                    config["mission"]["inst_shortnames"],
                )
            ),
            "inst_to_fullname": dict(
                zip(
                    config["mission"]["inst_names"], config["mission"]["inst_fullnames"]
                )
            ),
            "inst_to_targetname": dict(
                zip(
                    config["mission"]["inst_names"],
                    config["mission"]["inst_targetnames"],
                )
            ),
            "inst_to_extra_inst_names": dict(
                zip(
                    config["mission"]["inst_names"],
                    config["mission"]["extra_inst_names"],
                )
            ),
        }
    )

    if os.getenv("LAMBDA_ENVIRONMENT"):
        config["logger"]["log_to_file"] = False

    return config


def get_incoming_bucket() -> str:
    """
    Get the name of the incoming S3 bucket for the currently configured mission.

    The bucket name is read from ``swxsoc.config["mission"]["incoming_bucket"]``
    and prefixed with ``dev-`` unless the pipeline is running in production
    (see :func:`swxsoc.util.util.is_production_environment`).

    Returns
    -------
    str
        The incoming bucket name, `dev-` prefixed in non-production environments.
    """
    from swxsoc.util.util import is_production_environment

    bucket = swxsoc.config["mission"]["incoming_bucket"]
    return bucket if is_production_environment() else f"dev-{bucket}"


def get_instrument_bucket(instrument: str) -> str:
    """
    Get the name of the S3 bucket for the given instrument.

    The bucket name is read from
    ``swxsoc.config["mission"]["instr_to_bucket_name"]`` and prefixed with
    ``dev-`` unless the pipeline is running in production (see
    :func:`swxsoc.util.util.is_production_environment`).

    Parameters
    ----------
    instrument : str
        The instrument name (as configured in ``inst_names`` for the mission).

    Returns
    -------
    str
        The instrument bucket name, `dev-` prefixed in non-production environments.
    """
    from swxsoc.util.util import is_production_environment

    bucket = swxsoc.config["mission"]["instr_to_bucket_name"][instrument]
    return bucket if is_production_environment() else f"dev-{bucket}"


def get_all_instrument_buckets() -> list:
    """
    Get the S3 bucket names for all instruments of the currently configured mission.

    Returns
    -------
    list
        The instrument bucket names, `dev-` prefixed in non-production environments.
    """
    from swxsoc.util.util import is_production_environment

    prod = is_production_environment()
    return [
        bucket if prod else f"dev-{bucket}"
        for bucket in swxsoc.config["mission"]["instr_to_bucket_name"].values()
    ]


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


def _get_user_configdir():
    """
    Return the configuration directory path.

    The configuration directory is determined by the "SWXSOC_CONFIGDIR"
    environment variable or a default directory set by the application.

    Returns:
        str: The path to the configuration directory.

    Raises:
        RuntimeError: If the configuration directory is not writable.
    """
    configdir = os.environ.get("SWXSOC_CONFIGDIR", CONFIG_DIR)

    if not _is_writable_dir(configdir):
        raise RuntimeError(f'Could not write to SWXSOC_CONFIGDIR="{configdir}"')
    return configdir


def _is_writable_dir(path):
    """
    Check if the specified path is a writable directory.

    Args:
        path (str or Path): The path to check.

    Returns:
        bool: True if the path is a writable directory, False otherwise.

    Raises:
        FileExistsError: If a file exists at the path instead of a directory.
    """
    # Worried about multiple threads creating the directory at the same time.
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except FileExistsError:  # raised if there's an existing file instead of a directory
        return False
    else:
        return Path(path).is_dir() and os.access(path, os.W_OK)


def copy_default_config(overwrite=False):
    """
    Copy the default configuration file to the user's configuration directory.

    If the configuration file already exists, it will be overwritten if the
    `overwrite` parameter is set to True.

    Args:
        overwrite (bool): Whether to overwrite an existing configuration file.

    Raises:
        RuntimeError: If the configuration directory is not writable.
    """
    config_filename = "config.yml"
    config_file = Path(swxsoc.__file__).parent / "data" / config_filename
    user_config_dir = Path(_get_user_configdir())
    user_config_file = user_config_dir / config_filename

    if not _is_writable_dir(user_config_dir):
        raise RuntimeError(f"Could not write to config directory {user_config_dir}")

    if user_config_file.exists():
        if overwrite:
            message = (
                "User config file already exists. "
                "This will be overwritten with a backup written in the same location."
            )
            warn_user(message)
            os.rename(str(user_config_file), str(user_config_file) + ".bak")
            shutil.copyfile(config_file, user_config_file)
        else:
            message = (
                "User config file already exists. "
                "To overwrite it use `copy_default_config(overwrite=True)`"
            )
            warn_user(message)
    else:
        shutil.copyfile(config_file, user_config_file)


def print_config(config):
    """
    Print the current configuration options.

    Args:
        config (dict): The configuration data to print.
    """
    print("FILES USED:")
    for file_ in _find_config_files():
        print("  " + file_)

    print("\nCONFIGURATION:")
    for section, settings in config.items():
        if isinstance(settings, dict):  # Nested configuration
            print(f"  [{section}]")
            for option, value in settings.items():
                print(f"  {option} = {value}")
            print("")
        else:  # Not a nested configuration
            print(f"  {section} = {settings}")


def _find_config_files():
    """
    Find the locations of configuration files.

    Returns:
        list: A list of paths to the configuration files.
    """
    config_files = []
    config_filename = "config.yml"

    # find default configuration file
    module_dir = Path(swxsoc.__file__).parent
    config_files.append(str(module_dir / "data" / config_filename))

    # if a user configuration file exists, add that to list of files to read
    # so that any values set there will override ones specified in the default
    # config file
    config_path = Path(_get_user_configdir())
    if config_path.joinpath(config_filename).exists():
        config_files.append(str(config_path.joinpath(config_filename)))

    return config_files
