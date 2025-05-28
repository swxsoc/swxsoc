"""
This module provides configuration file functionality.

This code is based on that provided by SunPy see
    licenses/SUNPY.rst
"""

import os
import shutil
import yaml
from pathlib import Path

import swxsoc
from swxsoc.util.exceptions import warn_user

# This is to fix issue with AppDirs not writing to /tmp/ in AWS Lambda
if not os.getenv("LAMBDA_ENVIRONMENT"):
    from sunpy.extern.appdirs import AppDirs

__all__ = ["load_config", "copy_default_config", "print_config", "CONFIG_DIR"]

# Default directories for Lambda Environment
CONFIG_DIR = "/tmp/.config"
CACHE_DIR = "/tmp/.cache"

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

    selected_mission = os.getenv("SWXSOC_MISSION", config["selected_mission"])
    missions_data = config.get("missions_data", {})
    mission_data = missions_data.get(selected_mission, {})
    file_extension = mission_data.get("file_extension", "")

    config["mission"] = {
        "file_extension": (
            f".{file_extension}"
            if not file_extension.startswith(".")
            else file_extension
        ),
        "mission_name": selected_mission,
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
