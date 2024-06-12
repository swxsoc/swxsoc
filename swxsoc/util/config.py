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
    Read the configuration file.

    If one does not exist in the user's home directory then read in the defaults.
    """
    # Get the default config file
    config_path = Path(_get_user_configdir()) / "config.yml"

    if not config_path.exists():
        config_path = Path(swxsoc.__file__).parent / "data" / "config.yml"

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    # Extract the selected mission from environment variable or default to 'swxsoc'
    selected_mission = os.getenv("SWXSOC_MISSION", config["selected_mission"])

    # Filter the mission data for the selected mission
    missions_data = config.get("missions_data")
    config["mission"] = {}

    config["mission"]["mission_name"] = selected_mission
    instruments = missions_data[selected_mission]["instruments"]

    inst_names = [inst["name"] for inst in instruments]
    config["mission"]["inst_names"] = inst_names

    inst_shortnames = [inst["shortname"] for inst in instruments]
    config["mission"]["inst_shortnames"] = inst_shortnames

    inst_fullnames = [inst["fullname"] for inst in instruments]
    config["mission"]["inst_fullnames"] = inst_fullnames

    inst_targetnames = [inst["targetname"] for inst in instruments]
    config["mission"]["inst_targetnames"] = inst_targetnames

    config["mission"]["inst_to_shortname"] = dict(zip(inst_names, inst_shortnames))
    config["mission"]["inst_to_fullname"] = dict(zip(inst_names, inst_fullnames))
    config["mission"]["inst_to_targetname"] = dict(zip(inst_names, inst_targetnames))

    # This is to fix issue with AppDirs not writing to /tmp/ in AWS Lambda
    if os.getenv("LAMBDA_ENVIRONMENT"):
        config["logger"]["log_to_file"] = False

    # Specify the working directory as a default so that the user's home
    # directory can be located in an OS-independent manner
    if "working_dir" not in config["general"]:
        config["general"]["working_dir"] = str(Path.home() / "swxsoc")

    # Set the download_dir to be relative to the working_dir
    working_dir = Path(config["general"]["working_dir"])
    download_dir = Path(config["downloads"]["download_dir"])
    config["downloads"]["download_dir"] = str(
        (working_dir / download_dir).expanduser().resolve()
    )

    return config


def _get_user_configdir():
    """
    Return the string representing the configuration dir.

    The default is set by "AppDirs" and can be accessed by importing
    ``hermes.util.config.CONFIG_DIR``. You can override this with the
    "SWXSOC_CONFIGDIR" environment variable.
    """
    configdir = os.environ.get("SWXSOC_CONFIGDIR", CONFIG_DIR)

    if not _is_writable_dir(configdir):
        raise RuntimeError(f'Could not write to SWXSOC_CONFIGDIR="{configdir}"')
    return configdir


def _is_writable_dir(p):
    """
    Checks to see if a directory is writable.
    """
    # Worried about multiple threads creating the directory at the same time.
    try:
        Path(p).mkdir(parents=True, exist_ok=True)
    except FileExistsError:  # raised if there's an existing file instead of a directory
        return False
    else:
        return Path(p).is_dir() and os.access(p, os.W_OK)


def copy_default_config(overwrite=False):
    """
    Copies the default config file to the user's config directory.

    Parameters
    ----------
    overwrite : `bool`
        If True, existing config file will be overwritten.
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
    Print current configuration options.
    """
    print("FILES USED:")
    for file_ in _find_config_files():
        print("  " + file_)

    print("\nCONFIGURATION:")
    for section, settings in config.items():
        print(f"  [{section}]")
        for option, value in settings.items():
            print(f"  {option} = {value}")
        print("")


def _find_config_files():
    """
    Finds locations of configuration files.
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


def get_and_create_download_dir():
    """
    Get the config of download directory and create one if not present.
    """
    download_dir = os.environ.get("SWXSOC_CONFIGDIR")
    if download_dir:
        return download_dir

    config = load_config()
    download_dir = Path(config["downloads"]["download_dir"]).expanduser().resolve()
    if not _is_writable_dir(download_dir):
        raise RuntimeError(
            f'Could not write to swxsoc downloads directory="{download_dir}"'
        )

    return config["downloads"]["download_dir"]


def get_and_create_sample_dir():
    """
    Get the config of download directory and create one if not present.
    """
    config = load_config()
    sample_dir = Path(config["downloads"]["sample_dir"]).expanduser().resolve()
    if not _is_writable_dir(sample_dir):
        raise RuntimeError(
            f'Could not write to swxsoc sample data directory="{sample_dir}"'
        )

    return config["downloads"]["sample_dir"]
