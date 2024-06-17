"""
Tests for the config module
"""

import os
from pathlib import Path
from contextlib import redirect_stdout

import pytest
import yaml

import swxsoc
from swxsoc.util import SWXWarning
from swxsoc.util.config import (
    CONFIG_DIR,
    _find_config_files,
    _get_user_configdir,
    _is_writable_dir,
    copy_default_config,
    print_config,
)

USER = os.path.expanduser("~")


def test_is_writable_dir(tmpdir, tmp_path):
    assert _is_writable_dir(tmpdir)
    tmp_file = tmpdir.join("hello.txt")
    # Have to write to the file otherwise its seen as a directory(?!)
    tmp_file.write("content")
    # Checks directory with a file
    assert _is_writable_dir(tmpdir)
    # Checks a filepath instead of directory
    assert not _is_writable_dir(tmp_file)
