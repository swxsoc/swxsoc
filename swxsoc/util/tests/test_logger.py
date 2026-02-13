"""
Tests for the logging module
"""

import inspect
import logging
import os.path
import re
import warnings

import pytest
from astropy.logger import AstropyLogger
from astropy.utils.exceptions import AstropyUserWarning

from swxsoc import config, log
from swxsoc.util.exceptions import SWXUserWarning
from swxsoc.util.logger import MyLogger

"""This code is based on that provided by SunPy see
    licenses/SUNPY.rst
"""

level_to_numeric = {
    "CRITICAL": 50,
    "ERROR": 40,
    "WARNING": 30,
    "INFO": 20,
    "DEBUG": 10,
    "NOTSET": 0,
}


def test_logger_name():
    assert log.name == "swxsoc"


def test_is_the_logger_there():
    assert isinstance(log, logging.Logger)
    assert isinstance(log, AstropyLogger)
    assert isinstance(log, MyLogger)


def test_is_level_configed():
    """
    Test to make sure that the logger follows the config:

    log_level
    """
    config_level_numeric = level_to_numeric.get(config.get("logger")["log_level"])
    assert log.getEffectiveLevel() == config_level_numeric


def test_is_log_to_file_configed():
    """
    Test to make sure that the logger follows the config:

    log_to_file, log_file_level, log_file_path
    """

    if config["logger"]["log_file_level"] == "True":
        #  there must be two handlers, one streaming and one to file.
        assert len(log.handlers) == 2
        #  one of the handlers must be FileHandler
        assert isinstance(log.handlers[0], logging.FileHandler) or isinstance(
            log.handlers[1], logging.FileHandler
        )
        fh = None
        if isinstance(log.handlers[0], logging.FileHandler):
            fh = log.handlers[0]

        if isinstance(log.handlers[1], logging.FileHandler):
            fh = log.handlers[1]

        if fh is not None:
            log_file_level = config.get("logger", "log_file_level")
            assert level_to_numeric.get(log_file_level) == fh.level

            log_file_path = config.get("logger", "log_file_path")
            assert os.path.basename(fh.baseFilename) == os.path.basename(log_file_path)


def send_to_log(message, kind="INFO"):
    """
    A simple function to demonstrate the logger generating an origin.
    """
    if kind.lower() == "info":
        log.info(message)
    elif kind.lower() == "debug":
        log.debug(message)


def test_log_format():
    """
    Test that the log format matches the expected pattern from config.yml:
    "%(asctime)s, %(origin)s.%(funcName)s():%(lineno)d, %(levelname)s, %(message)s"
    """
    # Use log_to_list to capture log messages
    with log.log_to_list() as log_list:
        # Define a function that we'll call to generate a log with known function name
        def test_logging_function():
            log.info("Testing log format", extra={"origin": "test_module"})

        # Call the function to generate a log entry
        test_logging_function()

    # We should have captured one log entry
    assert len(log_list) == 1

    # Check that the log entry has all the required attributes from our format
    entry = log_list[0]

    # Check timestamp format: YYYY-MM-DD HH:MM:SS (milliseconds are optional)
    assert hasattr(entry, "asctime")
    assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d{3})?", entry.asctime)

    # Check origin (module name)
    assert hasattr(entry, "origin")
    assert entry.origin == "test_module"

    # Check function name
    assert hasattr(entry, "funcName")
    assert entry.funcName == "test_logging_function"

    # Check line number exists and is numeric
    assert hasattr(entry, "lineno")
    assert isinstance(entry.lineno, int)

    # Check level name
    assert hasattr(entry, "levelname")
    assert entry.levelname == "INFO"

    # Check message
    assert hasattr(entry, "message")
    assert entry.message == "Testing log format"


def test_log_format_real_output():
    """
    Test that when logging something, the output string has the expected format.
    This tests the actual string formatting rather than just the presence of attributes.
    """
    with log.log_to_list() as log_list:
        # Get the line number before logging
        lineno = inspect.currentframe().f_lineno + 1
        log.info("Test message", extra={"origin": "test_module"})

    # Get the formatted message as it would appear in the log
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s, %(origin)s.%(funcName)s():%(lineno)d, %(levelname)s, %(message)s"
    )
    handler.setFormatter(formatter)

    # Format the log record ourselves
    formatted_message = formatter.format(log_list[0])

    # Check that the formatted message matches our expected pattern
    # The function name should be test_log_format_real_output
    pattern = rf"\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}},\d{{3}}, test_module.test_log_format_real_output\(\):{lineno}, INFO, Test message"
    assert re.match(pattern, formatted_message), (
        f"Expected format not found in: {formatted_message}"
    )


# no obvious way to do the following
# TODO: test for the following configs  use_color, log_warnings, log_exceptions, log_file_format


# Most of the logging functionality is tested in Astropy's tests for AstropyLogger


def test_swxsoc_warnings_logging():
    # Test that our logger intercepts our warnings but not Astropy warnings

    # First, disable our warnings logging
    # We need to do this manually because pytest has overwritten warnings.showwarning()
    log._showwarning_orig, previous = None, log._showwarning_orig

    # Without warnings logging
    with pytest.warns(
        SWXUserWarning, match="This warning should not be captured"
    ) as warn_list:
        with log.log_to_list() as log_list:
            warnings.warn("This warning should not be captured", SWXUserWarning)
    assert len(log_list) == 0
    assert len(warn_list) == 1

    # With warnings logging, making sure that Astropy warnings are not intercepted
    with pytest.warns(
        AstropyUserWarning, match="This warning should not be captured"
    ) as warn_list:
        log.enable_warnings_logging()
        with log.log_to_list() as log_list:
            warnings.warn("This warning should be captured", SWXUserWarning)
            warnings.warn("This warning should not be captured", AstropyUserWarning)
        log.disable_warnings_logging()
    assert len(log_list) == 1
    assert len(warn_list) == 1
    assert log_list[0].levelname == "WARNING"
    assert log_list[0].message.startswith(
        "SWXUserWarning: This warning should be captured"
    )
    # assert log_list[0].origin == "swxsoc.util.tests.test_logger"

    # Restore the state of warnings logging prior to this test
    log._showwarning_orig = previous
