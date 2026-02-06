"""
Tests for the config module
"""

import os


import swxsoc
from swxsoc.util.config import (
    _is_writable_dir,
    load_config,
)

USER = os.path.expanduser("~")


def test_load_config_defaults(monkeypatch):
    """
    Test loading configuration with defaults (no env var override).
    """
    # Ensure no environment override affects this test
    monkeypatch.delenv("SWXSOC_MISSION", raising=False)
    monkeypatch.delenv("LAMBDA_ENVIRONMENT", raising=False)

    config = load_config()

    # Check top level keys
    assert "general" in config
    assert "selected_mission" in config
    assert "missions_data" in config
    assert "logger" in config

    # Check processed mission data structure
    assert "mission" in config
    mission_config = config["mission"]

    # Check derived fields exist
    assert "mission_name" in mission_config
    assert "file_extension" in mission_config
    assert "valid_data_levels" in mission_config
    # Check instrument lists
    assert "inst_names" in mission_config
    assert "inst_shortnames" in mission_config
    assert "inst_fullnames" in mission_config
    assert "inst_targetnames" in mission_config
    assert "extra_inst_names" in mission_config
    # Check file rules
    assert "inst_file_rules" in mission_config
    # Check mapping dictionaries
    assert "inst_to_shortname" in mission_config
    assert "inst_to_fullname" in mission_config
    assert "inst_to_targetname" in mission_config
    assert "inst_to_extra_inst_names" in mission_config

    # By default, it should pick up the selected_mission from yaml (likely 'swxsoc')
    default_mission = config["selected_mission"]
    assert mission_config["mission_name"] == default_mission
    # Ensure we have instruments loaded for the default mission
    assert len(mission_config["inst_names"]) > 0


def test_load_config_mission_override(monkeypatch):
    """
    Test overriding the selected mission via environment variable.
    """
    # Assuming 'hermes' is defined in the default config.yml
    target_mission = "hermes"
    monkeypatch.setenv("SWXSOC_MISSION", target_mission)

    config = load_config()

    mission_config = config["mission"]
    assert mission_config["mission_name"] == target_mission
    # Check that it actually loaded hermes-specific data (e.g. instruments)
    assert "eea" in mission_config["inst_names"]
    # Check that file_rules exists
    assert "inst_file_rules" in mission_config
    assert isinstance(mission_config["inst_file_rules"], dict)


def test_load_config_lambda_env(monkeypatch):
    """
    Test that LAMBDA_ENVIRONMENT variable disables log_to_file.
    """
    monkeypatch.setenv("LAMBDA_ENVIRONMENT", "true")

    config = load_config()

    # The code sets config["logger"]["log_to_file"] = False if env var is set
    assert config["logger"]["log_to_file"] is False


def test_load_config_unknown_mission(monkeypatch):
    """
    Test behavior when an unknown mission is selected.
    It should produce empty lists/dicts but not crash.
    """
    monkeypatch.setenv("SWXSOC_MISSION", "non_existent_mission")

    config = load_config()

    mission_config = config["mission"]
    assert mission_config["mission_name"] == "non_existent_mission"

    # Should have empty lists for instruments
    assert mission_config["inst_names"] == []
    assert mission_config["inst_shortnames"] == []
    assert mission_config["inst_fullnames"] == []
    assert mission_config["inst_targetnames"] == []
    assert mission_config["extra_inst_names"] == []
    # Should have empty dicts for mappings and file rules
    assert mission_config["inst_file_rules"] == {}
    assert mission_config["inst_to_shortname"] == {}
    assert mission_config["inst_to_fullname"] == {}
    assert mission_config["inst_to_targetname"] == {}
    assert mission_config["inst_to_extra_inst_names"] == {}


def test_is_writable_dir(tmpdir, tmp_path):
    """
    Test the _is_writable_dir function.
    """
    assert _is_writable_dir(tmpdir)
    tmp_file = tmpdir.join("hello.txt")
    # Have to write to the file otherwise its seen as a directory(?!)
    tmp_file.write("content")
    # Checks directory with a file
    assert _is_writable_dir(tmpdir)
    # Checks a filepath instead of directory
    assert not _is_writable_dir(tmp_file)


def test_print_config(capsys):
    """
    Test the print_config function.
    """
    # Run the functio to print the config
    swxsoc.print_config(swxsoc.config)
    # Capture the output
    captured = capsys.readouterr()
    assert isinstance(captured.out, str)
    # assert general section
    assert "[general]" in captured.out
    # assert mission data
    assert "[missions_data]" in captured.out
    # assert logger
    assert "[logger]" in captured.out
    # assert mission
    assert "[mission]" in captured.out
