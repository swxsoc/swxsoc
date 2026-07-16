"""
Tests for the Slack communication helpers in :mod:`swxsoc.comm.slack`.

These tests exercise the Slack client, message formatting, and pipeline-notification helpers.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from swxsoc import _data_directory
from swxsoc.comm.slack import (
    generate_file_pipeline_message,
    get_message_ts,
    get_slack_client,
    have_same_keys_and_values,
    is_file_manifest,
    parse_slack_message,
    send_pipeline_notification,
    send_slack_notification,
)


def test_get_slack_client_success():
    # Set the SLACK_TOKEN environment variable
    os.environ["SLACK_TOKEN"] = "test-token"

    # Call the function
    result = get_slack_client(None)

    # Check if the result is a WebClient
    assert isinstance(result, WebClient)

    # Set the SLACK_TOKEN environment variable
    test_token = "test-token"

    # Call the function
    result = get_slack_client(slack_token=test_token)

    # Check if the result is a WebClient
    assert isinstance(result, WebClient)


def test_get_slack_client_failure():
    # Remove the SLACK_TOKEN environment variable
    os.environ.pop("SLACK_TOKEN", None)

    # Call the function
    result = get_slack_client(None)

    # Check if the result is None
    assert result is None


@patch("slack_sdk.WebClient.chat_postMessage")
def test_send_slack_notification_success(mock_chat_postMessage):
    # Create a MagicMock for the WebClient
    slack_client = MagicMock(spec=WebClient)

    # Call the function
    response = send_slack_notification(
        slack_client, "test-channel", "Test message", "success"
    )

    # Check if chat_postMessage was called
    slack_client.chat_postMessage.assert_called()

    # Check if the response is not None
    assert response is not None

    # Set chat_postMessage to raise a SlackApiError
    slack_client.chat_postMessage.side_effect = SlackApiError(
        "Error", {"Error": {"Code": "404"}}
    )

    # Call the function
    try:
        response = send_slack_notification(
            slack_client, "test-channel", "Test message", "success", 1, 1
        )

    except SlackApiError as e:
        assert e is not None


@patch("slack_sdk.WebClient.chat_postMessage")
def test_send_slack_notification_failure(mock_chat_postMessage):
    # Create a MagicMock for the WebClient
    slack_client = MagicMock(spec=WebClient)

    # Set chat_postMessage to raise a SlackApiError
    slack_client.chat_postMessage.side_effect = SlackApiError(
        "Error", {"Error": {"Code": "404"}}
    )

    try:
        # Call the function and expect an error log
        send_slack_notification(
            slack_client, "test-channel", "Test message", "error", 2, 2
        )

    except SlackApiError:
        # Check if chat_postMessage was called
        slack_client.chat_postMessage.assert_called()


# Try to running slack notification not in mock mode to see if Token Exception is raised
def test_send_slack_notification_failure_no_token():
    try:
        slack_client = WebClient(token=None)
        # Call the function and expect an error log
        send_slack_notification(
            slack_client, "test-channel", "Test message", "error", 1, 1
        )

    except SlackApiError as e:
        assert e is not None


# Test is_file_manifest function
def test_is_file_manifest_true():
    test_path = Path(_data_directory) / "sample" / "file_manifest_test.txt"
    assert is_file_manifest(str(test_path)) is True


def test_is_file_manifest_false():
    test_path = Path(_data_directory) / "sample" / "test_file_manifest.txt"
    assert is_file_manifest(str(test_path)) is False


# Test generate_file_pipeline_message function
def test_generate_file_pipeline_message_default():
    assert (
        generate_file_pipeline_message("path/to/file.txt")
        == "Science File - ( _file.txt_ )"
    )


def test_generate_file_pipeline_message_alert_type_sorted():
    assert (
        generate_file_pipeline_message("path/to/file.txt", alert_type="sorted")
        == "File Sorted - ( _file.txt_ )"
    )


def test_generate_file_pipeline_message_unknown_alert_type():
    assert (
        generate_file_pipeline_message(
            "path/to/file.txt", alert_type="non-existing key"
        )
        == "Science File - ( _file.txt_ )"
    )


def test_generate_file_pipeline_message_alert_type_delete():
    assert (
        generate_file_pipeline_message("path/to/file.txt", alert_type="delete") is None
    )


@patch("swxsoc.comm.slack.is_file_manifest", return_value=True)
def test_generate_file_pipeline_message_manifest(
    mocked_manifest, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    with open("test_file_manifest.txt", "w") as f:
        f.write("Manifest content")
    result = generate_file_pipeline_message("test_file_manifest.txt")
    assert result[0] == "Manifest File - ( _test_file_manifest.txt_ )"
    assert result[1] == "Manifest content"


def test_generate_file_pipeline_message_sorted():
    assert (
        generate_file_pipeline_message("path/to/file.txt", alert_type="sorted")
        == "File Sorted - ( _file.txt_ )"
    )


# Test have_same_keys_and_values function
def test_have_same_keys_and_values_true():
    dicts = [{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "d": 4}]
    keys = ["a", "b"]
    assert have_same_keys_and_values(dicts, keys) is True


def test_have_same_keys_and_values_false():
    dicts = [{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 3, "d": 4}]
    keys = ["a", "b"]
    assert have_same_keys_and_values(dicts, keys) is False


# 4. Testing parse_slack_message
def test_parse_slack_message_valid():
    msg = "Science File - ( _path/to/file.txt_ )"
    assert parse_slack_message(msg) == "path/to/file.txt"


def test_parse_slack_message_invalid():
    msg = "Invalid message format"
    assert parse_slack_message(msg) is None


# Mocked tests
@patch("slack_sdk.WebClient")
@patch("swxsoc.comm.slack.log")
def test_send_slack_notification_success_mocked(mocked_log, mocked_client):
    # Mocking chat_postMessage to return without error
    mocked_client.chat_postMessage = MagicMock()
    success = send_slack_notification(
        mocked_client, "#channel", "Test Message", "upload"
    )
    assert success is True


@patch("slack_sdk.WebClient")
@patch("swxsoc.comm.slack.log")
def test_send_slack_notification_failure_mocked(mocked_log, mocked_client):
    # Mocking chat_postMessage to raise an error
    mocked_client.chat_postMessage.side_effect = Exception("SlackApiError")
    with pytest.raises(Exception):
        send_slack_notification(mocked_client, "#channel", "Test Message", "upload")

        # Test for parse_slack_message


def test_parse_slack_message():
    assert parse_slack_message("Hello, World!") is None
    assert parse_slack_message("Science File - ( _test.txt_ )") == "test.txt"
    assert parse_slack_message("Science File - ( test.txt )") == "test.txt"
    assert (
        parse_slack_message("Science File - ( _test with spaces.txt_ )")
        == "test with spaces.txt"
    )


@pytest.fixture
def mock_slack_client():
    client = Mock(spec=WebClient)
    client.conversations_history = Mock()
    return client


def test_get_message_ts(mock_slack_client):
    mock_slack_client.conversations_history.return_value = {
        "messages": [
            {"text": "Some random text"},
            {
                "text": "Science File - ( _hermes_eea_ql_20230205T000006_v1.0.01.cdf_ )",
                "ts": "12345",
            },
        ]
    }

    ts = get_message_ts(
        mock_slack_client, "#general", "hermes_eea_ql_20230205T000006_v1.0.01.cdf"
    )
    assert ts == "12345"


@patch("swxsoc.comm.slack.generate_file_pipeline_message")
@patch("swxsoc.comm.slack.send_slack_notification")
@patch("swxsoc.comm.slack.is_file_manifest")
@patch("swxsoc.comm.slack.get_message_ts")
def test_send_pipeline_notification(
    mock_get_message_ts,
    mock_is_file_manifest,
    mock_send_slack_notification,
    mock_generate_file_pipeline_message,
    mock_slack_client,
):
    mock_is_file_manifest.return_value = False
    mock_generate_file_pipeline_message.return_value = "Test message"
    mock_get_message_ts.return_value = None

    send_pipeline_notification(mock_slack_client, "#general", "test.txt")

    mock_send_slack_notification.assert_called()
