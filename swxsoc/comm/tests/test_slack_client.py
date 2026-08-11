"""
Tests for :class:`swxsoc.comm.slack.SlackClient`.
"""

from unittest.mock import Mock, patch

import pytest
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from swxsoc.comm.slack import SlackClient


@pytest.fixture
def slack_client(monkeypatch):
    monkeypatch.delenv("SDC_AWS_SLACK_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    monkeypatch.delenv("SDC_AWS_SLACK_CHANNEL", raising=False)
    client = SlackClient(slack_token="test-token", slack_channel="test-channel")
    client.client = Mock(spec=WebClient)
    return client


def test_init_uses_explicit_args(slack_client):
    assert slack_client.channel == "test-channel"


def test_init_falls_back_to_env_vars(monkeypatch):
    monkeypatch.setenv("SDC_AWS_SLACK_TOKEN", "env-token")
    monkeypatch.setenv("SDC_AWS_SLACK_CHANNEL", "env-channel")
    client = SlackClient()
    assert client.channel == "env-channel"


def test_init_missing_token_raises(monkeypatch):
    monkeypatch.delenv("SDC_AWS_SLACK_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    with pytest.raises(ValueError):
        SlackClient(slack_channel="test-channel")


def test_init_missing_channel_raises(monkeypatch):
    monkeypatch.delenv("SDC_AWS_SLACK_CHANNEL", raising=False)
    with pytest.raises(ValueError):
        SlackClient(slack_token="test-token")


def test_send_message_success(slack_client):
    result = slack_client._send_message("Test Message", alert_type="upload")
    slack_client.client.chat_postMessage.assert_called()
    assert result is True


def test_send_message_none_is_noop(slack_client):
    result = slack_client._send_message(None)
    slack_client.client.chat_postMessage.assert_not_called()
    assert result is False


def test_send_message_retries_then_raises(slack_client):
    slack_client.max_retries = 2
    slack_client.retry_delay = 0
    slack_client.client.chat_postMessage.side_effect = SlackApiError(
        "Error", {"Error": {"Code": "404"}}
    )
    with pytest.raises(SlackApiError):
        slack_client._send_message("Test Message", alert_type="error")
    assert slack_client.client.chat_postMessage.call_count == 2


def test_parse_slack_message():
    assert SlackClient._parse_slack_message("Hello, World!") is None
    assert (
        SlackClient._parse_slack_message("Science File - ( _test.txt_ )") == "test.txt"
    )
    assert SlackClient._parse_slack_message("Science File - ( test.txt )") == "test.txt"
    assert (
        SlackClient._parse_slack_message("Science File - ( _test with spaces.txt_ )")
        == "test with spaces.txt"
    )


def test_have_same_keys_and_values_true():
    dicts = [{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "d": 4}]
    assert SlackClient._have_same_keys_and_values(dicts, ["a", "b"]) is True


def test_have_same_keys_and_values_false():
    dicts = [{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 3, "d": 4}]
    assert SlackClient._have_same_keys_and_values(dicts, ["a", "b"]) is False


def test_get_message_ts_found(slack_client):
    slack_client.client.conversations_history.return_value = {
        "messages": [
            {"text": "Some random text"},
            {
                "text": "Science File - ( _hermes_eea_ql_20230205T000006_v1.0.01.cdf_ )",
                "ts": "12345",
            },
        ]
    }

    ts = slack_client._get_message_ts("hermes_eea_ql_20230205T000006_v1.0.01.cdf")
    assert ts == "12345"


def test_get_message_ts_api_error_returns_none(slack_client):
    slack_client.client.conversations_history.side_effect = SlackApiError(
        "Error", {"Error": {"Code": "404"}}
    )
    assert slack_client._get_message_ts("some_file.cdf") is None


@patch.object(SlackClient, "_send_message")
@patch.object(SlackClient, "_get_message_ts")
def test_send_notification_manifest_is_noop(mock_get_ts, mock_send, slack_client):
    slack_client.send_notification("file_manifest_test.txt")
    mock_get_ts.assert_not_called()
    mock_send.assert_not_called()


@patch.object(SlackClient, "_send_message")
@patch.object(SlackClient, "_get_message_ts")
def test_send_notification_creates_thread_when_missing(
    mock_get_ts, mock_send, slack_client
):
    mock_get_ts.side_effect = [None, "12345"]

    slack_client.send_notification("test.txt", alert_type="upload")

    assert mock_get_ts.call_count == 2
    assert mock_send.call_count == 2
    _, kwargs = mock_send.call_args
    assert kwargs["thread_ts"] == "12345"


@patch.object(SlackClient, "_send_message")
@patch.object(SlackClient, "_get_message_ts")
def test_send_notification_swallows_exceptions(mock_get_ts, mock_send, slack_client):
    mock_get_ts.side_effect = Exception("boom")
    # Should not raise.
    slack_client.send_notification("test.txt", alert_type="upload")


def test_close_does_not_raise(slack_client):
    slack_client.close()
