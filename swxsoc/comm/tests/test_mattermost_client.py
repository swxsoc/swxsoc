"""
Tests for :class:`swxsoc.comm.mattermost.MattermostClient`.
"""

from unittest.mock import Mock, patch

import pytest
from mattermostautodriver.exceptions import MattermostError

from swxsoc.comm.mattermost import MattermostClient, _parse_mattermost_url


@pytest.fixture
def mock_driver_cls(monkeypatch):
    driver_instance = Mock()
    driver_cls = Mock(return_value=driver_instance)
    monkeypatch.setattr("swxsoc.comm.mattermost.TypedDriver", driver_cls)
    return driver_cls


@pytest.fixture
def mattermost_client(mock_driver_cls, monkeypatch):
    monkeypatch.delenv("MATTERMOST_URL", raising=False)
    monkeypatch.delenv("MATTERMOST_TOKEN", raising=False)
    monkeypatch.delenv("MATTERMOST_CHANNEL_ID", raising=False)
    client = MattermostClient(
        url="localhost:8065", token="test-token", channel_id="test-channel-id"
    )
    return client


def test_parse_mattermost_url_host_port():
    host, port, scheme = _parse_mattermost_url("localhost:8065")
    assert host == "localhost"
    assert port == 8065
    assert scheme == "https"


def test_parse_mattermost_url_full_url():
    host, port, scheme = _parse_mattermost_url("http://mattermost.example.org:8065")
    assert host == "mattermost.example.org"
    assert port == 8065
    assert scheme == "http"


def test_parse_mattermost_url_host_only():
    host, port, scheme = _parse_mattermost_url("mattermost.example.org")
    assert host == "mattermost.example.org"
    assert port is None
    assert scheme == "https"


def test_init_logs_in(mattermost_client):
    mattermost_client.driver.login.assert_called_once()


def test_init_falls_back_to_env_vars(mock_driver_cls, monkeypatch):
    monkeypatch.setenv("MATTERMOST_URL", "localhost:8065")
    monkeypatch.setenv("MATTERMOST_TOKEN", "env-token")
    monkeypatch.setenv("MATTERMOST_CHANNEL_ID", "env-channel-id")
    client = MattermostClient()
    assert client.channel_id == "env-channel-id"


def test_init_missing_url_raises(mock_driver_cls, monkeypatch):
    monkeypatch.delenv("MATTERMOST_URL", raising=False)
    with pytest.raises(ValueError):
        MattermostClient(token="test-token", channel_id="test-channel-id")


def test_init_missing_token_raises(mock_driver_cls, monkeypatch):
    monkeypatch.delenv("MATTERMOST_TOKEN", raising=False)
    with pytest.raises(ValueError):
        MattermostClient(url="localhost:8065", channel_id="test-channel-id")


def test_init_missing_channel_id_raises(mock_driver_cls, monkeypatch):
    monkeypatch.delenv("MATTERMOST_CHANNEL_ID", raising=False)
    with pytest.raises(ValueError):
        MattermostClient(url="localhost:8065", token="test-token")


def test_send_message_success(mattermost_client):
    result = mattermost_client._send_message("Test Message")
    mattermost_client.driver.posts.create_post.assert_called_with(
        channel_id="test-channel-id", message="Test Message"
    )
    assert result is True


def test_send_message_retries_then_raises(mattermost_client):
    mattermost_client.max_retries = 2
    mattermost_client.retry_delay = 0
    mattermost_client.driver.posts.create_post.side_effect = MattermostError(
        "boom",
        status_code=500,
        error_id="internal_error",
        request_id="req-1",
        is_oauth_error=False,
    )
    with pytest.raises(MattermostError):
        mattermost_client._send_message("Test Message")
    assert mattermost_client.driver.posts.create_post.call_count == 2


@patch.object(MattermostClient, "_send_message")
def test_send_notification_delete_is_noop(mock_send, mattermost_client):
    mattermost_client.send_notification("test.txt", alert_type="delete")
    mock_send.assert_not_called()


@patch.object(MattermostClient, "_send_message")
def test_send_notification_sends_flat_message(mock_send, mattermost_client):
    mattermost_client.send_notification("test.txt", alert_type="upload")
    mock_send.assert_called_once()
    (text,), _ = mock_send.call_args
    assert "File Uploaded to S3" in text


@patch.object(MattermostClient, "_send_message")
def test_send_notification_swallows_exceptions(mock_send, mattermost_client):
    mock_send.side_effect = Exception("boom")
    # Should not raise.
    mattermost_client.send_notification("test.txt", alert_type="upload")


def test_close_calls_driver_close(mattermost_client):
    mattermost_client.close()
    mattermost_client.driver.close.assert_called_once()


def test_close_swallows_exceptions(mattermost_client):
    mattermost_client.driver.close.side_effect = Exception("boom")
    # Should not raise.
    mattermost_client.close()
