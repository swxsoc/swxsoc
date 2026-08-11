"""
Tests for :func:`swxsoc.comm.factory.get_comms_client`.
"""

from unittest.mock import Mock, patch

import pytest

from swxsoc.comm.factory import get_comms_client
from swxsoc.comm.mattermost import MattermostClient
from swxsoc.comm.multi_client import MultiCommsClient
from swxsoc.comm.slack import SlackClient


@pytest.fixture(autouse=True)
def clear_comms_platform(monkeypatch):
    monkeypatch.delenv("COMMS_PLATFORM", raising=False)


def test_missing_comms_platform_raises():
    with pytest.raises(ValueError):
        get_comms_client()


def test_empty_comms_platform_raises(monkeypatch):
    monkeypatch.setenv("COMMS_PLATFORM", "")
    with pytest.raises(ValueError):
        get_comms_client()


def test_unrecognized_platform_raises(monkeypatch):
    monkeypatch.setenv("COMMS_PLATFORM", "teams")
    with pytest.raises(ValueError):
        get_comms_client()


@patch("swxsoc.comm.factory.SlackClient")
def test_single_platform_returns_that_client(mock_slack_cls, monkeypatch):
    monkeypatch.setenv("COMMS_PLATFORM", "slack")
    mock_instance = Mock(spec=SlackClient)
    mock_slack_cls.return_value = mock_instance

    client = get_comms_client()

    assert client is mock_instance
    mock_slack_cls.assert_called_once()


@patch("swxsoc.comm.factory.MattermostClient")
@patch("swxsoc.comm.factory.SlackClient")
def test_multiple_platforms_returns_multi_comms_client(
    mock_slack_cls, mock_mattermost_cls, monkeypatch
):
    monkeypatch.setenv("COMMS_PLATFORM", "slack,mattermost")
    mock_slack_cls.return_value = Mock(spec=SlackClient)
    mock_mattermost_cls.return_value = Mock(spec=MattermostClient)

    client = get_comms_client()

    assert isinstance(client, MultiCommsClient)
    assert len(client.clients) == 2


@patch("swxsoc.comm.factory.MattermostClient")
@patch("swxsoc.comm.factory.SlackClient")
def test_partial_failure_returns_multi_comms_client_with_survivors(
    mock_slack_cls, mock_mattermost_cls, monkeypatch
):
    monkeypatch.setenv("COMMS_PLATFORM", "slack,mattermost")
    mock_slack_cls.side_effect = ValueError("slack token missing")
    mock_mattermost_cls.return_value = Mock(spec=MattermostClient)

    client = get_comms_client()

    assert isinstance(client, MultiCommsClient)
    assert len(client.clients) == 1


@patch("swxsoc.comm.factory.MattermostClient")
@patch("swxsoc.comm.factory.SlackClient")
def test_all_platforms_failing_raises(mock_slack_cls, mock_mattermost_cls, monkeypatch):
    monkeypatch.setenv("COMMS_PLATFORM", "slack,mattermost")
    mock_slack_cls.side_effect = ValueError("slack token missing")
    mock_mattermost_cls.side_effect = ValueError("mattermost token missing")

    with pytest.raises(RuntimeError):
        get_comms_client()


@patch("swxsoc.comm.factory.SlackClient")
def test_comms_platform_is_case_insensitive_and_trims_whitespace(
    mock_slack_cls, monkeypatch
):
    monkeypatch.setenv("COMMS_PLATFORM", " Slack ")
    mock_slack_cls.return_value = Mock(spec=SlackClient)

    client = get_comms_client()

    mock_slack_cls.assert_called_once()
    assert client is mock_slack_cls.return_value
