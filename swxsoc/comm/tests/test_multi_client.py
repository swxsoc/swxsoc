"""
Tests for :class:`swxsoc.comm.multi_client.MultiCommsClient`.
"""

from unittest.mock import Mock

import pytest

from swxsoc.comm.client import CommsClient
from swxsoc.comm.multi_client import MultiCommsClient


def _mock_client() -> Mock:
    return Mock(spec=CommsClient)


def test_requires_at_least_one_client():
    with pytest.raises(ValueError):
        MultiCommsClient([])


def test_send_notification_broadcasts_to_all_clients():
    client_a, client_b = _mock_client(), _mock_client()
    multi = MultiCommsClient([client_a, client_b])

    multi.send_notification("test.txt", alert_type="upload", bucket_name="bucket")

    client_a.send_notification.assert_called_once_with(
        "test.txt", alert_type="upload", bucket_name="bucket"
    )
    client_b.send_notification.assert_called_once_with(
        "test.txt", alert_type="upload", bucket_name="bucket"
    )


def test_send_notification_continues_after_one_client_fails():
    client_a, client_b = _mock_client(), _mock_client()
    client_a.send_notification.side_effect = Exception("boom")
    multi = MultiCommsClient([client_a, client_b])

    # Should not raise even though client_a fails.
    multi.send_notification("test.txt", alert_type="upload")

    client_b.send_notification.assert_called_once()


def test_close_closes_all_clients():
    client_a, client_b = _mock_client(), _mock_client()
    multi = MultiCommsClient([client_a, client_b])

    multi.close()

    client_a.close.assert_called_once()
    client_b.close.assert_called_once()


def test_close_continues_after_one_client_fails():
    client_a, client_b = _mock_client(), _mock_client()
    client_a.close.side_effect = Exception("boom")
    multi = MultiCommsClient([client_a, client_b])

    # Should not raise even though client_a fails to close.
    multi.close()

    client_b.close.assert_called_once()
