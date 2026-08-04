from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests
from astropy.time import Time

from swxsoc.util import grafana


def test_to_milliseconds_with_datetime():
    dt = datetime(2024, 9, 16, 13, 30, 0)
    result = grafana._to_milliseconds(dt)

    assert result == int(dt.timestamp() * 1000)


def test_to_milliseconds_with_astropy_time():
    dt = datetime(2024, 9, 16, 13, 30, 0, tzinfo=timezone.utc)
    time_obj = Time(dt)

    result = grafana._to_milliseconds(time_obj)

    assert result == int(dt.replace(tzinfo=None).timestamp() * 1000)


@pytest.fixture
def mock_requests():
    """Fixture to mock requests methods."""
    with (
        patch("requests.get") as mock_get,
        patch("requests.post") as mock_post,
        patch("requests.delete") as mock_delete,
    ):
        yield mock_get, mock_post, mock_delete


def test_get_dashboard_id_found(mock_requests):
    mock_get, _, _ = mock_requests

    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"title": "Solar flare", "uid": "fe0cbqalk99fkd"},
    ]
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    result = grafana.get_dashboard_id(
        dashboard_name="Solar flare", mission_dashboard="meddea"
    )

    assert result == "fe0cbqalk99fkd"
    mock_get.assert_called_once()


def test_get_dashboard_id_multiple_matches(mock_requests):
    mock_get, _, _ = mock_requests

    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"title": "Solar flare", "uid": "fe0cbqalk99fkd"},
        {"title": "Solar flare", "uid": "another-uid"},
    ]
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    result = grafana.get_dashboard_id(
        dashboard_name="Solar flare", mission_dashboard="meddea"
    )

    # Should use the first matching dashboard's uid
    assert result == "fe0cbqalk99fkd"
    mock_get.assert_called_once()


def test_get_dashboard_id_not_found(mock_requests):
    mock_get, _, _ = mock_requests

    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"title": "Unrelated dashboard", "uid": "some-uid"},
    ]
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    result = grafana.get_dashboard_id(
        dashboard_name="Solar flare", mission_dashboard="meddea"
    )

    assert result is None
    mock_get.assert_called_once()


def test_get_dashboard_id_http_error(mock_requests):
    mock_get, _, _ = mock_requests

    mock_get.side_effect = requests.exceptions.HTTPError("HTTP Error occurred")

    result = grafana.get_dashboard_id(
        dashboard_name="Solar flare", mission_dashboard="meddea"
    )

    assert result is None
    mock_get.assert_called_once()


def test_get_dashboard_id_connection_error(mock_requests):
    mock_get, _, _ = mock_requests

    mock_get.side_effect = requests.exceptions.ConnectionError(
        "Connection Error occurred"
    )

    result = grafana.get_dashboard_id(
        dashboard_name="Solar flare", mission_dashboard="meddea"
    )

    assert result is None
    mock_get.assert_called_once()


def test_get_panel_id_found(mock_requests):
    mock_get, _, _ = mock_requests

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "dashboard": {
            "panels": [
                {"title": "Test Panel", "id": 8},
            ]
        }
    }
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    result = grafana.get_panel_id(
        dashboard_id="fe0cbqalk99fkd",
        panel_name="Test Panel",
        mission_dashboard="meddea",
    )

    assert result == 8
    mock_get.assert_called_once()


def test_get_panel_id_multiple_matches(mock_requests):
    mock_get, _, _ = mock_requests

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "dashboard": {
            "panels": [
                {"title": "Test Panel", "id": 8},
                {"title": "Test Panel", "id": 9},
            ]
        }
    }
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    result = grafana.get_panel_id(
        dashboard_id="fe0cbqalk99fkd",
        panel_name="Test Panel",
        mission_dashboard="meddea",
    )

    # Should use the first matching panel's id
    assert result == 8
    mock_get.assert_called_once()


def test_get_panel_id_not_found(mock_requests):
    mock_get, _, _ = mock_requests

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "dashboard": {
            "panels": [
                {"title": "Unrelated Panel", "id": 1},
            ]
        }
    }
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    result = grafana.get_panel_id(
        dashboard_id="fe0cbqalk99fkd",
        panel_name="Test Panel",
        mission_dashboard="meddea",
    )

    assert result is None
    mock_get.assert_called_once()


def test_get_panel_id_http_error(mock_requests):
    mock_get, _, _ = mock_requests

    mock_get.side_effect = requests.exceptions.HTTPError("HTTP Error occurred")

    result = grafana.get_panel_id(
        dashboard_id="fe0cbqalk99fkd",
        panel_name="Test Panel",
        mission_dashboard="meddea",
    )

    assert result is None
    mock_get.assert_called_once()


def test_get_panel_id_connection_error(mock_requests):
    mock_get, _, _ = mock_requests

    mock_get.side_effect = requests.exceptions.ConnectionError(
        "Connection Error occurred"
    )

    result = grafana.get_panel_id(
        dashboard_id="fe0cbqalk99fkd",
        panel_name="Test Panel",
        mission_dashboard="meddea",
    )

    assert result is None
    mock_get.assert_called_once()


def test_query_annotations(mock_requests):
    mock_get, _, _ = mock_requests

    # Define mock response
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {
            "id": 43,
            "alertId": 0,
            "alertName": "",
            "dashboardId": 7,
            "dashboardUID": "fe0cbqalk99fkd",
            "uid": "fe0cbqalk99fkd",
            "panelId": 8,
            "userId": 0,
            "newState": "",
            "prevState": "",
            "created": 1730204275308,
            "updated": 1730204275308,
            "time": 1726489800000,
            "timeEnd": 1726490100000,
            "title": "Solar flare",
            "text": "Observed solar flare",
            "tags": ["meddea", "test"],
            "login": "",
            "email": "",
            "avatarUrl": "",
            "data": {},
        }
    ]
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    end_time = datetime(2024, 9, 16, 13, 35, 0)
    result = grafana.query_annotations(
        start_time=start_time,
        end_time=end_time,
        dashboard_name="Solar flare",
        tags=["meddea", "test"],
    )

    # Assertions
    assert result == [
        {
            "id": 43,
            "alertId": 0,
            "alertName": "",
            "dashboardId": 7,
            "dashboardUID": "fe0cbqalk99fkd",
            "uid": "fe0cbqalk99fkd",
            "panelId": 8,
            "userId": 0,
            "newState": "",
            "prevState": "",
            "created": 1730204275308,
            "updated": 1730204275308,
            "time": 1726489800000,
            "timeEnd": 1726490100000,
            "title": "Solar flare",
            "text": "Observed solar flare",
            "tags": ["meddea", "test"],
            "login": "",
            "email": "",
            "avatarUrl": "",
            "data": {},
        }
    ]
    mock_get.assert_called()


def test_query_annotations_http_error(mock_requests):
    mock_get, _, _ = mock_requests

    # Simulate HTTPError
    mock_get.side_effect = requests.exceptions.HTTPError(
        "HTTP Error occurred (proper test behavior)"
    )

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    result = grafana.query_annotations(
        start_time=start_time,
        dashboard_name="Test Dashboard",
        panel_name="Test Panel",
        tags=["test"],
    )

    # Assertions
    assert result == []
    # Ensure the function is called twice since it makes a GET request twice
    mock_get.assert_called()


def test_query_annotations_connection_error(mock_requests):
    mock_get, _, _ = mock_requests

    # Simulate ConnectionError
    mock_get.side_effect = requests.exceptions.ConnectionError(
        "Connection Error occurred (proper test behavior)"
    )

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    result = grafana.query_annotations(
        start_time=start_time,
        dashboard_name="Test Dashboard",
        panel_name="Test Panel",
        tags=["test"],
    )

    # Assertions
    assert result == []
    # Ensure the function is called twice since it makes a GET request twice
    mock_get.assert_called()


def test_create_annotation(mock_requests):
    _, mock_post, _ = mock_requests

    # Define mock response
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": 123}
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    end_time = datetime(2024, 9, 16, 13, 35, 0)
    result = grafana.create_annotation(
        start_time=start_time,
        end_time=end_time,
        text="Observed solar flare",
        tags=["meddea", "test"],
        dashboard_name="Test Dashboard",
        panel_name="Test Panel",
    )

    # Assertions
    assert result == {"id": 123}
    mock_post.assert_called_once()


def test_create_annotation_http_error(mock_requests):
    _, mock_post, _ = mock_requests

    # Simulate HTTPError
    mock_post.side_effect = requests.exceptions.HTTPError(
        "HTTP Error occurred (proper test behavior)"
    )

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    result = grafana.create_annotation(
        start_time=start_time,
        text="Observed solar flare",
        tags=["meddea", "test"],
        dashboard_name="Test Dashboard",
        panel_name="Test Panel",
    )

    # Assertions
    assert result == {}
    mock_post.assert_called_once()


def test_create_annotation_connection_error(mock_requests):
    _, mock_post, _ = mock_requests

    # Simulate ConnectionError
    mock_post.side_effect = requests.exceptions.ConnectionError(
        "Connection Error occurred (proper test behavior)"
    )

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    result = grafana.create_annotation(
        start_time=start_time,
        text="Observed solar flare",
        tags=["meddea", "test"],
        dashboard_name="Test Dashboard",
        panel_name="Test Panel",
    )

    # Assertions
    assert result == {}
    mock_post.assert_called_once()


def test_remove_annotation_by_id(mock_requests):
    _, _, mock_delete = mock_requests

    # Define mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_delete.return_value = mock_response

    # Call function
    result = grafana.remove_annotation_by_id(annotation_id=123)

    # Assertions
    assert result is True
    mock_delete.assert_called_once()


def test_remove_annotation_by_id_http_error(mock_requests):
    _, _, mock_delete = mock_requests

    # Simulate HTTPError
    mock_delete.side_effect = requests.exceptions.HTTPError(
        "HTTP Error occurred (proper test behavior)"
    )

    # Call function
    result = grafana.remove_annotation_by_id(annotation_id=123)

    # Assertions
    assert result is False
    mock_delete.assert_called_once()


def test_remove_annotation_by_id_connection_error(mock_requests):
    _, _, mock_delete = mock_requests

    # Simulate ConnectionError
    mock_delete.side_effect = requests.exceptions.ConnectionError(
        "Connection Error occurred (proper test behavior)"
    )

    # Call function
    result = grafana.remove_annotation_by_id(annotation_id=123)

    # Assertions
    assert result is False
    mock_delete.assert_called_once()
