import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from swxsoc.util import util
import requests


@pytest.fixture
def mock_requests():
    """Fixture to mock requests methods."""
    with (
        patch("requests.get") as mock_get,
        patch("requests.post") as mock_post,
        patch("requests.delete") as mock_delete,
    ):
        yield mock_get, mock_post, mock_delete


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
    result = util.query_annotations(
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
    mock_get.side_effect = requests.exceptions.HTTPError("HTTP Error occurred")

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    result = util.query_annotations(
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
        "Connection Error occurred"
    )

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    result = util.query_annotations(
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
    result = util.create_annotation(
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
    mock_post.side_effect = requests.exceptions.HTTPError("HTTP Error occurred")

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    result = util.create_annotation(
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
        "Connection Error occurred"
    )

    # Call function
    start_time = datetime(2024, 9, 16, 13, 30, 0)
    result = util.create_annotation(
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
    result = util.remove_annotation_by_id(annotation_id=123)

    # Assertions
    assert result is True
    mock_delete.assert_called_once()


def test_remove_annotation_by_id_http_error(mock_requests):
    _, _, mock_delete = mock_requests

    # Simulate HTTPError
    mock_delete.side_effect = requests.exceptions.HTTPError("HTTP Error occurred")

    # Call function
    result = util.remove_annotation_by_id(annotation_id=123)

    # Assertions
    assert result is False
    mock_delete.assert_called_once()


def test_remove_annotation_by_id_connection_error(mock_requests):
    _, _, mock_delete = mock_requests

    # Simulate ConnectionError
    mock_delete.side_effect = requests.exceptions.ConnectionError(
        "Connection Error occurred"
    )

    # Call function
    result = util.remove_annotation_by_id(annotation_id=123)

    # Assertions
    assert result is False
    mock_delete.assert_called_once()
