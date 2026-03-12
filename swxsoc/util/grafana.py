"""
Grafana annotation and dashboard utility functions.

This module provides functions for interacting with Grafana's annotation API,
including querying, creating, and removing annotations.
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Union

import requests
from astropy.time import Time

import swxsoc

__all__ = [
    "get_dashboard_id",
    "get_panel_id",
    "query_annotations",
    "create_annotation",
    "remove_annotation_by_id",
]


def _to_milliseconds(dt: datetime) -> int:
    """
    Converts a datetime object to milliseconds since epoch.

    Args:
        dt (datetime): Datetime object to convert.

    Returns:
        int: Milliseconds since epoch.
    """
    if isinstance(dt, Time):
        # Convert astropy Time object to a standard datetime object in UTC
        dt = dt.to_datetime(timezone=None)  # Convert to naive datetime in UTC
        return int(dt.timestamp() * 1000)

    return int(dt.timestamp() * 1000)


def get_dashboard_id(
    dashboard_name: str, mission_dashboard: Optional[str] = None
) -> Optional[int]:
    """
    Retrieves the dashboard UID by its name. Issues a warning if multiple dashboards with the same name are found.

    Args:
        dashboard_name (str): Name of the dashboard to retrieve.

    Returns:
        Optional[int]: The UID of the dashboard, or None if not found.
    """
    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        response = requests.get(
            f"{BASE_URL}/api/search", headers=HEADERS, params={"query": dashboard_name}
        )
        response.raise_for_status()
        dashboards = response.json()
    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(f"Failed to retrieve dashboards: {e}")
        return None
    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(f"Failed to retrieve panels for dashboard: {e}")
        return None

    matching_dashboards = [
        dashboard
        for dashboard in dashboards
        if "title" in dashboard and dashboard["title"] == dashboard_name
    ]

    if len(matching_dashboards) == 0:
        swxsoc.log.warning(
            f"Dashboard with title '{dashboard_name}' not found. Annotation will be created without a dashboard."
        )

    if len(matching_dashboards) > 1:
        swxsoc.log.warning(
            f"Multiple dashboards with title '{dashboard_name}' found. "
            f"Using the first matching dashboard UID ({matching_dashboards[0]['uid']}). Consider using unique dashboard titles."
        )

    return matching_dashboards[0]["uid"] if matching_dashboards else None


def get_panel_id(
    dashboard_id: int, panel_name: str, mission_dashboard: Optional[str] = None
) -> Optional[int]:
    """
    Retrieves the panel ID by dashboard UID and panel name. Issues a warning if multiple panels with the same name are found.

    Args:
        dashboard_id (int): UID of the dashboard.
        panel_name (str): Name of the panel to retrieve.

    Returns:
        Optional[int]: The ID of the panel, or None if not found.
    """
    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        response = requests.get(
            f"{BASE_URL}/api/dashboards/uid/{dashboard_id}", headers=HEADERS
        )
        response.raise_for_status()
        panels = response.json().get("dashboard", {}).get("panels", [])

    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(
            f"Failed to retrieve panels for dashboard ID {dashboard_id}: {e}"
        )
        return None

    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(
            f"Failed to retrieve panels for dashboard ID {dashboard_id}: {e}"
        )
        return None

    matching_panels = [panel for panel in panels if panel["title"] == panel_name]

    if len(matching_panels) == 0:
        swxsoc.log.warning(
            f"Panel with title '{panel_name}' not found in dashboard ID {dashboard_id}. Annotation will be created without a panel."
        )

    if len(matching_panels) > 1:
        swxsoc.log.warning(
            f"Multiple panels with title '{panel_name}' found in dashboard ID {dashboard_id}. "
            f"Using the first matching panel ID ({matching_panels[0]['id']}). Consider using unique panel titles."
        )

    return matching_panels[0]["id"] if matching_panels else None


def query_annotations(
    start_time: datetime,
    end_time: Optional[datetime] = None,
    tags: Optional[List[str]] = None,
    limit: Optional[int] = 100,
    dashboard_id: Optional[int] = None,
    panel_id: Optional[int] = None,
    dashboard_name: Optional[str] = None,
    panel_name: Optional[str] = None,
    mission_dashboard: Optional[str] = None,
) -> List[Dict[str, Union[str, int]]]:
    """
    Queries annotations within a specific timeframe with optional filters for tags, dashboard, and panel names.

    Args:
        start_time (datetime): Start time of the query in UTC.
        end_time (Optional[datetime]): End time of the query; defaults to start_time if None.
        tags (Optional[List[str]]): List of tags to filter the annotations.
        limit (Optional[int]): Maximum number of annotations to retrieve.
        dashboard_id (Optional[int]): UID of the dashboard to filter annotations.
        panel_id (Optional[int]): ID of the panel to filter annotations.
        dashboard_name (Optional[str]): Name of the dashboard to look up UID if `dashboard_id` is not provided.
        panel_name (Optional[str]): Name of the panel to look up ID if `panel_id` is not provided.

    Returns:
        List[Dict[str, Union[str, int]]]: List of annotations matching the query criteria.
    """
    # Look up dashboard and panel IDs if names are provided
    if dashboard_name and not dashboard_id:
        dashboard_id = get_dashboard_id(dashboard_name, mission_dashboard)
    if dashboard_id and panel_name and not panel_id:
        panel_id = get_panel_id(dashboard_id, panel_name, mission_dashboard)

    if not end_time:
        end_time = start_time

    params = {
        "from": _to_milliseconds(start_time),
        "to": _to_milliseconds(end_time),
        "limit": limit,
    }
    if tags:
        params["tags"] = tags
    if dashboard_id:
        params["dashboardUID"] = dashboard_id
    if panel_id:
        params["panelId"] = panel_id

    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        response = requests.get(
            f"{BASE_URL}/api/annotations", headers=HEADERS, params=params
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(f"Failed to query annotations: {e}")
        return []
    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(
            f"Failed to retrieve panels for dashboard ID {dashboard_id}: {e}"
        )
        return []


def create_annotation(
    start_time: datetime,
    text: str,
    tags: List[str],
    end_time: Optional[datetime] = None,
    dashboard_id: Optional[int] = None,
    panel_id: Optional[int] = None,
    dashboard_name: Optional[str] = None,
    panel_name: Optional[str] = None,
    mission_dashboard: Optional[str] = None,
    overwrite: bool = False,
) -> Dict[str, Union[str, int]]:
    """
    Creates a new annotation for a specified event or time period, with optional filtering by dashboard and panel names.

    Args:
        start_time (datetime): Start time of the annotation in UTC.
        text (str): Annotation text to display.
        tags (List[str]): List of tags for categorizing the annotation.
        end_time (Optional[datetime]): End time of the annotation, if applicable.
        dashboard_id (Optional[int]): UID of the dashboard to associate the annotation.
        panel_id (Optional[int]): ID of the panel to associate the annotation.
        dashboard_name (Optional[str]): Name of the dashboard to look up UID if `dashboard_id` is not provided.
        panel_name (Optional[str]): Name of the panel to look up ID if `panel_id` is not provided.

    Returns:
        Dict[str, Union[str, int]]: The created annotation data.
    """
    # Look up dashboard and panel IDs if names are provided
    if dashboard_name and not dashboard_id:
        dashboard_id = get_dashboard_id(dashboard_name, mission_dashboard)
    if dashboard_id and panel_name and not panel_id:
        panel_id = get_panel_id(dashboard_id, panel_name, mission_dashboard)

    # Overwrite functionality: query and remove existing identical annotations
    if overwrite:
        swxsoc.log.info("Overwriting existing annotations.")
        existing_annotations = query_annotations(
            start_time=start_time,
            end_time=end_time or start_time,
            tags=tags,
            dashboard_id=dashboard_id,
            panel_id=panel_id,
            mission_dashboard=mission_dashboard,
        )

        for annotation in existing_annotations:
            if annotation.get("text") == text:
                annotation_id = annotation.get("id")
                if annotation_id:
                    removed = remove_annotation_by_id(annotation_id, mission_dashboard)
                    if removed:
                        swxsoc.log.info(
                            f"Removed existing annotation with ID {annotation_id}."
                        )
    payload = {
        "time": _to_milliseconds(start_time),
        "text": text,
        "tags": tags,
    }
    if end_time:
        payload["timeEnd"] = _to_milliseconds(end_time)
    if dashboard_id:
        payload["dashboardUID"] = dashboard_id
    if panel_id:
        payload["panelId"] = panel_id

    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        response = requests.post(
            f"{BASE_URL}/api/annotations", headers=HEADERS, json=payload
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(f"Failed to create annotation: {e}")
        return {}
    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(
            f"Failed to retrieve panels for dashboard ID {dashboard_id}: {e}"
        )
        return {}


def remove_annotation_by_id(
    annotation_id: int, mission_dashboard: Optional[str] = None
) -> bool:
    """
    Deletes an annotation by its ID.

    Args:
        annotation_id (int): The ID of the annotation to delete.

    Returns:
        bool: True if the annotation was successfully deleted, False otherwise.
    """
    try:
        # Set the base URL and API key for Grafana Annotations API
        # You need to set the GRAFANA_API_KEY environment variables to use this feature
        API_KEY = os.environ.get("GRAFANA_API_KEY", None)
        HEADERS = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        BASE_URL = (
            f"https://grafana.{swxsoc.config['mission']['mission_name']}.swsoc.smce.nasa.gov"
            if not mission_dashboard
            else f"https://grafana.{mission_dashboard}.swsoc.smce.nasa.gov"
        )
        full_url = f"{BASE_URL}/api/annotations/{annotation_id}"
        response = requests.delete(full_url, headers=HEADERS)
        response.raise_for_status()
        return (
            response.status_code == 200
        )  # Returns True if annotation was deleted successfully (204 No Content)
    except requests.exceptions.HTTPError as e:
        swxsoc.log.error(
            f"Failed to remove annotation with ID {annotation_id}: {e} [swxsoc.util.util]"
        )
        return False
    except requests.exceptions.ConnectionError as e:
        swxsoc.log.error(f"Failed to connect to the server: {e}")
        return False
