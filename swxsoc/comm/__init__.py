"""
The `swxsoc.comm` subpackage contains communication and notification
integrations (e.g. Slack, Mattermost) used by the SWxSOC pipeline.
"""

from swxsoc.comm.client import ALERT_TYPES, CommsClient
from swxsoc.comm.factory import get_comms_client
from swxsoc.comm.mattermost import MattermostClient
from swxsoc.comm.multi_client import MultiCommsClient
from swxsoc.comm.slack import SlackClient

__all__ = [
    "ALERT_TYPES",
    "CommsClient",
    "SlackClient",
    "MattermostClient",
    "MultiCommsClient",
    "get_comms_client",
]
