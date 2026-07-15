"""
Slack notification helpers for the SWxSOC data pipeline.

These helpers are used by the SDC AWS Lambda pipelines to post science-file
pipeline notifications (uploads, sorting, processing) to Slack, threading
related messages together.
"""

import os
import re
import time
from datetime import datetime

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from swxsoc import log
from swxsoc.util.util import parse_science_filename

__all__ = [
    "get_slack_client",
    "is_file_manifest",
    "generate_file_pipeline_message",
    "send_slack_notification",
    "have_same_keys_and_values",
    "get_message_ts",
    "parse_slack_message",
    "send_pipeline_notification",
]


def get_slack_client(slack_token: str) -> WebClient:
    """
    Initialize a Slack client using the provided token.

    Parameters
    ----------
    slack_token : str
        The Slack API token. If falsy, falls back to the ``SLACK_TOKEN``
        environment variable.

    Returns
    -------
    WebClient or None
        The initialized Slack client, or ``None`` if no token is available.
    """
    if not slack_token:
        slack_token = os.environ.get("SLACK_TOKEN")

    if not slack_token:
        log.error(
            {
                "status": "ERROR",
                "message": "Slack Token is not set",
            }
        )
        return None

    return WebClient(token=slack_token)


def is_file_manifest(file_name: str) -> bool:
    """
    Check whether a file is a manifest file.

    Parameters
    ----------
    file_name : str
        The name of the file to check.

    Returns
    -------
    bool
        ``True`` if the file is a manifest file, ``False`` otherwise.
    """
    base_name = os.path.basename(file_name)
    return base_name.startswith("file_manifest")


def generate_file_pipeline_message(
    file_path: str, bucket_name: str | None = None, alert_type: str | None = None
) -> str | tuple:
    """
    Generate the Slack message text for a pipeline event on a given file.

    Parameters
    ----------
    file_path : str
        The path (or file name) of the file the event pertains to.
    bucket_name : str, optional
        The S3 bucket name to include in the message.
    alert_type : str, optional
        The pipeline event type (e.g. ``"upload"``, ``"sorted"``, ``"processed"``).

    Returns
    -------
    str or tuple
        The Slack message text, or a ``(message, manifest_contents)`` tuple
        if ``file_path`` is a manifest file.
    """
    if "/" in file_path:
        file_path = file_path.split("/")[-1]

    if alert_type != "delete":
        alert = {
            "upload": f"File Uploaded to S3 - ( _{file_path}_ )",
            "sorted": f"File Sorted - ( _{file_path}_ )",
            "sorted_error": f"File Not Sorted - ( _{file_path}_ )",
            "processed": f"File Processed - ( _{file_path}_ )",
            "processed_error": f"File Not Processed - ( _{file_path}_ )",
            "download": f"File Downloaded - ( _{file_path}_ )",
            "download_error": f"File Not Downloaded - ( _{file_path}_ )",
            "error": f"File Upload Failed - ( _{file_path}_ )",
        }

        slack_message = f"Science File - ( _{file_path}_ )"

        if is_file_manifest(file_path):
            slack_message = f"Manifest File - ( _{file_path}_ )"
            with open(file_path) as file:
                secondary_message = file.read()

            return (slack_message, secondary_message)

        if alert_type and alert_type in alert:
            slack_message = alert[alert_type]

        if bucket_name:
            slack_message += f" (Bucket: _{bucket_name}_ )"

        return slack_message


def send_slack_notification(
    slack_client: WebClient,
    slack_channel: str,
    slack_message: str,
    alert_type: str | None = None,
    slack_max_retries: int = 5,
    slack_retry_delay: int = 5,
    thread_ts: str | None = None,
) -> bool:
    """
    Send a Slack notification, retrying on transient Slack API errors.

    Parameters
    ----------
    slack_client : WebClient
        The Slack client to use.
    slack_channel : str
        The Slack channel ID or name to post to.
    slack_message : str or tuple
        The message text, or a ``(message, manifest_contents)`` tuple.
    alert_type : str, optional
        The pipeline event type, used to color-code the message attachment.
    slack_max_retries : int, optional
        The maximum number of send attempts. Defaults to 5.
    slack_retry_delay : int, optional
        The delay in seconds between retries. Defaults to 5.
    thread_ts : str, optional
        The timestamp of a parent message to thread this message under.

    Returns
    -------
    bool
        ``True`` if the message was sent successfully.

    Raises
    ------
    SlackApiError
        If all retry attempts fail.
    """
    log.debug(f"Sending Slack Notification to {slack_channel}")
    color = {
        "success": "#2ecc71",
        "error": "#ff0000",
        "delete": "#ff0000",
        "upload": "#3498db",
        "sorted": "#f39c12",
        "sorted_error": "#ff0000",
        "processed": "#2ecc71",
        "processed_error": "#f1c40f",
        "download": "#ffffff",
        "download_error": "#ff0000",
        "info": "#3498db",
        "warning": "#f1c40f",
        "orange": "#f39c12",
        "purple": "#9b59b6",
        "black": "#000000",
        "white": "#ffffff",
    }
    ct = datetime.now()
    ts = ct.strftime("%y-%m-%d %H:%M:%S")
    attachments = []
    if isinstance(slack_message, tuple):
        text = slack_message[0]
        attachments = [
            {
                "color": color["purple"],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"{slack_message[1]}",
                        },
                    }
                ],
                "fallback": f"{slack_message[1]}",
            }
        ]
        pretext = slack_message[0]
    else:
        text = slack_message
        pretext = slack_message
        if alert_type:
            attachments = [
                {
                    "color": color[alert_type],
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"{slack_message}",
                            },
                        }
                    ],
                }
            ]
            text = f"`{ts}` -"

    for i in range(slack_max_retries):
        try:
            slack_client.chat_postMessage(
                channel=slack_channel,
                text=text,
                pretext=pretext,
                attachments=attachments,
                thread_ts=thread_ts,
            )

            log.debug(f"Slack Notification Successfully Sent to {slack_channel}")

            return True

        except SlackApiError as e:
            if i < slack_max_retries - 1:
                log.warning(
                    f"Error sending Slack Notification (attempt {i + 1}): {e}."
                    f"Retrying in {slack_retry_delay} seconds..."
                )
                time.sleep(slack_retry_delay)
            else:
                log.error(
                    {
                        "status": "ERROR",
                        "message": f"Error sending Slack Notification (attempt {i + 1}): {e}",
                    }
                )
                raise e


def have_same_keys_and_values(dicts: list, keys_to_check) -> bool:
    """
    Check whether a list of dictionaries agree on the given keys' values.

    Parameters
    ----------
    dicts : list of dict
        Dictionaries to compare.
    keys_to_check : list or set
        Keys that must be identical (in value) across all dictionaries.

    Returns
    -------
    bool
        ``True`` if all dictionaries share the same values for the given keys.
    """
    return len({tuple((k, d[k]) for k in keys_to_check if k in d) for d in dicts}) == 1


def get_message_ts(
    slack_client: WebClient, slack_channel: str, science_filename: str
) -> str | None:
    """
    Find the timestamp of the existing top-level Slack message for a science file.

    Parameters
    ----------
    slack_client : WebClient
        The Slack client to use.
    slack_channel : str
        The Slack channel ID or name to search.
    science_filename : str
        The science file name to match against posted messages.

    Returns
    -------
    str or None
        The Slack message timestamp (``ts``) if found, else ``None``.
    """
    try:
        response = slack_client.conversations_history(channel=slack_channel)
        messages = response["messages"]

        for message in messages:
            if "text" in message:
                slack_science_filename = parse_slack_message(message["text"])

                if not slack_science_filename:
                    continue
                try:
                    if "/" in slack_science_filename:
                        slack_science_filename = slack_science_filename.split("/")[-1]
                    slack_science_file = parse_science_filename(slack_science_filename)
                except ValueError:
                    continue

                science_file = parse_science_filename(science_filename)
                if have_same_keys_and_values(
                    [slack_science_file, science_file],
                    ["instrument", "time"],
                ):
                    return message["ts"]

        return None
    except SlackApiError as e:
        log.error({"status": "ERROR", "message": f"Error retrieving message_ts: {e}"})
        return None


def parse_slack_message(message: str) -> str | None:
    """
    Extract the science file name from a "Science File - ( _name_ )" Slack message.

    Parameters
    ----------
    message : str
        The Slack message text.

    Returns
    -------
    str or None
        The extracted file name, or ``None`` if the message doesn't match.
    """
    match = re.search(r"Science File - \( _?(.+?)_? \)", message)
    if match:
        return match.group(1)
    return None


def send_pipeline_notification(
    slack_client: WebClient,
    slack_channel: str,
    path: str,
    bucket_name: str = None,
    alert_type: str = None,
) -> None:
    """
    Send a pipeline-related notification to a Slack channel, threaded under the file's message.

    Ensures that an initial top-level message about the given file (identified
    by ``path``) exists in the specified Slack channel, then posts an alert
    message as a thread reply to that top-level message.

    Parameters
    ----------
    slack_client : WebClient
        Authenticated Slack client used to send and query messages.
    slack_channel : str
        Slack channel ID or name where messages should be posted.
    path : str
        Filesystem or pipeline path that identifies the science file (used to
        generate message content and to correlate messages).
    bucket_name : str, optional
        S3 bucket name associated with the file.
    alert_type : str, optional
        Label or category for the alert (e.g. ``"upload"``, ``"processed_error"``).
        When provided, it will be included in the threaded alert message.

    Returns
    -------
    None

    Notes
    -----
    This function swallows exceptions and logs them; callers will not receive exceptions.
    """
    try:
        if not is_file_manifest(path):
            slack_message = generate_file_pipeline_message(
                file_path=path, bucket_name=bucket_name
            )

            ts = get_message_ts(
                slack_client=slack_client,
                slack_channel=slack_channel,
                science_filename=path,
            )

            if ts is None:
                slack_message = generate_file_pipeline_message(
                    file_path=path, bucket_name=bucket_name
                )
                send_slack_notification(
                    slack_client=slack_client,
                    slack_channel=slack_channel,
                    slack_message=slack_message,
                )
                ts = get_message_ts(
                    slack_client=slack_client,
                    slack_channel=slack_channel,
                    science_filename=path,
                )

            slack_message = generate_file_pipeline_message(
                file_path=path, bucket_name=bucket_name, alert_type=alert_type
            )

            send_slack_notification(
                slack_client=slack_client,
                slack_channel=slack_channel,
                slack_message=slack_message,
                alert_type=alert_type,
                thread_ts=ts,
            )

    except Exception as e:
        log.error({"status": "ERROR", "message": e})
