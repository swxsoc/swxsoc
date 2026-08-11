"""
Slack notification client for the SWxSOC data pipeline.

:class:`SlackClient` posts science-file pipeline notifications (uploads,
sorting, processing) to Slack, threading related messages for a given file
together under a single top-level message.
"""

import os
import re
import time
from datetime import datetime

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from swxsoc import log
from swxsoc.comm.client import ALERT_TYPES, CommsClient
from swxsoc.util.util import parse_science_filename

__all__ = ["SlackClient"]


class SlackClient(CommsClient):
    """
    Pipeline notification client for Slack.

    Parameters
    ----------
    slack_token : str, optional
        The Slack API bot token. Falls back to the ``SDC_AWS_SLACK_TOKEN``
        environment variable, then the legacy ``SLACK_TOKEN`` environment
        variable, if not provided.
    slack_channel : str, optional
        The Slack channel ID or name to post to. Falls back to the
        ``SDC_AWS_SLACK_CHANNEL`` environment variable if not provided.
    max_retries : int, optional
        The maximum number of send attempts per message. Defaults to 5.
    retry_delay : int, optional
        The delay in seconds between retries. Defaults to 5.

    Raises
    ------
    ValueError
        If no Slack token or channel is available from arguments or
        environment variables.
    """

    def __init__(
        self,
        slack_token: str | None = None,
        slack_channel: str | None = None,
        max_retries: int = 5,
        retry_delay: int = 5,
    ) -> None:
        slack_token = (
            slack_token
            or os.environ.get("SDC_AWS_SLACK_TOKEN")
            or os.environ.get("SLACK_TOKEN")
        )
        slack_channel = slack_channel or os.environ.get("SDC_AWS_SLACK_CHANNEL")

        if not slack_token:
            raise ValueError(
                "Slack token is not set. Pass `slack_token` explicitly or set "
                "the SDC_AWS_SLACK_TOKEN environment variable."
            )
        if not slack_channel:
            raise ValueError(
                "Slack channel is not set. Pass `slack_channel` explicitly or "
                "set the SDC_AWS_SLACK_CHANNEL environment variable."
            )

        self.channel = slack_channel
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client = WebClient(token=slack_token)
        log.debug(f"SlackClient initialized for channel {self.channel}")

    def send_notification(
        self,
        file_path: str,
        alert_type: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Send a pipeline-related notification to Slack, threaded under the file's message.

        Ensures that an initial top-level message about the given file
        exists in the configured Slack channel, then posts an alert message
        as a thread reply to that top-level message.

        Parameters
        ----------
        file_path : str
            Filesystem or pipeline path that identifies the science file
            (used to generate message content and to correlate messages).
        alert_type : str, optional
            Pipeline event type (see ``ALERT_TYPES``). Included in the
            threaded alert message when provided.
        bucket_name : str, optional
            S3 bucket name associated with the file.

        Returns
        -------
        None

        Notes
        -----
        This method swallows exceptions and logs them; callers will not
        receive exceptions. Manifest files are not sent through this method
        (matching the pre-existing behavior of ``send_pipeline_notification``).
        """
        try:
            if not self.is_file_manifest(file_path):
                message = self.generate_file_pipeline_message(
                    file_path=file_path, bucket_name=bucket_name
                )

                ts = self._get_message_ts(file_path)

                if ts is None:
                    message = self.generate_file_pipeline_message(
                        file_path=file_path, bucket_name=bucket_name
                    )
                    self._send_message(message)
                    ts = self._get_message_ts(file_path)

                message = self.generate_file_pipeline_message(
                    file_path=file_path, bucket_name=bucket_name, alert_type=alert_type
                )

                self._send_message(message, alert_type=alert_type, thread_ts=ts)

        except Exception as e:
            log.error({"status": "ERROR", "message": e})

    def close(self) -> None:
        """
        No-op: ``slack_sdk.WebClient`` does not hold a persistent connection
        that needs to be explicitly closed. Present for interface symmetry
        with other :class:`~swxsoc.comm.client.CommsClient` implementations.

        Returns
        -------
        None
        """
        log.debug("Closing SlackClient (no persistent connection to release)")

    def _send_message(
        self,
        message: str | tuple | None,
        alert_type: str | None = None,
        thread_ts: str | None = None,
    ) -> bool:
        """
        Send a Slack notification, retrying on transient Slack API errors.

        Parameters
        ----------
        message : str, tuple, or None
            The message text, or a ``(message, manifest_contents)`` tuple.
        alert_type : str, optional
            The pipeline event type, used to color-code the message attachment.
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
        if message is None:
            return False

        log.debug(f"Sending Slack Notification to {self.channel}")
        ct = datetime.now()
        ts = ct.strftime("%y-%m-%d %H:%M:%S")
        attachments = []
        if isinstance(message, tuple):
            text = message[0]
            attachments = [
                {
                    "color": ALERT_TYPES["purple"],
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"{message[1]}",
                            },
                        }
                    ],
                    "fallback": f"{message[1]}",
                }
            ]
            pretext = message[0]
        else:
            text = message
            pretext = message
            if alert_type:
                attachments = [
                    {
                        "color": ALERT_TYPES[alert_type],
                        "blocks": [
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": f"{message}",
                                },
                            }
                        ],
                    }
                ]
                text = f"`{ts}` -"

        for i in range(self.max_retries):
            try:
                self.client.chat_postMessage(
                    channel=self.channel,
                    text=text,
                    pretext=pretext,
                    attachments=attachments,
                    thread_ts=thread_ts,
                )

                log.debug(f"Slack Notification Successfully Sent to {self.channel}")

                return True

            except SlackApiError as e:
                if i < self.max_retries - 1:
                    log.warning(
                        f"Error sending Slack Notification (attempt {i + 1}): {e}."
                        f"Retrying in {self.retry_delay} seconds..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    log.error(
                        {
                            "status": "ERROR",
                            "message": f"Error sending Slack Notification (attempt {i + 1}): {e}",
                        }
                    )
                    raise e

    def _get_message_ts(self, science_filename: str) -> str | None:
        """
        Find the timestamp of the existing top-level Slack message for a science file.

        Parameters
        ----------
        science_filename : str
            The science file name to match against posted messages.

        Returns
        -------
        str or None
            The Slack message timestamp (``ts``) if found, else ``None``.
        """
        try:
            response = self.client.conversations_history(channel=self.channel)
            messages = response["messages"]

            for message in messages:
                if "text" in message:
                    slack_science_filename = self._parse_slack_message(message["text"])

                    if not slack_science_filename:
                        continue
                    try:
                        if "/" in slack_science_filename:
                            slack_science_filename = slack_science_filename.split("/")[
                                -1
                            ]
                        slack_science_file = parse_science_filename(
                            slack_science_filename
                        )
                    except ValueError:
                        continue

                    science_file = parse_science_filename(science_filename)
                    if self._have_same_keys_and_values(
                        [slack_science_file, science_file],
                        ["instrument", "time"],
                    ):
                        return message["ts"]

            return None
        except SlackApiError as e:
            log.error(
                {"status": "ERROR", "message": f"Error retrieving message_ts: {e}"}
            )
            return None

    @staticmethod
    def _parse_slack_message(message: str) -> str | None:
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

    @staticmethod
    def _have_same_keys_and_values(dicts: list, keys_to_check) -> bool:
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
        return (
            len({tuple((k, d[k]) for k in keys_to_check if k in d) for d in dicts}) == 1
        )
