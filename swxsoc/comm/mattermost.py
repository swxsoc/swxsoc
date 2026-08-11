"""
Mattermost notification client for the SWxSOC data pipeline.

:class:`MattermostClient` posts science-file pipeline notifications to a
Mattermost channel. Unlike Slack, Mattermost notifications are posted as flat
(non-threaded) messages, since the Mattermost API does not support the same
thread-lookup-by-content pattern used for Slack.
"""

import os
import time
from urllib.parse import urlparse

from mattermostautodriver import TypedDriver
from mattermostautodriver.exceptions import MattermostError

from swxsoc import log
from swxsoc.comm.client import CommsClient

__all__ = ["MattermostClient"]


def _parse_mattermost_url(url: str) -> tuple[str, int | None, str]:
    """
    Parse a ``MATTERMOST_URL`` value into its host, port, and scheme.

    Accepts plain hosts (``mattermost.example.org``), ``host:port`` pairs
    (``localhost:8065``), or full URLs (``https://mattermost.example.org``).

    Parameters
    ----------
    url : str
        The configured Mattermost server URL.

    Returns
    -------
    tuple of (str, int or None, str)
        The host, port (or ``None`` if not specified), and scheme. Scheme
        defaults to ``"https"`` unless the URL explicitly specifies otherwise.
    """
    parsed = urlparse(url if "//" in url else f"//{url}")
    return parsed.hostname or url, parsed.port, parsed.scheme or "https"


class MattermostClient(CommsClient):
    """
    Pipeline notification client for Mattermost.

    Parameters
    ----------
    url : str, optional
        The Mattermost server URL (e.g. ``"mattermost.example.org"`` or
        ``"localhost:8065"``). Falls back to the ``MATTERMOST_URL``
        environment variable if not provided.
    token : str, optional
        A Mattermost personal access / bot token. Falls back to the
        ``MATTERMOST_TOKEN`` environment variable if not provided.
    channel_id : str, optional
        The Mattermost channel ID to post to. Falls back to the
        ``MATTERMOST_CHANNEL_ID`` environment variable if not provided.
    max_retries : int, optional
        The maximum number of send attempts per message. Defaults to 5.
    retry_delay : int, optional
        The delay in seconds between retries. Defaults to 5.

    Raises
    ------
    ValueError
        If no Mattermost URL, token, or channel ID is available from
        arguments or environment variables.
    """

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        channel_id: str | None = None,
        max_retries: int = 5,
        retry_delay: int = 5,
    ) -> None:
        url = url or os.environ.get("MATTERMOST_URL")
        token = token or os.environ.get("MATTERMOST_TOKEN")
        channel_id = channel_id or os.environ.get("MATTERMOST_CHANNEL_ID")

        if not url:
            raise ValueError(
                "Mattermost URL is not set. Pass `url` explicitly or set the "
                "MATTERMOST_URL environment variable."
            )
        if not token:
            raise ValueError(
                "Mattermost token is not set. Pass `token` explicitly or set "
                "the MATTERMOST_TOKEN environment variable."
            )
        if not channel_id:
            raise ValueError(
                "Mattermost channel ID is not set. Pass `channel_id` explicitly "
                "or set the MATTERMOST_CHANNEL_ID environment variable."
            )

        host, port, scheme = _parse_mattermost_url(url)

        self.channel_id = channel_id
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        driver_options = {"url": host, "token": token, "scheme": scheme}
        if port is not None:
            driver_options["port"] = port

        self.driver = TypedDriver(driver_options)
        self.driver.login()
        log.debug(f"MattermostClient initialized for channel {self.channel_id}")

    def send_notification(
        self,
        file_path: str,
        alert_type: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Send a pipeline-related notification to Mattermost as a flat message.

        Parameters
        ----------
        file_path : str
            Filesystem or pipeline path that identifies the science file.
        alert_type : str, optional
            Pipeline event type (see ``ALERT_TYPES``).
        bucket_name : str, optional
            S3 bucket name associated with the file.

        Returns
        -------
        None

        Notes
        -----
        This method swallows exceptions and logs them; callers will not
        receive exceptions.
        """
        try:
            message = self.generate_file_pipeline_message(
                file_path=file_path, bucket_name=bucket_name, alert_type=alert_type
            )

            if message is None:
                return

            if isinstance(message, tuple):
                text = f"{message[0]}\n```\n{message[1]}\n```"
            else:
                text = message

            self._send_message(text)

        except Exception as e:
            log.error({"status": "ERROR", "message": e})

    def close(self) -> None:
        """
        Close the underlying Mattermost driver connection.

        Returns
        -------
        None
        """
        try:
            self.driver.close()
        except Exception as e:
            log.error(
                {"status": "ERROR", "message": f"Error closing MattermostClient: {e}"}
            )

    def _send_message(self, text: str) -> bool:
        """
        Post a message to the configured Mattermost channel, retrying on
        transient Mattermost API errors.

        Parameters
        ----------
        text : str
            The message text to post.

        Returns
        -------
        bool
            ``True`` if the message was sent successfully.

        Raises
        ------
        MattermostError
            If all retry attempts fail.
        """
        log.debug(f"Sending Mattermost Notification to channel {self.channel_id}")

        for i in range(self.max_retries):
            try:
                self.driver.posts.create_post(
                    channel_id=self.channel_id,
                    message=text,
                )

                log.debug(
                    f"Mattermost Notification Successfully Sent to {self.channel_id}"
                )

                return True

            except MattermostError as e:
                if i < self.max_retries - 1:
                    log.warning(
                        f"Error sending Mattermost Notification (attempt {i + 1}): {e}."
                        f"Retrying in {self.retry_delay} seconds..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    log.error(
                        {
                            "status": "ERROR",
                            "message": f"Error sending Mattermost Notification (attempt {i + 1}): {e}",
                        }
                    )
                    raise e
