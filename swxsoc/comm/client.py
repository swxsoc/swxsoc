"""
Abstract communications client interface for SWxSOC pipeline notifications.

Concrete platform clients (e.g. :class:`~swxsoc.comm.slack.SlackClient`,
:class:`~swxsoc.comm.mattermost.MattermostClient`) implement only the parts of
sending a notification that are genuinely platform-specific: authentication,
native message delivery, and (where supported) message threading.

Logic that is not specific to any single platform -- detecting manifest
files, generating the pipeline event message text, and the set of valid
alert types -- lives here so it isn't duplicated (or allowed to drift)
across clients.
"""

import os
from abc import ABC, abstractmethod
from functools import lru_cache

__all__ = [
    "CommsClient",
    "ALERT_TYPES",
]

#: Valid pipeline alert types and the color code historically used to
#: highlight them in chat-style notifications. Platform clients that support
#: colored formatting (e.g. Slack attachments) may use these colors directly;
#: platforms that don't (e.g. Mattermost) can ignore the color and just rely
#: on the message text produced by ``CommsClient.generate_file_pipeline_message``.
ALERT_TYPES: dict[str, str] = {
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


@lru_cache(maxsize=32)
def _read_manifest_contents(file_path: str) -> str:
    """
    Read and cache the contents of a manifest file.

    Cached so that a single pipeline event fanned out to multiple
    ``CommsClient`` implementations (see
    :class:`~swxsoc.comm.multi_client.MultiCommsClient`) only reads the file
    from disk once, regardless of how many platforms are notified.

    Parameters
    ----------
    file_path : str
        Path to the manifest file to read.

    Returns
    -------
    str
        The full contents of the manifest file.
    """
    with open(file_path) as file:
        return file.read()


class CommsClient(ABC):
    """
    Abstract base class for pipeline notification clients.

    Subclasses must implement :meth:`send_notification` and :meth:`close`
    using their platform's native API. The message-content helpers below are
    shared, since the message text itself doesn't depend on which platform
    it is ultimately delivered to.
    """

    @staticmethod
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
        # Check if the file starts with file_manifest prefix
        return base_name.startswith("file_manifest")

    @classmethod
    def generate_file_pipeline_message(
        cls,
        file_path: str,
        bucket_name: str | None = None,
        alert_type: str | None = None,
    ) -> str | tuple | None:
        """
        Generate the notification message text for a pipeline event on a given file.

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
        str, tuple, or None
            The message text, a ``(message, manifest_contents)`` tuple if
            ``file_path`` is a manifest file, or ``None`` if ``alert_type``
            is ``"delete"``.
        """
        if "/" in file_path:
            file_path = file_path.split("/")[-1]

        if alert_type == "delete":
            return None

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

        message = f"Science File - ( _{file_path}_ )"

        if cls.is_file_manifest(file_path):
            message = f"Manifest File - ( _{file_path}_ )"
            secondary_message = _read_manifest_contents(file_path)
            return (message, secondary_message)

        # Check for specific alert type message
        if alert_type and alert_type in alert:
            message = alert[alert_type]

        # Add bucket name to the message if provided
        if bucket_name:
            message += f" (Bucket: _{bucket_name}_ )"

        return message

    @abstractmethod
    def send_notification(
        self,
        file_path: str,
        alert_type: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Send a pipeline notification for a given file.

        Implementations should not raise: failures must be logged through
        ``swxsoc.log`` so that a single platform outage doesn't interrupt
        pipeline processing or (when used via
        :class:`~swxsoc.comm.multi_client.MultiCommsClient`) block delivery
        to other configured platforms.

        Parameters
        ----------
        file_path : str
            Filesystem or pipeline path that identifies the science file.
        alert_type : str, optional
            The pipeline event type (see ``ALERT_TYPES`` for valid values).
        bucket_name : str, optional
            S3 bucket name associated with the file.

        Returns
        -------
        None
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Release any resources (connections, sessions) held by the client.

        Returns
        -------
        None
        """
        raise NotImplementedError
