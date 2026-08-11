"""
Fan-out communications client for broadcasting pipeline notifications across
multiple platforms simultaneously.
"""

from swxsoc import log
from swxsoc.comm.client import CommsClient

__all__ = ["MultiCommsClient"]


class MultiCommsClient(CommsClient):
    """
    Broadcast pipeline notifications to multiple :class:`CommsClient` instances.

    Delivery to each configured platform is independent: a failure on one
    platform is logged but does not prevent delivery to the others, and this
    client never raises on a partial failure. This is intentional so that,
    e.g., a Mattermost outage does not stop Slack notifications (or vice
    versa) from being sent.

    Parameters
    ----------
    clients : list of CommsClient
        The underlying platform clients to broadcast to. Must contain at
        least one client.

    Raises
    ------
    ValueError
        If ``clients`` is empty.
    """

    def __init__(self, clients: list[CommsClient]) -> None:
        if not clients:
            raise ValueError(
                "MultiCommsClient requires at least one CommsClient instance."
            )
        self.clients = clients

    def send_notification(
        self,
        file_path: str,
        alert_type: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Send a pipeline notification to every configured client.

        Parameters
        ----------
        file_path : str
            Filesystem or pipeline path that identifies the science file.
        alert_type : str, optional
            The pipeline event type.
        bucket_name : str, optional
            S3 bucket name associated with the file.

        Returns
        -------
        None

        Notes
        -----
        Never raises: per-client failures are logged and the remaining
        clients still receive the notification.
        """
        failures = []
        for client in self.clients:
            try:
                client.send_notification(
                    file_path, alert_type=alert_type, bucket_name=bucket_name
                )
            except Exception as e:
                log.error(
                    {
                        "status": "ERROR",
                        "message": (
                            f"{client.__class__.__name__} failed to send "
                            f"notification: {e}"
                        ),
                    }
                )
                failures.append(client.__class__.__name__)

        if failures:
            log.warning(
                f"Partial notification failure: {len(failures)}/{len(self.clients)} "
                f"comms client(s) failed ({failures})"
            )

    def close(self) -> None:
        """
        Close every configured client.

        Returns
        -------
        None

        Notes
        -----
        Never raises: per-client close failures are logged and remaining
        clients are still closed.
        """
        for client in self.clients:
            try:
                client.close()
            except Exception as e:
                log.error(
                    {
                        "status": "ERROR",
                        "message": f"Failed to close {client.__class__.__name__}: {e}",
                    }
                )
