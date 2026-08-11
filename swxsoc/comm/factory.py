"""
Factory for constructing the active pipeline notification client(s) based on
environment configuration.
"""

import os

from swxsoc import log
from swxsoc.comm.client import CommsClient
from swxsoc.comm.mattermost import MattermostClient
from swxsoc.comm.multi_client import MultiCommsClient
from swxsoc.comm.slack import SlackClient

__all__ = ["get_comms_client"]


def get_comms_client() -> CommsClient:
    """
    Build the pipeline notification client(s) configured via ``COMMS_PLATFORM``.

    Reads the ``COMMS_PLATFORM`` environment variable, a comma-separated list
    of platform names (``"slack"``, ``"mattermost"``, or both, e.g.
    ``"slack,mattermost"``), and instantiates the corresponding client(s).
    Each platform-specific client reads its own required configuration
    (tokens, channel IDs, etc.) from its own environment variables.

    If multiple platforms are requested and only some fail to initialize,
    the failure(s) are logged and a client wrapping the platforms that did
    initialize successfully is returned. If *all* requested platforms fail
    to initialize, an exception is raised since there would be no viable
    notification path.

    Returns
    -------
    CommsClient
        A single platform client if only one platform is configured (and
        it initializes successfully), or a
        :class:`~swxsoc.comm.multi_client.MultiCommsClient` wrapping the
        successfully initialized clients if multiple platforms are
        configured.

    Raises
    ------
    ValueError
        If ``COMMS_PLATFORM`` is unset, empty, or contains an unrecognized
        platform name.
    RuntimeError
        If every requested platform's client fails to initialize.
    """
    raw_platforms = os.environ.get("COMMS_PLATFORM", "")
    platforms = [p.strip().lower() for p in raw_platforms.split(",") if p.strip()]

    # Built here (rather than at module import time) so that the platform
    # names always resolve to the current module-level SlackClient /
    # MattermostClient references.
    platform_clients: dict[str, type[CommsClient]] = {
        "slack": SlackClient,
        "mattermost": MattermostClient,
    }

    if not platforms:
        raise ValueError(
            "COMMS_PLATFORM environment variable is not set. Set it to one "
            "or more comma-separated values from: "
            f"{sorted(platform_clients)}."
        )

    unknown_platforms = [p for p in platforms if p not in platform_clients]
    if unknown_platforms:
        raise ValueError(
            f"Unrecognized COMMS_PLATFORM value(s): {unknown_platforms}. "
            f"Supported platforms: {sorted(platform_clients)}."
        )

    clients: list[CommsClient] = []
    failed_platforms: list[str] = []
    for platform in platforms:
        try:
            clients.append(platform_clients[platform]())
        except Exception as e:
            log.error(
                {
                    "status": "ERROR",
                    "message": f"Failed to initialize '{platform}' comms client: {e}",
                }
            )
            failed_platforms.append(platform)

    if not clients:
        raise RuntimeError(
            "Failed to initialize any comms client for "
            f"COMMS_PLATFORM={platforms}; all platforms failed: {failed_platforms}"
        )

    if failed_platforms:
        log.warning(
            f"Initialized {len(clients)}/{len(platforms)} comms client(s); "
            f"failed to initialize: {failed_platforms}"
        )

    if len(platforms) == 1:
        log.info(f"Initialized comms client for platform: {platforms[0]}")
        return clients[0]

    initialized_platforms = [p for p in platforms if p not in failed_platforms]
    log.info(f"Initialized multi-platform comms client for: {initialized_platforms}")
    return MultiCommsClient(clients)
