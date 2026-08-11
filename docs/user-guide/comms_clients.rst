.. _comms_clients:

*******************************
Pipeline Notifications (Comms)
*******************************

Overview
========

``swxsoc.comm`` provides a unified interface for sending pipeline notifications
(file uploads, sorting results, processing results, errors, etc.) to chat
platforms. Two platforms are currently supported:

* **Slack** — via :class:`~swxsoc.comm.slack.SlackClient` (threaded messages)
* **Mattermost** — via :class:`~swxsoc.comm.mattermost.MattermostClient` (flat messages)

Notifications can be sent to a single platform, or broadcast to multiple
platforms simultaneously via :class:`~swxsoc.comm.multi_client.MultiCommsClient`.
The Lambda functions that consume this module (``sdc_aws_sorting_lambda`` and
``sdc_aws_artifacts_lambda``) never construct platform-specific clients
directly — they call :func:`~swxsoc.comm.factory.get_comms_client`, which reads
the active configuration from environment variables and returns the
appropriate client (or a ``MultiCommsClient`` wrapping several).

Environment Variable Configuration
===================================

``COMMS_PLATFORM`` is **required** and has no default. It is a comma-separated,
case-insensitive list of platforms to enable:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Value
     - Result
   * - ``slack``
     - Notifications sent to Slack only
   * - ``mattermost``
     - Notifications sent to Mattermost only
   * - ``slack,mattermost``
     - Notifications broadcast to both platforms

Additional variables are required depending on which platforms are listed in
``COMMS_PLATFORM``:

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Variable
     - Platform
     - Description
   * - ``COMMS_PLATFORM``
     - —
     - Comma-separated list of active platforms
   * - ``SDC_AWS_SLACK_TOKEN``
     - Slack
     - Bot token used to authenticate with Slack
   * - ``SDC_AWS_SLACK_CHANNEL``
     - Slack
     - Destination Slack channel ID
   * - ``MATTERMOST_URL``
     - Mattermost
     - Server URL, e.g. ``localhost:8065``
   * - ``MATTERMOST_TOKEN``
     - Mattermost
     - Bot access token
   * - ``MATTERMOST_CHANNEL_ID``
     - Mattermost
     - Destination Mattermost channel ID

.. note::
   ``SDC_AWS_SLACK_TOKEN`` falls back to the legacy ``SLACK_TOKEN`` environment
   variable if unset, for backward compatibility with older deployments.

Example: single platform (Slack only)::

    export COMMS_PLATFORM="slack"
    export SDC_AWS_SLACK_TOKEN="xoxb-..."
    export SDC_AWS_SLACK_CHANNEL="C0123456789"

Example: multiple platforms (Slack and Mattermost)::

    export COMMS_PLATFORM="slack,mattermost"
    export SDC_AWS_SLACK_TOKEN="xoxb-..."
    export SDC_AWS_SLACK_CHANNEL="C0123456789"
    export MATTERMOST_URL="chat.example.org"
    export MATTERMOST_TOKEN="abcd1234"
    export MATTERMOST_CHANNEL_ID="channel-id-1234"

CommsClient Interface
======================

All clients implement the abstract :class:`~swxsoc.comm.client.CommsClient`
interface:

``send_notification(file_path, alert_type=None, bucket_name=None)``
    Sends a pipeline notification for ``file_path``. ``alert_type`` selects the
    message color/category (see ``ALERT_TYPES`` below); ``bucket_name`` is
    included in the generated message when provided. Returns ``None``.
    Implementations never raise — failures are logged and swallowed so that a
    notification failure never interrupts pipeline processing.

``close()``
    Releases any underlying connection/session held by the client. Returns
    ``None``. Safe to call even if no connection was ever opened; failures are
    logged and swallowed.

Shared, platform-agnostic helpers are also available on ``CommsClient``:

``is_file_manifest(file_name)``
    Returns ``True`` if ``file_name`` looks like a file-manifest artifact.

``generate_file_pipeline_message(file_path, bucket_name=None, alert_type=None)``
    Builds the notification message text for a file pipeline event. Returns
    ``None`` for ``alert_type="delete"``, a ``(message, manifest_contents)``
    tuple for manifest files, or a plain string message otherwise.

``ALERT_TYPES``
    A dict mapping alert type strings (``upload``, ``sorted``, ``sorted_error``,
    ``processed``, ``processed_error``, ``download``, ``download_error``,
    ``error``, ``delete``, ``info``, ``warning``, and a few color aliases) to
    their associated hex color codes, importable directly from ``swxsoc.comm``.

Using the Factory Function
===========================

In Lambda functions and most scripts, use
:func:`~swxsoc.comm.factory.get_comms_client` rather than instantiating a
client directly. It reads ``COMMS_PLATFORM`` (and the platform-specific
variables above) and returns a ready-to-use client:

    >>> import os
    >>> os.environ["COMMS_PLATFORM"] = "slack"
    >>> os.environ["SDC_AWS_SLACK_TOKEN"] = "xoxb-example-token"
    >>> os.environ["SDC_AWS_SLACK_CHANNEL"] = "C0123456789"
    >>> from swxsoc.comm import get_comms_client
    >>> comms_client = get_comms_client()  # doctest: +SKIP
    >>> comms_client.send_notification(  # doctest: +SKIP
    ...     file_path="hermes_eea_l0_20230101_v1.0.0.bin",
    ...     bucket_name="hermes-eea",
    ...     alert_type="upload",
    ... )
    >>> comms_client.close()  # doctest: +SKIP

If ``COMMS_PLATFORM`` lists more than one platform, ``get_comms_client()``
returns a :class:`~swxsoc.comm.multi_client.MultiCommsClient` wrapping all
configured clients, with the same ``send_notification``/``close`` interface.

Independently Instantiating a Client
=====================================

Direct instantiation is useful for standalone scripts, notebooks, or tests
where credentials come from something other than environment variables (e.g.
a secrets manager lookup), or where only one platform is ever needed and the
factory's environment-variable parsing is unnecessary overhead.

Slack::

    >>> from swxsoc.comm import SlackClient
    >>> slack_client = SlackClient(
    ...     slack_token="xoxb-example-token",
    ...     slack_channel="C0123456789",
    ... )
    >>> slack_client.send_notification(  # doctest: +SKIP
    ...     file_path="hermes_eea_l0_20230101_v1.0.0.bin",
    ...     alert_type="sorted",
    ... )
    >>> slack_client.close()  # doctest: +SKIP

Mattermost::

    >>> from swxsoc.comm import MattermostClient
    >>> mattermost_client = MattermostClient(  # doctest: +SKIP
    ...     url="chat.example.org",
    ...     token="abcd1234",
    ...     channel_id="channel-id-1234",
    ... )
    >>> mattermost_client.send_notification(  # doctest: +SKIP
    ...     file_path="hermes_eea_l0_20230101_v1.0.0.bin",
    ...     alert_type="processed",
    ... )
    >>> mattermost_client.close()  # doctest: +SKIP

Broadcasting to both platforms with ``MultiCommsClient``::

    >>> from swxsoc.comm import MultiCommsClient
    >>> multi_client = MultiCommsClient(  # doctest: +SKIP
    ...     clients=[slack_client, mattermost_client]
    ... )
    >>> multi_client.send_notification(  # doctest: +SKIP
    ...     file_path="hermes_eea_l0_20230101_v1.0.0.bin",
    ...     alert_type="processed",
    ... )
    >>> multi_client.close()  # doctest: +SKIP

Error Handling & Resilience
=============================

All clients follow a **log-and-continue** strategy: notification failures
never raise, so a chat outage never blocks pipeline processing.

* Each client's ``send_notification()``/``close()`` swallows exceptions
  internally and logs them at ``ERROR`` level via ``swxsoc.log``.
* :class:`~swxsoc.comm.multi_client.MultiCommsClient` attempts every configured
  client, logs an ``ERROR`` for each individual failure, and logs a ``WARNING``
  summary (failures vs. total clients) if any client failed. It never raises.
* :func:`~swxsoc.comm.factory.get_comms_client` behaves differently at
  *initialization* time: if some (but not all) requested platforms fail to
  initialize, it logs the failures and returns a ``MultiCommsClient`` wrapping
  the survivors. If **all** requested platforms fail to initialize, it raises
  ``RuntimeError`` (there is no viable notification path). An unset, empty, or
  unrecognized ``COMMS_PLATFORM`` raises ``ValueError`` before any client is
  constructed.

Transient failures (network timeouts, temporary API rate limiting) and
permanent failures (invalid token, unknown channel ID) are both handled the
same way at the ``send_notification``/ ``close`` level — logged and skipped.
Only misconfiguration discovered during ``get_comms_client()`` initialization
can raise, and only when no platform is left able to send anything.

Debugging & Troubleshooting
==============================

Enable debug-level logging to see client initialization, retry attempts, and
message send details::

    >>> from swxsoc import log
    >>> log.setLevel("DEBUG")  # doctest: +SKIP

Common configuration errors:

* ``ValueError: ... token ...`` / ``... channel ...`` — a required
  platform-specific environment variable (or constructor argument) is missing.
* ``ValueError`` from ``get_comms_client()`` — ``COMMS_PLATFORM`` is unset,
  empty, or contains a name other than ``slack``/``mattermost``.
* ``RuntimeError`` from ``get_comms_client()`` — every requested platform
  failed to initialize; check the ``ERROR``-level logs emitted just before it
  for the underlying cause per platform.
* Repeated retry log lines from ``send_notification()`` followed by a final
  ``ERROR`` — the platform's API rejected every retry attempt (bad token,
  unreachable server, or invalid channel).

Before relying on notifications in a new environment, construct the client
once and confirm no exception is raised (or check the logs, since
``get_comms_client()`` can still return a partially-degraded
``MultiCommsClient`` without raising).

Testing with Mock Clients
============================

Unit tests for each client live alongside the module and mock the underlying
SDK/driver so no network access is required:

* ``swxsoc/comm/tests/test_client.py`` — shared ``CommsClient`` logic
* ``swxsoc/comm/tests/test_slack_client.py`` — ``SlackClient`` (mocks
  ``slack_sdk.WebClient``)
* ``swxsoc/comm/tests/test_mattermost_client.py`` — ``MattermostClient``
  (mocks ``mattermostautodriver.TypedDriver``)
* ``swxsoc/comm/tests/test_multi_client.py`` — ``MultiCommsClient``
  broadcasting and partial-failure behavior (mocks ``CommsClient`` instances)
* ``swxsoc/comm/tests/test_factory.py`` — ``get_comms_client()`` platform
  parsing and partial/total-failure behavior

These tests are a good reference for mocking patterns when writing new tests
against code that consumes ``swxsoc.comm``.
