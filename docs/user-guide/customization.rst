.. _customization:

*************************************************
SWxSOC Environment and Mission Configuration
*************************************************

``swxsoc`` reads its settings, including the selected mission, from a :file:`config.yml` file and from environment variables.
This page explains where the configuration file lives, how to select a mission, and which environment variables change the behavior of ``swxsoc``.

The :file:`config.yml` file
===========================

This package uses a :file:`config.yml` configuration file to customize
certain properties. You can control a number of key features such as
where your data will download to. SWxSOC packages look for this configuration file
in a platform-specific directory, which you can see the path for by running::

  >>> import swxsoc
  >>> swxsoc.print_config(swxsoc.config)  # doctest: +ELLIPSIS
  FILES USED:
  ...

Using your own :file:`config.yml` file
======================================
To maintain your own customizations, you must place your customized :file:`config.yml` inside the appropriate configuration folder (which is based on the operating system you are working on). The `AppDirs module <https://github.com/sunpy/sunpy/blob/main/sunpy/extern/appdirs.py>`_ provided by the `sunpy` package is used to determine where to look for your configuration file.

.. warning::
    Do not edit the config.yml file directly in the Python package as it will get overwritten every time you re-install or update the package.

You can copy the file below, customize it, and then place your customized :file:`config.yml` file inside your config folder.

If you work in our developer environment you can place your configuration file in this directory:

.. code-block:: bash

  /home/vscode/.config/swxsoc/

You can also specify the configuration directory by setting the environment variable `SWXSOC_CONFIGDIR` to the path of your configuration directory. For example, you can set the environment variable in your terminal by running:

.. code-block:: bash

  export SWXSOC_CONFIGDIR=/path/to/your/config/dir

If you do not use our developer environment, you can run the following code to see where to place it on your specific machine as well:

.. doctest::

  >>> from swxsoc import util
  >>> print(util.config._get_user_configdir()) # doctest: +SKIP
  /home/vscode/.config/swxsoc

.. note:: 
  For more information on where to place your configuration file depending on your operating system, you can refer to the `AppDirs module docstrings <https://github.com/sunpy/sunpy/blob/1459206e11dc0c7bfeeeec6aede701ca60a8630c/sunpy/extern/appdirs.py#L165>`_.

Customizing the Mission Configuration
=====================================
The configuration file supports keeping multiple mission configurations. You must select one mission to be used either directly in the configuration file or via an environmental variable.

In the `config.yml` file, you can specify the mission by setting the `selected_mission` variable. Here is an example snippet from a `config.yml` file:

.. code-block:: yaml

  selected_mission: "mission_name"
  missions_data:
    mission_name:
      file_extension: ".txt"
      instruments:
        - name: "Instrument1"
          shortname: "Inst1"
          fullname: "Instrument 1"
          targetname: "Target 1"
        - name: "Instrument2"
          shortname: "Inst2"
          fullname: "Instrument 2"
          targetname: "Target 2"

You can override the selected mission by setting the `SWXSOC_MISSION` environment variable. This is useful for scenarios such as running in different environments (e.g., Lambda containers). For example:

.. code-block:: bash

  export SWXSOC_MISSION=another_mission

.. _customization_env_vars:

Environment Variables
=====================
The following environment variables change how ``swxsoc`` is configured.
They are read when ``swxsoc`` is imported or reconfigured.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Variable
     - Effect
   * - ``SWXSOC_MISSION``
     - Selects the mission, overriding ``selected_mission`` in :file:`config.yml`.
   * - ``SWXSOC_CONFIGDIR``
     - Directory to look in for your :file:`config.yml` file.
   * - ``SWXSOC_INCOMING_BUCKET``
     - Name of the AWS S3 bucket for incoming files, overriding the default ``<mission>-incoming``.
   * - ``LAMBDA_ENVIRONMENT``
     - When set, ``swxsoc`` assumes it is running in AWS Lambda and uses :file:`/tmp/.config` and :file:`/tmp/.cache` as its configuration and cache directories.
   * - ``AWS_REGION``
     - AWS region used for the Timestream client, defaults to ``us-east-1``.

Other features read their own environment variables, see :ref:`comms_clients`, :ref:`grafana_annotation_management`, and :ref:`logger`.

.. _customization_reconfigure:

Reloading the Configuration
===========================
If you change :file:`config.yml` or the ``SWXSOC_MISSION`` environment variable after importing ``swxsoc``, call :py:func:`swxsoc.reconfigure` to reload the configuration.
This function reloads the configuration and updates the global ``swxsoc.config`` variable.

.. code-block:: python

  import os

  import swxsoc

  os.environ["SWXSOC_MISSION"] = "hermes"

  # Reload the configuration so the new mission is used
  swxsoc.reconfigure()

.. note::
  ``swxsoc.reconfigure()`` does not update the MetaTracker database tables.
  If you use ``swxsoc.db.tracker`` or ``swxsoc.db.tables``, also call ``swxsoc.db.reconfigure()`` after changing the mission, see :ref:`metatracker_guide`.

To learn more about how to set up your development environment, see :ref:`dev_env`.

See below (:ref:`config.yml-sample`) for an example configuration file.

.. _config.yml-sample:


A sample config.yml file
--------------------------------------------------------------------

.. only:: html

    `(download) <../_static/config.yml>`__

.. literalinclude:: ../../swxsoc/data/config.yml