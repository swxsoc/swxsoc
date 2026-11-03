.. _installation:

******************
Installation Guide
******************

``swxsoc`` is available on PyPI and can also be installed directly from GitHub.
The base install provides the core package, and optional extras add CDF file support, MetaTracker database support, and development tools.

Requirements
============

- Python 3.10 or later.
- ``pip``, which is included with most Python installations.

Check your Python version with:

.. code-block:: bash

    python --version

Installing from PyPI
====================

We recommend installing into a virtual environment so that ``swxsoc`` and its dependencies do not conflict with other projects.

.. code-block:: bash

    python -m venv .venv
    source .venv/bin/activate

Install the base package:

.. code-block:: bash

    pip install swxsoc

The base install includes:

- Core ``SWXData``, configuration, and logging functionality.
- Data retrieval with ``swxsoc.net``, which depends on `SunPy <https://sunpy.org/>`_ with network support.
- Writing measurements to the timeseries database with ``swxsoc.db.timeseries``.
- Slack and Mattermost notifications with ``swxsoc.comm``.

The base install does not include CDF reading, writing, or validation, or the MetaTracker database. Install the extras described below for those features.

To upgrade an existing installation to the latest release:

.. code-block:: bash

    pip install --upgrade swxsoc

.. _install_extras:

Optional Extras
===============

Optional dependency groups are called *extras*. Install an extra by adding its name in square brackets after the package name.

.. code-block:: bash

    pip install "swxsoc[cdf]"

.. note::

   The quotes are required in ``zsh``, the default shell on macOS. Without them, ``zsh`` reports ``no matches found``.

Combine several extras with a comma and no spaces:

.. code-block:: bash

    pip install "swxsoc[cdf,tracker]"

The following extras are defined in :file:`pyproject.toml`.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Extra
     - What it adds
   * - ``cdf``
     - SpacePy, SAMMI (``sammi-cdf``), and matplotlib, for reading, writing, validating, and plotting CDF files. SpacePy includes the NASA CDF library, so you do not need to install the CDF library separately.
   * - ``fits``
     - Nothing new. Astropy, which provides FITS support, is already a base dependency.
   * - ``tracker``
     - SQLAlchemy and tenacity, for MetaTracker in ``swxsoc.db.tracker`` and ``swxsoc.db.tables``.
   * - ``all``
     - The ``cdf``, ``fits``, and ``tracker`` extras.
   * - ``docs``
     - Sphinx and the Sphinx extensions used to build the documentation, including ``sphinx-llms-txt``.
   * - ``test``
     - pytest and the plugins used by the test suite.
   * - ``style``
     - Ruff and rstcheck, for linting and formatting.
   * - ``dev``
     - The ``docs``, ``test``, and ``style`` extras.

Which Extras Do I Need?
-----------------------

.. list-table::
   :widths: 60 40
   :header-rows: 1

   * - If you want to
     - Install
   * - Retrieve or download mission data, send Slack or Mattermost notifications, or record measurements to the timeseries database
     - Base install (no extras)
   * - Read, write, validate, or plot CDF files
     - ``cdf``
   * - Track processed files in a relational database with MetaTracker
     - ``tracker``
   * - Use every file format and database feature
     - ``all``
   * - Contribute to the package or build its documentation
     - ``dev``, see :ref:`dev_env`

Using a feature without its extra raises an ``ImportError`` that names the extra to install.
For example, the ``cdf`` extra is required for ``SWXData`` file input and output, and the ``tracker`` extra is required for ``swxsoc.db.tracker``.

Installing the Latest Development Version from GitHub
=====================================================

The ``main`` branch of the `swxsoc repository <https://github.com/swxsoc/swxsoc>`_ contains changes that have not yet been released to PyPI.
To install the latest development version directly from GitHub, run:

.. code-block:: bash

    pip install "git+https://github.com/swxsoc/swxsoc.git@main"

To include extras, put the extra in brackets before the ``@`` sign:

.. code-block:: bash

    pip install "swxsoc[cdf] @ git+https://github.com/swxsoc/swxsoc.git@main"

To install a specific branch, tag, or commit, replace ``main`` with the name of the branch, the tag, or the commit hash.

If you plan to modify the code, clone the repository and install it in editable mode so that changes take effect without reinstalling:

.. code-block:: bash

    git clone https://github.com/swxsoc/swxsoc.git
    cd swxsoc
    pip install -e ".[dev]"

Clone the repository with ``git`` rather than downloading a ZIP archive. The package version is determined from the git history by ``setuptools_scm``, which a ZIP archive does not include.
See :ref:`dev_env` for the full developer setup.

Verifying the Installation
==========================

Check that the package imports and print its version:

.. code-block:: bash

    python -c "import swxsoc; print(swxsoc.__version__)"

Check any optional extras that you installed:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Extra
     - Command
   * - ``cdf``
     - ``python -c "from spacepy import pycdf; import sammi"``
   * - ``tracker``
     - ``python -c "import sqlalchemy, tenacity"``

If a command fails with a ``ModuleNotFoundError`` or ``ImportError``, confirm that you installed the extra into the active environment.
Run ``which python`` (or ``where python`` on Windows) to check which interpreter is active.

Next Steps
==========

- For one-line answers to common tasks, see the :ref:`quick_reference`.
- To create your first CDF file, see :ref:`reading_writing_cdf`.
- To set up a configuration file or choose a mission, see :ref:`customization`.
