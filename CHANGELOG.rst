This project uses `semantic versioning <https://semver.org>`_.

Latest
======
* Added Functionality from HERMES partner package. 

0.0.0 (2023-03-22)
==================
Initial project release 

- Initial Release 

Latest
======

* Added optional ``cdf`` dependency extra for reading and writing CDF files with the ``spacepy`` and ``sammi-cdf`` package.
* Added optional ``tracker`` dependency extra with SQLAlchemy and tenacity for MetaTracker under ``swxsoc.db.tracker``.
* Reorganized the documentation: the User's Guide is split into Data Management and Pipeline Management sections, the CDF guide was rewritten as a step-by-step guide, and new Quick Reference, CDF validation, and ``SWXData`` examples pages were added.
* Removed the CMAD and tour documentation pages.
* Fixed inaccurate documentation, including ``swxsoc.reconfigure()``, ``swxsoc.print_config(config)``, derived attribute names, and outdated ISTP links.
* Added ``sphinx-llms-txt`` to the ``docs`` extra so the docs build writes ``llms.txt`` and ``llms-full.txt``.
* Replaced ``black`` and ``flake8`` with ``ruff`` for linting and formatting.
* Issued a ``SWXUserWarning`` when ``SWXSOC_MISSION`` or ``selected_mission`` names a mission that is not defined in ``missions_data`` of the config file, instead of failing silently.
* Converted the code examples in the documentation that use the ``swxsoc`` package to ``doctest`` directives so they are executed by the test suite. Examples that need external services use ``+SKIP``.
* Created new Installation Guide (``installation.rst``) and Release Process page (``release_process.rst``).