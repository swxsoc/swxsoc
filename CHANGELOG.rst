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

* Added optional tracker support under ``swxsoc.db.tracker``.
* Added a new ``tracker`` extra for SQLAlchemy and tenacity.
* Reorganized the documentation: the User's Guide is split into Data Management and Pipeline Management sections, the CDF guide was rewritten as a step-by-step guide, and new Quick Reference, CDF validation, and ``SWXData`` examples pages were added.
* Removed the CMAD and tour documentation pages.
* Fixed inaccurate documentation, including ``swxsoc.reconfigure()``, ``swxsoc.print_config(config)``, derived attribute names, and outdated ISTP links.
* Added ``sphinx-llms-txt`` to the ``docs`` extra so the docs build writes ``llms.txt`` and ``llms-full.txt``.