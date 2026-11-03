.. _metatracker_guide:

*************************************
Tracking Science Files (MetaTracker)
*************************************

``swxsoc.db`` provides two independent database integrations. This guide covers
`~swxsoc.db.tracker.MetaTracker`, which records file-level provenance and
processing status in a relational database. If you are looking to
record scalar measurements (housekeeping or science values) to a timeseries database
for Grafana dashboards instead, see :ref:`recording_to_timeseries_database`.

Overview
========

`~swxsoc.db.tracker.MetaTracker` tracks the lifecycle of science files as they
move through a pipeline: it parses a file's metadata from its filename, records
it in a science-file table, associates it with a science-product record, and
optionally logs a processing-status entry (for example, success or failure of a
sorting/processing step). The schema is mission-aware — table names, foreign
keys, and even the number of instrument-identifier columns are rebuilt to match
whichever mission is currently active.

.. note::
    This functionality requires the optional ``tracker`` dependency group,
    which installs `SQLAlchemy <https://www.sqlalchemy.org/>`_ and
    `tenacity <https://tenacity.readthedocs.io/>`_::

        pip install swxsoc[tracker]

    Importing `swxsoc.db.tracker` or `swxsoc.db.tables` without these packages
    installed raises an ``ImportError`` with this same install instruction.

Schema Overview
================

The tracker schema is split into lookup/configuration tables and tracking
tables. All table names are prefixed with the active mission's name (for
example ``hermes_science_file`` or ``padre_status``).

.. list-table:: Lookup and configuration tables
   :header-rows: 1
   :widths: 30 70

   * - Table
     - Purpose
   * - ``file_level``
     - Valid data levels for the mission (for example ``raw``, ``l0``, ``l1``).
   * - ``file_type``
     - Valid file extensions/types for the mission.
   * - ``instrument``
     - One row per instrument defined for the active mission.
   * - ``instrument_configuration``
     - Named groupings of instruments. Has one dynamically generated
       ``instrument_N_id`` foreign-key column per instrument slot, sized to
       the active mission's instrument count.

.. list-table:: Tracking tables
   :header-rows: 1
   :widths: 30 70

   * - Table
     - Purpose
   * - ``science_product``
     - A logical data product (instrument, level, and related metadata) that
       one or more science files belong to.
   * - ``science_file``
     - One row per tracked file: parsed filename metadata, S3 location, and a
       foreign key to its ``science_product``.
   * - ``status``
     - Optional processing-status entries (for example the outcome of a
       sorting or processing step) associated with a ``science_file``.

Building and Reconfiguring Tables
===================================

The ORM classes in `swxsoc.db.tables` are built lazily, the first time each
table module's ``return_class()`` is called. Call `swxsoc.db.reconfigure`
once before the first use in a process (for example, at pipeline startup) to
build every table class for the currently active mission before creating
tables:

    >>> import swxsoc
    >>> import swxsoc.db
    >>> swxsoc.db.reconfigure()

`~swxsoc.db.tables.create_tables` is then idempotent: it creates every table
on first use, and on subsequent calls adds any new columns and upserts lookup
rows required by schema/config changes, without disturbing existing rows or
foreign-key references::

    >>> from swxsoc.db import create_engine
    >>> from swxsoc.db.tables import create_tables
    >>> engine = create_engine("sqlite://")
    >>> create_tables(engine)

Because the schema is mission-aware, any code path that changes the active
mission must repeat this cascade. Call `swxsoc.reconfigure` to reload the
mission configuration, then `swxsoc.db.reconfigure` to rebuild the tracker's
ORM classes against it:

    >>> swxsoc.reconfigure()  # doctest: +SKIP
    >>> swxsoc.db.reconfigure()  # doctest: +SKIP

This two-step pattern is what `swxsoc`'s ``default_test_mission`` and
``use_mission`` pytest fixtures do automatically; see :doc:`/dev-guide/tests`
and ``swxsoc/conftest.py`` for the reference implementation.

Tracking Files with MetaTracker
==================================

`~swxsoc.db.tracker.MetaTracker` is constructed with a database engine and a
``science_file_parser`` callable — a function that takes a file path and
returns a dictionary of parsed metadata. `~swxsoc.util.util.parse_science_filename`
is the parser used by swxsoc's own mission packages:

    >>> from pathlib import Path
    >>> from swxsoc.db import create_engine
    >>> from swxsoc.db.tracker import MetaTracker
    >>> from swxsoc.util.util import parse_science_filename
    >>> engine = create_engine("sqlite://")  # doctest: +SKIP
    >>> tracker = MetaTracker(  # doctest: +SKIP
    ...     engine=engine, science_file_parser=parse_science_filename
    ... )

`~swxsoc.db.tracker.MetaTracker.track` is the primary entry point. It parses
the file, creates or reuses a science-product record, inserts the science-file
record, and optionally records a status entry, returning the
``(science_file_id, science_product_id)`` pair::

    >>> science_file_id, science_product_id = tracker.track(  # doctest: +SKIP
    ...     file=Path("hermes_eea_l0_20230101_v1.0.0.bin"),
    ...     s3_key="hermes_eea_l0_20230101_v1.0.0.bin",
    ...     s3_bucket="hermes-eea",
    ...     status={"processing_status": "success"},
    ... )

If the file does not exist on disk, ``track`` raises ``FileNotFoundError``. If
the engine cannot be connected to, constructing ``MetaTracker`` raises
``ConnectionError``.

API Reference
==============

See `swxsoc.db`, `swxsoc.db.tables`, and `swxsoc.db.tracker` in the
:ref:`reference` for the full API.
