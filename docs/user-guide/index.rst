.. _guide:

************
User's Guide
************

The User's Guide is split into two parts.
**Data Management** covers creating, reading, validating, and documenting CDF data files with ``swxsoc``.
**Pipeline Management** covers the tools used to run a mission science data pipeline, such as configuration, logging, databases, dashboards, and notifications.
For function and class details, see the :ref:`reference`.

.. toctree::
   :maxdepth: 1

   installation
   quick_reference

.. toctree::
   :maxdepth: 2
   :caption: Data Management

   reading_writing_data
   cdf_validation
   fillval_and_masks
   schema_information_guide
   cdf_format_guide

.. toctree::
   :maxdepth: 2
   :caption: Pipeline Management

   customization
   recording_to_timeseries_database
   metatracker_guide
   grafana_annotation_management
   retrieving_data
   logger
   comms_clients
