.. _cdf_validation:

*******************
Validating CDF Data
*******************

Use :py:func:`swxsoc.util.validation.validate` to check a CDF file for missing metadata and common ISTP compliance problems before you share it or submit it to an archive.
``validate`` returns a list of problems, and an empty list means no problems were found.

The SAMMI package has more information on CDF validation, including other tools you can use to check your files.
See the `SAMMI CDF validation guide <https://sammi-cdf.readthedocs.io/en/latest/user-guide/cdf_validation.html>`_.

.. note::

   **Required install:** ``pip install swxsoc[cdf]``

Validating a File
=================

Pass the `~pathlib.Path` of a CDF file to :py:func:`~swxsoc.util.validation.validate`.
This example creates a small file that has only the three required global attributes, and then validates it:

    >>> import os
    >>> import tempfile
    >>> from pathlib import Path
    >>> import numpy as np
    >>> import astropy.units as u
    >>> from astropy.timeseries import TimeSeries
    >>> import swxsoc
    >>> from swxsoc.swxdata import SWXData
    >>> from swxsoc.util.validation import validate
    >>> os.environ["SWXSOC_MISSION"] = "hermes"
    >>> swxsoc.reconfigure()
    >>> ts = TimeSeries(
    ...     time_start="2016-03-22T12:30:31",
    ...     time_delta=3 * u.s,
    ...     data={"Bx": u.Quantity([1, 2, 3, 4], "nT", dtype=np.int16)},
    ... )
    >>> ts["Bx"].meta = {"CATDESC": "X component of the magnetic field"}
    >>> sw_data = SWXData(timeseries=ts, meta=SWXData.global_attribute_template("nemisis", "l1", "1.0.0"))
    >>> with tempfile.TemporaryDirectory() as tmp_dir:
    ...     cdf_file_path = sw_data.save(output_path=tmp_dir)
    ...     validation_errors = validate(cdf_file_path)
    >>> "Required attribute (PI_name) not present in global attributes." in validation_errors
    True

Each entry in the list is a human-readable description of one problem.
In this example, the file is missing several ISTP-required global attributes, such as ``PI_name`` and ``TEXT``.
Add them to ``sw_data.meta``, save the file again, and validate it again until the list is empty.

``validate`` only supports files with a ``.cdf`` extension, and raises a `ValueError` for any other file type.
If the file cannot be opened, the list contains a ``Could not open CDF File`` message.

What Is Checked
===============

``validate`` runs two kinds of checks.

**Schema checks** compare the file with the ``swxsoc`` metadata schema, :py:class:`~swxsoc.util.schema.SWXSchema`:

- Every global attribute marked as required in the schema is present and not empty.
- Every variable has a ``VAR_TYPE`` attribute.
- Every variable has the variable attributes that the schema requires for its ``VAR_TYPE``.
- Attributes that have a list of valid values in the schema use one of those values.

See :ref:`schema_information_guide` for the required attributes.

**ISTP checks** use the `spacepy.pycdf.istp` module from `SpacePy <https://spacepy.github.io/>`_:

- File checks: empty attribute entries, the file name, and that the times are monotonic and valid.
- Variable checks: ``DEPEND_*`` attributes, their sizes, empty entries, and record counts.

Some SpacePy ISTP checks are skipped on purpose, because they make assumptions that do not fit ``swxsoc`` files.
For example, ``swxsoc`` lets instrument teams set ``FILLVAL``, ``VALIDMIN``, and ``VALIDMAX`` to values other than the defaults for the CDF data type.

**Range checks** compare the data with ``VALIDMIN`` / ``VALIDMAX`` (ignoring ``FILLVAL`` entries), and check that ``SCALEMIN`` / ``SCALEMAX`` fit the variable's CDF data type.
``swxsoc`` runs these with its own copies of the ``validrange`` and ``validscale`` checks from SpacePy's ``spacepy.pycdf.istp.VariableChecks``.
Credit for the original checks goes to the SpacePy authors . The SpacePy license is included in the ``swxsoc`` repository at `licenses/SPACEPY.rst <https://github.com/swxsoc/swxsoc/blob/main/licenses/SPACEPY.rst>`_.
The ``swxsoc`` copies differ from the originals in two ways:

- An attribute's CDF data type does not have to match the variable's data type; its value only needs to be comparable with that type.
- The allowed range for each CDF data type comes from :py:class:`~swxsoc.util.schema.SWXSchema` instead of ``spacepy.pycdf.lib``.

Using a Custom Schema
=====================

If your mission uses a custom metadata schema, pass it to ``validate`` with the ``schema`` argument:

.. code-block:: python

    from swxsoc.util.schema import SWXSchema

    my_schema = SWXSchema(global_schema_layers=["my_global_schema.yaml"])
    validation_errors = validate(cdf_file_path, schema=my_schema)

See :ref:`schema_information_guide` for how to create a custom schema.
