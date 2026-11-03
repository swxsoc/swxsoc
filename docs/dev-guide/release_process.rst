.. _release_process:

***************
Release Process
***************

This page describes how to create an official ``swxsoc`` release.
Releases should be made frequently, as changes add up.
Publishing a GitHub release builds the source distribution and uploads it to PyPI.

Versioning
==========

``swxsoc`` uses `semantic versioning <https://semver.org/>`_, which is a three-part version number (Major.Minor.Patch).

* Increment the Major version for incompatible changes to the public API.
* Increment the Minor version for new functionality that is backwards compatible.
* Increment the Patch version for backwards compatible bug fixes.

While the Major version is ``0``, the API is not yet stable, so a Minor version increment may include breaking changes.

The version number is derived from git tags by `setuptools_scm <https://setuptools-scm.readthedocs.io/>`_.
It is written to :file:`swxsoc/_version.py` at build time and is not stored in the source code, so there is no version string to edit by hand.
Commits that are not tagged get a development version based on the most recent tag, such as ``0.0.3.dev84+g82aa303``.

Tagging
=======

A `tag <https://git-scm.com/book/en/v2/Git-Basics-Tagging>`_ is a marker at a particular commit in the git history.
``swxsoc`` uses lightweight tags with the bare version number and no ``v`` prefix.
For example, the tag is ``0.0.3``, not ``v0.0.3``.

Tag the commit on ``main`` that you want to release, then push the tag to the server:

.. code-block:: bash

    git checkout main
    git pull origin main
    git tag 0.0.3
    git push origin 0.0.3

Push the tag before creating the GitHub release, so that the release points to the intended commit.

Preparing a Release
===================

Complete these steps on ``main`` before tagging.

#. Confirm that the continuous integration workflows pass on ``main``: ``testing.yml``, ``test_core.yml``, ``docs.yml``, and ``codestyle.yml``.
#. Review the results of the :ref:`downstream_testing` workflow, which runs after each merge to ``main``.
#. Update :file:`CHANGELOG.rst`, as described below.
#. Merge the changelog update into ``main``, then tag the release.

Updating the Changelog
----------------------

The changelog is read by users, so write entries for SWxSOC users rather than for internal changes.

#. Rename the ``Latest`` heading to the new version and release date, using the format ``0.0.3 (YYYY-MM-DD)``.
#. Add a new ``Latest`` section above it for future changes.

For example:

.. code-block:: rst

    0.0.3 (YYYY-MM-DD)
    ==================

    * Describe the change for SWxSOC users.

Building and Checking the Package
=================================

The release workflow uploads only the source distribution (:file:`.tar.gz`) to PyPI.
Installing from PyPI builds the wheel from that source distribution.

To build and check the package locally, install the build tools and run the build from the root of the repository:

.. code-block:: bash

    python -m pip install build twine
    python -m build
    twine check dist/*

``python -m build`` writes a source distribution and a wheel to :file:`dist/`.
Only the source distribution is published, but the wheel is useful for a local install test.
Check the contents of the source distribution to confirm that it includes the expected files:

.. code-block:: bash

    tar -tzf dist/swxsoc-*.tar.gz

Publishing a Release
====================

Testing on TestPyPI (optional)
------------------------------

The **Build and upload to TestPyPI** workflow (:file:`test-release.yml`) runs on demand from the **Actions** tab.
It builds a source distribution from the selected branch and uploads it to `TestPyPI <https://test.pypi.org/>`_.
Each version can be uploaded only once, so a re-run for the same commit fails.

To install the uploaded version from TestPyPI, use the extra index so that dependencies resolve from PyPI:

.. code-block:: bash

    python -m pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ swxsoc==0.0.3

Creating the GitHub Release
---------------------------

#. On GitHub, go to **Releases** and click **Draft a new release**.
#. Select the tag that you pushed, for example ``0.0.3``.
#. Set the release title to the tag, for example ``0.0.3``.
#. In the description, add a heading with the tag and the release date, such as ``# 0.0.3 (YYYY-MM-DD)``.
   Below the heading, copy the matching entry from :file:`CHANGELOG.rst`.
#. Click **Publish release**.

What Happens on Publish
-----------------------

Publishing the release triggers the **Build and upload to PyPI** workflow (:file:`release.yml`), which runs two jobs:

* ``build_sdist`` builds the source distribution with ``python -m build --sdist`` on Python 3.11, checks it with ``twine check``, and uploads it as a workflow artifact.
* ``pypi-publish`` downloads the artifact and uploads it to PyPI through the ``pypi`` GitHub environment.

Verifying the Release
---------------------

#. Check the **Actions** tab to confirm that the **Build and upload to PyPI** run succeeded.
#. Confirm that the new version appears on `PyPI <https://pypi.org/project/swxsoc/>`_.
#. In a fresh environment, install the new version and check it:

   .. code-block:: bash

       python -m pip install swxsoc==0.0.3
       python -c "import swxsoc; print(swxsoc.__version__)"

Fixing a Bad Release
====================

PyPI does not allow a file to be uploaded again under the same version, even after it is deleted.
To fix a problem, release a new Patch version with the fix.
If a release is broken, yank it on PyPI so that new installs do not select it by default.
