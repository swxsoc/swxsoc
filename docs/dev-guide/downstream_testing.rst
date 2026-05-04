.. _downstream_testing:

*******************************
Downstream Integration Testing
*******************************

Overview
========

``swxsoc`` is a foundational library shared by multiple mission packages (``hermes_core``,
``padre_meddea``, ``padre_sharp``, ``padre_craft``, ``swxsoc_reach``, ``sdc_aws_utils``,
``MetaTracker``, and others). A change that appears safe in isolation can silently break a
downstream package that relies on a public API, a shared utility, or a calibration
helper.

The **Downstream Integration Tests** workflow
(``.github/workflows/downstream-testing.yml``) guards against this by checking out each
registered downstream package, installing it against the candidate ``swxsoc`` branch, and
running its own test suite. This ensures that refactoring, removals, or interface changes
are caught *before* they land on ``main``.

When the workflow runs
======================

The workflow is intentionally **not** triggered on every commit pushed to a pull request.
Running six or more full downstream test suites on every ``git push`` would be expensive and
noisy. Instead it runs at specific, meaningful moments:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Trigger
     - When it fires
   * - ``pull_request`` — ``opened``
     - First time a PR is opened against ``main``.
   * - ``pull_request`` — ``reopened``
     - When a previously-closed PR is reopened.
   * - ``pull_request`` — ``ready_for_review``
     - When a draft PR is promoted to ready.
   * - ``pull_request`` — ``labeled``
     - **Only** when the ``run-downstream`` label is applied (see below).
   * - ``push`` to ``main``
     - After every merge to ``main`` — final confirmation that the merged code does
       not break any downstream consumer.
   * - ``workflow_dispatch``
     - Manual run from the GitHub Actions tab; supports an optional
       ``package_filter`` input to target specific packages.

Running downstream tests on a PR
=================================

Because the workflow does not run on every commit, you have two options to get a fresh
downstream result during active development on a PR.

Option 1 — Apply the ``run-downstream`` label (recommended)
------------------------------------------------------------

1. Open or navigate to the pull request on GitHub.
2. In the **Labels** sidebar, apply the ``run-downstream`` label.
3. The Downstream Integration Tests workflow triggers automatically.
4. Remove and re-apply the label if you need to rerun after additional commits.

.. note::

   Only the ``run-downstream`` label triggers the workflow.  Adding any other label has
   no effect.

Option 2 — Manual dispatch from the Actions tab
------------------------------------------------

1. Go to **Actions → Downstream Integration Tests** in the ``swxsoc`` repository.
2. Click **Run workflow**.
3. Select the branch you want to test.
4. Optionally enter a comma-separated ``package_filter`` (e.g. ``padre_meddea,padre_sharp``)
   to run a subset of packages rather than the full matrix.
5. Click **Run workflow**.

This is useful when you want to validate only a subset of downstream packages or when
your PR branch is not yet ready to be labelled.

Adding or updating a downstream package
========================================

The list of downstream packages is maintained in ``.github/downstream-packages.json``.
Each entry is a JSON object with the following keys:

.. list-table::
   :header-rows: 1
   :widths: 20 10 70

   * - Key
     - Required
     - Description
   * - ``name``
     - yes
     - Short identifier used in the matrix job name and the ``package_filter`` input.
   * - ``enabled``
     - no
     - Set to ``false`` to temporarily exclude a package from all runs without
       removing the entry.  Defaults to ``true``.
   * - ``repository``
     - yes
     - GitHub ``owner/repo`` slug for the downstream package.
   * - ``ref``
     - yes
     - Branch, tag, or SHA to check out.  Typically ``"main"``.
   * - ``python_version``
     - yes
     - Python version used to run this package's tests (e.g. ``"3.10"``).
   * - ``install_command``
     - yes
     - Shell command run from the downstream repo root to install the package and its
       test dependencies (e.g. ``"python -m pip install -e .[test]"``).
   * - ``test_command``
     - yes
     - Shell command that runs the downstream test suite (e.g.
       ``"pytest padre_meddea --tb=short -q"``).

Example entry::

    {
      "name": "my_mission_pkg",
      "enabled": true,
      "repository": "my-org/my_mission_pkg",
      "ref": "main",
      "python_version": "3.10",
      "install_command": "python -m pip install -e .[test]",
      "test_command": "pytest my_mission_pkg --tb=short -q"
    }

To temporarily skip a package without losing its configuration, set ``"enabled": false``.

Best practices
==============

* **Run before requesting a review.** Apply the ``run-downstream`` label early so that
  failure feedback arrives while you are still in active development rather than just
  before merge.
* **Check the post-merge run.** Every merge to ``main`` triggers a fresh downstream run.
  If it fails, open a follow-up PR promptly — downstream consumers will be broken against
  the published ``main`` until a fix lands.
* **Narrow the scope when iterating.** Use ``package_filter`` in a manual dispatch to
  test only the package affected by your change, saving CI minutes during rapid iteration.
* **Keep downstream refs pinned to ``main``.** This ensures the workflow always validates
  against the latest released state of each consumer rather than a stale snapshot.
