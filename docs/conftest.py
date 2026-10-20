"""
Configuration for pytest doctests in documentation.

This file provides automatic skipping of documentation files that require
optional dependencies when those dependencies are not installed.
"""

import os
from importlib.util import find_spec

import pytest

import swxsoc
import swxsoc.db

# Check if sammi-cdf is available.
HAS_SAMMI = (
    find_spec("sammi") is not None
    and find_spec("sammi.cdf_attribute_manager") is not None
)

# SQLAlchemy is the dependency needed by the active doctest examples in the
# MetaTracker guide.
HAS_TRACKER = find_spec("sqlalchemy") is not None


@pytest.fixture(autouse=True)
def restore_mission_config():
    """
    Restore the mission configuration after each documentation doctest.

    Documentation examples select their mission explicitly with
    ``SWXSOC_MISSION`` and ``swxsoc.reconfigure()``, so this fixture keeps that
    change from leaking into later tests.
    """
    original_mission = os.environ.get("SWXSOC_MISSION")
    yield
    if original_mission is None:
        os.environ.pop("SWXSOC_MISSION", None)
    else:
        os.environ["SWXSOC_MISSION"] = original_mission
    swxsoc.reconfigure()
    if swxsoc.db.HAS_SQLALCHEMY:
        swxsoc.db.reconfigure()


def pytest_collection_modifyitems(config, items):
    """
    Automatically skip doctests whose optional dependencies are unavailable.

    Parameters
    ----------
    config : pytest.Config
        The pytest configuration object for the current test session.
    items : list of pytest.Item
        The tests collected for the current test session. Matching doctest
        items are marked as skipped when their optional dependency is absent.

    Returns
    -------
    None
        The collected items are modified in place.

    This allows:
    - venv-base: Doc examples requiring CDF are automatically skipped
    - venv-cdf: CDF doc examples run and are validated
    - venv-tracker: MetaTracker doc examples run and are validated
    """
    skip_sammi = pytest.mark.skip(
        reason="requires sammi-cdf (install with: pip install swxsoc[cdf])"
    )
    skip_tracker = pytest.mark.skip(
        reason="requires SQLAlchemy (install with: pip install swxsoc[tracker])"
    )

    # Files that contain CDF-specific examples.
    cdf_doc_files = [
        "tutorial1.rst",
        "swxdata_examples.rst",
        "reading_writing_data.rst",
        "cdf_validation.rst",
        "schema_information_guide.rst",
        "quick_reference.rst",
    ]

    # Files that contain relational database tracker examples.
    tracker_doc_files = [
        "metatracker_guide.rst",
    ]

    for item in items:
        item_path = str(getattr(item, "fspath", ""))
        if not HAS_SAMMI and any(doc_file in item_path for doc_file in cdf_doc_files):
            item.add_marker(skip_sammi)
        if not HAS_TRACKER and any(
            doc_file in item_path for doc_file in tracker_doc_files
        ):
            item.add_marker(skip_tracker)
