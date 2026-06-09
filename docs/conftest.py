"""
Configuration for pytest doctests in documentation.

This file provides automatic skipping of documentation files that require
optional dependencies like sammi-cdf when those dependencies are not installed.
"""

import pytest

# Check if sammi-cdf is available
try:
    import sammi
    HAS_SAMMI = True
except ImportError:
    HAS_SAMMI = False


def pytest_collection_modifyitems(config, items):
    """
    Automatically skip doctests in files that require sammi when it's not installed.
    
    This allows:
    - venv-base: Doc examples requiring CDF are automatically skipped
    - venv-cdf: All doc examples run and are validated
    """
    if HAS_SAMMI:
        return  # Don't skip anything if sammi is available
    
    skip_sammi = pytest.mark.skip(reason="requires sammi-cdf (install with: pip install swxsoc[cdf])")
    
    # Files that contain CDF-specific examples
    cdf_doc_files = [
        'tutorial1.rst',
        'reading_writing_data.rst',
    ]
    
    for item in items:
        # Check if this is a doctest item
        if "DoctestItem" in str(type(item)):
            # Check if the item is from a CDF-requiring doc file
            item_path = str(item.fspath)
            if any(doc_file in item_path for doc_file in cdf_doc_files):
                item.add_marker(skip_sammi)
