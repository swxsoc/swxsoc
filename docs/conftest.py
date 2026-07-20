"""
Configuration for pytest doctests in documentation.

This file provides automatic skipping of documentation files that require
optional dependencies like sammi-cdf when those dependencies are not installed.
"""

import pytest

# Check if sammi-cdf is available
try:
    from sammi.cdf_attribute_manager import CdfAttributeManager 
    HAS_SAMMI = True
except Exception:
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
        'CITATION.rst',
        'README.rst',
        'index.rst',
        'tour.rst',
        'retrieving_data.rst',
        'logger.rst',
        'fillval_and_masks.rst',
        'recording_to_timestream.rst',
        'grafana_annotation_management.rst',
        'customization.rst',
        'cdf_format_guide.rst',
        'schema_information_guide.rst',
        'changelog.rst',
        'downstream_testing.rst',
        'maintainer_workflow.rst',
        'dev_env.rst',
        'config.rst',
        'tests.rst',
        'docs.rst',
        'code_standards.rst',
    ]
    
    for item in items:
         item_path = str(getattr(item, "fspath", ""))
         if any(doc_file in item_path for doc_file in cdf_doc_files):
             item.add_marker(skip_sammi)
