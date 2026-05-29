"""
Input/output handlers for SWxSOC heliophysics data.

This package provides the abstract :class:`SWXIOHandler` interface together
with concrete file-format handlers (currently :class:`CDFHandler` for CDF
files) and shared utilities such as :mod:`swxsoc.io.fillval` for converting
between in-memory NaN/mask representations and on-disk FILLVAL sentinels.
"""

from swxsoc.io.base_handler import SWXIOHandler
from swxsoc.io.cdf_handler import CDFHandler

__all__ = ["SWXIOHandler", "CDFHandler"]
