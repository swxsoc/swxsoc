"""
Abstract base class for SWxSOC input/output handlers.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Tuple

from ndcube import NDCollection

__all__ = ["SWXIOHandler"]


class SWXIOHandler(ABC):
    """
    Abstract base class for handling input/output operations of heliophysics data.
    """

    @abstractmethod
    def load_data(self, file_path: Path) -> Tuple[dict, dict, NDCollection, dict]:
        """
        Load data from a file.

        Parameters
        ----------
        file_path : `pathlib.Path`
            A fully specified file path of the data file to load.

        Returns
        -------
        timeseries : `dict[~astropy.time.TimeSeries]`
            An instance of `TimeSeries` containing the loaded data.
        support : `dict[astropy.nddata.NDData]`
            Non-record-varying data contained in the file
        spectra : `ndcube.NDCollection`
            Spectral or High-dimensional measurements in the loaded data.
        meta: `dict`
            Global metadata attributes.
        """
        pass

    @abstractmethod
    def save_data(self, data, file_path: Path):
        """
        Save data to a file.

        Parameters
        ----------
        data : `swxsoc.swxdata.SWXData`
            An instance of `SWXData` containing the data to be saved.
        file_path : `pathlib.Path`
            A fully specified path to the directory where the file is to be saved.
        """
        pass
