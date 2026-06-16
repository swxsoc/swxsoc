from abc import ABC, abstractmethod
from pathlib import Path
from typing import Tuple
from collections import OrderedDict
from datetime import datetime
from astropy.timeseries import TimeSeries
from astropy.time import Time
from astropy.nddata import NDData
from astropy.wcs import WCS
import astropy.units as u
from ndcube import NDCollection
from ndcube import NDCube
from swxsoc.swxdata import SWXData
from swxsoc.util.exceptions import warn_user
from swxsoc.util.schema import SWXSchema

__all__ = ["SWXIOHandler"]

# ================================================================================================
#                                   ABSTRACT HANDLER
# ================================================================================================


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

