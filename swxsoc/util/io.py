from abc import ABC, abstractmethod
from pathlib import Path
from typing import Tuple
from collections import OrderedDict
from datetime import datetime
import numpy as np
from astropy.timeseries import TimeSeries
from astropy.time import Time
from astropy.nddata import NDData
from astropy.wcs import WCS
import astropy.units as u
from ndcube import NDCollection
from ndcube import NDCube
from swxsoc import log
from swxsoc.util.exceptions import warn_user
from swxsoc.util.schema import SWXSchema

__all__ = ["SWXIOHandler", "CDFHandler", "FITSHandler"]

# ================================================================================================
#                                   ABSTRACT HANDLER
# ================================================================================================


class SWXIOHandler(ABC):
    """
    Abstract base class for handling input/output operations of heliophysics data.
    """

    @abstractmethod
    def load_data(self, file_path: str) -> Tuple[TimeSeries, dict, NDCollection]:
        """
        Load data from a file.

        Parameters
        ----------
        file_path : `str`
            A fully specified file path.

        Returns
        -------
        data : `~astropy.time.TimeSeries`
            An instance of `TimeSeries` containing the loaded data.
        support : `dict[astropy.nddata.NDData]`
            Non-record-varying data contained in the file
        spectra : `ndcube.NDCollection`
            Spectral or High-dimensional measurements in the loaded data.
        """
        pass

    @abstractmethod
    def save_data(self, data, file_path: str, overwrite: bool = False) -> str:
        """
        Save data to a file.

        Parameters
        ----------
        data : `swxsoc.swxdata.SWXData`
            An instance of `SWXData` containing the data to be saved.
        file_path : `str`
            The fully specified file path to save into.
        overwrite : `bool`
            If set, overwrites existing file of the same name.

        Returns
        -------
        path : `str`
            A path to the saved file.
        """
        pass


# ================================================================================================
#                                   CDF HANDLER
# ================================================================================================


class CDFHandler(SWXIOHandler):
    """
    A concrete implementation of SWXIOHandler for handling heliophysics data in CDF format.

    This class provides methods to load and save heliophysics data from/to a CDF file.
    """

    def __init__(self):
        super().__init__()

        # CDF Schema
        self.schema = SWXSchema()

    def load_data(self, file_path: str) -> Tuple[TimeSeries, dict, NDCollection]:
        """
        Load heliophysics data from a CDF file.

        Parameters
        ----------
        file_path : `str`
            The path to the CDF file.

        Returns
        -------
        data : `~astropy.time.TimeSeries`
            An instance of `TimeSeries` containing the loaded data.
        support : `dict[astropy.nddata.NDData]`
            Non-record-varying data contained in the file
        spectra : `ndcube.NDCollection`
            Spectral or High-dimensional measurements in the loaded data.
        """
        from spacepy.pycdf import CDF

        if not Path(file_path).exists():
            raise FileNotFoundError(f"CDF Could not be loaded from path: {file_path}")

        # Create a new TimeSeries
        ts = TimeSeries()
        # Create a Data Structure for Non-record Varying Data
        support = {}
        # Intermediate Type
        spectra = []

        # Open CDF file with context manager
        with CDF(file_path) as input_file:
            # Add Global Attributes from the CDF file to TimeSeries
            input_global_attrs = {}
            for attr_name in input_file.attrs:
                if len(input_file.attrs[attr_name]) == 0:
                    # gAttr is not set
                    input_global_attrs[attr_name] = ("", "")
                elif len(input_file.attrs[attr_name]) > 1:
                    # gAttr is a List
                    input_global_attrs[attr_name] = (input_file.attrs[attr_name][:], "")
                else:
                    # gAttr is a single value
                    input_global_attrs[attr_name] = (input_file.attrs[attr_name][0], "")
            ts.meta.update(input_global_attrs)

            # First Variable we need to add is time/Epoch
            if "Epoch" in input_file:
                time_data = Time(input_file["Epoch"][:].copy())
                time_attrs = self._load_metadata_attributes(input_file["Epoch"])
                # Create the Time object
                ts["time"] = time_data
                # Create the Metadata
                ts["time"].meta = OrderedDict()
                ts["time"].meta.update(time_attrs)

            # Get all the Keys for Measurement Variable Data
            # These are Keys where the underlying object is a `dict` that contains
            # additional data, and is not the `EPOCH` variable
            variable_keys = filter(lambda key: key != "Epoch", list(input_file.keys()))
            # Add Variable Attributtes from the CDF file to TimeSeries
            for var_name in variable_keys:
                # Extract the Variable's Metadata
                var_attrs = self._load_metadata_attributes(input_file[var_name])

                # Extract the Variable's Data
                var_data = input_file[var_name][...]
                if input_file[var_name].rv():
                    # See if it is record-varying data with Units
                    if "UNITS" in var_attrs and len(var_data) == len(ts["time"]):
                        # Check if the variable is multi-dimensional
                        if len(var_data.shape) > 1:
                            try:
                                # Create an NDCube Object for the data
                                self._load_spectra_variable(
                                    spectra, var_name, var_data, var_attrs, ts.time
                                )
                            except ValueError:
                                warn_user(
                                    f"Cannot create NDCube for Spectra {var_name} with UNITS {var_attrs['UNITS']}. Creating Quantity with UNITS 'dimensionless_unscaled'."
                                )
                                # Swap Units
                                var_attrs["UNITS_DESC"] = (var_attrs["UNITS"], "")
                                var_attrs["UNITS"] = (
                                    u.dimensionless_unscaled.to_string(),
                                    "",
                                )
                                self._load_spectra_variable(
                                    spectra, var_name, var_data, var_attrs, ts.time
                                )
                        else:
                            # Load as Record-Varying `data`
                            try:
                                self._load_timeseries_variable(
                                    ts, var_name, var_data, var_attrs
                                )
                            except ValueError:
                                warn_user(
                                    f"Cannot create Quantity for Variable {var_name} with UNITS {var_attrs['UNITS']}. Creating Quantity with UNITS 'dimensionless_unscaled'."
                                )
                                # Swap Units
                                var_attrs["UNITS_DESC"] = (var_attrs["UNITS"], "")
                                var_attrs["UNITS"] = (
                                    u.dimensionless_unscaled.to_string(),
                                    "",
                                )
                                self._load_timeseries_variable(
                                    ts, var_name, var_data, var_attrs
                                )
                    else:
                        # Load as `support`
                        self._load_support_variable(
                            support, var_name, var_data, var_attrs
                        )
                else:
                    # Load Non-Record-Varying Data as `support`
                    self._load_support_variable(support, var_name, var_data, var_attrs)

        # Create a NDCollection
        if len(spectra) > 0:
            # Implement assertion that all spectra are aligned along time-varying dimension
            aligned_axes = tuple(0 for _ in spectra)
            spectra = NDCollection(spectra, aligned_axes=aligned_axes)
        else:
            spectra = NDCollection(spectra)

        # Return the given TimeSeries, NRV Data, NDCollection (spectra)
        return ts, support, spectra

    def _load_metadata_attributes(self, var_data):
        var_attrs = {}
        for attr_name in var_data.attrs:
            if isinstance(var_data.attrs[attr_name], datetime):
                # Metadata Attribute is a Datetime - we want to convert to Astropy Time
                var_attrs[attr_name] = (Time(var_data.attrs[attr_name]), "")
            else:
                # Metadata Attribute loaded without modifications
                var_attrs[attr_name] = (var_data.attrs[attr_name], "")
        return var_attrs

    def _load_timeseries_variable(self, ts, var_name, var_data, var_attrs):
        # Create the Quantity object
        var_units, _ = var_attrs["UNITS"]
        var_data = u.Quantity(value=var_data, unit=var_units, copy=False)
        ts[var_name] = var_data
        # Create the Metadata
        ts[var_name].meta = OrderedDict()
        ts[var_name].meta.update(var_attrs)

    def _load_support_variable(self, support, var_name, var_data, var_attrs):
        # Create a NDData entry for the variable
        support[var_name] = NDData(data=var_data, meta=var_attrs)

    def _get_tensor_attribute(
        self, var_attrs, naxis, attribute_name, default_attribute
    ):
        """
        Function to get the `attribute_name` for each dimension of a multi-dimensional variable.

        For example if we have variable 'des_dist_brst' and we want to get the `.cunit` member
        for the WCS corresponding to the 'CUNIT' Keyword Attribute:
        - 'CUNIT1': 'eV'    (DEPEND_3: 'mms1_des_energy_brst')
        - 'CUNIT2': 'deg'   (DEPEND_2: 'mms1_des_theta_brst')
        - 'CUNIT3': 'deg'   (DEPEND_1: 'mms1_des_phi_brst' )
        - 'CUNIT4': 'ns'    (DEPEND_0: 'Epoch')

        We want to return a list of these units:
        ['eV', 'deg', 'deg', 'ns']
        """
        # Get `attribute_name` for each of the dimensions
        attr_values = []
        for dimension_i in range(naxis):
            dimension_attr_name = (
                f"{attribute_name}{dimension_i+1}"  # KeynameName Indexed 1-4 vs 0-3
            )
            if dimension_attr_name in var_attrs:
                dimension_attr_val, _ = var_attrs[dimension_attr_name]
                attr_values.append(dimension_attr_val)
            else:
                attr_values.append(default_attribute)

        return attr_values

    def _get_world_coords(self, var_data, var_attrs, time):
        # Define WCS transformations in an astropy WCS object.

        # Get the N in var_attrs:
        if "WCSAXES" in var_attrs:
            # NOTE We have to cast this to an INT because spacepy does not let us directly set a
            # zAttr type when writing a variable attribute to a CDF. It tries to guess the type
            # of the attribute based on they type of the data.
            naxis, _ = var_attrs["WCSAXES"]
            naxis = int(naxis)
        else:
            naxis = len(var_data.shape)
        wcs = WCS(naxis=naxis)

        for keyword, prop, default in self.schema.wcs_keyword_to_astropy_property:
            prop_value = self._get_tensor_attribute(
                var_attrs=var_attrs,
                naxis=naxis,
                attribute_name=keyword,
                default_attribute=default,
            )
            setattr(wcs.wcs, prop, prop_value)

        # wcs.wcs.ctype = 'WAVE', 'HPLT-TAN', 'HPLN-TAN'
        # wcs.wcs.cunit = 'keV', 'deg', 'deg'
        # wcs.wcs.cdelt = 0, 0, 0
        # wcs.wcs.crpix = 0, 0, 01
        # wcs.wcs.crval = 0, 0, 0
        # wcs.wcs.cname = 'wavelength', 'HPC lat', 'HPC lon'

        # TIME ATTRIBUTES
        wcs.wcs.timesys = "UTC"
        # Set the MJDREF (Modified  Julian Date Reference) to the start of the TimeSeries
        # An unexpected (feature?) of the WCS API is that MJDREF is an vector
        # attribute rather than a scalar attribute
        wcs.wcs.mjdref = [time[0].mjd, 0]
        wcs.wcs.timeunit = "ns"
        time_delta = time[1] - time[0]
        wcs.wcs.timedel = time_delta.to("ns").value

        return wcs

    def _load_spectra_variable(self, spectra, var_name, var_data, var_attrs, time):
        # Create a World Cordinate System for the Tensor
        var_wcs = self._get_world_coords(var_data, var_attrs, time)
        # Create a Cube
        var_cube = NDCube(
            data=var_data, wcs=var_wcs, meta=var_attrs, unit=var_attrs["UNITS"]
        )
        # Add to Spectra
        spectra.append((var_name, var_cube))

    def save_data(self, data, file_path: str, overwrite: bool = False):
        """
        Save heliophysics data to a CDF file.

        Parameters
        ----------
        data : `swxsoc.swxdata.SWXData`
            An instance of `SWXData` containing the data to be saved.
        file_path : `str`
            The path to save the CDF file.
        overwrite : `bool`
            If set, overwrites existing file of the same name.

        Returns
        -------
        path : `str`
            A path to the saved file.
        """
        from spacepy.pycdf import CDF

        # if overwrite is set, remove the file if it exists
        if overwrite:
            logical_file_id, _ = data.meta["Logical_file_id"]
            cdf_file_path = Path(file_path) / (logical_file_id + ".cdf")
            if cdf_file_path.exists():
                cdf_file_path.unlink()

        # Initialize a new CDF
        logical_file_id, _ = data.meta["Logical_file_id"]
        cdf_filename = f"{logical_file_id}.cdf"
        output_cdf_filepath = str(Path(file_path) / cdf_filename)
        with CDF(output_cdf_filepath, masterpath="") as cdf_file:
            # Add Global Attriubtes to the CDF File
            self._convert_global_attributes_to_cdf(data, cdf_file)

            # Add zAttributes
            self._convert_variables_to_cdf(data, cdf_file)
        return output_cdf_filepath

    def _convert_global_attributes_to_cdf(self, data, cdf_file):
        # Loop though Global Attributes in target_dict
        # Ignore the Comment Values in CDF Format
        for attr_name, attr_contents in data.meta.items():
            # Unpack the Contents into a (value, comment) tuple
            if not isinstance(attr_contents, tuple) or len(attr_contents) != 2:
                raise ValueError(
                    f"Cannot Add gAttr: {attr_name}. Content was '{str(attr_contents)}'. Content must be a tuple of (value, comment)"
                )
            else:
                # Ignore the Comment Values in CDF Format
                (attr_value, attr_comment) = attr_contents

            # Make sure the Value is not None
            # We cannot add None Values to the CDF Global Attrs
            if attr_value is None:
                cdf_file.attrs[attr_name] = ""
            else:
                # Add the Attribute to the CDF File
                cdf_file.attrs[attr_name] = attr_value

    def _convert_variables_to_cdf(self, data, cdf_file):
        # Loop through Scalar TimeSeries Variables
        for var_name in data.timeseries.colnames:
            var_data = data.timeseries[var_name]
            if var_name == "time":
                # Add 'time' in the TimeSeries as 'Epoch' within the CDF
                cdf_file["Epoch"] = var_data.to_datetime()
                # Add the Variable Attributes
                self._convert_variable_attributes_to_cdf("Epoch", var_data, cdf_file)
            else:
                # Add the Variable to the CDF File
                cdf_file[var_name] = var_data.value
                # Add the Variable Attributes
                self._convert_variable_attributes_to_cdf(var_name, var_data, cdf_file)

        # Loop through Non-Record-Varying Data
        for var_name, var_data in data.support.items():
            # Guess the data type to store
            # Documented in https://github.com/spacepy/spacepy/issues/707
            _, var_data_types, _ = self.schema._types(var_data.data)
            # Add the Variable to the CDF File
            cdf_file.new(
                name=var_name,
                data=var_data.data,
                type=var_data_types[0],
                recVary=False,
            )

            # Add the Variable Attributes
            self._convert_variable_attributes_to_cdf(var_name, var_data, cdf_file)

        # Loop through High-Dimensional/Spectra Variables
        for var_name in data.spectra:
            var_data = data.spectra[var_name]
            # Add the Variable to the CDF File
            cdf_file[var_name] = var_data.data
            # Add the Variable Attributes
            self._convert_variable_attributes_to_cdf(var_name, var_data, cdf_file)

    def _convert_variable_attributes_to_cdf(self, var_name, var_data, cdf_file):
        # Loop though Variable Attributes in target_dict
        for var_attr_name, var_attr_contents in var_data.meta.items():
            # Unpack the Contents into a (value, comment) tuple
            if not isinstance(var_attr_contents, tuple) or len(var_attr_contents) != 2:
                raise ValueError(
                    f"Variable {var_name}: Cannot Add vAttr: {var_attr_name}. Content was '{str(var_attr_contents)}'. Content must be a tuple of (value, comment)"
                )
            else:
                # Ignore the Comment Values in CDF Format
                (var_attr_val, var_attr_comment) = var_attr_contents

            # Make sure the Value is not None
            # We cannot add None Values to the CDF Attrs
            if var_attr_val is None:
                raise ValueError(
                    f"Variable {var_name}: Cannot Add vAttr: {var_attr_name}. Value was {str(var_attr_val)}"
                )
            elif isinstance(var_attr_val, Time):
                # Convert the Attribute to Datetime before adding to CDF File
                cdf_file[var_name].attrs[var_attr_name] = var_attr_val.to_datetime()
            else:
                # Add the Attribute to the CDF File
                cdf_file[var_name].attrs[var_attr_name] = var_attr_val


# ================================================================================================
#                                   FITS HANDLER
# ================================================================================================


class FITSHandler(SWXIOHandler):
    """
    A concrete implementation of SWXIOHandler for handling heliophysics data in FITS format.

    This class provides methods to load and save heliophysics data from/to a FITS file.
    """

    def __init__(self):
        super().__init__()

        # CDF Schema
        self.schema = SWXSchema()

    def load_data(self, file_path: str) -> Tuple[TimeSeries, dict, NDCollection]:
        """
        Load heliophysics data from a FITS file.

        Parameters
        ----------
        file_path : `str`
            The path to the FITS file.

        Returns
        -------
        data : `~astropy.time.TimeSeries`
            An instance of `TimeSeries` containing the loaded data.
        support : `dict[astropy.nddata.NDData]`
            Non-record-varying data contained in the file
        spectra : `ndcube.NDCollection`
            Spectral or High-dimensional measurements in the loaded data.
        """
        from astropy.io import fits

        if not Path(file_path).exists():
            raise FileNotFoundError(f"CDF Could not be loaded from path: {file_path}")

        # Load the FITS File
        with fits.open(file_path) as hdul:

            # Global Metadata of Cards (Comes from PrimaryHDU for each file, hdul[0])
            global_meta = {}
            # Create a new TimeSeries
            ts = TimeSeries()
            # Create a Data Structure for Non-record Varying Data
            support = {}
            # Intermediate Type
            spectra = []

            header_summary = hdul.info(output=False)
            # Loop through eah Header Data Unit (HDU) in the FITS File
            for (
                _ix,
                _name,
                _version,
                _type,
                _n_cards,
                _dimensions,
                _format,
                _,
            ) in header_summary:
                # Get the Header Data Unit at the given Index
                hdu = hdul[_ix]

                # Cannot use `match` case while we're supporting Python 3.9
                if _type == "PrimaryHDU":

                    # Get the Global Metadata from HDU
                    header_meta = FITSHandler.header_to_dict(hdu.header)
                    # Update the Global Metadata
                    global_meta.update(header_meta)

                    # Check for a Data Array in the HDU
                    if len(hdu.shape) > 0:
                        self._load_image_data(
                            spectra, _name, _type, hdu.data, hdu.header
                        )
                    else:
                        self._load_image_data(
                            spectra, _name, _type, np.array([]), hdu.header
                        )
                elif _type == "ImageHDU":
                    pass
                elif _type == "TableHDU":
                    pass
                elif _type == "BinTableHDU":
                    pass
                elif _type == "GroupsHDU":
                    pass
                else:
                    raise ValueError(f"Unknown HDU Type: {_type}")

            # Update TimeSeries Global Metadata
            ts.meta = global_meta.copy()

            # Create a NDCollection
            if len(spectra) > 0:
                # Implement assertion that all spectra are aligned along time-varying dimension
                aligned_axes = tuple(0 for _ in spectra)
                spectra = NDCollection(spectra, aligned_axes=aligned_axes)
            else:
                spectra = NDCollection(spectra)

            # Return the given TimeSeries, NRV Data, NDCollection (spectra)
            return ts, support, spectra

    @staticmethod
    def header_to_dict(header):
        """
        Convert a FITS header to a dictionary.

        Parameters
        ----------
        header : `astropy.io.fits.Header`
            A FITS header object.

        Returns
        -------
        header_dict : `dict`
            A dictionary representation of the FITS header.
        """
        from astropy.io import fits

        header_dict = {}
        for attr_name in header:
            # Check if it's a special Commentary Card Attribute (e.g. COMMENT, HISTORY)
            if isinstance(header[attr_name], fits.header._HeaderCommentaryCards):
                header_dict[attr_name] = (
                    # We need to replace New Lines with Empty Strings
                    # This is how astropy handles multiple entries in the commentary card
                    str(header.get(attr_name)).replace("\n", ""),
                    header.comments[attr_name],
                )
            else:
                header_dict[attr_name] = (
                    header.get(attr_name),
                    header.comments[attr_name],
                )
        return header_dict

    def _load_image_data(self, spectra, var_name, var_type, var_data, var_header):
        # Create a WCS Object from the Image HDU Header
        var_wcs = WCS(header=var_header)

        # Try to Extract the UNITS from the HDU header
        var_units = (
            var_header["UNITS"]
            if "UNITS" in var_header
            else u.dimensionless_unscaled.to_string()
        )

        # Collect Metadata Info as a Dict
        var_attrs = FITSHandler.header_to_dict(var_header)

        # Create a Cube
        var_cube = NDCube(data=var_data, wcs=var_wcs, meta=var_attrs, unit=var_units)
        # Add to Spectra
        spectra.append((var_name, var_cube))

    def save_data(self, data, file_path: str, overwrite: bool = False):
        """
        Save heliophysics data to a FITS file.

        Parameters
        ----------
        data : `swxsoc.swxdata.SWXData`
            An instance of `SWXData` containing the data to be saved.
        file_path : `str`
            The path to save the FITS file.
        overwrite : `bool`
            If set, overwrites existing file of the same name.

        Returns
        -------
        path : `str`
            A path to the saved file.
        """
        from astropy.io import fits

        # if overwrite is set, remove the file if it exists
        if overwrite:
            # logical_file_id, _ = data.meta["Logical_file_id"]
            fits_file_path = Path(file_path)
            if fits_file_path.exists():
                fits_file_path.unlink()

        # Initialize a new FITS File
        # logical_file_id, _ = data.meta["Logical_file_id"]
        # cdf_filename = f"{logical_file_id}.cdf"
        output_fits_filepath = str(Path(file_path))

        # Create a new HDU List
        hdul = fits.HDUList()

        # Create a Primary HDU
        # Create a Header
        header = fits.Header()
        # Add Global Attributes to the FITS Header
        for attr_name, (attr_value, attr_comment) in data["PRIMARY"].meta.items():
            header[attr_name] = (attr_value, attr_comment)
        # Compile to a Primary HDU
        primary_hdu = fits.PrimaryHDU(data=data["PRIMARY"].data, header=header)

        # Create a HDU List containing all HDUs
        hdul.append(primary_hdu)

        # Write The HDUL to a FITS File
        hdul.writeto(output_fits_filepath)

        return output_fits_filepath
