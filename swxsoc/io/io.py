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

__all__ = ["SWXIOHandler", "CDFHandler"]

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

    @staticmethod
    def _cdf_name_to_dict_key(cdf_name: str) -> str:
        """Convert CDF variable name format to dict key format.
        CDF uses underscores, dict keys use hyphens."""
        return cdf_name.replace("_", "-")

    @staticmethod
    def _dict_key_to_cdf_name(dict_key: str) -> str:
        """Convert dict key format to CDF variable name format.
        Dict keys use hyphens, CDF uses underscores."""
        return dict_key.replace("-", "_")

    def load_data(self, file_path: Path) -> Tuple[dict, dict, NDCollection, dict]:
        """
        Load heliophysics data from a CDF file.

        Parameters
        ----------
        file_path : `pathlib.Path`
            A fully specified file path to the CDF file to load.

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
        from spacepy.pycdf import CDF

        if not file_path.exists():
            raise FileNotFoundError(f"CDF Could not be loaded from path: {file_path}")

        # Create a Struct for Global Metadata
        meta = {}
        # Create a struct for storing TimeSeries
        timeseries = {}
        # Create a Data Structure for Non-record Varying Data
        support = {}
        # Intermediate Type
        spectra = []

        # Open CDF file with context manager
        with CDF(str(file_path)) as input_file:
            # Add Global Attributes from the CDF file to TimeSeries
            input_global_attrs = {}
            for attr_name in input_file.attrs:
                if len(input_file.attrs[attr_name]) == 0:
                    # gAttr is not set
                    input_global_attrs[attr_name] = ""
                elif len(input_file.attrs[attr_name]) > 1:
                    # gAttr is a List
                    input_global_attrs[attr_name] = input_file.attrs[attr_name][:]
                else:
                    # gAttr is a single value
                    input_global_attrs[attr_name] = input_file.attrs[attr_name][0]
            meta.update(input_global_attrs)

            # First Variables we need to add are time/Epoch
            # Look for variables ending with "_Epoch" (prefixed format)
            # or matching "Epoch" exactly (legacy format)
            epoch_variables = [
                var_name for var_name in input_file.keys() if "Epoch" in var_name
            ]
            
            # Check if there's a Default_Timeseries_Key metadata attribute
            # allows loader to track the non prefixed epocch var with the mandatory dict name for the timeseries dict key. 
            # This is needed to support legacy CDF files with a single timeseries that uses the unprefixed "Epoch" variable,
            # as well as multi-timeseries CDF files that use prefixed epoch variables (e.g., "REACH_165_Epoch") 
            # but also want to designate one of them as the default for unprefixed variables.
            default_ts_key = None
            if "Default_Timeseries_Key" in input_global_attrs:
                # Treat empty string as None (for single-timeseries files)
                value = input_global_attrs["Default_Timeseries_Key"]
                default_ts_key = value if value != "" else None
            
            # Build a mapping of epoch_var_name -> epoch_key (for TimeSeries dict key)
            epoch_var_to_key = {}
            for epoch_var in epoch_variables:
                if epoch_var.endswith("_Epoch"):
                    # Prefixed format: "REACH_165_Epoch" -> "REACH-165"
                    epoch_key = self._cdf_name_to_dict_key(epoch_var[:-6])  # Remove "_Epoch" suffix
                elif epoch_var == "Epoch" and default_ts_key is not None:
                    # Unprefixed "Epoch" with a default key specified in metadata
                    epoch_key = default_ts_key
                else:
                    # Legacy format: use "Epoch" as-is (for single timeseries files)
                    epoch_key = epoch_var
                epoch_var_to_key[epoch_var] = epoch_key
            
            # Build prefix mapping once (epoch_key -> CDF prefix with underscores)
            # to avoid repeated string replacements when processing variables
            epoch_key_to_prefix = {key: self._dict_key_to_cdf_name(key) for key in epoch_var_to_key.values()}
            
            # Make sure at least one Epoch variable is present in the CDF
            if len(epoch_variables) == 0:
                warn_user(
                    f"No Epoch variables found in CDF file: {file_path}"
                )
            
            # Loop for each Epoch Variable
            for epoch_var, epoch_key in epoch_var_to_key.items():
                time_data = Time(input_file[epoch_var][:].copy())
                time_attrs = self._load_metadata_attributes(input_file[epoch_var])
                # Create a new TimeSeries with the epoch_key
                timeseries[epoch_key] = TimeSeries()
                # Create the Time object
                timeseries[epoch_key]["time"] = time_data
                # Create the Metadata
                timeseries[epoch_key]["time"].meta = OrderedDict()
                timeseries[epoch_key]["time"].meta.update(time_attrs)

            # Get all the Keys for Measurement Variable Data
            # These are Keys where the underlying object is a `dict` that contains
            # additional data, and is not the `EPOCH` variable
            variable_keys = [
                var_name for var_name in input_file.keys() if "Epoch" not in var_name
            ]
            for var_name in variable_keys:
                # Extract the Variable's Metadata
                var_attrs = self._load_metadata_attributes(input_file[var_name])

                # Extract the Variable's Data
                var_data = input_file[var_name][...]
                if input_file[var_name].rv():
                    # Find the TimeSeries Epoch for this Record-Varying Variable
                    # get_timeseres_epoch_key may return "Epoch" or a dict key like "REACH-134"
                    result_key = SWXData.get_timeseres_epoch_key(
                        timeseries, var_data, var_attrs
                    )
                    # Map CDF epoch variable name to dict key if needed
                    # (e.g., "Epoch" -> "REACH-165"; "REACH-134" already converted)
                    if result_key in epoch_var_to_key:
                        epoch_key = epoch_var_to_key[result_key]
                    else:
                        # Already a dict key (from prefixed epoch or legacy fallback)
                        epoch_key = result_key
                    ts = timeseries[epoch_key]
                    
                    # Check if this variable has a prefix matching its epoch key
                    # and strip it to get the original column name - multi-timeseries code.
                    # CDF names have restrictions so real world names with hyphen conventions
                    # need to be replaced by underscores in the CDF.
                    prefix = epoch_key_to_prefix[epoch_key]
                    if var_name.startswith(f"{prefix}_"):
                        original_var_name = var_name[len(prefix) + 1:]  # Strip "prefix_"
                    else:
                        original_var_name = var_name

                    # See if it is record-varying data with UNITS
                    if "UNITS" in var_attrs and len(var_data) == len(ts["time"]):
                        # Check if the variable is multi-dimensional
                        if len(var_data.shape) > 1:
                            # Load as Spectra Data (keep original CDF name for spectra)
                            self._load_spectra_variable(
                                spectra, original_var_name, var_data, var_attrs, ts.time
                            )
                        else:
                            # Load as Record-Varying `data` (use unprefixed column name)
                            self._load_timeseries_variable(
                                ts, original_var_name, var_data, var_attrs
                            )
                    else:
                        # Load as `support` (keep original CDF name for support)
                        self._load_support_variable(
                            support, original_var_name, var_data, var_attrs
                        )
                else:
                    # Load Non-Record-Varying Data as `support` (keep original CDF name)
                    self._load_support_variable(support, var_name, var_data, var_attrs)

        # Create a NDCollection
        spectra = NDCollection(spectra)

        # Return the given TimeSeries, NRV Data, Spectra Data, Global Metadata
        return timeseries, support, spectra, meta

    def _load_metadata_attributes(self, var_data):
        var_attrs = {}
        for attr_name in var_data.attrs:
            if isinstance(var_data.attrs[attr_name], datetime):
                # Metadata Attribute is a Datetime - we want to convert to Astropy Time
                var_attrs[attr_name] = Time(var_data.attrs[attr_name])
            else:
                # Metadata Attribute loaded without modifications
                var_attrs[attr_name] = var_data.attrs[attr_name]
        return var_attrs

    def _load_timeseries_variable(self, timeseries, var_name, var_data, var_attrs):
        def _load_data(timeseries, var_name, var_data, var_attrs):
            # Create a Quantity object for the variable
            timeseries[var_name] = u.Quantity(
                value=var_data, unit=var_attrs["UNITS"], copy=False
            )
            # Create the Metadata
            timeseries[var_name].meta = OrderedDict()
            timeseries[var_name].meta.update(var_attrs)

        try:
            _load_data(timeseries, var_name, var_data, var_attrs)
        except ValueError:
            warn_user(
                f"Cannot create Quantity for Variable {var_name} with UNITS {var_attrs['UNITS']}. Creating Quantity with UNITS 'dimensionless_unscaled'."
            )
            # Swap UNITS
            var_attrs["UNITS_DESC"] = var_attrs["UNITS"]
            var_attrs["UNITS"] = u.dimensionless_unscaled.to_string()
            _load_data(timeseries, var_name, var_data, var_attrs)

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
                f"{attribute_name}{dimension_i + 1}"  # KeynameName Indexed 1-4 vs 0-3
            )
            if dimension_attr_name in var_attrs:
                attr_values.append(var_attrs[dimension_attr_name])
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
            naxis = int(var_attrs["WCSAXES"])
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
        if len(time) > 1:
            time_delta = time[1] - time[0]
        else: # If there is only one time entry, we cannot calculate a time delta. We will default to 1 ns
            time_delta = 1 * u.ns
        wcs.wcs.timedel = time_delta.to("ns").value

        return wcs

    def _load_spectra_variable(self, spectra, var_name, var_data, var_attrs, time):
        def _load_data(spectra, var_name, var_data, var_attrs, time):
            # Create a World Cordinate System for the Tensor
            var_wcs = self._get_world_coords(var_data, var_attrs, time)
            # Create a Cube
            var_cube = NDCube(
                data=var_data, wcs=var_wcs, meta=var_attrs, unit=var_attrs["UNITS"]
            )
            # Add to Spectra
            spectra.append((var_name, var_cube))

        try:
            # Create an NDCube Object for the data
            _load_data(spectra, var_name, var_data, var_attrs, time)
        except ValueError:
            warn_user(
                f"Cannot create NDCube for Spectra {var_name} with UNITS {var_attrs['UNITS']}. Creating Quantity with UNITS 'dimensionless_unscaled'."
            )
            # Swap UNITS
            var_attrs["UNITS_DESC"] = var_attrs["UNITS"]
            var_attrs["UNITS"] = u.dimensionless_unscaled.to_string()
            _load_data(spectra, var_name, var_data, var_attrs, time)

    def save_data(self, data, file_path: Path):
        """
        Save heliophysics data to a CDF file.

        Parameters
        ----------
        data : `swxsoc.swxdata.SWXData`
            An instance of `SWXData` containing the data to be saved.
        file_path : `pathlib.Path`
            A fully specified path to the directory where the CDF file is to be saved.

        Returns
        -------
        path : `pathlib.Path`
            A path to the saved file.
        """
        from spacepy.pycdf import CDF

        # Initialize a new CDF
        cdf_filename = f"{data.meta['Logical_file_id']}.cdf"
        output_cdf_filepath = str(Path(file_path) / cdf_filename)
        with CDF(output_cdf_filepath, masterpath="") as cdf_file:
            # Add Global Attriubtes to the CDF File
            self._convert_global_attributes_to_cdf(data, cdf_file)

            # Add zAttributes
            self._convert_variables_to_cdf(data, cdf_file)
        return Path(output_cdf_filepath)

    def _convert_global_attributes_to_cdf(self, data, cdf_file):
        # Loop though Global Attributes in target_dict
        for attr_name, attr_value in data.meta.items():
            # Make sure the Value is not None
            # We cannot add None Values to the CDF Global Attrs
            if attr_value is None:
                cdf_file.attrs[attr_name] = ""
            else:
                # Add the Attribute to the CDF File
                cdf_file.attrs[attr_name] = attr_value

    def _convert_variables_to_cdf(self, data, cdf_file):
        # Make sure at least one TimeSeries is present
        if len(data.data["timeseries"]) == 0:
            warn_user(
                f"No TimeSeries data found to write to CDF file: {cdf_file}"
            )

        # Detect which variable names actually conflict across timeseries
        has_multiple_timeseries = len(data.data["timeseries"]) > 1
        conflicting_vars = set()
        
        # Get the first (default) timeseries key for ISTP compliance
        # In Python 3.7+, dict iteration order is guaranteed to be insertion order
        default_epoch_key = next(iter(data.data["timeseries"].keys())) if has_multiple_timeseries else None
        
        if has_multiple_timeseries:
            # Build a dict of var_name -> list of epoch_keys that have it
            var_to_epochs = {}
            for epoch_key, ts in data.data["timeseries"].items():
                for var_name in ts.colnames:
                    # For CDF purposes, "time" becomes "Epoch"
                    cdf_name = "Epoch" if var_name == "time" else var_name
                    if cdf_name not in var_to_epochs:
                        var_to_epochs[cdf_name] = []
                    var_to_epochs[cdf_name].append(epoch_key)
            
            # A variable conflicts if it appears in more than one timeseries
            for var_name, epoch_keys in var_to_epochs.items():
                if len(epoch_keys) > 1:
                    conflicting_vars.add(var_name)
        
        # Track which conflicting variables have been written (for asymmetric prefixing)
        # First occurrence stays unprefixed, subsequent ones get prefixed
        written_conflicting_vars = set()

        for epoch_key, ts in data.data["timeseries"].items():
            # Sanitize the epoch_key for use as a prefix (replace hyphens with underscores)
            prefix = self._dict_key_to_cdf_name(epoch_key)
            
            # Determine the Epoch variable name for this timeseries
            # First timeseries uses unprefixed "Epoch" for ISTP compliance
            # Others are prefixed for uniqueness
            if has_multiple_timeseries and epoch_key != default_epoch_key:
                epoch_cdf_var_name = f"{prefix}_Epoch"
            else:
                epoch_cdf_var_name = "Epoch"
            
            # Loop through Scalar TimeSeries Variables
            for var_name in ts.colnames:
                var_data = ts[var_name]
                if var_name == "time":
                    # Add 'time' in the TimeSeries as Epoch within the CDF
                    cdf_file[epoch_cdf_var_name] = var_data.to_datetime()
                    # Add the Variable Attributes (excluding DEPEND_0 for Epoch variables)
                    self._convert_variable_attributes_to_cdf(
                        epoch_cdf_var_name, var_data, cdf_file, skip_depend_0=True
                    )
                else:
                    # Add the Variable to the CDF File
                    # For conflicting variables, use asymmetric prefixing:
                    # - First occurrence: unprefixed
                    # - Subsequent occurrences: prefixed
                    if var_name in conflicting_vars:
                        if var_name not in written_conflicting_vars:
                            # First occurrence - leave unprefixed
                            cdf_var_name = var_name
                            written_conflicting_vars.add(var_name)
                        else:
                            # Subsequent occurrence - add prefix
                            cdf_var_name = f"{prefix}_{var_name}"
                    else:
                        # Non-conflicting variables never need prefixing
                        cdf_var_name = var_name
                    cdf_file[cdf_var_name] = var_data.value
                    # Add the Variable Attributes
                    self._convert_variable_attributes_to_cdf(
                        cdf_var_name, var_data, cdf_file
                    )
                    # Set DEPEND_0 to point to the correct Epoch variable
                    cdf_file[cdf_var_name].attrs["DEPEND_0"] = epoch_cdf_var_name

        # Loop through the NDData Data Structure (Not all record-varying)
        for var_name, var_data in data.support.items():
            # Guess the data type to store
            # Documented in https://github.com/spacepy/spacepy/issues/707
            _, var_data_types, _ = self.schema._types(var_data.data)
            # Add the Variable to the CDF File
            cdf_file.new(
                name=var_name,
                data=var_data.data,
                type=var_data_types[0],
                recVary=(var_data.meta["VAR_TYPE"] == "data"),
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

    def _convert_variable_attributes_to_cdf(self, var_name, var_data, cdf_file, skip_depend_0=False):
        for var_attr_name, var_attr_val in var_data.meta.items():
            # Skip DEPEND_0 for Epoch variables (they don't need self-referencing DEPEND_0)
            if skip_depend_0 and var_attr_name == "DEPEND_0":
                continue
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
