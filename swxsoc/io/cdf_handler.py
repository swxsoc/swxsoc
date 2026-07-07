"""
CDF (Common Data Format) implementation of :class:`SWXIOHandler`.

Handles reading SWxSOC-style CDF files into the SWXData container and writing
SWXData instances back out to CDF.
"""

from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import astropy.units as u
import numpy as np
from astropy.nddata import NDData
from astropy.time import Time
from astropy.timeseries import TimeSeries
from astropy.utils.masked import Masked
from astropy.wcs import WCS
from ndcube import NDCollection, NDCube
from spacepy import pycdf

import swxsoc
from swxsoc.io import fillval as fv
from swxsoc.io.base_handler import SWXIOHandler
from swxsoc.swxdata import SWXData
from swxsoc.util import const
from swxsoc.util.exceptions import warn_user
from swxsoc.util.schema import SWXSchema

__all__ = ["CDFHandler"]


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
    def _sanitize_epoch_key(epoch_key: str) -> str:
        # Current naming convention reserves "_" as the CDF-safe substitute for "-".
        return epoch_key.replace("-", "_")

    @staticmethod
    def _desanitize_epoch_key(epoch_key: str) -> str:
        # Reverse mapping for SWxSOC-generated names; assumes "_" originated from "-".
        return epoch_key.replace("_", "-")

    @staticmethod
    def _is_epoch_variable_name(var_name: str) -> bool:
        return var_name == "Epoch" or var_name.endswith("_Epoch")

    def _get_cdf_epoch_var_name(self, epoch_key: str) -> str:
        default_timeseries_key = swxsoc.config["general"]["default_timeseries_key"]
        if epoch_key == default_timeseries_key:
            return "Epoch"
        return f"{self._sanitize_epoch_key(epoch_key)}_Epoch"

    def _get_epoch_key_from_cdf_epoch_var(self, epoch_var_name: str) -> str:
        if epoch_var_name == "Epoch":
            return swxsoc.config["general"]["default_timeseries_key"]
        return self._desanitize_epoch_key(epoch_var_name[: -len("_Epoch")])

    def _get_cdf_data_var_name(self, epoch_key: str, var_name: str) -> str:
        default_timeseries_key = swxsoc.config["general"]["default_timeseries_key"]
        if epoch_key == default_timeseries_key:
            return var_name
        return f"{self._sanitize_epoch_key(epoch_key)}_{var_name}"

    def _strip_epoch_prefix(self, var_name: str, epoch_key: str) -> str:
        default_timeseries_key = swxsoc.config["general"]["default_timeseries_key"]
        if epoch_key == default_timeseries_key:
            return var_name
        prefix = self._sanitize_epoch_key(epoch_key)
        if var_name.startswith(f"{prefix}_"):
            return var_name[len(prefix) + 1 :]
        return var_name

    # ================================================================================================
    #                                   CDF READER
    # ================================================================================================

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

        if not file_path.exists():
            raise FileNotFoundError(f"CDF Could not be loaded from path: {file_path}")

        # Create a Struct for Global Metadata
        meta = {}
        # Create a struct for storing TimeSeries
        timeseries = {}
        default_timeseries_key = swxsoc.config["general"]["default_timeseries_key"]
        # Create a Data Structure for Non-record Varying Data
        support = {}
        # Intermediate Type
        spectra = []

        # Open CDF file with context manager
        with pycdf.CDF(str(file_path)) as input_file:
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
            epoch_variables = [
                var_name
                for var_name in input_file.keys()
                if self._is_epoch_variable_name(var_name)
            ]
            epoch_var_to_key = {
                epoch_var: self._get_epoch_key_from_cdf_epoch_var(epoch_var)
                for epoch_var in epoch_variables
            }
            # Make sure the Default "Epoch" is present in the CDF
            if len(epoch_variables) == 0:
                warn_user(
                    "Epoch Variable "
                    f"{swxsoc.config['general']['default_timeseries_key']} "
                    f"not found in CDF file: {file_path}"
                )
            # Loop for each Epoch Variable
            for epoch_var in epoch_variables:
                time_data = self._load_epoch_variable(input_file, epoch_var)
                time_attrs = self._load_metadata_attributes(input_file[epoch_var])
                epoch_key = epoch_var_to_key[epoch_var]
                # Create a new TimeSeries
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
                var_name
                for var_name in input_file.keys()
                if not self._is_epoch_variable_name(var_name)
            ]
            epoch_prefix_to_key = {
                self._sanitize_epoch_key(epoch_key): epoch_key
                for epoch_key in timeseries.keys()
                # Keep default epoch data variables unprefixed to preserve
                # backward compatibility with existing CDF files.
                if epoch_key != default_timeseries_key
            }
            for var_name in variable_keys:
                # Extract the Variable's Metadata
                var_attrs = self._load_metadata_attributes(input_file[var_name])

                # Extract the Variable's Data
                var_data: np.ndarray = input_file[var_name][...]
                if input_file[var_name].rv():
                    column_name = var_name
                    epoch_key = None

                    depend_0 = var_attrs.get("DEPEND_0")
                    if isinstance(depend_0, str) and depend_0 in epoch_var_to_key:
                        epoch_key = epoch_var_to_key[depend_0]
                        # For SWxSOC-written files, non-default epoch data
                        # variables are prefixed by sanitized epoch key.
                        column_name = self._strip_epoch_prefix(var_name, epoch_key)
                    else:
                        for prefix, ts_epoch_key in epoch_prefix_to_key.items():
                            if var_name.startswith(f"{prefix}_"):
                                epoch_key = ts_epoch_key
                                column_name = self._strip_epoch_prefix(
                                    var_name, ts_epoch_key
                                )
                                break

                    # Find the TimeSeries Epoch for this Record-Varying Variable
                    if epoch_key is None:
                        epoch_key = SWXData.get_timeseres_epoch_key(
                            timeseries, var_data, var_attrs
                        )
                    ts = timeseries[epoch_key]

                    # See if it is record-varying data with UNITS
                    if "UNITS" in var_attrs and len(var_data) == len(ts["time"]):
                        # Check if the variable is multi-dimensional
                        if len(var_data.shape) > 1:
                            # Load as Spectra Data
                            self._load_spectra_variable(
                                spectra, column_name, var_data, var_attrs, ts.time
                            )
                        else:
                            # Load as Record-Varying `data`
                            self._load_timeseries_variable(
                                ts, column_name, var_data, var_attrs
                            )
                    else:
                        # Load as `support`
                        self._load_support_variable(
                            support, column_name, var_data, var_attrs
                        )
                else:
                    # Load Non-Record-Varying Data as `support`
                    self._load_support_variable(support, var_name, var_data, var_attrs)

        # Create a NDCollection
        spectra = NDCollection(spectra)

        # Return the given TimeSeries, NRV Data, Spectra Data, Global Metadata
        return timeseries, support, spectra, meta

    def _load_metadata_attributes(self, var_data: pycdf.Var) -> Dict[str, Any]:
        """
        Load the variable-level (zVariable) attributes from a CDF variable.

        Datetime values are converted to :class:`~astropy.time.Time`; all other
        attribute values are returned unchanged.

        For time-variable ``FILLVAL`` attributes, keep the canonical ISTP
        numeric sentinel value instead of converting the CDF library's
        datetime display representation to ``Time``. This preserves write/read
        idempotency for load -> save round trips.

        Parameters
        ----------
        var_data : `spacepy.pycdf.Var`
            The CDF variable whose ``.attrs`` mapping should be read.

        Returns
        -------
        var_attrs : dict[str, Any]
            Mapping from attribute name to attribute value, with any
            :class:`datetime.datetime` values promoted to
            :class:`~astropy.time.Time`.
        """
        var_attrs: Dict[str, Any] = {}
        for attr_name in var_data.attrs:
            if attr_name == "FILLVAL" and var_data.type() in self.schema.timetypes:
                var_attrs[attr_name] = fv.get_fillval(var_data.type())
            elif isinstance(var_data.attrs[attr_name], datetime):
                # Metadata Attribute is a Datetime - we want to convert to Astropy Time
                var_attrs[attr_name] = Time(var_data.attrs[attr_name])
            else:
                # Metadata Attribute loaded without modifications
                var_attrs[attr_name] = var_data.attrs[attr_name]
        return var_attrs

    def _load_epoch_variable(self, input_file: pycdf.CDF, epoch_var: str) -> Time:
        """
        Read an Epoch variable from a CDF file as an astropy :class:`~astropy.time.Time`.

        The underlying raw numeric values (``int64`` for ``TT2000``, ``float64``
        for ``EPOCH``) are inspected for the ISTP fill sentinel.  Positions
        equal to the sentinel are marked using ``Time``'s native masking so the
        result remains a :class:`~astropy.time.Time` compatible with
        :class:`~astropy.timeseries.TimeSeries`.

        Parameters
        ----------
        input_file : `spacepy.pycdf.CDF`
            The open CDF file to read from.
        epoch_var : str
            The name of the Epoch variable to load.

        Returns
        -------
        time_data : `~astropy.time.Time`
            The Epoch column as an astropy ``Time``.  If any value equals the
            ISTP fill sentinel, ``time_data.masked`` is ``True`` and
            ``time_data.mask`` is set at those positions.
        """
        # Datetime-converted values for building Time
        time_data = Time(input_file[epoch_var][:].copy())
        # Raw numeric values (int64 for TT2000, float64 for EPOCH,
        # complex128 for EPOCH16) for sentinel detection.
        raw_values = input_file.raw_var(epoch_var)[:]
        raw_arr = np.asarray(raw_values)
        # Determine the FILLVAL sentinel for this Epoch data type
        if raw_arr.dtype == np.int64:
            sentinel = fv.get_fillval(cdf_type=const.CDF_TIME_TT2000.value)
        elif raw_arr.dtype == np.float64:
            sentinel = fv.get_fillval(cdf_type=const.CDF_EPOCH.value)
        else:
            sentinel = None

        # If not FILLVAL is defined for this data type, return the Time as-is without masking
        if sentinel is None:
            return time_data

        mask = raw_arr == sentinel
        # If there is no mask, return the Time as-is without masking
        if not mask.any():
            return time_data

        # Use astropy.Time's native masking (compatible with TimeSeries).
        time_data[mask] = np.ma.masked
        return time_data

    def _load_timeseries_variable(
        self,
        timeseries: TimeSeries,
        var_name: str,
        var_data: np.ndarray,
        var_attrs: Dict[str, Any],
    ) -> None:
        """
        Add a record-varying scalar variable to a :class:`~astropy.timeseries.TimeSeries`.

        The variable is wrapped in an :class:`~astropy.units.Quantity` using the
        ``UNITS`` zAttribute.  Fill values are converted to a boolean mask using
        the ``FILLVAL`` attribute; for floating-point data, fill positions are
        also normalized to ``NaN`` and the column is wrapped in
        :class:`~astropy.utils.masked.Masked` when any mask bit is set.
        Falls back to ``dimensionless_unscaled`` (with a warning) if the
        ``UNITS`` string cannot be parsed.

        Parameters
        ----------
        timeseries : `~astropy.timeseries.TimeSeries`
            The target TimeSeries to add the column to.  Modified in place.
        var_name : str
            The column / variable name.
        var_data : `numpy.ndarray`
            The raw array of values read from the CDF.
        var_attrs : dict[str, Any]
            The variable-level metadata, including ``UNITS`` and (optionally)
            ``FILLVAL``.

        Returns
        -------
        None
        """
        # Get the FILLVAL MASK and apply FILLVAL -> NaN for float dtypes
        fillval = var_attrs.get("FILLVAL")
        mask = fv.compute_fill_mask(var_data, fillval)
        if fv.is_float_dtype(var_data):
            var_data = fv.apply_fillval_to_nan(var_data, fillval)

        def _load_data(timeseries, var_name, var_data, var_attrs):
            # Create a Quantity object for the variable
            quantity = u.Quantity(value=var_data, unit=var_attrs["UNITS"], copy=False)
            if mask.any():
                quantity = Masked(quantity, mask=mask)
            timeseries[var_name] = quantity
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

    def _load_support_variable(
        self,
        support: Dict[str, NDData],
        var_name: str,
        var_data: np.ndarray,
        var_attrs: Dict[str, Any],
    ) -> None:
        """
        Add a non-record-varying or support variable as an :class:`~astropy.nddata.NDData`.

        Fill values are converted to a boolean mask using the ``FILLVAL``
        attribute; for floating-point data, fill positions are additionally
        normalized to ``NaN``.  Integer and string dtypes are preserved.

        Parameters
        ----------
        support : dict[str, `~astropy.nddata.NDData`]
            Mapping of support variable names to ``NDData`` containers.
            Modified in place.
        var_name : str
            The variable name (used as the dict key).
        var_data : `numpy.ndarray`
            The raw array of values read from the CDF.
        var_attrs : dict[str, Any]
            The variable-level metadata; stored on the resulting ``NDData``
            as ``.meta``.

        Returns
        -------
        None
        """
        # Get the FILLVAL MASK and apply FILLVAL -> NaN for float dtypes
        fillval = var_attrs.get("FILLVAL")
        mask = fv.compute_fill_mask(var_data, fillval)
        if fv.is_float_dtype(var_data):
            var_data = fv.apply_fillval_to_nan(var_data, fillval)

        # Create a NDData entry for the variable
        support[var_name] = NDData(
            data=var_data,
            mask=mask if mask.any() else None,
            meta=var_attrs,
        )

    def _get_tensor_attribute(
        self,
        var_attrs: Dict[str, Any],
        naxis: int,
        attribute_name: str,
        default_attribute: Any,
    ) -> List[Any]:
        """
        Collect per-dimension values for a 1-indexed WCS-style attribute.

        For each axis ``i`` in ``range(naxis)``, looks for the zAttribute
        ``f"{attribute_name}{i + 1}"`` in ``var_attrs`` and falls back to
        ``default_attribute`` if it is missing.

        For example, for a variable ``'des_dist_brst'`` and ``attribute_name='CUNIT'``
        with ``naxis=4``::

            'CUNIT1': 'eV'    (DEPEND_3: 'mms1_des_energy_brst')
            'CUNIT2': 'deg'   (DEPEND_2: 'mms1_des_theta_brst')
            'CUNIT3': 'deg'   (DEPEND_1: 'mms1_des_phi_brst')
            'CUNIT4': 'ns'    (DEPEND_0: 'Epoch')

        returns ``['eV', 'deg', 'deg', 'ns']``.

        Parameters
        ----------
        var_attrs : dict[str, Any]
            The variable's zAttribute mapping.
        naxis : int
            Number of axes to collect a value for.
        attribute_name : str
            Base WCS attribute name (e.g. ``'CUNIT'`` or ``'CTYPE'``).
        default_attribute : Any
            Value to use when ``f"{attribute_name}{i + 1}"`` is absent.

        Returns
        -------
        attr_values : list[Any]
            Length-``naxis`` list of per-dimension attribute values.
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

    def _get_world_coords(
        self,
        var_data: np.ndarray,
        var_attrs: Dict[str, Any],
        time: Time,
    ) -> WCS:
        """
        Build an astropy :class:`~astropy.wcs.WCS` for a multi-dimensional variable.

        The number of axes is taken from the ``WCSAXES`` zAttribute when
        present, otherwise from ``var_data.shape``.  WCS properties
        (``ctype``, ``cunit``, ``cdelt``, ``crpix``, ``crval``, ``cname``,
        ``...``) are populated from per-axis zAttributes via
        :meth:`_get_tensor_attribute`, using the mapping defined in
        :attr:`SWXSchema.wcs_keyword_to_astropy_property`.  Time-related
        WCS fields (``timesys``, ``mjdref``, ``timeunit``, ``timedel``)
        are filled in from the provided ``time`` column.

        Parameters
        ----------
        var_data : `numpy.ndarray`
            The variable data array (used to infer ``naxis`` when ``WCSAXES``
            is not provided).
        var_attrs : dict[str, Any]
            The variable's zAttribute mapping.
        time : `~astropy.time.Time`
            The Epoch column associated with this variable; used to set
            ``MJDREF`` and the time cadence (``TIMEDEL``).

        Returns
        -------
        wcs : `~astropy.wcs.WCS`
            A configured WCS object suitable for attaching to an
            :class:`~ndcube.NDCube`.
        """
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
        else:  # If there is only one time entry, we cannot calculate a time delta. We will default to 1 ns
            time_delta = 1 * u.ns
        wcs.wcs.timedel = time_delta.to("ns").value

        return wcs

    def _load_spectra_variable(
        self,
        spectra: List[Tuple[str, NDCube]],
        var_name: str,
        var_data: np.ndarray,
        var_attrs: Dict[str, Any],
        time: Time,
    ) -> None:
        """
        Add a multi-dimensional variable to the spectra list as an :class:`~ndcube.NDCube`.

        A WCS is constructed via :meth:`_get_world_coords`.  Fill values are
        converted to a boolean mask using the ``FILLVAL`` attribute, and for
        floating-point data fill positions are additionally normalized to
        ``NaN``.  Integer and string dtypes are preserved and only the mask is
        attached to the cube.  Falls back to ``dimensionless_unscaled`` (with a
        warning) if the ``UNITS`` string cannot be parsed.

        Parameters
        ----------
        spectra : list[tuple[str, `~ndcube.NDCube`]]
            The accumulating list of ``(name, cube)`` pairs that will be
            wrapped in an :class:`~ndcube.NDCollection` after all variables
            have been loaded.  Modified in place.
        var_name : str
            The variable name.
        var_data : `numpy.ndarray`
            The raw multi-dimensional array of values read from the CDF.
        var_attrs : dict[str, Any]
            The variable-level metadata, including ``UNITS`` and (optionally)
            ``FILLVAL`` and WCS-related axis attributes.
        time : `~astropy.time.Time`
            The Epoch column associated with this variable; passed through to
            :meth:`_get_world_coords` for the time axis.

        Returns
        -------
        None
        """
        # Get the FILLVAL MASK and apply FILLVAL -> NaN for float dtypes
        fillval = var_attrs.get("FILLVAL")
        mask = fv.compute_fill_mask(var_data, fillval)
        if fv.is_float_dtype(var_data):
            var_data = fv.apply_fillval_to_nan(var_data, fillval)
        cube_mask: Optional[np.ndarray] = mask if mask.any() else None

        def _load_data(spectra, var_name, var_data, var_attrs, time):
            # Create a World Cordinate System for the Tensor
            var_wcs = self._get_world_coords(var_data, var_attrs, time)
            # Create a Cube
            var_cube = NDCube(
                data=var_data,
                wcs=var_wcs,
                meta=var_attrs,
                unit=var_attrs["UNITS"],
                mask=cube_mask,
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

    # ================================================================================================
    #                                   CDF WRITER
    # ================================================================================================

    def save_data(self, data, file_path: Path, filename: str = None):
        """
        Save heliophysics data to a CDF file.

        Parameters
        ----------
        data : `swxsoc.swxdata.SWXData`
            An instance of `SWXData` containing the data to be saved.
        file_path : `pathlib.Path`
            A fully specified path to the directory where the CDF file is to be saved.
        filename : `str`, optional
            Custom filename for the output file (including .cdf extension).
            If not provided, uses the Logical_file_id from metadata.

        Returns
        -------
        path : `pathlib.Path`
            A path to the saved file.
        """
        # Initialize a new CDF
        if filename:
            cdf_filename = filename
        else:
            cdf_filename = f"{data.meta['Logical_file_id']}.cdf"
        output_cdf_filepath = str(Path(file_path) / cdf_filename)
        with pycdf.CDF(output_cdf_filepath, masterpath="") as cdf_file:
            # Add Global Attriubtes to the CDF File
            self._convert_global_attributes_to_cdf(data, cdf_file)

            # Add zAttributes
            self._convert_variables_to_cdf(data, cdf_file)
        return Path(output_cdf_filepath)

    def _convert_global_attributes_to_cdf(self, data: SWXData, cdf_file: pycdf.CDF):
        # Loop though Global Attributes in target_dict
        for attr_name, attr_value in data.meta.items():
            # Make sure the Value is not None
            # We cannot add None Values to the CDF Global Attrs
            if attr_value is None:
                cdf_file.attrs[attr_name] = ""
            else:
                # Add the Attribute to the CDF File
                cdf_file.attrs[attr_name] = attr_value

    def _convert_variables_to_cdf(self, data: SWXData, cdf_file: pycdf.CDF):
        # Make sure the Default "Epoch" is present in the CDF
        default_timeseries_key = swxsoc.config["general"]["default_timeseries_key"]
        if default_timeseries_key not in data.data["timeseries"]:
            warn_user(
                f"Epoch Variable {default_timeseries_key} not found in CDF file: {cdf_file}"
            )

        for epoch_key, ts in data.data["timeseries"].items():
            # Loop through Scalar TimeSeries Variables
            for var_name in ts.colnames:
                var_data = ts[var_name]
                if var_name == "time":
                    cdf_var_name = self._get_cdf_epoch_var_name(epoch_key)
                    self._write_time_variable(cdf_var_name, var_data, cdf_file)
                    self._convert_variable_attributes_to_cdf(
                        cdf_var_name, var_data, cdf_file
                    )
                else:
                    cdf_var_name = self._get_cdf_data_var_name(epoch_key, var_name)
                    self._write_timeseries_variable(cdf_var_name, var_data, cdf_file)
                    self._convert_variable_attributes_to_cdf(
                        cdf_var_name, var_data, cdf_file
                    )

        # Loop through the NDData Data Structure (Not all record-varying)
        for var_name, var_data in data.support.items():
            self._write_support_variable(var_name, var_data, cdf_file)
            self._convert_variable_attributes_to_cdf(var_name, var_data, cdf_file)

        # Loop through High-Dimensional/Spectra Variables
        for var_name in data.spectra:
            var_data = data.spectra[var_name]
            self._write_spectra_variable(var_name, var_data, cdf_file)
            self._convert_variable_attributes_to_cdf(var_name, var_data, cdf_file)

    @staticmethod
    def _get_mask(var_data):
        """
        Return the boolean ``.mask`` array from a container (Masked Quantity,
        Masked Time, NDData, NDCube), or ``None`` if none is set.
        """
        mask = getattr(var_data, "mask", None)
        if mask is None:
            return None
        mask = np.asarray(mask, dtype=bool)
        if not mask.any():
            return None
        return mask

    @staticmethod
    def _unmasked_quantity_value(var_data):
        """
        Return the raw numpy values of a (possibly Masked) Quantity, with the
        unit stripped.
        """
        if isinstance(var_data, Masked):
            return np.asarray(var_data.unmasked.value)
        return np.asarray(var_data.value)

    def _write_timeseries_variable(
        self, var_name: str, var_data: Any, cdf_file: pycdf.CDF
    ):
        fillval = var_data.meta.get("FILLVAL")
        if fillval is None:
            swxsoc.log.debug(
                f"FILLVAL not set for variable {var_name}; writing values unchanged."
            )
        mask = self._get_mask(var_data)
        values = self._unmasked_quantity_value(var_data)
        out_data = fv.apply_fill_on_write(values, mask, fillval)
        # Get the CDF Data Type for the variable
        _, var_data_types, _ = self.schema.types(values)
        # Insert the Variable into the CDF File
        cdf_file.new(
            name=var_name,
            data=out_data,
            type=var_data_types[0],
            recVary=True,
        )

    def _write_support_variable(
        self, var_name: str, var_data: Any, cdf_file: pycdf.CDF
    ):
        fillval = var_data.meta.get("FILLVAL")
        if fillval is None:
            swxsoc.log.debug(
                f"FILLVAL not set for variable {var_name}; writing values unchanged."
            )
        mask = self._get_mask(var_data)
        raw_data = np.asarray(var_data.data)
        out_data = fv.apply_fill_on_write(raw_data, mask, fillval)
        # Guess the data type to store
        # Documented in https://github.com/spacepy/spacepy/issues/707
        _, var_data_types, _ = self.schema.types(raw_data)
        # Insert the Variable into the CDF File
        cdf_file.new(
            name=var_name,
            data=out_data,
            type=var_data_types[0],
            recVary=(var_data.meta["VAR_TYPE"] == "data"),
        )

    def _write_spectra_variable(
        self, var_name: str, var_data: Any, cdf_file: pycdf.CDF
    ):
        fillval = var_data.meta.get("FILLVAL")
        if fillval is None:
            swxsoc.log.debug(
                f"FILLVAL not set for variable {var_name}; writing values unchanged."
            )
        mask = self._get_mask(var_data)
        raw_data = np.asarray(var_data.data)
        out_data = fv.apply_fill_on_write(raw_data, mask, fillval)
        # Get the CDF Data Type for the variable
        _, var_data_types, _ = self.schema.types(raw_data)
        # Insert the Variable into the CDF File
        cdf_file.new(
            name=var_name,
            data=out_data,
            type=var_data_types[0],
            recVary=(var_data.meta["VAR_TYPE"] == "data"),
        )

    def _write_time_variable(
        self, cdf_var_name: str, var_data: Time, cdf_file: pycdf.CDF
    ):
        """
        Write the Epoch column. Falls back to the historical datetime-based
        write path when the column is not masked; for a masked time column
        we write raw TT2000 nanoseconds (int64) with the ISTP sentinel at
        masked positions to preserve precision.
        """
        mask = self._get_mask(var_data)
        if mask is None:
            cdf_file[cdf_var_name] = var_data.to_datetime()
            return

        # ``Time`` supports native masking; ``Masked(Time)`` exposes ``.unmasked``.
        if isinstance(var_data, Masked):
            unmasked = var_data.unmasked
        else:
            unmasked = var_data
        # Convert to datetime; masked positions are filled with an arbitrary
        # valid datetime so v_datetime_to_tt2000 succeeds, then overwritten
        # with the int64 sentinel below.
        dt_array = np.asarray(unmasked.to_datetime(), dtype=object)
        if mask.any():
            # Insert temp mask sentinel to ensure valid for conversion
            dt_array[mask] = datetime(2000, 1, 1)
        ttns = np.asarray(pycdf.lib.v_datetime_to_tt2000(dt_array), dtype=np.int64)
        ttns[mask] = fv.get_fillval(cdf_type=const.CDF_TIME_TT2000.value)
        cdf_file.new(
            name=cdf_var_name,
            data=ttns,
            type=const.CDF_TIME_TT2000,
        )

    def _convert_variable_attributes_to_cdf(
        self, var_name: str, var_data: Any, cdf_file: pycdf.CDF
    ):
        var: pycdf.Var = cdf_file[var_name]
        var_cdf_type = var.type()
        # Epoch-type variables need FILLVAL written with a matching CDF type so
        # downstream ISTP validators see the sentinel as the same type as the
        # variable (otherwise pycdf would infer CDF_INT8 / CDF_REAL8 from the
        # raw numeric value returned by ``swxsoc.io.fillval.get_fillval``).
        epoch_types = set(self.schema.timetypes)

        for var_attr_name, var_attr_val in var_data.meta.items():
            if var_attr_val is None:
                raise ValueError(
                    f"Variable {var_name}: Cannot Add vAttr: {var_attr_name}. Value was {str(var_attr_val)}"
                )
            elif var_attr_name == "FILLVAL" and var_cdf_type in epoch_types:
                # Set FILLVAL with the variable's own CDF type
                var.attrs.new(
                    name=var_attr_name,
                    data=var_attr_val,
                    type=var_cdf_type,
                )
            elif isinstance(var_attr_val, Time):
                # Convert the Attribute to Datetime before adding to CDF File
                var.attrs[var_attr_name] = var_attr_val.to_datetime()
            else:
                # Add the Attribute to the CDF File
                var.attrs[var_attr_name] = var_attr_val
