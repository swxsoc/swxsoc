"""
This module provides schema metadata derivations.

This code is based on that provided by SpacePy see
    licenses/SPACEPY.rst
"""

from pathlib import Path
from collections import OrderedDict
from copy import deepcopy
from typing import Optional
import math
import yaml

import numpy as np
from astropy.table import Table
from astropy.time import Time
from astropy import units as u
from ndcube import NDCube

from sammi.cdf_attribute_manager import CdfAttributeManager
import swxsoc
from swxsoc import log
from swxsoc.util import util, const
from swxsoc.util.exceptions import warn_user

__all__ = ["SWXSchema"]

DEFAULT_GLOBAL_CDF_ATTRS_SCHEMA_FILE = "swxsoc_default_global_cdf_attrs_schema.yaml"
DEFAULT_VARIABLE_CDF_ATTRS_SCHEMA_FILE = "swxsoc_default_variable_cdf_attrs_schema.yaml"


class SWXSchema(CdfAttributeManager):
    """
    Class representing a schema for data requirements and formatting. The SWxSOC Default Schema
    only includes attributes required for ISTP compliance. Additional mission-specific attributes
    or requirements should be added through additional global and variable schema layers. For an
    example of how to layer schema files, please see the HERMES mission core package, and
    `HermesDataSchema` extension of the `SWXSchema` class.

    There are two main components to the Space Weather Data Schema, including both global and
    variable attribute information.

    Global schema information is loaded from YAML (dict-like) files in the following format:

    .. code-block:: yaml

        attribute_name:
            description: >
                Include a meaningful description of the attribute and context needed to understand
                its values.
            default: <string> # A default value for the attribute if needed/desired
            derived: <bool> # Whether or not the attribute's value can be derived using a python function
            derivation_fn: <string> # The name of a Python function to derive the value. Must be a function member of the schema class and match the signature below.
            required: <bool> # Whether the attribute is required
            overwrite: <bool> # Whether an existing value for the attribute should be overwritten if a different value is derived.

    The signature for all functions to derive global attributes should follow the format below.
    The function takes in a parameter `data` which is a `SWXData` object, or that of an
    extended data class, and returns a single attribute value for the given attribute to be
    derived.

    .. code-block:: python

        def derivation_fn(self, data: SWXData):
            # ... do manipulations as needed from `data`
            return "attribute_value"

    Variable schema information is loaded from YAML (dict-like) files in the following format:

    .. code-block:: yaml

        attribute_key:
            attribute_name:
                description: >
                    Include a meaningful description of the attribute and context needed to understand
                    its values.
                derived: <bool> # Whether or not the attribute's value can be derived using a python function
                derivation_fn: <string> # The name of a Python function to derive the value. Must be a function member of the schema class and match the signature below.
                required: <bool> # Whether the attribute is required
                overwrite: <bool> # Whether an existing value for the attribute should be overwritten if a different value is derived.
                valid_values: <list> # A list of valid values that the attribute can take. The value of the attribute is checked against the `valid_values` in the Validation module.
                alternate: <string> An additional attribute name that can be treated as an alternative of the given attribute.
        data:
            - attribute_name
            - ...
        support_data:
            - ...
        metadata:
            - ...

    The signature for all functions to derive variable attributes should follow the format below.
    The function takes in parameters `var_name`, `var_data`, and `guess_type`, where:

    - `var_name` is the variable name of the variable for which the attribute is being derived
    - `var_data` is the variable data of the variable for which the attribute is being derived
    - `guess_type` is the guessed CDF variable type of the data for which the attribute is being derived.

    The function must return a single attribute value for the given attribute to be derived.

    .. code-block:: python

        def derivation_fn(self, var_name: str, var_data: Union[Quantity, NDData, NDCube], guess_type: ctypes.c_long):
            # ... do manipulations as needed from data
            return "attribute_value"

    Parameters
    ----------
    global_schema_layers :  `Optional[list[Path]]`
        Absolute file paths to global attribute schema files. These schema files are layered
        on top of one another in a latest-priority ordering. That is, the latest file that modifies
        a common schema attribute will take precedence over earlier values for a given attribute.
    variable_schema_layers :  `Optional[list[Path]]`
        Absolute file paths to variable attribute schema files. These schema files are layered
        on top of one another in a latest-priority ordering. That is, the latest file that modifies
        a common schema attribute will take precedence over earlier values for a given attribute.
    use_defaults: `Optional[bool]`
        Whether or not to load the default global and variable attribute schema files. These
        default schema files contain only the requirements for CDF ISTP validation.
    """

    def __init__(
        self,
        global_schema_layers: Optional[list[str]] = None,
        variable_schema_layers: Optional[list[str]] = None,
        use_defaults: Optional[bool] = True,
    ):

        # SWxSOC Default Global Schema
        global_schema_path = str(
            Path(swxsoc.__file__).parent / "data" / DEFAULT_GLOBAL_CDF_ATTRS_SCHEMA_FILE
        )
        # SWxSOC Default Variable Schema
        variable_schema_path = str(
            Path(swxsoc.__file__).parent
            / "data"
            / DEFAULT_VARIABLE_CDF_ATTRS_SCHEMA_FILE
        )

        # Seed Layers with Default
        if not use_defaults:
            _global_schema_layers = []
            _variable_schema_layers = []
        else:
            _global_schema_layers = [global_schema_path]
            _variable_schema_layers = [variable_schema_path]

        # Extend Custom Layers
        if global_schema_layers is not None and len(global_schema_layers) > 0:
            _global_schema_layers.extend(global_schema_layers)
        if variable_schema_layers is not None and len(variable_schema_layers) > 0:
            _variable_schema_layers.extend(variable_schema_layers)

        # Call SAMMI Initialization to populate Schema
        super().__init__(
            global_schema_layers=_global_schema_layers,
            variable_schema_layers=_variable_schema_layers,
            use_defaults=use_defaults,
        )

        self.cdftypenames = {
            const.CDF_BYTE.value: "CDF_BYTE",
            const.CDF_CHAR.value: "CDF_CHAR",
            const.CDF_INT1.value: "CDF_INT1",
            const.CDF_UCHAR.value: "CDF_UCHAR",
            const.CDF_UINT1.value: "CDF_UINT1",
            const.CDF_INT2.value: "CDF_INT2",
            const.CDF_UINT2.value: "CDF_UINT2",
            const.CDF_INT4.value: "CDF_INT4",
            const.CDF_UINT4.value: "CDF_UINT4",
            const.CDF_INT8.value: "CDF_INT8",
            const.CDF_FLOAT.value: "CDF_FLOAT",
            const.CDF_REAL4.value: "CDF_REAL4",
            const.CDF_DOUBLE.value: "CDF_DOUBLE",
            const.CDF_REAL8.value: "CDF_REAL8",
            const.CDF_EPOCH.value: "CDF_EPOCH",
            const.CDF_EPOCH16.value: "CDF_EPOCH16",
            const.CDF_TIME_TT2000.value: "CDF_TIME_TT2000",
        }
        self.numpytypedict = {
            const.CDF_BYTE.value: np.int8,
            const.CDF_CHAR.value: np.int8,
            const.CDF_INT1.value: np.int8,
            const.CDF_UCHAR.value: np.uint8,
            const.CDF_UINT1.value: np.uint8,
            const.CDF_INT2.value: np.int16,
            const.CDF_UINT2.value: np.uint16,
            const.CDF_INT4.value: np.int32,
            const.CDF_UINT4.value: np.uint32,
            const.CDF_INT8.value: np.int64,
            const.CDF_FLOAT.value: np.float32,
            const.CDF_REAL4.value: np.float32,
            const.CDF_DOUBLE.value: np.float64,
            const.CDF_REAL8.value: np.float64,
            const.CDF_EPOCH.value: np.float64,
            const.CDF_EPOCH16.value: np.dtype((np.float64, 2)),
            const.CDF_TIME_TT2000.value: np.int64,
        }
        self.timetypes = [
            const.CDF_EPOCH.value,
            const.CDF_EPOCH16.value,
            const.CDF_TIME_TT2000.value,
        ]

        # List of Tuple of (WCS Keyword, Astropy Property, Default Value)
        # There is one entry for each keyword/property along each dimension of
        # the spectra scored in the astropy.wcs.WCS object
        self.wcs_keyword_to_astropy_property = [
            ("CNAME", "cname", "NoName"),
            ("CTYPE", "ctype", "TEST"),
            ("CUNIT", "cunit", u.dimensionless_unscaled.to_string()),
            ("CRPIX", "crpix", 0),
            ("CRVAL", "crval", 1),
            ("CDELT", "cdelt", 1),
        ]

    @property
    def default_global_attributes(self) -> dict:
        """
        Function to load the default global attributes from the SWxSOC schema.

        Returns
        -------
        default_global_attributes: `dict`
            A dictionary of default global attributes.
        """
        return self._global_attributes

    def global_attribute_template(self) -> OrderedDict:
        """
        Function to generate a template of required global attributes
        that must be set for a valid CDF.

        Returns
        -------
        template : `OrderedDict`
            A template for required global attributes that must be provided.
        """
        template = OrderedDict()
        for attr_name, attr_schema in self.global_attribute_schema.items():
            if (
                attr_schema["required"]
                and not attr_schema["derived"]
                and attr_name not in self.default_global_attributes
            ):
                template[attr_name] = None
        return template

    def measurement_attribute_template(self) -> OrderedDict:
        """
        Function to generate a template of required measurement attributes
        that must be set for a valid CDF measurement variable.

        Returns
        -------
        template: `OrderedDict`
            A template for required variable attributes that must be provided.
        """
        template = OrderedDict()
        for attr_name, attr_schema in self.variable_attribute_schema[
            "attribute_key"
        ].items():
            if attr_schema["required"] and not attr_schema["derived"]:
                template[attr_name] = None
        return template

    def global_attribute_info(self, attribute_name: Optional[str] = None) -> Table:
        """
        Function to generate a `astropy.table.Table` of information about each global
        metadata attribute. The `astropy.table.Table` contains all information in the SWxSOC
        global attribute schema including:

        - description: (`str`) A brief description of the attribute
        - default: (`str`) The default value used if none is provided
        - derived: (`bool`) Whether the attibute can be derived by the SWxSOC
            :py:class:`~swxsoc.util.schema.SWXSchema` class
        - required: (`bool`) Whether the attribute is required by SWxSOC standards
        - overwrite: (`bool`) Whether the :py:class:`~swxsoc.util.schema.SWXSchema`
            attribute derivations will overwrite an existing attribute value with an updated
            attribute value from the derivation process.

        Parameters
        ----------
        attribute_name : `str`, optional, default None
            The name of the attribute to get specific information for.

        Returns
        -------
        info: `astropy.table.Table`
            A table of information about global metadata.

        Raises
        ------
        KeyError: If attribute_name is not a recognized global attribute.
        """
        # Strip the Description of New Lines
        for attr_name in self.global_attribute_schema.keys():
            self.global_attribute_schema[attr_name]["description"] = (
                self.global_attribute_schema[attr_name]["description"].strip()
            )

        # Get all the Attributes from the Schema
        attribute_names = list(self.global_attribute_schema.keys())
        table_rows = [info for _, info in self.global_attribute_schema.items()]

        # Create the Info Table
        info = Table(rows=table_rows)
        # Add the Attribute Name as a Column
        info.add_column(col=attribute_names, name="Attribute", index=0)
        # Remove the Derivation Function Column, since this is not needed for the Docs
        info.remove_column("derivation_fn")

        # Limit the Info to the requested Attribute
        if attribute_name and attribute_name in info["Attribute"]:
            info = info[info["Attribute"] == attribute_name]
        elif attribute_name and attribute_name not in info["Attribute"]:
            raise KeyError(
                f"Cannot find Global Metadata for attribute name: {attribute_name}"
            )

        return info

    def measurement_attribute_info(self, attribute_name: Optional[str] = None) -> Table:
        """
        Function to generate a `astropy.table.Table` of information about each variable
        metadata attribute. The `astropy.table.Table` contains all information in the SWxSOC
        variable attribute schema including:

        - description: (`str`) A brief description of the attribute
        - derived: (`bool`) Whether the attibute can be derived by the SWxSOC
            :py:class:`~swxsoc.util.schema.SWXSchema` class
        - required: (`bool`) Whether the attribute is required by SWxSOC standards
        - overwrite: (`bool`) Whether the :py:class:`~swxsoc.util.schema.SWXSchema`
            attribute derivations will overwrite an existing attribute value with an updated
            attribute value from the derivation process.
        - valid_values: (`str`) List of allowed values the attribute can take for SWxSOC products,
            if applicable
        - alternate: (`str`) An additional attribute name that can be treated as an alternative
            of the given attribute. Not all attributes have an alternative and only one of a given
            attribute or its alternate are required.
        - var_types: (`str`) A list of the variable types that require the given
            attribute to be present.

        Parameters
        ----------
        attribute_name : `str`, optional, default None
            The name of the attribute to get specific information for.

        Returns
        -------
        info: `astropy.table.Table`
            A table of information about variable metadata.

        Raises
        ------
        KeyError: If attribute_name is not a recognized global attribute.
        """
        measurement_attribute_key = self.variable_attribute_schema["attribute_key"]

        # Strip the Description of New Lines
        for attr_name in measurement_attribute_key.keys():
            measurement_attribute_key[attr_name]["description"] = (
                measurement_attribute_key[attr_name]["description"].strip()
            )

        # Create New Column to describe which VAR_TYPE's require the given attribute
        for attr_name in measurement_attribute_key.keys():
            # Create a new list to store the var types
            measurement_attribute_key[attr_name]["var_types"] = []
            for var_type in ["data", "support_data", "metadata"]:
                # If the attribute is required for the given var type
                if attr_name in self.variable_attribute_schema[var_type]:
                    measurement_attribute_key[attr_name]["var_types"].append(var_type)
            # Convert the list to a string that can be written to a CSV from the table
            measurement_attribute_key[attr_name]["var_types"] = " ".join(
                measurement_attribute_key[attr_name]["var_types"]
            )

        # Get all the Attributes from the Schema
        attribute_names = list(measurement_attribute_key.keys())
        table_rows = [info for _, info in measurement_attribute_key.items()]

        # Create the Info Table
        info = Table(rows=table_rows)
        # Add the Attribute Name as a Column
        info.add_column(col=attribute_names, name="Attribute", index=0)
        # Remove the Derivation Function Column, since this is not needed for the Docs
        info.remove_columns(["derivation_fn", "iterable"])

        # Limit the Info to the requested Attribute
        if attribute_name and attribute_name in info["Attribute"]:
            info = info[info["Attribute"] == attribute_name]
        elif attribute_name and attribute_name not in info["Attribute"]:
            raise KeyError(
                f"Cannot find Variable Metadata for attribute name: {attribute_name}"
            )

        return info

    @staticmethod
    def _check_well_formed(data):
        """Checks if input data is well-formed, regular array

        Returns
        -------
        :class:`~numpy.ndarray`s
            The input data as a well-formed array; may be the input
            data exactly.
        """
        msg = (
            "Data must be well-formed, regular array of number, string, or astropy.time"
        )
        try:
            d = np.asanyarray(data)
        except ValueError:
            raise ValueError(msg)
        # In a future numpy, the case tested below will raise ValueError,
        # so can remove entire if block.
        if d.dtype == object:  # this is probably going to be bad
            if d.shape != () and not len(d):
                # Completely empty, so "well-formed" enough
                return d
            if np.array(d.flat[0]).shape != ():
                # Sequence-like, so we know it's ragged
                raise ValueError(msg)
        return d

    def _types(self, data, backward=False, encoding="utf-8"):
        """
        Find dimensions and valid types of a nested list-of-lists

        Any given data may be representable by a range of CDF types; infer
        the CDF types which can represent this data. This breaks down to:
          1. Proper kind (numerical, string, time)
          2. Proper range (stores highest and lowest number)
          3. Sufficient resolution (EPOCH16 or TT2000 required if astropy.time has
             microseconds or below.)

        If more than one value satisfies the requirements, types are returned
        in preferred order:
          1. Type that matches precision of data first, then
          2. integer type before float type, then
          3. Smallest type first, then
          4. signed type first, then
          5. specifically-named (CDF_BYTE) vs. generically named (CDF_INT1)
        So for example, EPOCH_16 is preferred over EPOCH if L{data} specifies
        below the millisecond level (rule 1), but otherwise EPOCH is preferred
        (rule 2). TIME_TT2000 is always preferred as of 0.3.0.

        For floats, four-byte is preferred unless eight-byte is required:
          1. absolute values between 0 and 3e-39
          2. absolute values greater than 1.7e38
        This will switch to an eight-byte double in some cases where four bytes
        would be sufficient for IEEE 754 encoding, but where DEC formats would
        require eight.

        @param data: data for which dimensions and CDF types are desired
        @type data: list (of lists)
        @param backward: limit to pre-CDF3 types
        @type backward: bool
        @param encoding: Encoding to use for Unicode input, default utf-8
        @type backward: str
        @return: dimensions of L{data}, in order outside-in;
                 CDF types which can represent this data;
                 number of elements required (i.e. length of longest string)
        @rtype: 3-tuple of lists ([int], [ctypes.c_long], [int])
        @raise ValueError: if L{data} has irregular dimensions

        """
        d = SWXSchema._check_well_formed(data)
        dims = d.shape
        elements = 1
        types = []

        if d.dtype.kind in ("S", "U"):  # it's a string
            types = [const.CDF_CHAR, const.CDF_UCHAR]
            # Length of string from type (may be longer than contents)
            elements = d.dtype.itemsize
            if d.dtype.kind == "U":
                # Big enough for contents (bytes/char are encoding-specific)
                elements = max(
                    elements // 4,  # numpy stores as 4-byte
                    np.char.encode(d, encoding=encoding).dtype.itemsize,
                )
        elif isinstance(data, Time):
            types = [const.CDF_TIME_TT2000, const.CDF_EPOCH16, const.CDF_EPOCH]
        elif d is data or isinstance(data, np.generic):
            # np array came in, use its type (or byte-swapped)
            types = [
                k
                for k in self.numpytypedict
                if (
                    self.numpytypedict[k] == d.dtype
                    or self.numpytypedict[k] == d.dtype.newbyteorder()
                )
                and k not in self.timetypes
            ]
            # Maintain priority to match the ordered lists below:
            # float/double (44, 45) before real (21/22), and
            # byte (41) before int (1) before char (51). So hack.
            # Consider making typedict an ordered dict once 2.6 is dead.
            types.sort(key=lambda x: x % 50, reverse=True)

        if not types:  # not a numpy array, or can't parse its type
            if d.dtype.kind == "O":  # Object. Try to make it numeric
                if d.shape != () and not len(d):
                    raise ValueError("Cannot determine CDF type of empty object array.")
                # Can't do safe casting from Object, so try and compare
                # Basically try most restrictive to least restrictive
                trytypes = (np.uint64, np.int64, np.float64)
                for t in trytypes:
                    try:
                        newd = d.astype(dtype=t)
                    except TypeError:  # Failure to cast, try next type
                        continue
                    if (newd == d).all():  # Values preserved, use this type
                        d = newd
                        # Continue with normal guessing, as if a list
                        break
                else:
                    # fell through without a match
                    raise ValueError("Cannot convert generic objects to CDF type.")
            if d.dtype.kind in ("i", "u"):  # integer
                minval = np.min(d)
                maxval = np.max(d)
                if minval < 0:
                    types = [
                        const.CDF_BYTE,
                        const.CDF_INT1,
                        const.CDF_INT2,
                        const.CDF_INT4,
                        const.CDF_INT8,
                        const.CDF_FLOAT,
                        const.CDF_REAL4,
                        const.CDF_DOUBLE,
                        const.CDF_REAL8,
                    ]
                    cutoffs = [
                        2**7,
                        2**7,
                        2**15,
                        2**31,
                        2**63,
                        1.7e38,
                        1.7e38,
                        8e307,
                        8e307,
                    ]
                else:
                    types = [
                        const.CDF_BYTE,
                        const.CDF_INT1,
                        const.CDF_UINT1,
                        const.CDF_INT2,
                        const.CDF_UINT2,
                        const.CDF_INT4,
                        const.CDF_UINT4,
                        const.CDF_INT8,
                        const.CDF_FLOAT,
                        const.CDF_REAL4,
                        const.CDF_DOUBLE,
                        const.CDF_REAL8,
                    ]
                    cutoffs = [
                        2**7,
                        2**7,
                        2**8,
                        2**15,
                        2**16,
                        2**31,
                        2**32,
                        2**63,
                        1.7e38,
                        1.7e38,
                        8e307,
                        8e307,
                    ]
                types = [
                    t
                    for (t, c) in zip(types, cutoffs)
                    if c > maxval and (minval >= 0 or minval >= -c)
                ]
            else:  # float
                if dims == ():
                    if d != 0 and (abs(d) > 1.7e38 or abs(d) < 3e-39):
                        types = [const.CDF_DOUBLE, const.CDF_REAL8]
                    else:
                        types = [
                            const.CDF_FLOAT,
                            const.CDF_REAL4,
                            const.CDF_DOUBLE,
                            const.CDF_REAL8,
                        ]
                else:
                    absolutes = np.abs(d[d != 0])
                    if len(absolutes) > 0 and (
                        np.max(absolutes) > 1.7e38 or np.min(absolutes) < 3e-39
                    ):
                        types = [const.CDF_DOUBLE, const.CDF_REAL8]
                    else:
                        types = [
                            const.CDF_FLOAT,
                            const.CDF_REAL4,
                            const.CDF_DOUBLE,
                            const.CDF_REAL8,
                        ]
        types = [t.value if hasattr(t, "value") else t for t in types]
        # If data has a type, might be a VarCopy, prefer that type
        if hasattr(data, "type"):
            try:
                t = data.type()
            except AttributeError:
                t = None
                pass
            if t in types:
                types = [t]
            # If passed array, types prefers its dtype, so try for compatible
            # and let type() override
            elif d is data:
                try:
                    _ = data.astype(dtype=self.numpytypedict[t])
                except ValueError:
                    pass
                finally:
                    types = [t]
        # And if the VarCopy specifies a number of elements, use that
        # if compatible
        if hasattr(data, "nelems"):
            ne = data.nelems()
            if ne > elements:
                elements = ne
        return (dims, types, elements)

    def _get_minmax(self, cdftype):
        """Find minimum, maximum possible value based on CDF type.

        This returns the processed value (e.g. astropy.times for Epoch
        types) because comparisons to EPOCH16s are otherwise
        difficult.

        Parameters
        ==========
        cdftype : int
            CDF type number from :mod:`~const`

        Raises
        ======
        ValueError : if can't match the type

        Returns
        =======
        out : tuple
            minimum, maximum value supported by type (of type matching the
            CDF type).

        """
        if hasattr(cdftype, "value"):
            cdftype = cdftype.value
        if cdftype in [
            const.CDF_EPOCH.value,
            const.CDF_EPOCH16.value,
            const.CDF_TIME_TT2000.value,
        ]:
            return (
                Time("1900-1-1T00:00:00.000", format="isot"),
                Time("2250-1-1T00:00:00.000", format="isot"),
            )
        dtype = self.numpytypedict.get(cdftype, None)
        if dtype is None:
            raise ValueError("Unknown data type: {}".format(cdftype))
        if np.issubdtype(dtype, np.integer):
            inf = np.iinfo(dtype)
        elif np.issubdtype(dtype, np.floating):
            inf = np.finfo(dtype)
        else:
            raise ValueError("Unknown data type: {}".format(cdftype))
        return (inf.min, inf.max)

    def derive_global_attributes(self, data) -> OrderedDict:
        """
        Function to derive global attributes for the given measurement data.

        Parameters
        ----------
        data : `swxsoc.swxdata.SWXData`
            An instance of `SWXData` to derive metadata from.

        Returns
        -------
        attributes : `OrderedDict`
            A dict containing `key: value` pairs of global metadata attributes.
        """
        global_attributes = OrderedDict()
        # Loop through Global Attributes
        derived_attributes = filter(
            lambda attr_info: attr_info[1]["derived"],
            self.global_attribute_schema.items(),
        )
        for attr_name, attr_schema in derived_attributes:
            derivation_fn = getattr(self, attr_schema["derivation_fn"])
            global_attributes[attr_name] = derivation_fn(data)

        return global_attributes

    def derive_measurement_attributes(
        self,
        data,
        var_name: str,
        guess_types: Optional[list[int]] = None,
    ) -> OrderedDict:
        """
        Function to derive metadata for the given measurement.

        Parameters
        ----------
        data : `swxsoc.swxdata.SWXData`
            An instance of `SWXData` to derive metadata from
        var_name : `str`
            The name of the measurement to derive metadata for
        guess_types : `list[int]`, optional
            Guessed CDF Type of the variable

        Returns
        -------
        attributes: `OrderedDict`
            A dict containing `key: value` pairs of derived metadata attributes.
        """
        measurement_attributes = OrderedDict()

        # Guess the const CDF Data Type
        var_data = data[var_name]
        if not guess_types:
            if var_name == "time":
                # Guess the const CDF Data Type
                (guess_dims, guess_types, guess_elements) = self._types(var_data)
            elif hasattr(var_data, "value"):
                # Support NDData use `.value`
                (guess_dims, guess_types, guess_elements) = self._types(var_data.value)
            else:
                # TimeSeries Quantity and Spectra NDCube use `.data`
                (guess_dims, guess_types, guess_elements) = self._types(var_data.data)

        # Check the Attributes that can be derived
        var_type = self._get_var_type(var_name, var_data, guess_types[0])

        # Identify / Select Attributes that can be Derived for the given measurement
        derived_attributes = []
        # Extend by attributes for the given variable type
        if var_type in ["data", "support_data", "metadata"]:
            var_atttibutes = list(
                filter(
                    lambda attr_info: attr_info[0]
                    in self.variable_attribute_schema[var_type]
                    and attr_info[1]["derived"],
                    self.variable_attribute_schema["attribute_key"].items(),
                )
            )
            derived_attributes.extend(var_atttibutes)
        # Extend by Time/Epoch Attributes
        if var_name == "time":
            time_attributes = list(
                filter(
                    lambda attr_info: attr_info[0]
                    in self.variable_attribute_schema["epoch"]
                    and attr_info[1]["derived"],
                    self.variable_attribute_schema["attribute_key"].items(),
                )
            )
            derived_attributes.extend(time_attributes)
        # Extend by Spectral Attributes
        if hasattr(var_data, "wcs") and getattr(var_data, "wcs") is not None:
            spectra_attributes = list(
                filter(
                    lambda attr_info: attr_info[0]
                    in self.variable_attribute_schema["spectra"]
                    and attr_info[1]["derived"],
                    self.variable_attribute_schema["attribute_key"].items(),
                )
            )
            derived_attributes.extend(spectra_attributes)

        # Derive Attributes Specific to VAR_TYPE
        for attr_name, attr_schema in derived_attributes:
            # If the attribute can take values for multiple dimensions of the var data
            if "iterable" in attr_schema and attr_schema["iterable"]:
                # Get the "root" attriubte name.
                # Ex: CNAMEi -> CNAME , DEPEND_i -> DEPEND_
                attr_root = attr_name.rstrip("i")
                # Get the number of dimensions to iterate over for the attribute
                num_dimensions = self._get_num_dimensions(
                    var_name, var_data, guess_types[0]
                )
                # Loop through each dimension we want to derive for
                for dimension_i in range(num_dimensions):
                    # Attribute Name for the given dimension_i
                    dimension_attr_name = (
                        f"{attr_root}{dimension_i+1}"  # Dimension Indexed 1-4 vs 0-3
                    )
                    # Get the Derivation Function to be used for the given attribute
                    derivation_fn = getattr(self, attr_schema["derivation_fn"])
                    # Derive the Metadata Attribute using the configured function
                    measurement_attributes[dimension_attr_name] = derivation_fn(
                        var_name, var_data, guess_types[0], dimension_i
                    )
            # else the attribute can only take one value for the main dimension of var data
            else:
                # Get the Derivation Function to be used for the given attribute
                derivation_fn = getattr(self, attr_schema["derivation_fn"])
                # Derive the Metadata Attribute using the configured function
                measurement_attributes[attr_name] = derivation_fn(
                    var_name,
                    var_data,
                    guess_types[0],
                    timeseries_dict=data.data["timeseries"],
                )

        return measurement_attributes

    def _get_num_dimensions(self, var_name, var_data, guess_type, **kwargs):
        """
        Function to get the number of dimensions of a measurement.
        Currently this is just implemented for NDCube measurement objects,
        however we can extend this in the future if we want to accomodate
        other multi-dimensional data structures.
        """
        # Get the number of WCS Axes from the NDCube
        return self._get_wcs_naxis(var_name, var_data, guess_type)

    # =============================================================================================
    #                             VARIABLE METADATA DERIVATIONS
    # =============================================================================================

    def _get_depend(self, var_name, var_data, guess_type, **kwargs):
        # Find the TimeSeries Epoch for this Record-Varying Variable
        from swxsoc.swxdata import SWXData

        if "timeseries_dict" in kwargs:
            timeseries_dict = kwargs["timeseries_dict"]

            epoch_key = SWXData.get_timeseres_epoch_key(
                timeseries_dict, var_data, var_data.meta
            )
        else:
            epoch_key = swxsoc.config["general"]["default_timeseries_key"]

        return epoch_key

    def _get_display_type(self, var_name, var_data, guess_type, **kwargs):
        return "time_series"

    def _get_fieldnam(self, var_name, var_data, guess_type, **kwargs):
        if var_name != "time":
            return deepcopy(var_name)
        else:
            return "Epoch"

    def _get_fillval(self, var_name, var_data, guess_type, **kwargs):
        # Get the Variable Data
        if guess_type == const.CDF_TIME_TT2000.value:
            return Time("9999-12-31T23:59:59.999999", format="isot")
        else:
            # Get the FILLVAL for the gussed data type
            fillval = self._fillval_helper(cdf_type=guess_type)
            return fillval

    def _fillval_helper(self, cdf_type):
        # Fill value, indexed by the CDF type (numeric)
        fillvals = {}
        # Integers
        for i in (1, 2, 4, 8):
            fillvals[getattr(const, "CDF_INT{}".format(i)).value] = -(2 ** (8 * i - 1))
            if i == 8:
                continue
            fillvals[getattr(const, "CDF_UINT{}".format(i)).value] = 2 ** (8 * i) - 1
        fillvals[const.CDF_EPOCH16.value] = (-1e31, -1e31)
        fillvals[const.CDF_REAL8.value] = -1e31
        fillvals[const.CDF_REAL4.value] = -1e31
        fillvals[const.CDF_CHAR.value] = " "
        fillvals[const.CDF_UCHAR.value] = " "
        # Equivalent pairs
        for cdf_t, equiv in (
            (const.CDF_TIME_TT2000, const.CDF_INT8),
            (const.CDF_EPOCH, const.CDF_REAL8),
            (const.CDF_BYTE, const.CDF_INT1),
            (const.CDF_FLOAT, const.CDF_REAL4),
            (const.CDF_DOUBLE, const.CDF_REAL8),
        ):
            fillvals[cdf_t.value] = fillvals[equiv.value]
        value = fillvals[cdf_type]
        return value

    def _get_format(self, var_name, var_data, cdftype, **kwargs):
        """
        Format can be specified using either Fortran or C format codes.
        For instance, "F10.3" indicates that the data should be displayed across 10 characters
        where 3 of those characters are to the right of the decimal. For a description of FORTRAN
        formatting codes see the docs here:
        https://docs.oracle.com/cd/E19957-01/805-4939/z40007437a2e/index.html
        """
        minn = "VALIDMIN"
        maxx = "VALIDMAX"

        if cdftype in (
            const.CDF_INT1.value,
            const.CDF_INT2.value,
            const.CDF_INT4.value,
            const.CDF_INT8.value,
            const.CDF_UINT1.value,
            const.CDF_UINT2.value,
            const.CDF_UINT4.value,
            const.CDF_BYTE.value,
        ):
            if minn in var_data.meta:  # Just use validmin or scalemin
                minval = var_data.meta[minn]
            elif cdftype in (
                const.CDF_UINT1.value,
                const.CDF_UINT2.value,
                const.CDF_UINT4.value,
            ):  # unsigned, easy
                minval = 0
            elif cdftype == const.CDF_BYTE.value:
                minval = -(2**7)
            else:  # Signed, harder
                size = next(
                    (
                        i
                        for i in (1, 2, 4, 8)
                        if getattr(const, "CDF_INT{}".format(i)).value == cdftype
                    )
                )
                minval = -(2 ** (8 * size - 1))
            if maxx in var_data.meta:  # Just use max
                maxval = var_data.meta[maxx]
            elif cdftype == const.CDF_BYTE.value:
                maxval = 2**7 - 1
            else:
                size = next(
                    (
                        8 * i
                        for i in (1, 2, 4)
                        if getattr(const, "CDF_UINT{}".format(i)).value == cdftype
                    ),
                    None,
                )
                if size is None:
                    size = (
                        next(
                            (
                                8 * i
                                for i in (1, 2, 4, 8)
                                if getattr(const, "CDF_INT{}".format(i)).value
                                == cdftype
                            )
                        )
                        - 1
                    )
                maxval = 2**size - 1
            # Two tricks:
            # -Truncate and add 1 rather than ceil so get
            # powers of 10 (log10(10) = 1 but needs two digits)
            # -Make sure not taking log of zero
            if minval < 0:  # Need an extra space for the negative sign
                fmt = "I{}".format(
                    int(math.log10(max(abs(maxval), abs(minval), 1))) + 2
                )
            else:
                fmt = "I{}".format(int(math.log10(maxval) if maxval != 0 else 1) + 1)
        elif cdftype == const.CDF_TIME_TT2000.value:
            fmt = "A{}".format(len("9999-12-31T23:59:59.999999999"))
        elif cdftype == const.CDF_EPOCH16.value:
            fmt = "A{}".format(len("31-Dec-9999 23:59:59.999.999.000.000"))
        elif cdftype == const.CDF_EPOCH.value:
            fmt = "A{}".format(len("31-Dec-9999 23:59:59.999"))
        elif cdftype in (
            const.CDF_REAL8.value,
            const.CDF_REAL4.value,
            const.CDF_FLOAT.value,
            const.CDF_DOUBLE.value,
        ):
            if "VALIDMIN" in var_data.meta and "VALIDMAX" in var_data.meta:
                range = var_data.meta["VALIDMAX"] - var_data.meta["VALIDMIN"]
            # If not, just use nothing.
            else:
                range = None
            # Find how many spaces we need for the 'integer' part of the number
            # (Use maxx-minn for this...effectively uses VALIDMIN/MAX for most
            # cases.)
            if range and (minn in var_data.meta and maxx in var_data.meta):
                if len(str(int(var_data.meta[maxx]))) >= len(
                    str(int(var_data.meta[minn]))
                ):
                    ln = str(int(var_data.meta[maxx]))
                else:
                    ln = str(int(var_data.meta[minn]))
            if range and ln and range < 0:  # Cover all our bases:
                range = None
            # Switch on Range
            if (
                range and ln and range <= 11
            ):  # If range <= 11, we want 2 decimal places:
                # Need extra for '.', and 3 decimal places (4 extra)
                fmt = "F{}.3".format(len([i for i in ln]) + 4)
            elif range and ln and 11 < range <= 101:
                # Need extra for '.' (1 extra)
                fmt = "F{}.2".format(len([i for i in ln]) + 3)
            elif range and ln and 101 < range <= 1000:
                # Need extra for '.' (1 extra)
                fmt = "F{}.1".format(len([i for i in ln]) + 2)
            else:
                # No range, must not be populated, copied from REAL4/8(s) above
                # OR we don't care because it's a 'big' number:
                fmt = "G10.8E3"
        elif cdftype in (
            const.CDF_CHAR.value,
            const.CDF_UCHAR.value,
        ):
            if hasattr(var_data, "data"):
                var_data = var_data.data
            fmt = "A{}".format(len(var_data))
        else:
            raise ValueError(
                "Couldn't find FORMAT for type {}".format(
                    self.cdftypenames.get(cdftype, "UNKNOWN")
                )
            )
        return fmt

    def _get_lablaxis(self, var_name, var_data, guess_type, **kwargs):
        return f"{var_name} [{self._get_units(var_name, var_data, guess_type)}]"

    def _get_reference_position(self, var_name, var_data, guess_type, **kwargs):
        if guess_type == const.CDF_TIME_TT2000.value:
            return "rotating Earth geoid"
        else:
            msg = f"Reference Position for Time type ({guess_type}) not found."
            raise TypeError(msg)

    def _get_resolution(self, var_name, var_data, guess_type, **kwargs):
        if len(var_data) < 2:
            raise ValueError(
                f"Can not derive Time Resolution, need 2 samples, found {var_data}."
            )
        # Calculate the Timedelta between two time samples
        delta = var_data[1] - var_data[0]
        # Get the number of second between samples.
        delta_seconds = delta.to_value("s")
        return f"{delta_seconds}s"

    def _get_si_conversion(self, var_name, var_data, guess_type, **kwargs):
        if var_name == "time":
            conversion_rate = u.ns.to(u.s)
            si_conversion = f"{conversion_rate:e}>{u.s}"
        else:
            # Get the Units as a String
            if isinstance(var_data, u.Quantity):
                try:
                    conversion_rate = var_data.unit.to(var_data.si.unit)
                    si_conversion = f"{conversion_rate:e}>{var_data.si.unit}"
                except u.UnitConversionError:
                    si_conversion = f"1.0>{var_data.unit}"
            else:
                si_conversion = " > "
        return si_conversion

    def _get_time_base(self, var_name, var_data, guess_type, **kwargs):
        if guess_type == const.CDF_TIME_TT2000.value:
            return "J2000"
        else:
            raise TypeError(f"Time Base for Time type ({guess_type}) not found.")

    def _get_time_scale(self, var_name, var_data, guess_type, **kwargs):
        if guess_type == const.CDF_TIME_TT2000.value:
            return "Terrestrial Time (TT)"
        else:
            raise TypeError(f"Time Scale for Time type ({guess_type}) not found.")

    def _get_units(self, var_name, var_data, guess_type, **kwargs):
        unit = ""
        # Get the Unit from the TimeSeries Quantity if it exists
        if var_name == "time" and guess_type == const.CDF_TIME_TT2000.value:
            return "ns"
        elif var_name == "time":
            raise TypeError(f"Time Units for Time type ({guess_type}) not found.")
        elif hasattr(var_data, "unit") and var_data.unit is not None:
            unit = var_data.unit.to_string()
        # Try to ge the UNITS from the metadata
        elif "UNITS" in var_data.meta and var_data.meta["UNITS"] is not None:
            unit = var_data.meta["UNITS"]
        return unit

    def _get_validmin(self, var_name, var_data, guess_type, **kwargs):
        # Get the Min Value
        minval, _ = self._get_minmax(guess_type)
        return minval

    def _get_validmax(self, var_name, var_data, guess_type, **kwargs):
        # Get the Max Value
        _, maxval = self._get_minmax(guess_type)
        return maxval

    def _get_var_type(self, var_name, var_data, guess_type, **kwargs):
        attr_name = "VAR_TYPE"
        if (attr_name not in var_data.meta) or (not var_data.meta[attr_name]):
            var_type = "data"
        else:
            var_type = var_data.meta[attr_name]
        return var_type

    # =============================================================================================
    #                             SPECTRA METADATA DERIVATIONS
    # =============================================================================================

    def _get_wcs_naxis(self, var_name, var_data, guess_type, **kwargs):
        """
        Function to get the number of axes within a spectra WCS member
        """
        attr_name = "WCSAXES"
        if (attr_name not in var_data.meta) or (not var_data.meta[attr_name]):
            attr_value = var_data.wcs.wcs.naxis
        else:
            attr_value = var_data.meta[attr_name]
        return int(attr_value)

    def _get_wcs_timeref(self, var_name, var_data, guess_type, **kwargs):
        """
        Function to get the reference time within a spectra WCS member
        """
        attr_name = "MJDREF"
        if (attr_name not in var_data.meta) or (not var_data.meta[attr_name]):
            attr_value = var_data.wcs.wcs.mjdref[0]
        else:
            attr_value = var_data.meta[attr_name]
        return attr_value

    def _get_wcs_timeunit(self, var_name, var_data, guess_type, **kwargs):
        """
        Function to get the time units within a spectra WCS member
        """
        attr_name = "TIMEUNIT"
        if (attr_name not in var_data.meta) or (not var_data.meta[attr_name]):
            attr_value = var_data.wcs.wcs.timeunit
        else:
            attr_value = var_data.meta[attr_name]
        return attr_value

    def _get_wcs_timedel(self, var_name, var_data, guess_type, **kwargs):
        """
        Function to get the time delta (between points) within a spectra WCS member
        """
        attr_name = "TIMEDEL"
        if (attr_name not in var_data.meta) or (not var_data.meta[attr_name]):
            attr_value = var_data.wcs.wcs.timedel
        else:
            attr_value = var_data.meta[attr_name]
        return attr_value

    def _get_wcs_dimension_attr(self, var_data, keyword, dimension):
        """
        Function to get the spectra's WCS keywork property along the given axis
        """
        (_, prop, default) = list(
            filter(lambda x: x[0] == keyword, self.wcs_keyword_to_astropy_property)
        )[0]
        # Get the Property for the given WCS Keyword for the given Axis
        property_value = getattr(var_data.wcs.wcs, prop)[dimension]
        # Convert to a String as needed
        if isinstance(property_value, u.UnitBase):
            property_value = property_value.to_string()
        return property_value

    def _get_cnamei(self, var_name, var_data, guess_type, dimension_i):
        keyword = "CNAME"
        # Add the Property Value for the given Axis as a Metadata Attribute
        return self._get_wcs_dimension_attr(
            var_data=var_data, keyword=keyword, dimension=dimension_i
        )

    def _get_ctypei(self, var_name, var_data, guess_type, dimension_i):
        keyword = "CTYPE"
        # Add the Property Value for the given Axis as a Metadata Attribute
        return self._get_wcs_dimension_attr(
            var_data=var_data, keyword=keyword, dimension=dimension_i
        )

    def _get_cuniti(self, var_name, var_data, guess_type, dimension_i):
        keyword = "CUNIT"
        # Add the Property Value for the given Axis as a Metadata Attribute
        return self._get_wcs_dimension_attr(
            var_data=var_data, keyword=keyword, dimension=dimension_i
        )

    def _get_crpixi(self, var_name, var_data, guess_type, dimension_i):
        keyword = "CRPIX"
        # Add the Property Value for the given Axis as a Metadata Attribute
        return self._get_wcs_dimension_attr(
            var_data=var_data, keyword=keyword, dimension=dimension_i
        )

    def _get_crvali(self, var_name, var_data, guess_type, dimension_i):
        keyword = "CRVAL"
        # Add the Property Value for the given Axis as a Metadata Attribute
        return self._get_wcs_dimension_attr(
            var_data=var_data, keyword=keyword, dimension=dimension_i
        )

    def _get_cdelti(self, var_name, var_data, guess_type, dimension_i):
        keyword = "CDELT"
        # Add the Property Value for the given Axis as a Metadata Attribute
        return self._get_wcs_dimension_attr(
            var_data=var_data, keyword=keyword, dimension=dimension_i
        )

    # =============================================================================================
    #                             GLOBAL METADATA DERIVATIONS
    # =============================================================================================

    def _get_logical_file_id(self, data):
        """
        Function to get the `Logical_file_id` required global attribute.

        The attribute stores the name of the CDF File without the file
        extension (e.g. '.cdf'). This attribute is requires to avoid
        loss of the originial source in case of renaming.
        """
        attr_name = "Logical_file_id"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            # Get Parts
            instrument_id = self._get_instrument_id(data)
            start_time = self._get_start_time(data)
            data_level = self._get_data_level(data)
            version = self._get_version(data)
            mode = self._get_instrument_mode(data)

            # Build Derivation
            science_filename = util.create_science_filename(
                instrument=instrument_id,
                time=start_time,
                level=data_level,
                version=version,
                mode=mode,
            )
            science_filename = science_filename.rstrip(
                swxsoc.config["mission"]["file_extension"]
            )
        else:
            science_filename = data.meta[attr_name]
        return science_filename

    def _get_logical_source(self, data):
        """
        Function to get the `Logical_source` required global attribute.

        This attribute determines the file naming convention in the SKT Editor
        and is used by CDA Web.
        """
        attr_name = "Logical_source"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            # Get Parts
            spacecraft_id = self._get_spacecraft_id(data)
            instrument_id = self._get_instrument_id(data)
            data_type = self._get_data_type(data)
            data_type_short_name, _ = data_type.split(">")

            # Build Derivation
            logical_source = f"{spacecraft_id}_{instrument_id}_{data_type_short_name}"
        else:
            logical_source = data.meta[attr_name]
        return logical_source

    def _get_logical_source_description(self, data):
        """
        Function to get the `Logical_source_description` required global attribute.

        This attribute writes out the full words associated with the encryped
        `Logical_source`  attribute.
        """
        attr_name = "Logical_source_description"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            # Get Parts
            spacecraft_long_name = self._get_spacecraft_long_name(data)
            instrument_long_name = self._get_instrument_long_name(data)
            data_type = self._get_data_type(data)
            _, data_type_long_name = data_type.split(">")
            logical_source_description = (
                f"{spacecraft_long_name} {instrument_long_name} {data_type_long_name}"
            )
        else:
            logical_source_description = data.meta[attr_name]
        return logical_source_description

    def _get_data_type(self, data):
        """
        Function to get the `Data_type` required global attribute.

        This attribute is used by the CDF Writing software to create the filename.
        It is a combination of the following components:
            - mode
            - data_level
            - optional_data_product_descriptor
        """
        attr_name = "Data_type"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            short_parts = []
            long_parts = []

            # Get `mode`
            mode_short_name = self._get_instrument_mode(data)
            mode_long_name = self._get_instrument_mode(data)
            if bool(mode_short_name and mode_long_name):
                short_parts.append(mode_short_name)
                long_parts.append(mode_long_name)

            # Get `data level`
            data_level_short_name = self._get_data_level(data)
            data_level_long_name = self._get_data_level_long_name(data)
            if bool(data_level_short_name and data_level_long_name):
                short_parts.append(data_level_short_name)
                long_parts.append(data_level_long_name)

            # Get `data product descriptor`
            odpd_short_name = self._get_data_product_descriptor(data)
            odpd_long_name = self._get_data_product_descriptor(data)
            if bool(odpd_short_name and odpd_long_name):
                short_parts.append(odpd_short_name)
                long_parts.append(odpd_long_name)

            # Build Derivation
            data_type = "_".join(short_parts) + ">" + " ".join(long_parts)
        else:
            data_type = data.meta[attr_name]
        return data_type

    def _get_spacecraft_id(self, data):
        """Function to get Spacecraft ID from Source_name Global Attribute"""
        attr_name = "Source_name"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            # Get Module Default
            sc_id = swxsoc.config["mission"]["mission_name"]
        else:
            sc_id = data.meta["Source_name"]
            # Formatting
            if ">" in sc_id:
                short_name, _ = sc_id.split(">")
                sc_id = short_name.lower()  # Makse sure its all lowercase
        return sc_id

    def _get_spacecraft_long_name(self, data):
        """Function to get Spacecraft ID from Source_name Global Attribute"""
        attr_name = "Source_name"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            # Get Module Default
            sc_id = swxsoc.config["mission"]["mission_name"]
        else:
            sc_id = data.meta["Source_name"]
            # Formatting
            if ">" in sc_id:
                _, long_name = sc_id.split(">")
                sc_id = long_name
        return sc_id

    def _get_instrument_id(self, data):
        """
        Function to get Instrument ID from Descriptor Global Attribute

        Instrument of investigation identifier shortened to three
        letter acronym.
        """
        attr_name = "Descriptor"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            instr_id = None
        else:
            instr_id = data.meta["Descriptor"]
            # Formatting
            if ">" in instr_id:
                short_name, _ = instr_id.split(">")
                instr_id = short_name.lower()  # Makse sure its all lowercase
        return instr_id

    def _get_instrument_long_name(self, data):
        """
        Function to get Instrument ID from Descriptor Global Attribute

        Instrument of investigation identifier shortened to three
        letter acronym.
        """
        attr_name = "Descriptor"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            instr_id = None
        else:
            instr_id = data.meta["Descriptor"]
            # Formatting
            if ">" in instr_id:
                _, long_name = instr_id.split(">")
                instr_id = long_name
        return instr_id

    def _get_data_level(self, data):
        """
        Function to get Data Level of CDF data

        The level to which the data product has been processed.
        """
        attr_name = "Data_level"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            data_level = None
        else:
            data_level = data.meta["Data_level"]
            # Formatting
            if ">" in data_level:
                short_name, _ = data_level.split(">")
                data_level = short_name.lower()  # Makse sure its all lowercase
        return data_level

    def _get_data_level_long_name(self, data):
        """
        Function to get Data Level of CDF data

        The level to which the data product has been processed.
        """
        attr_name = "Data_level"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            data_level = None
        else:
            data_level = data.meta["Data_level"]
            # Formatting
            if ">" in data_level:
                _, long_name = data_level.split(">")
                data_level = long_name
        return data_level

    def _get_data_product_descriptor(self, data):
        """
        Function to get the (Optional) Data Product Descriptor.

        This is an optional field that may not be needed for all products. Where it is used,
        identifier shouls be short (3-8 charachters) descriptors that are helpful to end users.
        If a descriptor contains multiple components, underscores are used top separate
        hose components.
        """
        attr_name = "Data_product_descriptor"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            odpd = ""
        else:
            odpd = data.meta["Data_product_descriptor"]
        return odpd

    def _get_generation_date(self, data):
        """
        Function to get the date that the CDF was generated.
        """
        return Time.now().strftime("%Y-%m-%d")

    def _get_start_time(self, data):
        """
        Function to get the start time of the data contained in the CDF
        given in format `YYYYMMDDThhmmss`
        """
        # Get the Start Time from the TimeSeries
        return data["time"][0].isot

    def _get_version(self, data):
        """
        Function to get the 3-part version number of the data product.
        """
        attr_name = "Data_version"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            version = None
        else:
            version_str = data.meta["Data_version"].lower()
            if "v" in version_str:
                _, version = version_str.split("v")
            else:
                version = version_str
        return version

    def _get_instrument_mode(self, data):
        """Function to get the mode attribute (TBS)"""
        attr_name = "Instrument_mode"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            instr_mode = ""
        else:
            instr_mode = data.meta["Instrument_mode"]
        return instr_mode.lower()  # Makse sure its all lowercase

    def _get_swxsoc_version(self, data):
        """Function to get the version of SWxSOC used to generate the data"""
        attr_name = "SWxSOC_version"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            swxsoc_version = swxsoc.__version__
        else:
            swxsoc_version = data.meta[attr_name]
        return swxsoc_version

    def _get_cdf_lib_version(self, data):
        """Function to get the version of CDF library used to generate the data"""
        attr_name = "CDF_Lib_version"
        if (attr_name not in data.meta) or (not data.meta[attr_name]):
            try:
                import spacepy.pycdf as pycdf

                cdf_lib_version = pycdf.lib.version
            except (ImportError, AttributeError) as e:
                cdf_lib_version = "unknown version"
        else:
            cdf_lib_version = data.meta[attr_name]
        return cdf_lib_version
