.. _schema_information_guide:

***********************************************************
Using SWxSOC Schema for Metadata Attributes
***********************************************************

Overview
========

The :py:class:`~swxsoc.util.schema.SWXSchema` class provides an interface to configure
how metadata attributes are formatted in SWxSOC affiliated data products. 
The class represents a schema for metadata attribute requirements, validation, and formatting. 

The schema class is used in many parts of the package:

    - Loading/Writing data attributes in :py:class:`~swxsoc.util.io.SWXIOHandler` classes
    - Deriving data attributes for :py:class:`~swxsoc.swxdata.SWXData` objects
    - Validating data attributes in the :py:class:`~swxsoc.util.validation.SWXDataValidator` class

It is important to understand the configuration options of :py:class:`~swxsoc.util.schema.SWXSchema` 
objects in order to attain the desired behavior of metadata attributes. 

The :py:class:`~swxsoc.util.schema.SWXSchema` class has two main attributes.
The class contains a :py:attr:`~swxsoc.util.schema.SWXSchema.global_attribute_schema`
member which configures global, or file level, metadata attributes. 
Second, the class contains a  :py:attr:`~swxsoc.util.schema.SWXSchema.variable_attribute_schema`
member which configures variable or measurement level metadata attributes. 
This guide contains two sections <INSERT SECTIONS HERE> detailing the format of each of these class members, 
how they're used, and how you can extend or modify them to meet your specific needs. 

Each of the global and variable schemas are loaded from YAML (dict-like) files which can be 
combined to layer multiple schema elements into a single unified schema. 
This allows extensions and overrides to the default schema, and allows you to create new 
schema configurations for specific archive file types and specific metadata requirements.

Creating a SWxSOC Schema
========================

Creating a :py:class:`~swxsoc.util.schema.SWXSchema` object directly includes passing one or more paths
to schema files to layer on top of one another, and optionally whether to use the default base layer
schema files. For more information on the default, base layer, schema files please see our :doc:`CDF Format Guide </user-guide/cdf_format_guide>`.

Here is an example of instantiation of a :py:class:`~swxsoc.util.schema.SWXSchema` object: 

.. code-block:: python

    global_layers = ["my_global_layer_1.yaml", "my_global_layer_2.yaml"]
    variable_layers = ["my_variable_layer_1.yaml", "my_variable_layer_2.yaml"]
    my_schema = SWXSchema(
        global_schema_layers=global_layers,
        variable_schema_layers=variable_layers,
        use_defaults=False
    )

This will create a new schema object from scratch, without using the default CDF schema, and will overlay 
the `layer_2` files over the `layer_1` files. If there are no conflicts within the schema files, then 
their attributes will be merged, to create a superset of the two files.
If there are conflicts in the combination of schema layers, this is resolved in a latest-priority 
ordering. That is, if the are conflicts or duplicate keys in `layer_1` that also appear in `layer_2`, 
then the second layer will overwrite the values from the first layer in the resulting schema. 

For an example of how to extend the :py:class:`~swxsoc.util.schema.SWXSchema` class please see the code
documentation for our affiliated HERMES mission schema :py:class:`~hermes_core.util.schema.HermesDataSchema`. 

Global Attribute Schemas
========================

Global metadata attribute schemas are used to define requirements, formatting, and derivations 
at the global or file level. The global attribute schema is configured through YAML files, with 
the default configuration in :file:`swxsoc/data/swxsoc_default_global_cdf_attrs_schema.yaml`

The YAML file represents a dictionary of attribute information, keyed by the metadata attribute name. 
Information on the file format can be seen below:

.. code-block:: yaml

    attribute_name:
        description: <string>
        default: <string>
        derived: <bool>
        derivation_fn: <string>
        required: <bool>
        validate: <bool>
        overwrite: <bool>
    attriubte_name: 
        description: <string> ...

Each of the keys for global metadata requirements are defined in the table below. 

.. list-table:: Global Attribute Schema
    :widths: 20 50 10 10
    :header-rows: 1

    * - Schema Key
      - Description
      - Data Type
      - Is Required?
    * - `attribute_name`
      - the name of the global metadata attribute as it should appear in your data products
      - `str`
      - `True`
    * - `description`
      - a description for the global metadata attribute and context needed to understand its values
      - `str`
      - `True`
    * - `default`
      - a default value for the attribute if needed/desired
      - `str` or `null`
      - `True`
    * - `derived`
      - whether or not the attribute's value can be derived using a python function
      - `bool`
      - `True`
    * - `derivation_fn`
      - the name of a Python function to derive the value. Must be a function member of the schema class and match the signature below.
      - `str`
      - required only of `derived=True`
    * - `required`
      - whether the global attribute is required in your data products 
      - `bool`
      - `True`
    * - `validate`
      - whether the attribute should be validated in your data products by the :py:class:`~swxsoc.util.validation.SWXDataValidator` module
      - `bool`
      - `True`
    * - `overwrite`
      - whether an existing value for the attribute should be overwritten if a different value is derived.
      - `bool`
      - `True`

For more information on the default CDF schema, conforming to ISTP standards, please see the :doc:`CDF Format Guide </user-guide/cdf_format_guide>`. 

Global Attribute Derivation
---------------------------

Global attributes can be derived by:

    - Setting the `derived: true` keyword in the global attribute schema file
    - Setting the `derivation_fn: _my_derivation_fn` keyword in the global attribute schema file
    - Writing a python function `my_derivation_fn` in a sub-class of the :py:class:`~swxsoc.util.schema.SWXSchema` class

Global attributes are derived in the :py:class:`~swxsoc.util.schema.SWXSchema` class in the following way. 
The code provided here is just pseudocode, and not the actual function code. For the actual code please see the :py:func:`~swxsoc.util.schema.SWXSchema.derive_global_attributes` function documentation. 

.. rstcheck: ignore-next-code-block
.. code-block:: python

    for attr_name, attr_schema in self.global_attribute_schema:
        derivation_fn = getattr(self, attr_schema["derivation_fn"])
        global_attributes[attr_name] = derivation_fn(my_data_container)

where `my_data_container` is an instance of a :py:class:`~swxsoc.swxdata.SWXData` object or an extended class object. 

The derivation functions, which must me a class function of a :py:class:`~swxsoc.util.schema.SWXSchema` class, must follow the following signature:

.. code-block:: python

    def _my_derivation_fn(self, my_data_container: SWXData):
        # ... do manipulations as needed from `data`
        return "attribute_value"

These functions must take in a single parameter, an instance of a :py:class:`~swxsoc.swxdata.SWXData` object or an extended class object. 
These functions must return a single Python primitive type (`str`, `int`, `float`) or `~astropy.time.Time` object representing the value for the given attribute. 

Variable Attribute Schemas
==========================

Variable metadata attribute schemas are used to define requirements, formatting, and derivations
at the variable or measurement level. The variable attribute schema is configured through YAML files,
with the default configuration in file :file:`swxsoc/data/swxsoc_default_variable_cdf_attrs_schema.yaml`.

The variable attribute schema YAML file has two main parts.

    - The first part is the `attribute_key`, which is a dictionary of attribute information, keyed by the metadata attribute name. This part of the schema is formatted similarly to the global schema above. 
    - The second part is an index of what metadata attributes are required for different variable types. This defines what attributes are required for `data` variable types compared to `support_data` and `metadata` variable types. Additional indexes are used for unique `epoch` (time-specific) and `spectra` (uniquely multi-dimensional) variable types. 

An example of a valid file format can be seen below. 

.. code-block:: yaml

    attribute_key: 
        attribute_name_1:
            description: <string>
            derived: <bool>
            derivation_fn: <string>
            iterable: <bool>
            required: <bool>
            valid_values: <bool>
            overwrite: <bool>
            alternate: <string>
        attribute_name_2: 
            description: <string> ...
        time_attribute:
            description: <string> ...
        spectra_attribute_i:
            description: <string> ...
    data:
      - attribute_name_1
      - attribute_name_2
    support_data:
      - attribute_name_2
    metadata:
      - attribute_name_3
    epoch:
      - time_attribute
    spectra: 
      - spectra_attribute_i

Each of the keys for variable metadata requirements are defined in the table below. 

.. list-table:: Variable Attribute Schema
    :widths: 15 50 7 18
    :header-rows: 1

    * - Schema Key
      - Description
      - Data Type
      - Is Required?
    * - `attribute_name`
      - the name of the variable metadata attribute as it should appear in your data products
      - `str`
      - `True`
    * - `description`
      - a description for the variable metadata attribute and context needed to understand its values
      - `str`
      - `True`
    * - `derived`
      - whether or not the attribute's value can be derived using a python function
      - `bool`
      - `True`
    * - `derivation_fn`
      - the name of a Python function to derive the value. Must be a function member of the schema class and match the signature below.
      - `str`
      - required only of `derived=True`
    * - `iterable`
      - whether the attribute should be derived multiple times for different axes of the measurement or spectra. For example the `CNAMEi` attribute, used for WCS coordinate transformations is `iterable` since its value can be derived for each WCS axis of the measurement. 
      - `bool`
      - required only if `derived=True` AND attribute takes multiple values for different dimensions
    * - `required`
      - whether the variable attribute is required in your data products 
      - `bool`
      - `True`
    * - `overwrite`
      - whether an existing value for the attribute should be overwritten if a different value is derived.
      - `bool`
      - `True`
    * - `valid_values`
      - values that the attribute should be checked against by the :py:class:`~swxsoc.util.validation.SWXDataValidator` module
      - `list[str]` or `null`
      - `True`
    * - `alternate`
      - the potential name of a different attribute should be considered in replacement of the given attribute. For example, only one of `LABLAXIS` or `LABL_PTR_i` are required in ISTP guidelines and are treated as alternates here. 
      - `str` or `null`
      - `True`

For more information on the default CDF schema, conforming to ISTP standards, please see the :doc:`CDF Format Guide </user-guide/cdf_format_guide>`. 

Variable Attribute Derivation
-----------------------------

Variable attributes can be derived by:

    - Setting the `derived: true` keyword in the variable attribute schema file
    - Setting the `derivation_fn: _my_derivation_fn` keyword in the variable attribute schema file
    - Writing a python function `my_derivation_fn` in a sub-class of the :py:class:`~swxsoc.util.schema.SWXSchema` class

Variable attributes are derived in the :py:class:`~swxsoc.util.schema.SWXSchema` class in the following way. 
The code provided here is just pseudocode, and not the actual function code. For the actual code please see the :py:func:`~swxsoc.util.schema.SWXSchema.derive_measurement_attributes` function documentation. 

.. rstcheck: ignore-next-code-block
.. code-block:: python

    derived_attributes = [] # collect derived attributes based on Index, whether the variable is an Epoch variable, and whether it is a Spectra variable
    for attr_name, attr_schema in derived attributes:
        if attr_schema["iterable"]:
            num_dimensions = self.get_num_dimensions(variable_data)
            for dimension_i in num_dimensions:
                derivation_fn = getattr(self, attr_schema["derivation_fn"])
                variable_attributes[dimension_attr_name] = derivation_fn(
                    variable_name, variable_data, cdf_data_type, dimension_i
                )
        else:
            derivation_fn = getattr(self, attr_schema["derivation_fn"])
            variable_attributes[attr_name] = derivation_fn(
                variable_name, variable_data, cdf_data_type
            )

The signature for functions to derive variable attributes depends on whether the attribute is `iterable`.
However, they all share the three common parameters below/ 
The function takes in parameters `var_name`, `var_data`, and `guess_type`, where:

    - `var_name` is the variable name of the variable for which the attribute is being derived
    - `var_data` is the variable data of the variable for which the attribute is being derived
    - `guess_type` is the guessed CDF variable type of the data for which the attribute is being derived.

Derivation functions for `iterable` attributes take an extra parameter `dimension_i` which the 0-based index for which dimension to derive the attribute for. 

"Standard" Variable Attribute Derivations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The derivation functions single-dimensional attriubtes, which must me a class function of a :py:class:`~swxsoc.util.schema.SWXSchema` class, must follow the following signature:

.. code-block:: python

    def _my_derivation_fn(self, var_name: str, var_data: Union[Quantity, NDData, NDCube], guess_type: ctypes.c_long):
        # ... do manipulations as needed from data
        return "attribute_value"

These functions must return a single Python primitive type (`str`, `int`, `float`) or `~astropy.time.Time` object representing the value for the given attribute. 

Time/Epoch-Specific Variable Attribute Derivations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The derivation functions for time-specific attributes follow the same requirements as "standard" attributes. 
Additionally time-specific attributes must by listed in the `epoch` index of the variable attribute schema file. 
The derivation functions follow the same signature as "standard" derivation fucntions:

.. code-block:: python

    def _my_derivation_fn(self, var_name: str, var_data: Union[Quantity, NDData, NDCube], guess_type: ctypes.c_long):
        # ... do manipulations as needed from data
        return "attribute_value"

These functions must return a single Python primitive type (`str`, `int`, `float`) or `~astropy.time.Time` object representing the value for the given attribute. 

Spectra-Specific Variable Attribute Derivations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The derivation functions for spectra-specific attributes, or attributes that can take multiple values for different dimensions of the variable, follow a similar signature as "standard" attributes. 
Additionally spectra-specific attributes must be listed in the `spectra` index of the variable attribute schema file. 
The derivation functions have an added `dimension_i` parameter which is a 0-based index of the dimension to derive the attribute for. 

.. code-block:: python

    def _my_derivation_fn(self, var_name: str, var_data: Union[NDData, NDCube], guess_type: ctypes.c_long, dimension_i: int):
        # ... do manipulations as needed from data
        return "attribute_value"

These functions must return a single Python primitive type (`str`, `int`, `float`) or `~astropy.time.Time` object representing the value for the given attribute. 
