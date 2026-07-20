"""Tests for Loading and Saving data from data containers"""

import tempfile
from collections import OrderedDict
from pathlib import Path

import astropy.units as u
import numpy as np
import pytest
from astropy.nddata import NDData
from astropy.table import Table
from astropy.time import Time
from astropy.timeseries import TimeSeries
from astropy.units import Quantity
from astropy.utils.masked import Masked
from astropy.wcs import WCS
from ndcube import NDCollection, NDCube
from numpy.random import random
pytest.importorskip("spacepy.pycdf")
from spacepy.pycdf import CDF, CDFError

from swxsoc.io import fillval as fv
from swxsoc.swxdata import SWXData
from swxsoc.util import const
from swxsoc.util.exceptions import SWXUserWarning


def save_cdf_for_examination(sw_data, filename=None):
    """Save a copy to current dir for examination with custom filename or logical id.
    No output path will put it in the current directory which is the point of this
    function."""
    if False:  # change to True if you'd like to use this feature
        if filename:
            # Add .cdf suffix if not already present
            if not filename.endswith(".cdf"):
                filename = filename + ".cdf"
        sw_data.save(output_path=filename, overwrite=True)


def get_test_sw_data():
    """
    Function to get test swxsoc.swxdata.SWXData objects to re-use in other tests
    """
    ts = TimeSeries()
    ts.meta.update(
        {
            "Descriptor": "EEA>Electron Electrostatic Analyzer",
            "Data_level": "l1>Level 1",
            "Data_version": "v0.0.1",
            "MODS": [
                "v0.0.0 - Original version.",
                "v1.0.0 - Include trajectory vectors and optics state.",
                "v1.1.0 - Update metadata: counts -> flux.",
                "v1.2.0 - Added flux error.",
                "v1.3.0 - Trajectory vector errors are now deltas.",
            ],
        }
    )

    # Create an astropy.Time object
    time = np.arange(10)
    time_col = Time(time, format="unix")
    ts["time"] = time_col

    # Add Measurement
    quant = Quantity(value=random(size=(10)), unit="m", dtype=np.uint16)
    ts["measurement"] = quant
    ts["measurement"].meta = OrderedDict(
        {
            "VAR_TYPE": "data",
            "CATDESC": "Test Data",
        }
    )

    # Support Data / Non-Time Varying Data
    support = {
        "support_counts": NDData(
            data=[1], meta={"CATDESC": "variable counts", "VAR_TYPE": "support_data"}
        )
    }

    # Spectra Data
    spectra = NDCollection(
        [
            (
                "test_spectra",
                NDCube(
                    data=random(size=(10, 10)),
                    wcs=WCS(naxis=2),
                    meta={"CATDESC": "Test Spectra Variable"},
                    unit="eV",
                ),
            )
        ]
    )

    # Create SWXData Object
    sw_data = SWXData(timeseries=ts, support=support, spectra=spectra)

    return sw_data


def test_cdf_io():
    """Test CDF IO Handler on Default Data"""
    # Get Test Datas
    td = get_test_sw_data()

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Convert SWXData the to a CDF File
        test_file_output_path = td.save(output_path=tmpdirname)
        save_cdf_for_examination(td, "io")

        # Verify single-timeseries files don't have Default_Timeseries_Key
        with CDF(str(test_file_output_path)) as cdf_file:
            # Single timeseries should not have this attribute at all
            assert "Default_Timeseries_Key" not in cdf_file.attrs

        # Load the CDF to a SWXData Object
        td_loaded = SWXData.load(test_file_output_path)

        assert len(td.timeseries) == len(td_loaded.timeseries)
        assert len(td.timeseries.columns) == len(td_loaded.timeseries.columns)

        with pytest.raises(CDFError):
            td_loaded.save(output_path=tmpdirname)


def test_cdf_bad_file_path():
    """Test Loading CDF from a non-existant file"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        # Try loading from non-existant_path
        with pytest.raises(FileNotFoundError):
            _ = SWXData.load(tmp_path / "non_existant_file.cdf")


def test_cdf_nrv_support_data():
    """
    Test Loading Non-Record-Varying data with CDF IO Handler
    """
    # Get Test Datas
    td = get_test_sw_data()

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        # Convert HermesData the to a CDF File
        test_file_output_path = td.save(output_path=tmp_path)
        save_cdf_for_examination(td, "nrv_support_data")

        # Load the JSON file as JSON
        with CDF(str(test_file_output_path), readonly=False) as cdf_file:
            # Add Non-Record-Varying Variable
            cdf_file.new(
                name="Test_NRV_Var", data=[1, 2, 3], type=const.CDF_INT4, recVary=False
            )
            cdf_file["Test_NRV_Var"].meta["VAR_TYPE"] = "support_data"

            # Add Support Data Variable
            cdf_file["Test_Support_Var"] = np.arange(10)
            cdf_file["Test_Support_Var"].meta["UNITS"] = "counts"
            cdf_file["Test_Support_Var"].meta["VAR_TYPE"] = "support_data"

        # Make sure we can load the modified JSON
        td_loaded = SWXData.load(test_file_output_path)

        assert "Test_NRV_Var" in td_loaded.support
        assert "Test_Support_Var" in td_loaded.timeseries.columns


def test_with_no_epoch_var():
    """
    Test loading a CDF file with no epoch variables (only support/NRV data).

    This tests that:
    1. A warning is issued when no Epoch variables are found in the CDF file
    2. Loading fails with a clear ValueError explaining that SWXData requires time series data

    SWXData cannot be created without at least one timeseries with time data,
    as it is fundamental to the data model and required for metadata derivation.
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        test_file_path = tmp_path / "no_epoch_test.cdf"

        # Manually create a CDF file with only NRV/support variables (no Epoch)
        with CDF(str(test_file_path), "") as cdf_file:
            # Add global attributes
            cdf_file.attrs["Descriptor"] = "TEST>Test Instrument"
            cdf_file.attrs["Data_level"] = "l1>Level 1"
            cdf_file.attrs["Data_version"] = "v0.0.1"

            # Add Non-Record-Varying Variable
            cdf_file.new(
                name="Config_Value",
                data=[1, 2, 3],
                type=pycdf.const.CDF_INT4,
                recVary=False,
            )
            cdf_file["Config_Value"].attrs["VAR_TYPE"] = "support_data"
            cdf_file["Config_Value"].attrs["CATDESC"] = "Configuration values"
            cdf_file["Config_Value"].attrs["FIELDNAM"] = "Config_Value"

            # Add another NRV variable
            cdf_file.new(
                name="Calibration_Factor",
                data=1.5,
                type=pycdf.const.CDF_FLOAT,
                recVary=False,
            )
            cdf_file["Calibration_Factor"].attrs["VAR_TYPE"] = "support_data"
            cdf_file["Calibration_Factor"].attrs["CATDESC"] = "Calibration factor"
            cdf_file["Calibration_Factor"].attrs["FIELDNAM"] = "Calibration_Factor"
            cdf_file["Calibration_Factor"].attrs["UNITS"] = ""

        # Attempting to load should issue a warning AND then fail with ValueError
        # because SWXData requires time series data
        with pytest.warns(
            SWXUserWarning, match="No Epoch variables found in CDF file"
        ) as warning_list:
            with pytest.raises(
                ValueError, match="Cannot load CDF file without Epoch variables"
            ):
                SWXData.load(test_file_path)

        # Verify the warning was issued from the CDF handler
        assert any(
            "No Epoch variables found in CDF file" in str(w.message)
            for w in warning_list
        )
        # Find the specific warning about no epoch variables
        target_warnings = [
            w
            for w in warning_list
            if "No Epoch variables found in CDF file" in str(w.message)
            and w.category == SWXUserWarning
        ]
        assert len(target_warnings) >= 1, "Expected warning about no Epoch variables"


def test_epoch_key_with_hyphen_rejected():
    """
    Test that epoch keys with hyphens are rejected with a clear error message.

    Multi-timeseries dict keys MUST use underscores only (no hyphens) to match
    CDF variable naming conventions. Hyphens are not valid in CDF variable names,
    so using them in dict keys would cause round-trip failures.

    This test verifies that SWXData.__init__() validates dict keys and rejects
    invalid characters with a helpful error message.

    Invalid: "REACH-134", "SAT-A" (contain hyphens)
    Valid: "REACH_134", "SAT_A" (underscores only)
    """
    # Create two TimeSeries
    ts1 = TimeSeries()
    ts1["time"] = Time([1704067200, 1704067201, 1704067202], format="unix")
    ts1["voltage"] = Quantity([1.0, 2.0, 3.0], unit="V", dtype=np.float32)
    ts1["voltage"].meta = {"VAR_TYPE": "data", "CATDESC": "Voltage measurement"}

    ts2 = TimeSeries()
    ts2["time"] = Time([1704067200, 1704067201, 1704067202], format="unix")
    ts2["current"] = Quantity([4.0, 5.0, 6.0], unit="A", dtype=np.float32)
    ts2["current"].meta = {"VAR_TYPE": "data", "CATDESC": "Current measurement"}

    # Attempt to create SWXData with invalid hyphenated dict keys
    timeseries_dict = {
        "REACH_134": ts1,  # Valid (underscores)
        "REACH-172": ts2,  # Invalid (contains hyphen)
    }

    meta = {
        "Descriptor": "EEA>Electron Electrostatic Analyzer",
        "Data_level": "l1>Level 1",
        "Data_version": "v0.0.1",
    }

    # Should raise ValueError about invalid characters in dict key
    with pytest.raises(
        ValueError, match=".*hyphen.*|.*invalid.*character.*|.*REACH-172.*"
    ):
        SWXData(timeseries=timeseries_dict, meta=meta)


def test_cdf_spectra_data():
    """
    Test Loading High-Dimensional/ Spectra data with CDF IO Handler
    """
    # Get Test Datas
    td = get_test_sw_data()

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        # Convert HermesData the to a CDF File
        test_file_output_path = td.save(output_path=tmp_path)
        save_cdf_for_examination(td, "spectra_data")

        # Load the JSON file as JSON
        with CDF(str(test_file_output_path), readonly=False) as cdf_file:
            # Add Spectra Data Variable
            cdf_file["Test_Spectra_Var"] = np.random.random(size=(10, 10))
            cdf_file["Test_Spectra_Var"].meta["UNITS"] = "counts"

        # Make sure we can load the modified JSON
        td_loaded = SWXData.load(test_file_output_path)

        assert "Test_Spectra_Var" in td_loaded.spectra


def test_cdf_auto_prefixing_prevents_duplicates():
    """
    Test that selective prefixing only applies to conflicting variable names.
    Variables that appear in multiple timeseries get prefixed, while unique
    variables remain unprefixed.
    """
    # Create multiple TimeSeries with some duplicate and some unique column names
    # With the epoch_key fix, all TimeSeries can now have the same length
    ts1 = TimeSeries()
    # Use 2024 timestamps (unix timestamp for 2024-01-01 is ~1704067200)
    ts1["time"] = Time(1704067200 + np.arange(5), format="unix")
    # Lat and Lon are duplicated across all satellites (will be prefixed)
    ts1["Lat"] = Quantity(value=np.random.random(5), unit="deg", dtype=np.float32)
    ts1["Lat"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Latitude from satellite 1",
    }
    ts1["Lon"] = Quantity(value=np.random.random(5), unit="deg", dtype=np.float32)
    ts1["Lon"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Longitude from satellite 1",
    }
    # Sensor_A is unique to this satellite (no prefix needed)
    ts1["Sensor_A"] = Quantity(value=np.random.random(5), unit="count", dtype=np.uint16)
    ts1["Sensor_A"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor A from satellite 1",
    }

    ts2 = TimeSeries()
    ts2["time"] = Time(1704067200 + np.arange(5), format="unix")
    # Lat and Lon are duplicated (will be prefixed)
    ts2["Lat"] = Quantity(value=np.random.random(5), unit="deg", dtype=np.float32)
    ts2["Lat"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Latitude from satellite 2",
    }
    ts2["Lon"] = Quantity(value=np.random.random(5), unit="deg", dtype=np.float32)
    ts2["Lon"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Longitude from satellite 2",
    }
    # Sensor_B is unique to this satellite (no prefix needed)
    ts2["Sensor_B"] = Quantity(value=np.random.random(5), unit="count", dtype=np.uint16)
    ts2["Sensor_B"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor B from satellite 2",
    }

    ts3 = TimeSeries()
    ts3["time"] = Time(1704067200 + np.arange(5), format="unix")
    # Lat and Lon are duplicated (will be prefixed)
    ts3["Lat"] = Quantity(value=np.random.random(5), unit="deg", dtype=np.float32)
    ts3["Lat"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Latitude from satellite 3",
    }
    ts3["Lon"] = Quantity(value=np.random.random(5), unit="deg", dtype=np.float32)
    ts3["Lon"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Longitude from satellite 3",
    }
    # Sensor_C is unique to this satellite (no prefix needed)
    ts3["Sensor_C"] = Quantity(value=np.random.random(5), unit="count", dtype=np.uint16)
    ts3["Sensor_C"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor C from satellite 3",
    }

    # Create SWXData with multiple TimeSeries keyed by satellite name
    timeseries_dict = {
        "REACH_165": ts1,
        "REACH_134": ts2,
        "REACH_099": ts3,
    }

    meta = {
        "Descriptor": "EEA>Electron Electrostatic Analyzer",
        "Data_level": "l1>Level 1",
        "Data_version": "v0.0.1",
    }

    sw_data = SWXData(timeseries=timeseries_dict, meta=meta)

    # Verify .time and .time_range work before saving (tests the fallback to first key)
    # This should NOT raise KeyError even though meta doesn't have Default_Timeseries_Key yet
    original_time = sw_data.time  # Should access REACH_165's time (the first dict key)
    assert len(original_time) == 5
    original_time_range = sw_data.time_range
    assert original_time_range[0] < original_time_range[1]

    # Test that selective prefixing prevents collisions
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)

        test_file_output_path = sw_data.save(output_path=tmp_path)
        save_cdf_for_examination(sw_data, "auto_prefixing_prevents_duplicates")

        # Verify the CDF file was created
        assert test_file_output_path.exists()

        # Verify all prefixed variables exist in the CDF file
        with CDF(str(test_file_output_path)) as cdf_file:
            # First timeseries uses unprefixed "Epoch" for ISTP compliance
            assert "Epoch" in cdf_file  # REACH_165 is first, gets default "Epoch"
            assert "REACH_134_Epoch" in cdf_file
            assert "REACH_099_Epoch" in cdf_file
            assert (
                "REACH_165_Epoch" not in cdf_file
            )  # REACH_165 uses unprefixed "Epoch"

            # Lat and Lon conflict - first occurrence unprefixed, rest prefixed
            assert "Lat" in cdf_file  # REACH_165 (first) gets unprefixed
            assert "Lon" in cdf_file  # REACH_165 (first) gets unprefixed
            assert "REACH_134_Lat" in cdf_file
            assert "REACH_134_Lon" in cdf_file
            assert "REACH_099_Lat" in cdf_file
            assert "REACH_099_Lon" in cdf_file
            assert "REACH_165_Lat" not in cdf_file  # First occurrence stays unprefixed
            assert "REACH_165_Lon" not in cdf_file  # First occurrence stays unprefixed

            # Sensor columns are unique - should NOT be prefixed
            assert "Sensor_A" in cdf_file
            assert "Sensor_B" in cdf_file
            assert "Sensor_C" in cdf_file

            # Should NOT have prefixed sensor variables
            assert "REACH_165_Sensor_A" not in cdf_file
            assert "REACH_134_Sensor_B" not in cdf_file
            assert "REACH_099_Sensor_C" not in cdf_file

            # Verify each has the correct length
            assert len(cdf_file["Lat"]) == 5  # First occurrence unprefixed
            assert len(cdf_file["REACH_134_Lat"]) == 5
            assert len(cdf_file["REACH_099_Lat"]) == 5

            # Verify DEPEND_0 points to the correct epoch for all variables
            assert (
                cdf_file["Lat"].attrs["DEPEND_0"] == "Epoch"
            )  # First occurrence unprefixed
            assert (
                cdf_file["Lon"].attrs["DEPEND_0"] == "Epoch"
            )  # First occurrence unprefixed
            assert (
                cdf_file["Sensor_A"].attrs["DEPEND_0"] == "Epoch"
            )  # First timeseries uses unprefixed Epoch
            assert cdf_file["REACH_134_Lat"].attrs["DEPEND_0"] == "REACH_134_Epoch"
            assert cdf_file["Sensor_B"].attrs["DEPEND_0"] == "REACH_134_Epoch"
            assert cdf_file["REACH_099_Lat"].attrs["DEPEND_0"] == "REACH_099_Epoch"
            assert cdf_file["Sensor_C"].attrs["DEPEND_0"] == "REACH_099_Epoch"

            # Verify Default_Timeseries_Key global attribute is written for multi-timeseries files
            assert "Default_Timeseries_Key" in cdf_file.attrs
            assert cdf_file.attrs["Default_Timeseries_Key"][0] == "REACH_165"

        # Test round-trip: Load the file back and verify structure
        sw_data_loaded = SWXData.load(test_file_output_path)

        # Verify the TimeSeries structure is reconstructed correctly
        # All original keys should be preserved after round-trip
        assert (
            "REACH_165" in sw_data_loaded.data["timeseries"]
        )  # Original key preserved
        assert "REACH_134" in sw_data_loaded.data["timeseries"]
        assert "REACH_099" in sw_data_loaded.data["timeseries"]

        # Verify columns are unprefixed in the loaded TimeSeries
        ts_165 = sw_data_loaded.data["timeseries"][
            "REACH_165"
        ]  # Original key preserved
        assert "Lat" in ts_165.colnames
        assert "Lon" in ts_165.colnames
        assert "Sensor_A" in ts_165.colnames

        ts_134 = sw_data_loaded.data["timeseries"]["REACH_134"]
        assert "Lat" in ts_134.colnames
        assert "Lon" in ts_134.colnames
        assert "Sensor_B" in ts_134.colnames

        ts_099 = sw_data_loaded.data["timeseries"]["REACH_099"]
        assert "Lat" in ts_099.colnames
        assert "Lon" in ts_099.colnames
        assert "Sensor_C" in ts_099.colnames

        # Verify data integrity: each timeseries has correct number of records
        assert len(ts_165) == 5
        assert len(ts_134) == 5
        assert len(ts_099) == 5

        # Verify that each timeseries has exactly the expected columns (no extras, no missing)
        assert set(ts_165.colnames) == {"time", "Lat", "Lon", "Sensor_A"}
        assert set(ts_134.colnames) == {"time", "Lat", "Lon", "Sensor_B"}
        assert set(ts_099.colnames) == {"time", "Lat", "Lon", "Sensor_C"}

        # CRITICAL: Test that .time and .time_range properties work after loading
        # This would fail with KeyError: 'Epoch' without the Default_Timeseries_Key override fix
        loaded_time = (
            sw_data_loaded.time
        )  # Should access REACH_165's time (the default)
        assert len(loaded_time) == 5
        time_range = sw_data_loaded.time_range
        assert time_range[0] == loaded_time.min()
        assert time_range[1] == loaded_time.max()


def test_cdf_selective_prefixing_unique_columns():
    """
    Test that prefixing only applies to variables that actually conflict.
    When some columns are unique and others conflict, only conflicting ones get prefixed.
    - Voltage, Current, Temperature, Altitude, Speed are unique (no prefix)
    - Pressure conflicts between SAT-B and SAT-C (gets prefixed)
    - Epoch conflicts across all (gets prefixed for non-default)
    """
    # Create 3 TimeSeries with unique column names (no data conflicts)
    ts1 = TimeSeries()
    ts1["time"] = Time(1704067200 + np.arange(5), format="unix")
    ts1["Voltage"] = Quantity(value=np.random.random(5), unit="V", dtype=np.float32)
    ts1["Voltage"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Voltage measurement",
    }
    ts1["Current"] = Quantity(value=np.random.random(5), unit="A", dtype=np.float32)
    ts1["Current"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Current measurement",
    }

    ts2 = TimeSeries()
    ts2["time"] = Time(1704067200 + np.arange(5), format="unix")
    ts2["Temperature"] = Quantity(value=np.random.random(5), unit="K", dtype=np.float32)
    ts2["Temperature"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Temperature measurement",
    }
    ts2["Pressure"] = Quantity(value=np.random.random(5), unit="Pa", dtype=np.float32)
    ts2["Pressure"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Pressure measurement",
    }

    ts3 = TimeSeries()
    ts3["time"] = Time(1704067200 + np.arange(5), format="unix")
    ts3["Altitude"] = Quantity(value=np.random.random(5), unit="km", dtype=np.float32)
    ts3["Altitude"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Altitude measurement",
    }
    ts3["Speed"] = Quantity(value=np.random.random(5), unit="m/s", dtype=np.float32)
    ts3["Speed"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Speed measurement",
    }
    ts3["Pressure"] = Quantity(value=np.random.random(5), unit="Pa", dtype=np.float32)
    ts3["Pressure"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Pressure measurement from SAT-C",
    }

    # Create SWXData with multiple TimeSeries with unique column names
    timeseries_dict = {
        "SAT_A": ts1,
        "SAT_B": ts2,
        "SAT_C": ts3,
    }

    meta = {
        "Descriptor": "EEA>Electron Electrostatic Analyzer",
        "Data_level": "l1>Level 1",
        "Data_version": "v0.0.1",
    }

    sw_data = SWXData(timeseries=timeseries_dict, meta=meta)

    # CRITICAL: Verify .time and .time_range work BEFORE saving
    # This tests the fallback logic when Default_Timeseries_Key isn't in meta yet
    # Without the fix, this would raise KeyError: 'Epoch'
    pre_save_time = sw_data.time  # Should access SAT_A's time (first dict key)
    assert len(pre_save_time) == 5
    pre_save_time_range = sw_data.time_range
    assert pre_save_time_range[0] < pre_save_time_range[1]
    # Verify it's using the first key as default
    assert sw_data._default_timeseries_key == "SAT_A"

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        test_file_output_path = sw_data.save(output_path=tmp_path)
        save_cdf_for_examination(sw_data, "selective_prefixing_unique_columns")

        assert test_file_output_path.exists()

        with CDF(str(test_file_output_path)) as cdf_file:
            # First timeseries uses unprefixed "Epoch" for ISTP compliance
            assert "Epoch" in cdf_file  # SAT_A is first, gets default "Epoch"
            assert "SAT_B_Epoch" in cdf_file
            assert "SAT_C_Epoch" in cdf_file
            assert "SAT_A_Epoch" not in cdf_file  # SAT_A uses unprefixed "Epoch"

            # Unique columns should NOT be prefixed
            assert "Voltage" in cdf_file
            assert "Current" in cdf_file
            assert "Temperature" in cdf_file
            assert "Altitude" in cdf_file
            assert "Speed" in cdf_file

            # Pressure conflicts between SAT_B and SAT_C - first occurrence unprefixed
            # SAT_B is first to have Pressure, so it's unprefixed
            assert "Pressure" in cdf_file  # SAT_B (first occurrence) gets unprefixed
            assert (
                "SAT_C_Pressure" in cdf_file
            )  # SAT_C (second occurrence) gets prefixed
            assert "SAT_B_Pressure" not in cdf_file  # First occurrence stays unprefixed

            # Should NOT have prefixed unique variables
            assert "SAT_A_Voltage" not in cdf_file
            assert "SAT_B_Temperature" not in cdf_file
            assert "SAT_C_Altitude" not in cdf_file
            assert "SAT_C_Speed" not in cdf_file

            # Verify DEPEND_0 linkage - first epoch unprefixed, others prefixed
            assert (
                cdf_file["Voltage"].attrs["DEPEND_0"] == "Epoch"
            )  # SAT_A uses unprefixed Epoch
            assert (
                cdf_file["Current"].attrs["DEPEND_0"] == "Epoch"
            )  # SAT_A uses unprefixed Epoch
            assert cdf_file["Temperature"].attrs["DEPEND_0"] == "SAT_B_Epoch"
            assert (
                cdf_file["Pressure"].attrs["DEPEND_0"] == "SAT_B_Epoch"
            )  # First Pressure occurrence
            assert cdf_file["Altitude"].attrs["DEPEND_0"] == "SAT_C_Epoch"
            assert cdf_file["Speed"].attrs["DEPEND_0"] == "SAT_C_Epoch"
            assert cdf_file["SAT_C_Pressure"].attrs["DEPEND_0"] == "SAT_C_Epoch"

            # Verify Default_Timeseries_Key global attribute is written for multi-timeseries files
            assert "Default_Timeseries_Key" in cdf_file.attrs
            assert cdf_file.attrs["Default_Timeseries_Key"][0] == "SAT_A"

        # Test round-trip
        sw_data_loaded = SWXData.load(test_file_output_path)

        # All original keys should be preserved after round-trip
        assert "SAT_A" in sw_data_loaded.data["timeseries"]  # Original key preserved
        assert "SAT_B" in sw_data_loaded.data["timeseries"]
        assert "SAT_C" in sw_data_loaded.data["timeseries"]

        # Verify unique columns are preserved
        ts_a = sw_data_loaded.data["timeseries"]["SAT_A"]  # Original key preserved
        assert "Voltage" in ts_a.colnames
        assert "Current" in ts_a.colnames

        ts_b = sw_data_loaded.data["timeseries"]["SAT_B"]
        assert "Temperature" in ts_b.colnames
        assert "Pressure" in ts_b.colnames

        ts_c = sw_data_loaded.data["timeseries"]["SAT_C"]
        assert "Altitude" in ts_c.colnames
        assert "Speed" in ts_c.colnames
        assert "Pressure" in ts_c.colnames

        # Verify data integrity: each timeseries has correct number of records
        assert len(ts_a) == 5
        assert len(ts_b) == 5
        assert len(ts_c) == 5

        # Verify that each timeseries has exactly the expected columns
        assert set(ts_a.colnames) == {"time", "Voltage", "Current"}
        assert set(ts_b.colnames) == {"time", "Temperature", "Pressure"}
        assert set(ts_c.colnames) == {"time", "Altitude", "Speed", "Pressure"}

        # CRITICAL: Test that .time and .time_range properties work after loading
        # This would fail with KeyError: 'Epoch' without the Default_Timeseries_Key override fix
        loaded_time = sw_data_loaded.time  # Should access SAT_A's time (the default)
        assert len(loaded_time) == 5
        time_range = sw_data_loaded.time_range
        assert time_range[0] == loaded_time.min()
        assert time_range[1] == loaded_time.max()


def test_cdf_epoch_substring_not_confused():
    """
    Test that variables containing "Epoch" as a substring but not matching
    the epoch patterns (exactly "Epoch" or ending with "_Epoch") are correctly
    treated as regular measurement variables, not epoch variables.

    This tests the fix for: epoch_variables should use exact matching, not substring.
    """
    # Create TimeSeries with a variable that contains "Epoch" substring
    ts = TimeSeries()
    ts["time"] = Time(1704067200 + np.arange(5), format="unix")

    # Add a variable with "Epoch" in the name but not an actual epoch
    ts["Epoch_quality"] = Quantity(
        value=np.array([0, 1, 2, 3, 4], dtype=np.uint8), unit="", dtype=np.uint8
    )
    ts["Epoch_quality"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Quality flag for epoch (not an epoch variable itself)",
    }

    ts["data"] = Quantity(value=np.random.random(5), unit="count", dtype=np.float32)
    ts["data"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Test measurement",
    }

    meta = {
        "Descriptor": "EEA>Electron Electrostatic Analyzer",
        "Data_level": "l1>Level 1",
        "Data_version": "v0.0.1",
    }

    sw_data = SWXData(timeseries=ts, meta=meta)

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        test_file_output_path = sw_data.save(output_path=tmp_path)
        save_cdf_for_examination(sw_data, test_file_output_path.name)
        assert test_file_output_path.exists()

        with CDF(str(test_file_output_path)) as cdf_file:
            # Should have exactly one epoch variable: "Epoch"
            assert "Epoch" in cdf_file

            # Epoch_quality should be a regular variable, not treated as epoch
            assert "Epoch_quality" in cdf_file

            # Verify Epoch_quality has DEPEND_0 pointing to Epoch
            assert cdf_file["Epoch_quality"].attrs["DEPEND_0"] == "Epoch"

            # Verify it's record-varying (like other measurements)
            assert cdf_file["Epoch_quality"].rv() is True

        # Test round-trip: this is where it fails without the fix
        sw_data_loaded = SWXData.load(test_file_output_path)

        # Should have one timeseries
        assert len(sw_data_loaded.data["timeseries"]) == 1

        # Get the loaded timeseries (should be keyed by "Epoch" for single-timeseries)
        loaded_ts = sw_data_loaded.timeseries

        # Verify Epoch_quality was loaded as a regular measurement, not confused as epoch
        assert "Epoch_quality" in loaded_ts.colnames
        assert "data" in loaded_ts.colnames

        # Verify data integrity
        assert len(loaded_ts) == 5
        np.testing.assert_array_equal(loaded_ts["Epoch_quality"].value, [0, 1, 2, 3, 4])


def test_cdf_prefix_stripping_heuristic():
    """
    Test that the prefix-stripping heuristic doesn't corrupt original variable names
    that happen to start with a timeseries prefix.

    If a variable name starts with a prefix but the unprefixed version doesn't exist
    in the file, it's an original name and should NOT be stripped.

    Example: timeseries["REACH_134"] has column "REACH_134_Status" (original name).
    Without heuristic: incorrectly strips to "Status"
    With heuristic: keeps "REACH_134_Status" because "Status" doesn't exist in file
    """
    # Create two timeseries with different epoch keys
    ts_a = TimeSeries()
    ts_a["time"] = Time(1704067200 + np.arange(3), format="unix")
    ts_a["data"] = Quantity(
        value=np.array([1.0, 2.0, 3.0]), unit="count", dtype=np.float32
    )
    ts_a["data"].meta = {"VAR_TYPE": "data", "CATDESC": "Data from A"}

    ts_b = TimeSeries()
    ts_b["time"] = Time(1704067200 + np.arange(3), format="unix")
    # This variable name happens to start with the prefix but is an original name
    ts_b["REACH_134_Status"] = Quantity(
        value=np.array([0, 1, 2], dtype=np.uint8), unit="", dtype=np.uint8
    )
    ts_b["REACH_134_Status"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Original variable name (not a prefixed 'Status')",
    }

    meta = {
        "Descriptor": "EEA>Test Instrument",
        "Data_level": "l1>Level 1",
        "Data_version": "v0.0.1",
    }

    timeseries_dict = {
        "Epoch": ts_a,
        "REACH_134": ts_b,  # We, like CDF, no longer support hyphens
    }

    sw_data = SWXData(timeseries=timeseries_dict, meta=meta)

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        test_file_output_path = sw_data.save(output_path=tmp_path)
        save_cdf_for_examination(sw_data, "heuristic")
        assert test_file_output_path.exists()

        # Verify CDF structure
        with CDF(str(test_file_output_path)) as cdf_file:
            # Should have unprefixed variables from first timeseries
            assert "Epoch" in cdf_file
            assert "data" in cdf_file

            # Should have prefixed epoch and the original variable name
            assert "REACH_134_Epoch" in cdf_file
            assert "REACH_134_Status" in cdf_file

            # "Status" should NOT exist (it's not a conflicting variable)
            assert "Status" not in cdf_file

        # Test round-trip: verify the heuristic preserves the original name
        sw_data_loaded = SWXData.load(test_file_output_path)

        # Should have two timeseries
        assert len(sw_data_loaded.data["timeseries"]) == 2

        # Check first timeseries
        loaded_ts_a = sw_data_loaded.data["timeseries"]["Epoch"]
        assert "data" in loaded_ts_a.colnames

        # Check second timeseries - the critical test
        loaded_ts_b = sw_data_loaded.data["timeseries"]["REACH_134"]
        # The heuristic should preserve "REACH_134_Status" because "Status" doesn't exist
        assert "REACH_134_Status" in loaded_ts_b.colnames
        # Should NOT have been corrupted to "Status"
        assert "Status" not in loaded_ts_b.colnames

        # Verify data integrity
        np.testing.assert_array_equal(loaded_ts_b["REACH_134_Status"].value, [0, 1, 2])


def test_cdf_custom_filename():
    """
    Test that a custom filename can be provided instead of using Logical_file_id.
    Tests the smart path logic where output_path can be either:
    - A directory (uses Logical_file_id)
    - A full file path (uses that filename)
    """
    # Get Test Data
    td = get_test_sw_data()
    assert isinstance(td.timeseries, TimeSeries)
    assert isinstance(td.timeseries, Table)

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        expected_name = f"{td.meta['Logical_file_id']}.cdf"

        # Test 1: Save with directory path only (uses Logical_file_id)

        test_file_output_path = td.save(output_path=tmp_path)
        save_cdf_for_examination(td)

        # Verify the file was created with Logical_file_id
        assert test_file_output_path.exists()
        assert test_file_output_path.name == expected_name
        assert test_file_output_path.parent == tmp_path

        # Test 2: Save with full file path (custom filename)
        custom_filename = "my_custom_test_file.cdf"
        test_file_output_path = td.save(output_path=tmp_path / custom_filename)
        save_cdf_for_examination(td, custom_filename)
        # Verify the file was created with the custom name
        assert test_file_output_path.name == custom_filename
        assert test_file_output_path.exists()
        assert test_file_output_path.parent == tmp_path

        # Load the file back and verify it's correct
        td_loaded = SWXData.load(test_file_output_path)

        assert len(td.timeseries) == len(td_loaded.timeseries)
        assert len(td.timeseries.columns) == len(td_loaded.timeseries.columns)

        # Test 3: Overwrite with directory path
        test_file_output_path_default = td.save(output_path=tmp_path, overwrite=True)
        assert test_file_output_path_default.name == expected_name
        assert test_file_output_path_default.exists()

        # Test 4: Save with no path (saves to current dir with Logical_file_id)
        test_file_output_path_cwd = td.save(overwrite=True)
        assert test_file_output_path_cwd.exists()
        assert test_file_output_path_cwd.name == expected_name
        # Clean up file in current directory
        test_file_output_path_cwd.unlink()

        # Test 5: Custom filename with overwrite (covers both parameters together)
        another_custom_filename = "overwrite_test"
        test_file_output_path_overwrite = td.save(
            output_path=tmp_path / another_custom_filename, overwrite=True
        )
        assert test_file_output_path_overwrite.name == another_custom_filename + ".cdf"
        assert test_file_output_path_overwrite.exists()

        # Test 6: Create actual FITS file for comparison with CDF
        # This demonstrates that FITS and CDF are different formats
        fits_filename = "real_fits_format.fits"
        fits_path = tmp_path / fits_filename

        # Convert TimeSeries to Table and write as actual FITS format
        fits_table = Table(td.timeseries)
        fits_table.write(fits_path, format="fits", overwrite=True)

        # Verify FITS file was created
        assert fits_path.exists()

        # Astropy can read it back as a Table
        loaded_fits_table = Table.read(fits_path, format="fits")
        assert len(loaded_fits_table) == len(td.timeseries)

        # But SWXData.load() cannot read FITS format (only CDF)
        with pytest.raises(Exception):
            SWXData.load(fits_path)  # Will fail - no FITS handler exists

        # Test 7: Cannot create SWXData with empty TimeSeries (even with metadata)
        # This demonstrates SWXData validation requires actual time points, not just metadata

        # Create a TimeSeries and then remove all rows to make it empty
        ts_with_data = TimeSeries(
            time_start="2024-01-01T00:00:00",
            time_delta=1 * u.s,
            n_samples=2,
            data={"value": [1, 2] * u.dimensionless_unscaled},
        )
        empty_ts = ts_with_data[:0]  # Slice to get empty TimeSeries (length 0)

        # Add metadata - but it won't help, still needs time points
        empty_ts.meta = {
            "Descriptor": "EEA>Empty Test Data",
            "Data_level": "l0>Level 0",
            "Data_version": "v0.0.1",
            "Logical_file_id": "hermes_eea_l0_test_empty",
        }

        with pytest.raises(ValueError, match="timeseries cannot be empty"):
            SWXData(timeseries=empty_ts)  # Will fail - metadata alone isn't enough


# ======================================================================
# NaN / mask <-> FILLVAL round-trip tests (Phase 5)
# ======================================================================


def _make_float_sw_data(values, dtype=np.float32, unit="m"):
    """Build a minimal SWXData with one float Quantity measurement."""
    ts = TimeSeries()
    ts.meta.update(
        {
            "Descriptor": "EEA>Electron Electrostatic Analyzer",
            "Data_level": "l1>Level 1",
            "Data_version": "v0.0.1",
        }
    )
    time_col = Time(np.arange(len(values)), format="unix")
    ts["time"] = time_col
    ts["measurement"] = Quantity(value=np.asarray(values, dtype=dtype), unit=unit)
    ts["measurement"].meta = OrderedDict(
        {"VAR_TYPE": "data", "CATDESC": "Test Float Measurement"}
    )
    return SWXData(timeseries=ts)


def test_roundtrip_float_nan():
    """Float NaN values are written as FILLVAL and restored to NaN + mask."""
    values = [1.0, np.nan, 3.0, np.nan, 5.0]
    td = _make_float_sw_data(values, dtype=np.float32)
    fillval = td.timeseries["measurement"].meta["FILLVAL"]

    with tempfile.TemporaryDirectory() as tmpdirname:
        out_path = td.save(output_path=tmpdirname)

        # On-disk values should contain FILLVAL where NaN was
        with CDF(str(out_path)) as cdf_file:
            raw = cdf_file["measurement"][...]
        assert raw[1] == pytest.approx(fillval)
        assert raw[3] == pytest.approx(fillval)
        assert raw[0] == pytest.approx(1.0)

        # Reload via SWXData and confirm NaN + mask
        td_loaded = SWXData.load(out_path)
        col = td_loaded.timeseries["measurement"]
        assert isinstance(col, Masked)
        np.testing.assert_array_equal(col.mask, [False, True, False, True, False])
        underlying = np.asarray(col.unmasked.value)
        assert np.isnan(underlying[1])
        assert np.isnan(underlying[3])
        assert underlying[0] == pytest.approx(1.0)


def test_roundtrip_float_mask_and_nan_combined():
    """Combined mask + NaN: both round-trip as masked positions."""
    values = np.array([1.0, np.nan, 3.0, 4.0, 5.0], dtype=np.float32)
    td = _make_float_sw_data(values)
    # Wrap the column in Masked with an extra masked position at index 4.
    col = td.timeseries["measurement"]
    explicit_mask = np.array([False, False, False, False, True])
    td.timeseries["measurement"] = Masked(col, mask=explicit_mask)
    td.timeseries["measurement"].meta = col.meta

    with tempfile.TemporaryDirectory() as tmpdirname:
        out_path = td.save(output_path=tmpdirname)
        td_loaded = SWXData.load(out_path)
        loaded_col = td_loaded.timeseries["measurement"]
        assert isinstance(loaded_col, Masked)
        np.testing.assert_array_equal(
            loaded_col.mask, [False, True, False, False, True]
        )


def test_roundtrip_integer_with_mask():
    """Integer NDData with explicit mask: FILLVAL written, mask restored on read."""
    td = get_test_sw_data()
    int_data = np.array([10, 20, 30, 40], dtype=np.int16)
    int_mask = np.array([False, True, False, True])
    td.add_support(
        "int_with_mask",
        NDData(
            data=int_data,
            mask=int_mask,
            meta={"CATDESC": "Int with mask", "VAR_TYPE": "support_data"},
        ),
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        out_path = td.save(output_path=tmpdirname)

        with CDF(str(out_path)) as cdf_file:
            raw = cdf_file["int_with_mask"][...]
            on_disk_fillval = cdf_file["int_with_mask"].attrs["FILLVAL"]
        assert raw[1] == on_disk_fillval
        assert raw[3] == on_disk_fillval
        assert raw[0] == 10
        assert raw.dtype == np.int16

        td_loaded = SWXData.load(out_path)
        loaded = td_loaded.support["int_with_mask"]
        assert loaded.mask is not None
        np.testing.assert_array_equal(loaded.mask, int_mask)
        assert loaded.data.dtype == np.int16
        assert loaded.data[1] == on_disk_fillval


def test_integer_fillval_sentinel_is_marked_masked_on_read():
    """A bare FILLVAL sentinel in int data is interpreted as fill on read."""
    td = get_test_sw_data()
    int_data = np.array([1, 2, 3, 4], dtype=np.int16)
    td.add_support(
        "int_no_mask",
        NDData(
            data=int_data,
            meta={"CATDESC": "Int no mask", "VAR_TYPE": "support_data"},
        ),
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        out_path = td.save(output_path=tmpdirname)
        with CDF(str(out_path), readonly=False) as cdf_file:
            sentinel = cdf_file["int_no_mask"].attrs["FILLVAL"]
            raw = cdf_file["int_no_mask"][...]
            raw[2] = sentinel
            cdf_file["int_no_mask"][...] = raw

        td_loaded = SWXData.load(out_path)
        loaded = td_loaded.support["int_no_mask"]
        assert loaded.mask is not None
        assert loaded.mask[2]
        assert not loaded.mask[0]


def _make_time_masked_sw_data():
    ts = TimeSeries()
    ts.meta.update(
        {
            "Descriptor": "EEA>Electron Electrostatic Analyzer",
            "Data_level": "l1>Level 1",
            "Data_version": "v0.0.1",
        }
    )
    times = Time(np.arange(5), format="unix")
    time_mask = np.array([False, False, True, False, False])
    times[time_mask] = np.ma.masked
    ts["time"] = times
    ts["measurement"] = Quantity(np.arange(5, dtype=np.float32), unit="m")
    ts["measurement"].meta = OrderedDict({"VAR_TYPE": "data", "CATDESC": "Test"})
    return SWXData(timeseries=ts), time_mask


def test_roundtrip_time_masked():
    """Masked(Time) column round-trips via raw TT2000 nanoseconds."""
    td, time_mask = _make_time_masked_sw_data()

    with tempfile.TemporaryDirectory() as tmpdirname:
        out_path = td.save(output_path=tmpdirname)

        with CDF(str(out_path)) as cdf_file:
            epoch_var = next(k for k in cdf_file.keys() if "Epoch" in k)
            raw_epoch = cdf_file.raw_var(epoch_var)[:]
        assert raw_epoch.dtype == np.int64
        assert raw_epoch[2] == fv.get_fillval(cdf_type=const.CDF_TIME_TT2000.value)

        td_loaded = SWXData.load(out_path)
        loaded_time = td_loaded.timeseries["time"]
        assert getattr(loaded_time, "masked", False)
        np.testing.assert_array_equal(loaded_time.mask, time_mask)


def test_roundtrip_string_nan_convenience():
    """np.nan placed in a string array round-trips as b" " and a mask bit."""
    td = get_test_sw_data()
    str_data = np.array(["abc", "def", "ghi", "jkl"], dtype="S3")
    str_data[1] = np.nan  # numpy coerces to b"nan"
    td.add_support(
        "str_var",
        NDData(
            data=str_data,
            meta={"CATDESC": "String var", "VAR_TYPE": "support_data"},
        ),
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        out_path = td.save(output_path=tmpdirname)

        with CDF(str(out_path)) as cdf_file:
            raw = cdf_file["str_var"][...]
        assert str(raw[1]).strip() == ""
        assert str(raw[0]) == "abc"

        td_loaded = SWXData.load(out_path)
        loaded = td_loaded.support["str_var"]
        assert loaded.mask is not None
        assert loaded.mask[1]
        assert not loaded.mask[0]
        assert str(loaded.data[1]).strip().lower() != "nan"


def test_roundtrip_string_mask_only():
    """Explicit mask on a string NDData round-trips."""
    td = get_test_sw_data()
    str_data = np.array(["abc", "def", "ghi"], dtype="S3")
    str_mask = np.array([False, True, False])
    td.add_support(
        "str_masked",
        NDData(
            data=str_data,
            mask=str_mask,
            meta={"CATDESC": "Masked str", "VAR_TYPE": "support_data"},
        ),
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        out_path = td.save(output_path=tmpdirname)

        with CDF(str(out_path)) as cdf_file:
            raw = cdf_file["str_masked"][...]
        assert str(raw[1]).strip() == ""

        td_loaded = SWXData.load(out_path)
        loaded = td_loaded.support["str_masked"]
        assert loaded.mask is not None
        np.testing.assert_array_equal(loaded.mask, str_mask)


def test_epoch_var_stale_reference_bug():
    """
    Regression test for the epoch_var stale reference bug in prefix stripping.

    The bug: When loading a multi-timeseries CDF, the prefix stripping logic
    used `epoch_var` from an earlier loop, which referenced the LAST epoch
    variable processed, not the current variable's actual DEPEND_0 epoch.

    This test manually creates a CDF file with epochs in an order that exposes
    the bug: having "Epoch" be the LAST epoch processed causes the stale
    `epoch_var == "Epoch"` to prevent prefix stripping for non-default timeseries.
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        test_file_path = tmp_path / "epoch_bug_test.cdf"

        # Manually create a CDF file with specific epoch ordering
        # We want "Epoch" to be the LAST epoch in the file so that after the
        # epoch loop, epoch_var == "Epoch", which causes the bug
        with CDF(str(test_file_path), "") as cdf_file:
            # Write epochs in order: BETA_Epoch FIRST, then Epoch LAST
            # This makes "Epoch" be the last one processed in the reader's loop

            # Create BETA timeseries (with prefixed epoch)
            cdf_file.new(
                "BETA_Epoch",
                data=[1704067200000, 1704067300000, 1704067400000],
                type=pycdf.const.CDF_TIME_TT2000,
            )
            cdf_file["BETA_Epoch"].attrs["VAR_TYPE"] = "support_data"
            cdf_file["BETA_Epoch"].attrs["CATDESC"] = "BETA Epoch"
            cdf_file["BETA_Epoch"].attrs["FIELDNAM"] = "BETA_Epoch"

            cdf_file.new(
                "BETA_Voltage", data=[10.0, 20.0, 30.0], type=pycdf.const.CDF_FLOAT
            )
            cdf_file["BETA_Voltage"].attrs["VAR_TYPE"] = "data"
            cdf_file["BETA_Voltage"].attrs["CATDESC"] = "BETA Voltage"
            cdf_file["BETA_Voltage"].attrs["DEPEND_0"] = "BETA_Epoch"
            cdf_file["BETA_Voltage"].attrs["UNITS"] = "V"
            cdf_file["BETA_Voltage"].attrs["FIELDNAM"] = "Voltage"

            # Create default timeseries (with unprefixed Epoch) AFTER BETA
            cdf_file.new(
                "Epoch",
                data=[1704067500000, 1704067600000, 1704067700000],
                type=pycdf.const.CDF_TIME_TT2000,
            )
            cdf_file["Epoch"].attrs["VAR_TYPE"] = "support_data"
            cdf_file["Epoch"].attrs["CATDESC"] = "Default Epoch"
            cdf_file["Epoch"].attrs["FIELDNAM"] = "Epoch"

            cdf_file.new("Voltage", data=[1.0, 2.0, 3.0], type=pycdf.const.CDF_FLOAT)
            cdf_file["Voltage"].attrs["VAR_TYPE"] = "data"
            cdf_file["Voltage"].attrs["CATDESC"] = "Default Voltage"
            cdf_file["Voltage"].attrs["DEPEND_0"] = "Epoch"
            cdf_file["Voltage"].attrs["UNITS"] = "V"
            cdf_file["Voltage"].attrs["FIELDNAM"] = "Voltage"

            # Set global attributes to indicate this is multi-timeseries
            # with DEFAULT as the default timeseries
            cdf_file.attrs["Default_Timeseries_Key"] = "DEFAULT"
            cdf_file.attrs["Descriptor"] = "EEA>Electron Electrostatic Analyzer"
            cdf_file.attrs["Data_level"] = "l1"
            cdf_file.attrs["Data_version"] = "v0.0.1"

        # Now load this CDF file
        # Epochs will be processed in order: ["BETA_Epoch", "Epoch"]
        # After the loop, epoch_var == "Epoch"
        #
        # When processing "BETA_Voltage":
        # - BUG version: checks `epoch_var != "Epoch"` → FALSE (epoch_var IS "Epoch")
        #   So NO prefix stripping, column stays as "BETA_Voltage" (WRONG!)
        # - FIX version: checks `result_key != "Epoch"` → TRUE (result_key is "BETA_Epoch")
        #   So prefix IS stripped, column becomes "Voltage" (CORRECT!)

        sw_data_loaded = SWXData.load(test_file_path)

        # Should have two timeseries
        assert "DEFAULT" in sw_data_loaded.data["timeseries"]
        assert "BETA" in sw_data_loaded.data["timeseries"]

        ts_default = sw_data_loaded.data["timeseries"]["DEFAULT"]
        ts_beta = sw_data_loaded.data["timeseries"]["BETA"]

        # CRITICAL TEST: Both should have "Voltage" as the column name (prefix stripped)
        # With the BUG (using stale epoch_var == "Epoch"), BETA_Voltage won't be stripped
        # and will appear as "BETA_Voltage" in the timeseries (WRONG!)
        assert "Voltage" in ts_default.colnames, (
            f"Expected 'Voltage' in DEFAULT timeseries, got {ts_default.colnames}"
        )
        assert "Voltage" in ts_beta.colnames, (
            f"Expected 'Voltage' in BETA timeseries (prefix should be stripped), got {ts_beta.colnames}"
        )

        # Should NOT have the prefixed name in the loaded timeseries
        assert "BETA_Voltage" not in ts_beta.colnames, (
            f"BETA_Voltage should have been stripped to 'Voltage', but found in colnames: {ts_beta.colnames}"
        )

        # Verify data integrity
        assert len(ts_default) == 3
        assert len(ts_beta) == 3
        np.testing.assert_array_almost_equal(
            ts_default["Voltage"].value, [1.0, 2.0, 3.0]
        )
        np.testing.assert_array_almost_equal(
            ts_beta["Voltage"].value, [10.0, 20.0, 30.0]
        )
