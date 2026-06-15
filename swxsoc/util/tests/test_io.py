"""Tests for Loading and Saving data from data containers"""

from collections import OrderedDict
from pathlib import Path
import pytest
import numpy as np
from numpy.random import random
import tempfile
from astropy.timeseries import TimeSeries
from astropy.time import Time
from astropy.units import Quantity
from astropy.nddata import NDData
from astropy.wcs import WCS
from ndcube import NDCube, NDCollection
from spacepy.pycdf import CDFError, CDF
from swxsoc.swxdata import SWXData
from swxsoc.util import const


def save_for_examination(sw_data, filename):
    """Save a copy to current dir for examination with custom filename."""
    if False:
       sw_data.meta["Logical_file_id"] = filename
       sw_data.save(overwrite=True)


def get_test_sw_data():
    """
    Function to get test swxsoc.swxdata.SWXData objects to re-use in
    other tests
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
            data=[1],
            meta={"CATDESC": "variable counts", "VAR_TYPE": "support_data"},
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
        save_for_examination(td, "io")

        # Verify single-timeseries files don't have Default_Timeseries_Key
        with CDF(str(test_file_output_path)) as cdf_file:
            # Single timeseries should either not have this attribute or have it empty
            if "Default_Timeseries_Key" in cdf_file.attrs:
                assert cdf_file.attrs["Default_Timeseries_Key"][0] == ""

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
        save_for_examination(td, "nrv_support_data")

        # Load the JSON file as JSON
        with CDF(str(test_file_output_path), readonly=False) as cdf_file:
            # Add Non-Record-Varying Variable
            cdf_file.new(
                name="Test_NRV_Var",
                data=[1, 2, 3],
                type=const.CDF_INT4,
                recVary=False,
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
        save_for_examination(td, "spectra_data")

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
    ts1["Lat"] = Quantity(
        value=np.random.random(5), unit="deg", dtype=np.float32
    )
    ts1["Lat"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Latitude from satellite 1",
    }
    ts1["Lon"] = Quantity(
        value=np.random.random(5), unit="deg", dtype=np.float32
    )
    ts1["Lon"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Longitude from satellite 1",
    }
    # Sensor_A is unique to this satellite (no prefix needed)
    ts1["Sensor_A"] = Quantity(
        value=np.random.random(5), unit="count", dtype=np.uint16
    )
    ts1["Sensor_A"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor A from satellite 1",
    }

    ts2 = TimeSeries()
    ts2["time"] = Time(1704067200 + np.arange(5), format="unix")
    # Lat and Lon are duplicated (will be prefixed)
    ts2["Lat"] = Quantity(
        value=np.random.random(5), unit="deg", dtype=np.float32
    )
    ts2["Lat"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Latitude from satellite 2",
    }
    ts2["Lon"] = Quantity(
        value=np.random.random(5), unit="deg", dtype=np.float32
    )
    ts2["Lon"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Longitude from satellite 2",
    }
    # Sensor_B is unique to this satellite (no prefix needed)
    ts2["Sensor_B"] = Quantity(
        value=np.random.random(5), unit="count", dtype=np.uint16
    )
    ts2["Sensor_B"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor B from satellite 2",
    }

    ts3 = TimeSeries()
    ts3["time"] = Time(1704067200 + np.arange(5), format="unix")
    # Lat and Lon are duplicated (will be prefixed)
    ts3["Lat"] = Quantity(
        value=np.random.random(5), unit="deg", dtype=np.float32
    )
    ts3["Lat"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Latitude from satellite 3",
    }
    ts3["Lon"] = Quantity(
        value=np.random.random(5), unit="deg", dtype=np.float32
    )
    ts3["Lon"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Longitude from satellite 3",
    }
    # Sensor_C is unique to this satellite (no prefix needed)
    ts3["Sensor_C"] = Quantity(
        value=np.random.random(5), unit="count", dtype=np.uint16
    )
    ts3["Sensor_C"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor C from satellite 3",
    }

    # Create SWXData with multiple TimeSeries keyed by satellite name
    timeseries_dict = {
        "REACH-165": ts1,
        "REACH-134": ts2,
        "REACH-099": ts3,
    }
    
    meta = {
        "Descriptor": "EEA>Electron Electrostatic Analyzer",
        "Data_level": "l1>Level 1",
        "Data_version": "v0.0.1",
    }
    
    sw_data = SWXData(timeseries=timeseries_dict, meta=meta)
    
    # Test that selective prefixing prevents collisions
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        
        test_file_output_path = sw_data.save(output_path=tmp_path)
        save_for_examination(sw_data, "auto_prefixing_prevents_duplicates")
        
        # Verify the CDF file was created
        assert test_file_output_path.exists()
        
        # Verify all prefixed variables exist in the CDF file
        with CDF(str(test_file_output_path)) as cdf_file:
            # First timeseries uses unprefixed "Epoch" for ISTP compliance
            assert "Epoch" in cdf_file  # REACH-165 is first, gets default "Epoch"
            assert "REACH_134_Epoch" in cdf_file
            assert "REACH_099_Epoch" in cdf_file
            assert "REACH_165_Epoch" not in cdf_file  # REACH-165 uses unprefixed "Epoch"
            
            # Lat and Lon conflict - first occurrence unprefixed, rest prefixed
            assert "Lat" in cdf_file  # REACH-165 (first) gets unprefixed
            assert "Lon" in cdf_file  # REACH-165 (first) gets unprefixed
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
            assert cdf_file["Lat"].attrs["DEPEND_0"] == "Epoch"  # First occurrence unprefixed
            assert cdf_file["Lon"].attrs["DEPEND_0"] == "Epoch"  # First occurrence unprefixed
            assert cdf_file["Sensor_A"].attrs["DEPEND_0"] == "Epoch"  # First timeseries uses unprefixed Epoch
            assert cdf_file["REACH_134_Lat"].attrs["DEPEND_0"] == "REACH_134_Epoch"
            assert cdf_file["Sensor_B"].attrs["DEPEND_0"] == "REACH_134_Epoch"
            assert cdf_file["REACH_099_Lat"].attrs["DEPEND_0"] == "REACH_099_Epoch"
            assert cdf_file["Sensor_C"].attrs["DEPEND_0"] == "REACH_099_Epoch"
            
            # Verify Default_Timeseries_Key global attribute is written for multi-timeseries files
            assert "Default_Timeseries_Key" in cdf_file.attrs
            assert cdf_file.attrs["Default_Timeseries_Key"][0] == "REACH-165"
        
        # Test round-trip: Load the file back and verify structure
        sw_data_loaded = SWXData.load(test_file_output_path)
        
        # Verify the TimeSeries structure is reconstructed correctly
        # All original keys should be preserved after round-trip
        assert "REACH-165" in sw_data_loaded.data["timeseries"]  # Original key preserved
        assert "REACH-134" in sw_data_loaded.data["timeseries"]
        assert "REACH-099" in sw_data_loaded.data["timeseries"]
        
        # Verify columns are unprefixed in the loaded TimeSeries
        ts_165 = sw_data_loaded.data["timeseries"]["REACH-165"]  # Original key preserved
        assert "Lat" in ts_165.colnames
        assert "Lon" in ts_165.colnames
        assert "Sensor_A" in ts_165.colnames
        
        ts_134 = sw_data_loaded.data["timeseries"]["REACH-134"]
        assert "Lat" in ts_134.colnames
        assert "Lon" in ts_134.colnames
        assert "Sensor_B" in ts_134.colnames
        
        ts_099 = sw_data_loaded.data["timeseries"]["REACH-099"]
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
        
        # BREAKPOINT: Set breakpoint here to copy the CDF file
        # File path: test_file_output_path
        # Example: import shutil; shutil.copy(test_file_output_path, "/tmp/reach_test.cdf")
        pass  # <-- SET BREAKPOINT HERE


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
    ts1["Voltage"] = Quantity(
        value=np.random.random(5), unit="V", dtype=np.float32
    )
    ts1["Voltage"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Voltage measurement",
    }
    ts1["Current"] = Quantity(
        value=np.random.random(5), unit="A", dtype=np.float32
    )
    ts1["Current"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Current measurement",
    }

    ts2 = TimeSeries()
    ts2["time"] = Time(1704067200 + np.arange(5), format="unix")
    ts2["Temperature"] = Quantity(
        value=np.random.random(5), unit="K", dtype=np.float32
    )
    ts2["Temperature"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Temperature measurement",
    }
    ts2["Pressure"] = Quantity(
        value=np.random.random(5), unit="Pa", dtype=np.float32
    )
    ts2["Pressure"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Pressure measurement",
    }

    ts3 = TimeSeries()
    ts3["time"] = Time(1704067200 + np.arange(5), format="unix")
    ts3["Altitude"] = Quantity(
        value=np.random.random(5), unit="km", dtype=np.float32
    )
    ts3["Altitude"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Altitude measurement",
    }
    ts3["Speed"] = Quantity(
        value=np.random.random(5), unit="m/s", dtype=np.float32
    )
    ts3["Speed"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Speed measurement",
    }
    ts3["Pressure"] = Quantity(
        value=np.random.random(5), unit="Pa", dtype=np.float32
    )
    ts3["Pressure"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Pressure measurement from SAT-C",
    }

    # Create SWXData with multiple TimeSeries with unique column names
    timeseries_dict = {
        "SAT-A": ts1,
        "SAT-B": ts2,
        "SAT-C": ts3,
    }
    
    meta = {
        "Descriptor": "EEA>Electron Electrostatic Analyzer",
        "Data_level": "l1>Level 1",
        "Data_version": "v0.0.1",
    }
    
    sw_data = SWXData(timeseries=timeseries_dict, meta=meta)
    
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        test_file_output_path = sw_data.save(output_path=tmp_path)
        save_for_examination(sw_data, "selective_prefixing_unique_columns")
        
        assert test_file_output_path.exists()
        
        with CDF(str(test_file_output_path)) as cdf_file:
            # First timeseries uses unprefixed "Epoch" for ISTP compliance
            assert "Epoch" in cdf_file  # SAT-A is first, gets default "Epoch"
            assert "SAT_B_Epoch" in cdf_file
            assert "SAT_C_Epoch" in cdf_file
            assert "SAT_A_Epoch" not in cdf_file  # SAT-A uses unprefixed "Epoch"
            
            # Unique columns should NOT be prefixed
            assert "Voltage" in cdf_file
            assert "Current" in cdf_file
            assert "Temperature" in cdf_file
            assert "Altitude" in cdf_file
            assert "Speed" in cdf_file
            
            # Pressure conflicts between SAT-B and SAT-C - first occurrence unprefixed
            # SAT-B is first to have Pressure, so it's unprefixed
            assert "Pressure" in cdf_file  # SAT-B (first occurrence) gets unprefixed
            assert "SAT_C_Pressure" in cdf_file  # SAT-C (second occurrence) gets prefixed
            assert "SAT_B_Pressure" not in cdf_file  # First occurrence stays unprefixed
            
            # Should NOT have prefixed unique variables
            assert "SAT_A_Voltage" not in cdf_file
            assert "SAT_B_Temperature" not in cdf_file
            assert "SAT_C_Altitude" not in cdf_file
            assert "SAT_C_Speed" not in cdf_file
            
            # Verify DEPEND_0 linkage - first epoch unprefixed, others prefixed
            assert cdf_file["Voltage"].attrs["DEPEND_0"] == "Epoch"  # SAT-A uses unprefixed Epoch
            assert cdf_file["Current"].attrs["DEPEND_0"] == "Epoch"  # SAT-A uses unprefixed Epoch
            assert cdf_file["Temperature"].attrs["DEPEND_0"] == "SAT_B_Epoch"
            assert cdf_file["Pressure"].attrs["DEPEND_0"] == "SAT_B_Epoch"  # First Pressure occurrence
            assert cdf_file["Altitude"].attrs["DEPEND_0"] == "SAT_C_Epoch"
            assert cdf_file["Speed"].attrs["DEPEND_0"] == "SAT_C_Epoch"
            assert cdf_file["SAT_C_Pressure"].attrs["DEPEND_0"] == "SAT_C_Epoch"
            
            # Verify Default_Timeseries_Key global attribute is written for multi-timeseries files
            assert "Default_Timeseries_Key" in cdf_file.attrs
            assert cdf_file.attrs["Default_Timeseries_Key"][0] == "SAT-A"
        
        # Test round-trip
        sw_data_loaded = SWXData.load(test_file_output_path)
        
        # All original keys should be preserved after round-trip
        assert "SAT-A" in sw_data_loaded.data["timeseries"]  # Original key preserved
        assert "SAT-B" in sw_data_loaded.data["timeseries"]
        assert "SAT-C" in sw_data_loaded.data["timeseries"]
        
        # Verify unique columns are preserved
        ts_a = sw_data_loaded.data["timeseries"]["SAT-A"]  # Original key preserved
        assert "Voltage" in ts_a.colnames
        assert "Current" in ts_a.colnames
        
        ts_b = sw_data_loaded.data["timeseries"]["SAT-B"]
        assert "Temperature" in ts_b.colnames
        assert "Pressure" in ts_b.colnames
        
        ts_c = sw_data_loaded.data["timeseries"]["SAT-C"]
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
        
        # BREAKPOINT: Set breakpoint here to copy the CDF file
        # File path: test_file_output_path
        # Example: import shutil; shutil.copy(test_file_output_path, "/tmp/unique_cols_test.cdf")
        pass  # <-- SET BREAKPOINT HERE

