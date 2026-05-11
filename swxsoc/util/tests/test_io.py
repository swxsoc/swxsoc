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
    Test that auto-prefixing prevents duplicate variable names when writing
    CDF files with multiple TimeSeries objects that have identical column
    names
    """
    # Create multiple TimeSeries with duplicate column names
    # (like REACH demo scenario)
    # With the epoch_key fix, all TimeSeries can now have the same length
    ts1 = TimeSeries()
    # Use 2024 timestamps (unix timestamp for 2024-01-01 is ~1704067200)
    ts1["time"] = Time(1704067200 + np.arange(5), format="unix")
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
    ts1["Sensor_A"] = Quantity(
        value=np.random.random(5), unit="count", dtype=np.uint16
    )
    ts1["Sensor_A"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor A from satellite 1",
    }

    ts2 = TimeSeries()
    ts2["time"] = Time(1704067200 + np.arange(5), format="unix")
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
    ts2["Sensor_A"] = Quantity(
        value=np.random.random(5), unit="count", dtype=np.uint16
    )
    ts2["Sensor_A"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor A from satellite 2",
    }

    ts3 = TimeSeries()
    ts3["time"] = Time(1704067200 + np.arange(5), format="unix")
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
    ts3["Sensor_A"] = Quantity(
        value=np.random.random(5), unit="count", dtype=np.uint16
    )
    ts3["Sensor_A"].meta = {
        "VAR_TYPE": "data",
        "CATDESC": "Sensor A from satellite 3",
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
    
    # Test that auto-prefixing prevents collisions
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        
        # No warnings should be raised with auto-prefixing
        test_file_output_path = sw_data.save(output_path=tmp_path)
        
        # Verify the CDF file was created
        assert test_file_output_path.exists()
        
        # Verify all prefixed variables exist in the CDF file
        with CDF(str(test_file_output_path)) as cdf_file:
            # All three prefixed epoch variables should exist
            assert "REACH_165_Epoch" in cdf_file
            assert "REACH_134_Epoch" in cdf_file
            assert "REACH_099_Epoch" in cdf_file
            
            # All prefixed data columns should exist (no overwriting!)
            assert "REACH_165_Lat" in cdf_file
            assert "REACH_165_Lon" in cdf_file
            assert "REACH_165_Sensor_A" in cdf_file
            
            assert "REACH_134_Lat" in cdf_file
            assert "REACH_134_Lon" in cdf_file
            assert "REACH_134_Sensor_A" in cdf_file
            
            assert "REACH_099_Lat" in cdf_file
            assert "REACH_099_Lon" in cdf_file
            assert "REACH_099_Sensor_A" in cdf_file
            
            # Verify each has the correct length
            assert len(cdf_file["REACH_165_Lat"]) == 5
            assert len(cdf_file["REACH_134_Lat"]) == 5
            assert len(cdf_file["REACH_099_Lat"]) == 5
            
            # Verify DEPEND_0 points to the correct prefixed epoch
            assert (
                cdf_file["REACH_165_Lat"].attrs["DEPEND_0"]
                == "REACH_165_Epoch"
            )
            assert (
                cdf_file["REACH_134_Lat"].attrs["DEPEND_0"]
                == "REACH_134_Epoch"
            )
            assert (
                cdf_file["REACH_099_Lat"].attrs["DEPEND_0"]
                == "REACH_099_Epoch"
            )
        
        # Test round-trip: Load the file back and verify structure
        sw_data_loaded = SWXData.load(test_file_output_path)
        
        # Verify the TimeSeries structure is reconstructed correctly
        assert "REACH-165" in sw_data_loaded.data["timeseries"]
        assert "REACH-134" in sw_data_loaded.data["timeseries"]
        assert "REACH-099" in sw_data_loaded.data["timeseries"]
        
        # Verify columns are unprefixed in the loaded TimeSeries
        ts_165 = sw_data_loaded.data["timeseries"]["REACH-165"]
        assert "Lat" in ts_165.colnames
        assert "Lon" in ts_165.colnames
        assert "Sensor_A" in ts_165.colnames

