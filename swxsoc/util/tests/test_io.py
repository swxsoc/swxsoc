"""Tests for Loading and Saving data from data containers"""

from collections import OrderedDict
from pathlib import Path
import pytest
import numpy as np
from numpy.random import random
import tempfile
from astropy.timeseries import TimeSeries
from astropy.table import Table
from astropy.time import Time
from astropy.units import Quantity
from astropy.nddata import NDData
from astropy.wcs import WCS
from ndcube import NDCube, NDCollection
from spacepy.pycdf import CDFError, CDF
from swxsoc.swxdata import SWXData
from swxsoc.util import const


def save_cdf_for_examination(sw_data, filename=None):
    """Save a copy to current dir for examination with custom filename or logical id.
       No output path will put it in the current directory which is the point of this
       function."""
    if True: # change to True if you'd like to use this feature
        if filename:
            # Add .cdf suffix if not already present
            if not filename.endswith('.cdf'):
                filename = filename + '.cdf'
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
        fits_table.write(fits_path, format='fits', overwrite=True)
        
        # Verify FITS file was created
        assert fits_path.exists()
        
        # Astropy can read it back as a Table
        loaded_fits_table = Table.read(fits_path, format='fits')
        assert len(loaded_fits_table) == len(td.timeseries)
        
        # But SWXData.load() cannot read FITS format (only CDF)
        with pytest.raises(Exception):
            SWXData.load(fits_path)  # Will fail - no FITS handler exists
        
        
        # Test 7: CDF can store text data - demonstrate CDF is a rich format
        # Add text as a global attribute
        td.meta['Custom_text_description'] = "This is txt in a CDF file"
        
        # Save and reload
        cdf_with_text_path = tmp_path / "cdf_with_text.cdf"
        td.save(output_path=cdf_with_text_path, overwrite=True)
        
        # Load back and verify text is preserved
        td_with_text = SWXData.load(cdf_with_text_path)
        assert td_with_text.meta['Custom_text_description'] == "This is txt in a CDF file"
        

        

