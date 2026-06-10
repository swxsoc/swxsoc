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
from spacepy.pycdf import CDF, CDFError

from swxsoc.io import fillval as fv
from swxsoc.swxdata import SWXData
from swxsoc.util import const


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
