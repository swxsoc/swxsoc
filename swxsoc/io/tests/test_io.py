"""Tests for Loading and Saving data from data containers"""

from collections import OrderedDict
from pathlib import Path
import pytest
import numpy as np
from numpy.random import random
import tempfile
from astropy.timeseries import TimeSeries
from astropy.time import Time
from astropy.utils.masked import Masked

from swxsoc.io import fillval as fv
from astropy.units import Quantity
from astropy.nddata import NDData
from astropy.wcs import WCS
from ndcube import NDCube, NDCollection
from spacepy.pycdf import CDFError, CDF
from swxsoc.swxdata import SWXData
from swxsoc.util import const


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

        # Load the JSON file as JSON
        with CDF(str(test_file_output_path), readonly=False) as cdf_file:
            # Add Spectra Data Variable
            cdf_file["Test_Spectra_Var"] = np.random.random(size=(10, 10))
            cdf_file["Test_Spectra_Var"].meta["UNITS"] = "counts"

        # Make sure we can load the modified JSON
        td_loaded = SWXData.load(test_file_output_path)

        assert "Test_Spectra_Var" in td_loaded.spectra


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
        assert raw_epoch[2] == fv.tt2000_fillval_int64()

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
