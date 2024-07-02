from collections import OrderedDict
from pathlib import Path
import pytest
import tempfile
import numpy as np
from numpy.random import random
import datetime
from astropy.time import Time
from astropy.timeseries import TimeSeries
import astropy.units as u
from astropy.io import fits
from astropy.wcs import WCS
from ndcube import NDCube, NDCollection
from spacepy.pycdf import CDF
import swxsoc
from swxsoc import log
from swxsoc.swxdata import SWXData
from swxsoc.util.schema import SWXSchema
from swxsoc.util import const
from swxsoc.util.validation import validate, CDFValidator, FITSValidator

SAMPLE_CDF_FILE = "swxsoc_nms_default_l1_20160322_123031_v0.0.1.cdf"


def get_test_timeseries():
    """Get Test Data"""
    ts = TimeSeries()

    # Create an astropy.Time object
    time = np.arange(10)
    time_col = Time(time, format="unix")
    ts["time"] = time_col
    ts["time"].meta = OrderedDict({"CATDESC": "Epoch Time"})

    # Add Measurement
    quant = u.Quantity(value=random(size=(10)), unit="m", dtype=np.uint16)
    ts["measurement"] = quant
    ts["measurement"].meta = OrderedDict(
        {
            "VAR_TYPE": "data",
            "CATDESC": "Test Data",
        }
    )
    return ts


def test_non_cdf_file():
    """Function to Test a file using the CDFValidator that is not a CDF File"""
    invlid_path = str(Path(swxsoc.__file__).parent / "data" / "README.rst")
    with pytest.raises(ValueError):
        _ = validate(invlid_path)


def test_non_existant_file():
    """Function to Test a file using the CDFValidator that does not exist"""
    invlid_path = str(Path(swxsoc.__file__).parent / "data" / "test.cdf")
    result = validate(invlid_path)
    assert len(result) == 1
    assert "Could not open CDF File at path:" in result[0]


def test_missing_global_attrs():
    """Function to ensure missing global attributes are reported in validation"""

    # Create a Test SWXData
    ts = get_test_timeseries()
    template = SWXData.global_attribute_template("eea", "l2", "0.0.0")
    td = SWXData(timeseries=ts, meta=template)

    # Convert to a CDF File and Validate
    with tempfile.TemporaryDirectory() as tmpdirname:
        out_file = td.save(tmpdirname)

        with CDF(out_file, readonly=False) as cdf:
            del cdf.meta["Descriptor"]

        # Validate
        result = validate(out_file)
        assert (
            "Required attribute (Mission_group) not present in global attributes."
            in result
        )


def test_missing_var_type():
    """Function to ensure missing variable attributes are reported in validation"""

    # Create a Test SWXData
    ts = get_test_timeseries()
    template = SWXData.global_attribute_template("eea", "l2", "0.0.0")
    td = SWXData(timeseries=ts, meta=template)

    # Convert to a CDF File and Validate
    with tempfile.TemporaryDirectory() as tmpdirname:
        out_file = td.save(tmpdirname)

        with CDF(out_file, readonly=False) as cdf:
            del cdf["measurement"].meta["VAR_TYPE"]

        # Validate
        result = validate(out_file)
        assert (
            "Variable: measurement missing 'VAR_TYPE' attribute. Cannot Validate Variable."
            in result
        )


def test_missing_variable_attrs():
    """Function to ensure missing variable attributes are reported in validation"""

    # Create a Test SWXData
    ts = get_test_timeseries()
    template = SWXData.global_attribute_template("eea", "l2", "0.0.0")
    td = SWXData(timeseries=ts, meta=template)

    # Convert to a CDF File and Validate
    with tempfile.TemporaryDirectory() as tmpdirname:
        out_file = td.save(tmpdirname)

        with CDF(out_file, readonly=False) as cdf:
            del cdf["measurement"].meta["CATDESC"]
            del cdf["measurement"].meta["UNITS"]
            cdf["measurement"].meta["DISPLAY_TYPE"] = "bad_type"

        # Validate
        result = validate(out_file)
        assert "Variable: measurement missing 'CATDESC' attribute." in result
        assert (
            "Variable: measurement missing 'UNITS' attribute. Alternative: UNIT_PTR not found."
            in result
        )
        assert (
            "Variable: measurement Attribute 'DISPLAY_TYPE' not one of valid options.",
            "Was bad_type, expected one of time_series time_series>noerrorbars spectrogram stack_plot image",
        ) in result


def test_valid_range_dimensioned():
    """Validmin/validmax with multiple elements"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", data=[[1, 10], [2, 20], [3, 30]])
        v.attrs["VALIDMIN"] = [1, 20]
        v.attrs["VALIDMAX"] = [3, 30]
        v.attrs["FILLVAL"] = -100
        errs = CDFValidator()._validrange(v)
        assert 1 == len(errs)
        assert "Value 10 at index [0 1] under VALIDMIN [ 1 20]." == errs[0]
        v.attrs["VALIDMIN"] = [1, 10]
        errs = CDFValidator()._validrange(v)
        assert 0 == len(errs)
        v[0, 0] = -100
        errs = CDFValidator()._validrange(v)
        assert 0 == len(errs)


def test_valid_range_dimension_mismatch():
    """Validmin/validmax with something wrong in dimensionality"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", data=[[1, 10], [2, 20], [3, 30]])
        v.attrs["VALIDMIN"] = [1, 10, 100]
        v.attrs["VALIDMAX"] = [3, 30, 127]
        errs = CDFValidator()._validrange(v)
        assert 2 == len(errs)
        assert (
            "VALIDMIN element count 3 does not match "
            "first data dimension size 2." == errs[0]
        )
        assert (
            "VALIDMAX element count 3 does not match "
            "first data dimension size 2." == errs[1]
        )


def test_valid_range_high_dimension():
    """Validmin/validmax with high-dimension variables"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new(
            "var1",
            data=np.reshape(
                np.arange(27.0),
                (
                    3,
                    3,
                    3,
                ),
            ),
        )
        v.attrs["VALIDMIN"] = [1, 10, 100]
        v.attrs["VALIDMAX"] = [3, 30, 300]
        errs = CDFValidator()._validrange(v)
        assert 2 == len(errs)
        assert "Multi-element VALIDMIN only valid with 1D variable." == errs[0]
        assert "Multi-element VALIDMAX only valid with 1D variable." == errs[1]


def test_valid_range_wrong_type():
    """Validmin/validmax not matching variable type"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", data=[1, 2, 3], type=const.CDF_INT4)
        v.attrs.new("VALIDMIN", data=1, type=const.CDF_INT2)
        v.attrs.new("VALIDMAX", data=3, type=const.CDF_INT2)
        errs = CDFValidator()._validrange(v)
        errs.sort()
        assert 0 == len(errs)


def test_valid_range_incompatible_type():
    """Validmin/validmax can't be compared to variable type"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", data=[1, 2, 3], type=const.CDF_INT4)
        v.attrs.new("VALIDMIN", data="2")
        v.attrs.new("VALIDMAX", data="5")
        errs = CDFValidator()._validrange(v)
        errs.sort()
        assert 2 == len(errs)
        assert [
            "VALIDMAX type CDF_CHAR not comparable to variable type CDF_INT4.",
            "VALIDMIN type CDF_CHAR not comparable to variable type CDF_INT4.",
        ] == errs


def test_valid_range_nrv():
    """Validmin/validmax"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", recVary=False, data=[1, 2, 3])
        v.attrs["VALIDMIN"] = 1
        v.attrs["VALIDMAX"] = 3
        assert 0 == len(CDFValidator()._validrange(v))
        v.attrs["VALIDMIN"] = 2
        errs = CDFValidator()._validrange(v)
        assert 1 == len(errs)
        assert "Value 1 at index 0 under VALIDMIN 2." == errs[0]
        v.attrs["VALIDMAX"] = 2
        errs = CDFValidator()._validrange(v)
        assert 2 == len(errs)
        assert "Value 3 at index 2 over VALIDMAX 2." == errs[1]


def test_valid_range_nrv_fillval():
    """Validmin/validmax with fillval set"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", recVary=False, data=[1, 2, 3])
        v.attrs["VALIDMIN"] = 1
        v.attrs["VALIDMAX"] = 3
        v.attrs["FILLVAL"] = 99
        assert 0 == len(CDFValidator()._validrange(v))

        v.attrs["VALIDMIN"] = 2
        errs = CDFValidator()._validrange(v)
        assert 1 == len(errs)
        assert "Value 1 at index 0 under VALIDMIN 2." == errs[0]

        v.attrs["VALIDMAX"] = 2
        errs = CDFValidator()._validrange(v)
        assert 2 == len(errs)
        assert "Value 3 at index 2 over VALIDMAX 2." == errs[1]

        v.attrs["FILLVAL"] = 3
        errs = CDFValidator()._validrange(v)
        assert 1 == len(errs)
        assert "Value 1 at index 0 under VALIDMIN 2." == errs[0]

        v.attrs["FILLVAL"] = 1
        errs = CDFValidator()._validrange(v)
        assert 1 == len(errs)
        assert "Value 3 at index 2 over VALIDMAX 2." == errs[0]


def test_valid_range_fillval_float():
    """Validmin/validmax with fillval set, floating-point"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", recVary=False, data=[1, 2, 3], type=const.CDF_DOUBLE)
        v.attrs["VALIDMIN"] = 0
        v.attrs["VALIDMAX"] = 10
        # This is a bit contrived to force a difference between attribute
        # and value that's only the precision of the float
        v.attrs.new("FILLVAL", -1e31, type=const.CDF_FLOAT)
        assert 0 == len(CDFValidator()._validrange(v))

        v[0] = -100
        errs = CDFValidator()._validrange(v)
        assert 1 == len(errs)
        assert "Value -100.0 at index 0 under VALIDMIN 0.0." == errs[0]

        v[0] = -1e31
        assert 0 == len(CDFValidator()._validrange(v))


def test_valid_range_fillval_float_wrong_type():
    """Validmin/validmax with fillval, floating-point, but fillval string"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", recVary=False, data=[-1e31, 2, 3], type=const.CDF_DOUBLE)
        v.attrs["VALIDMIN"] = 0
        v.attrs["VALIDMAX"] = 10
        v.attrs.new("FILLVAL", b"badstuff", type=const.CDF_CHAR)
        expected = ["Value -1e+31 at index 0 under VALIDMIN 0.0."]
        errs = CDFValidator()._validrange(v)
        assert len(expected) == len(errs)
        for a, e in zip(sorted(errs), sorted(expected)):
            assert e == a


def test_valid_range_fillval_datetime():
    """Validmin/validmax with fillval set, Epoch var"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new(
            "var1",
            data=[datetime.datetime(2010, 1, i) for i in range(1, 6)],
            type=const.CDF_EPOCH,
        )
        v.attrs["VALIDMIN"] = datetime.datetime(2010, 1, 1)
        v.attrs["VALIDMAX"] = datetime.datetime(2010, 1, 31)
        v.attrs["FILLVAL"] = datetime.datetime(9999, 12, 31, 23, 59, 59, 999000)
        assert 0 == len(CDFValidator()._validrange(v))

        v[-1] = datetime.datetime(2010, 2, 1)
        errs = CDFValidator()._validrange(v)
        assert 1 == len(errs)
        assert (
            "Value 2010-02-01 00:00:00 at index 4 over VALIDMAX "
            "2010-01-31 00:00:00." == errs[0]
        )

        v[-1] = datetime.datetime(9999, 12, 31, 23, 59, 59, 999000)
        assert 0 == len(CDFValidator()._validrange(v))


def test_valid_range_scalar():
    """Check validmin/max on a scalar"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", recVary=False, data=1)
        v.attrs["VALIDMIN"] = 0
        v.attrs["VALIDMAX"] = 2
        v.attrs["FILLVAL"] = -100
        assert 0 == len(CDFValidator()._validrange(v))
        v.attrs["VALIDMIN"] = 2
        v.attrs["VALIDMAX"] = 3
        errs = CDFValidator()._validrange(v)
        assert 1 == len(errs)
        assert "Value 1 under VALIDMIN 2." == errs[0]


def test_valid_scale():
    """Check scale min and max."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", recVary=False, data=[1, 2, 3])
        v.attrs["SCALEMIN"] = 1
        v.attrs["SCALEMAX"] = 3
        assert 0 == len(CDFValidator()._validscale(v))
        v.attrs["SCALEMIN"] = 5
        v.attrs["SCALEMAX"] = 3
        assert 1 == len(CDFValidator()._validscale(v))
        errs = CDFValidator()._validscale(v)
        assert "SCALEMIN > SCALEMAX." == errs[0]
        v.attrs["SCALEMIN"] = -200
        errs = CDFValidator()._validscale(v)
        assert 1 == len(errs)
        errs.sort()
        assert ["SCALEMIN (-200) outside valid data range (-128,127)."] == errs
        v.attrs["SCALEMIN"] = 200
        errs = CDFValidator()._validscale(v)
        assert 2 == len(errs)
        errs.sort()
        assert [
            "SCALEMIN (200) outside valid data range (-128,127).",
            "SCALEMIN > SCALEMAX.",
        ] == errs
        v.attrs["SCALEMAX"] = -200
        errs = CDFValidator()._validscale(v)
        assert 3 == len(errs)
        errs.sort()
        assert [
            "SCALEMAX (-200) outside valid data range (-128,127).",
            "SCALEMIN (200) outside valid data range (-128,127).",
            "SCALEMIN > SCALEMAX.",
        ] == errs
        v.attrs["SCALEMAX"] = 200
        errs = CDFValidator()._validscale(v)
        assert 2 == len(errs)
        errs.sort()
        assert [
            "SCALEMAX (200) outside valid data range (-128,127).",
            "SCALEMIN (200) outside valid data range (-128,127).",
        ] == errs


def test_valid_scale_dimensioned():
    """Validmin/validmax with multiple elements"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", data=[[1, 10], [2, 20], [3, 30]])
        v.attrs["SCALEMIN"] = [2, 20]
        v.attrs["SCALEMAX"] = [300, 320]
        v.attrs["FILLVAL"] = -100
        errs = CDFValidator()._validscale(v)
        assert 1 == len(errs)
        errs.sort()
        assert [
            "SCALEMAX ([300 320]) outside valid data range (-128,127).",
        ] == errs
        v.attrs["SCALEMAX"] = [30, 32]
        errs = CDFValidator()._validscale(v)
        assert 0 == len(errs)


def test_valid_scale_dimension_mismatch():
    """Validmin/validmax with something wrong in dimensionality"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new("var1", data=[[1, 10], [2, 20], [3, 30]])
        v.attrs["SCALEMIN"] = [1, 10, 100]
        v.attrs["SCALEMAX"] = [3, 30, 126]
        errs = CDFValidator()._validscale(v)
        assert 2 == len(errs)
        errs.sort()
        assert [
            "SCALEMAX element count 3 does not match " "first data dimension size 2.",
            "SCALEMIN element count 3 does not match " "first data dimension size 2.",
        ] == errs


def test_valid_scale_high_dimension():
    """scalemin/scalemax with high-dimension variables"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create a Test CDF
        cdf = CDF(tmpdirname + "test.cdf", create=True)

        v = cdf.new(
            "var1",
            data=np.reshape(
                np.arange(27.0),
                (
                    3,
                    3,
                    3,
                ),
            ),
        )
        v.attrs["SCALEMIN"] = [1, 10, 100]
        v.attrs["SCALEMAX"] = [3, 30, 300]
        errs = CDFValidator()._validscale(v)
        assert 2 == len(errs)
        assert "Multi-element SCALEMIN only valid with 1D variable." == errs[0]
        assert "Multi-element SCALEMAX only valid with 1D variable." == errs[1]


def test_fits_validation_demo_data():
    """
    Function to test the FITS Validation
    """
    fits_image_filename = fits.util.get_testdata_filepath("test1.fits")

    # Validate the FITS File
    errs = validate(fits_image_filename)
    assert isinstance(errs, list)


def test_fits_validation_valid_data():
    """
    Function to test FITS Validation with a valid FITS File
    """
    # fmt: off
    metadata = {
        "AUTHOR": ("Tester", "Author"),
        "BLANK": (0, "Value of missing pixels (floating HDU)"),
        "BTYPE": ("Test Data", "Data label"),
        "BUNIT": ("J", "Units"),
        "CAMERA": ("Test Camera", "Name of the camera"),
        "CAMPAIGN": ("SWxSOC Testing", "Observation campaign"),
        "CCURRENT": ("n/a", "Concurrent files"),
        "CDELTia": (0.0, "Coordinate scale increment"),
        "CDi_ja": (0.0, "Linear-transformation matrix element"),
        "CHECKSUM": ("Test", "Checksum"),
        "CNAMEia": ("Axis", "Coordinate axis name"),
        "CPDISia": ("None", "Distortion correction"),
        "CPERRia": ("None", "Maximum value of prior distortion correction for axis i"),
        "CQDISia": ("None", "Distortion correction"),
        "CQERRia": ("None", "Maximum value of posterior distortion correction for axis i",),
        "CTYPEia": ("Test", "Coordinate type"),
        "CUNITia": ("J", "Coordinate units"),
        "CWDISia": ("None", "Distortion correction"),
        "CWERRia": ("None", "Maximum value of weighted distortion correction for axis i"),
        "DATASUM": (0.0, "Sum of data"),
        "DATATAGS": ("TestData", "Data Tags"),
        "DATE": ("19700101", "File Creation Date"),
        "DATE-BEG": ("19700101", "Aquisition start time"),
        "DATEREF": ("19700101", "Reference date"),
        "DETECTOR": ("Test", "detector"),
        "DPja": (0.0, "Primary distortion parameter"),
        "DQia": (0.0, "Posterior distortion parameter"),
        "DSUN_OBS": (0.0, "Distance to Sun"),
        "DWia": (0.0, "Distortion correction"),
        "EXTNAME": ("PRIMARY", "Extension name"),
        "FILENAME": ("Test", "Filename"),
        "FILEVERP": ("Test", "File Version Pattern"),
        "FILTER": ("Test", "Filters"),
        "GEOX_OBS": (0.0, "Observer X Position"),
        "GEOY_OBS": (0.0, "Observer Y Position"),
        "GEOZ_OBS": (0.0, "Observer Z Position"),
        "GRATING": ("Test", "Grating"),
        "HGLN_OBS": (0.0, "Observer Heliographic Longitude"),
        "HGLT_OBS": (0.0, "Observer Heliographic Latitude"),
        "INSTRUME": ("Test", "Instrument"),
        "MISSION": ("Test", "Mission"),
        "NAXIS": (2, "Number of axes"),
        "NASIXn": (10, "Number of elements along axis"),
        "NSUMEXP": (1, "Number of exposures"),
        "OBSERVER": ("Test", "Observer"),
        "OBSGEO-X": (0.0, "Observer X Position"),
        "OBSGEO-Y": (0.0, "Observer Y Position"),
        "OBSGEO-Z": (0.0, "Observer Z Position"),
        "OBSRVTRY": ("Test", "Observatory"),
        "OBSTITLE": ("Test", "Observation title"),
        "OBS_DESC": ("Test", "Observation description"),
        "OBS_HDU": (1, "Contains observational data"),
        "OBS_MODE": ("Test", "Observation mode"),
        "OBS_VR": (0.0, "Observer velocity"),
        "ORIGIN": ("Test", "File originator"),
        "PCi_ja": (0.0, "linear transformation matrix element"),
        "PLANNER": ("Test", "Planner"),
        "POINT_ID": ("Test", "Pointing ID"),
        "POLCANGL": (0.0, "Polarimetric angle"),
        "POLCCONV": ("Test", "Polarimetric conversion"),
        "PROJECT": ("Test", "Project"),
        "REQUESTR": ("Test", "Requester"),
        "RSUN_ARC": (0.0, "Solar angular radius"),
        "SETTINGS": ("Test", "Settings"),
        "SLIT_WID": (0.0, "Slit width"),
        "SOLARNET": (1.0, "Solarnet compatibility"),
        "SPECSYS": ("SOURCE", "Spectral reference frame"),
        "TELCONFG": ("Test", "Telescope configuration"),
        "TELESCOP": ("Test", "Telescope"),
        "TEXPOSURE": (0.0, "Single exposure time"),
        "VELOSYS": (0.0, "Velocity system"),
        "WAVEMAX": (0.0, "Maximum wavelength"),
        "WAVEMIN": (0.0, "Minimum wavelength"),
        "WAVEREF": ("air", "Wavelength reference"),
        "WAVEUNIT": ("m", "Wavelength unit"),
        "WCSAXES": (2, "WCS axes"),
        "XPOSURE": (0.0, "Exposure time"),
        "iCRVLn": (0.0, "Reference value"),
        "iCTYPn": (0.0, "Coordinate type"),
        "jCRPXn": (0.0, "Reference pixel"),
        # "VAR_TYPE": ("data", "Variable Type"),
    }
    # fmt: on

    # Assert all metadata keys are in the global attribute template
    cached_template = SWXSchema(defaults="fits").global_attribute_template()
    for attr_name in metadata:
        assert attr_name in cached_template
    # Assert all template keys are in the metadata
    for attr_name in cached_template:
        assert attr_name in metadata

    ts = TimeSeries()
    # Spectra Data
    spectra = NDCollection(
        [
            (
                "PRIMARY",
                NDCube(
                    data=np.zeros((10, 10)),
                    wcs=WCS(naxis=2),
                    meta=metadata,
                    unit="eV",
                ),
            )
        ]
    )

    # Create SWXData Object
    sw_data = SWXData(timeseries=ts, spectra=spectra, schema=SWXSchema(defaults="fits"))
    with tempfile.TemporaryDirectory() as tmpdirname:
        test_file_output_path = sw_data.save(
            Path(tmpdirname) / "test.fits", file_extension=".fits"
        )

        # Validate the FITS File
        errs = validate(test_file_output_path)
        assert isinstance(errs, list)
        # TODO: Make it not complain about var_type
        assert len(errs) <= 1
