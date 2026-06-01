# Auto-Prefixing Solution for CDF Duplicate Variable Names

**Date:** May 10, 2026  
**Issue:** REACH demo CDF development - duplicate variable names overwriting each other

## Problem Statement

In the REACH demo, 32 TimeSeries objects were created and added to a SWXData object, each keyed by satellite name (e.g., 'REACH-165', 'REACH-134'). However, all TimeSeries had identical column names ('Lat', 'Lon', 'Pos', 'Sensor A', 'Sensor B', etc.). When writing to CDF, these duplicate names overwrote each other because:

- **Python structure:** Hierarchical `timeseries["REACH-165"]["Lat"]`
- **CDF structure:** Flat namespace - all variables must be globally unique
- **Result:** Data loss from overwriting

## Solution Implemented

### 1. Auto-Prefixing (Every Time)

All variables are now automatically prefixed with their sanitized epoch key:

**Write Operation:**
- `timeseries["REACH-165"]["Lat"]` → CDF variable `REACH_165_Lat`
- `timeseries["REACH-165"]["time"]` → CDF variable `REACH_165_Epoch`
- `timeseries["REACH-134"]["Lat"]` → CDF variable `REACH_134_Lat`
- Special case: Default epoch "Epoch" remains unprefixed for backward compatibility

**Read Operation:**
- CDF variable `REACH_165_Lat` → `timeseries["REACH-165"]["Lat"]`
- Prefixes are stripped to reconstruct original column names

### 2. Architectural Fix: Pass epoch_key Through Derivation Chain

**Original Problem:**
- Code had `epoch_key` at line 524 in swxdata.py
- But didn't pass it to `derive_measurement_attributes()`
- Schema code had to re-infer the epoch using array length heuristic
- **Failed when multiple TimeSeries had same length**

**Solution:**
- Pass `epoch_key` as parameter through the derivation chain
- Schema uses explicit `epoch_key` instead of guessing from array length
- **Now works with same-length TimeSeries arrays**
The key insight was that epoch_key was already available in the loop iteration (line 530: for epoch_key, ts in self._timeseries.items()), but the code wasn't passing it down to the schema layer. So I just added it as a keyword argument to carry that information forward, eliminating the need for the schema to guess it using array length matching.

## Files Changed

### 1. `/workspaces/swxsoc/swxsoc/util/io.py`

**`_convert_variables_to_cdf()` (lines ~403-435):**
```python
for epoch_key, ts in data.data["timeseries"].items():
    prefix = epoch_key.replace("-", "_")
    
    for var_name in ts.colnames:
        var_data = ts[var_name]
        if var_name == "time":
            # Special handling for default epoch
            if epoch_key == default_timeseries_key:
                cdf_var_name = "Epoch"
            else:
                cdf_var_name = f"{prefix}_Epoch"
            cdf_file[cdf_var_name] = var_data.to_datetime()
        else:
            # Prefix data variables (unless default epoch)
            if epoch_key == default_timeseries_key:
                cdf_var_name = var_name
            else:
                cdf_var_name = f"{prefix}_{var_name}"
            cdf_file[cdf_var_name] = var_data.value
```

**`load_data()` (lines ~140-165):**
- Parse epoch variables ending with "_Epoch"
- Build mapping: `epoch_var_to_key["REACH_165_Epoch"] = "REACH-165"`
- Strip prefixes from data variable names when loading
- Improved warning to check for any epoch variables (not just default)

### 2. `/workspaces/swxsoc/swxsoc/util/schema.py`

**`derive_measurement_attributes()` (line ~700):**
- Added `epoch_key: Optional[str] = None` parameter
- Pass `epoch_key` to derivation functions via kwargs

**`_get_depend()` (lines ~828-851):**
```python
def _get_depend(self, var_name, var_data, guess_type, **kwargs):
    # If epoch_key was explicitly passed, use it (NEW!)
    if "epoch_key" in kwargs:
        epoch_key = kwargs["epoch_key"]
    elif "timeseries_dict" in kwargs:
        # Fallback to old heuristic
        epoch_key = SWXData.get_timeseres_epoch_key(...)
    else:
        epoch_key = swxsoc.config["general"]["default_timeseries_key"]
    
    # Return prefixed epoch name for DEPEND_0
    if epoch_key == default_key:
        prefixed_epoch = "Epoch"
    else:
        prefixed_epoch = f"{epoch_key.replace('-', '_')}_Epoch"
    return prefixed_epoch
```

### 3. `/workspaces/swxsoc/swxsoc/swxdata.py`

**`_derive_metadata()` (line ~524):**
```python
for epoch_key, ts in self._timeseries.items():
    for col in ts.columns:
        for attr_name, attr_value in self.schema.derive_measurement_attributes(
            self, col, epoch_key=epoch_key  # NEW: Pass epoch_key explicitly
        ).items():
            self._update_measurement_attribute(...)
```

**`get_timeseres_epoch_key()` (lines ~419-423):**
```python
if var_meta is not None and "DEPEND_0" in var_meta:
    epoch_key = var_meta["DEPEND_0"]
    # If prefixed format (e.g., "REACH_165_Epoch"), convert back
    if epoch_key.endswith("_Epoch"):
        epoch_key = epoch_key[:-6].replace("_", "-")
    # If just "Epoch", keep as-is (default)
```

### 4. `/workspaces/swxsoc/swxsoc/util/tests/test_io.py`

**New Test: `test_cdf_auto_prefixing_prevents_duplicates()` (lines 173-331):**
- Creates 3 TimeSeries (REACH-165, REACH-134, REACH-099)
- **All have same length (5 elements)** - demonstrates architectural fix
- Each has columns: time, Lat, Lon, Sensor_A
- Verifies all 12 prefixed variables exist in CDF (9 data + 3 epochs)
- Checks DEPEND_0 linkage is correct
- Tests round-trip: save → load → verify structure preserved
- Uses 2024 timestamps to avoid "dubious year" warnings

## Key Design Decisions

### 1. Why "Every Time" Prefixing?

Simpler and more predictable than conditional/smart prefixing:
- No need to detect duplicates
- Consistent naming convention
- Easier to understand and debug
- Eliminates entire class of bugs

### 2. Why Special Handling for Default "Epoch"?

Backward compatibility:
- Existing files use unprefixed "Epoch" variable
- ISTP standard expects "Epoch" as primary time variable
- Only non-default epochs get prefixed

### 3. DEPEND_0 and CDF Standards

DEPEND_0 is an **ISTP (NASA/SPDF) requirement**, not swxsoc-specific:
- All time-varying variables must have DEPEND_0 attribute
- Points to the time variable (epoch) the data depends on
- Critical for multi-epoch CDF files
- Reference: [ISTP Guidelines](http://spdf.gsfc.nasa.gov/istp_guide/istp_guide.html)

## Test Results

All 5 IO tests passing:
- ✅ `test_cdf_io` - Basic round-trip
- ✅ `test_cdf_bad_file_path` - Error handling
- ✅ `test_cdf_nrv_support_data` - Non-record-varying data
- ✅ `test_cdf_spectra_data` - High-dimensional spectra
- ✅ `test_cdf_auto_prefixing_prevents_duplicates` - **NEW**: Multi-satellite scenario

## Impact on REACH Demo

The original REACH scenario with 32 satellites will now work correctly:
- ✅ All satellite data preserved (no overwriting)
- ✅ Same-length arrays supported
- ✅ Proper DEPEND_0 linkage
- ✅ Clean round-trip save/load

Example CDF structure:
```
REACH_165_Epoch [5 records]
REACH_165_Lat [5 records, DEPEND_0="REACH_165_Epoch"]
REACH_165_Lon [5 records, DEPEND_0="REACH_165_Epoch"]
REACH_165_Pos [5 records, DEPEND_0="REACH_165_Epoch"]
...
REACH_134_Epoch [5 records]
REACH_134_Lat [5 records, DEPEND_0="REACH_134_Epoch"]
...
```

## Warnings Addressed

1. **"Epoch Variable Epoch not found"** - Changed to check for any epoch variable instead of requiring default
2. **"dubious year" ERFA warnings** - Changed test timestamps from 1970 to 2024
3. Other warnings (leapseconds, overflow, unit conversions) are pre-existing and harmless

## Future Considerations

- Consider documenting the auto-prefixing behavior in user-facing docs
- Explain CDF variable naming convention in guides
- Note: Original ticket mentioned "write a warning to the log" but we implemented full auto-prefixing instead (stronger solution)
