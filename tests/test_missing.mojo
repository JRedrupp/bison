"""Tests for DataFrame missing-data handling."""
from std.python import Python, PythonObject
from testing import assert_true, assert_equal, TestSuite
from bison import DataFrame, Series, DFScalar


# ------------------------------------------------------------------
# isna / isnull
# ------------------------------------------------------------------

def test_df_isna_no_nulls() raises:
    """Isna on a DataFrame without nulls returns all-False boolean columns."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var result = df.isna()
    assert_equal(result.shape()[0], 3)
    assert_equal(result.shape()[1], 1)
    var col_a = result["a"]
    for i in range(3):
        assert_true(not col_a._col._data[List[Bool]][i])


def test_df_isna_with_nulls() raises:
    """Isna correctly marks NaN rows as True."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var result = df.isna()
    var col_a = result["a"]
    assert_true(not col_a._col._data[List[Bool]][0])
    assert_true(col_a._col._data[List[Bool]][1])
    assert_true(not col_a._col._data[List[Bool]][2])


def test_df_isnull_alias() raises:
    """Isnull is an alias for isna."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1.0, None]}")))
    var r1 = df.isna()
    var r2 = df.isnull()
    var c1 = r1["x"]
    var c2 = r2["x"]
    for i in range(2):
        assert_equal(c1._col._data[List[Bool]][i], c2._col._data[List[Bool]][i])


# ------------------------------------------------------------------
# notna / notnull
# ------------------------------------------------------------------

def test_df_notna_with_nulls() raises:
    """Notna returns True for non-null entries."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var result = df.notna()
    var col_a = result["a"]
    assert_true(col_a._col._data[List[Bool]][0])
    assert_true(not col_a._col._data[List[Bool]][1])
    assert_true(col_a._col._data[List[Bool]][2])


def test_df_notnull_alias() raises:
    """Notnull is an alias for notna."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [None, 2.0]}")))
    var r1 = df.notna()
    var r2 = df.notnull()
    var c1 = r1["x"]
    var c2 = r2["x"]
    for i in range(2):
        assert_equal(c1._col._data[List[Bool]][i], c2._col._data[List[Bool]][i])


# ------------------------------------------------------------------
# fillna
# ------------------------------------------------------------------

def test_df_fillna_float() raises:
    """Fillna with 0.0 replaces NaN in a float column."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var result = df.fillna(DFScalar(Float64(0.0)))
    assert_equal(result.shape()[0], 3)
    var col_a = result["a"]
    assert_equal(col_a._col._data[List[Float64]][1], Float64(0.0))


def test_df_fillna_no_nulls() raises:
    """Fillna on a DataFrame without nulls returns identical shape."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.fillna(DFScalar(Float64(99.0)))
    assert_equal(result.shape()[0], 3)
    assert_equal(result.shape()[1], 1)


def test_df_fillna_int() raises:
    """Fillna with an integer value fills Int64 columns."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, None, 3]}")))
    var result = df.fillna(DFScalar(Int64(0)))
    assert_equal(result.shape()[0], 3)


# ------------------------------------------------------------------
# dropna
# ------------------------------------------------------------------

def test_df_dropna_any() raises:
    """Dropna with how=any removes rows that have at least one null."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var result = df.dropna()
    assert_equal(result.shape()[0], 2)


def test_df_dropna_all() raises:
    """Dropna with how=all keeps rows that have at least one non-null value."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0], 'b': [None, None, 6.0]}")))
    var result = df.dropna(how="all")
    # Row 0: a=1.0, b=None  -> not ALL null -> kept
    # Row 1: a=None, b=None -> ALL null      -> dropped
    # Row 2: a=3.0, b=6.0  -> not ALL null -> kept
    assert_equal(result.shape()[0], 2)


def test_df_dropna_no_nulls() raises:
    """Dropna on a DataFrame without nulls returns all rows."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.dropna()
    assert_equal(result.shape()[0], 3)


def test_df_dropna_thresh() raises:
    """Dropna with thresh keeps rows with enough non-null values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0], 'b': [None, None, 6.0]}")))
    # thresh=2: keep rows with at least 2 non-null values
    var result = df.dropna(thresh=2)
    # Row 0: a=1.0, b=None  -> 1 non-null -> dropped
    # Row 1: a=None, b=None -> 0 non-null -> dropped
    # Row 2: a=3.0, b=6.0  -> 2 non-null -> kept
    assert_equal(result.shape()[0], 1)


def test_df_dropna_subset() raises:
    """Dropna with subset checks only specified columns."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0], 'b': [None, None, 6.0]}")))
    # Only check column 'a' for nulls
    var cols = List[String]()
    cols.append("a")
    var result = df.dropna(subset=cols^)
    # Row 1 has a=None -> dropped; rows 0 and 2 have a non-null -> kept
    assert_equal(result.shape()[0], 2)


# ------------------------------------------------------------------
# ffill
# ------------------------------------------------------------------

def test_df_ffill() raises:
    """Ffill propagates the last non-null value forward."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, None, 4.0]}")))
    var result = df.ffill()
    var col_a = result["a"]
    assert_equal(col_a._col._data[List[Float64]][1], Float64(1.0))
    assert_equal(col_a._col._data[List[Float64]][2], Float64(1.0))
    assert_equal(col_a._col._data[List[Float64]][3], Float64(4.0))


def test_df_ffill_no_nulls() raises:
    """Ffill on a DataFrame without nulls returns identical values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.ffill()
    assert_equal(result.shape()[0], 3)


# ------------------------------------------------------------------
# bfill
# ------------------------------------------------------------------

def test_df_bfill() raises:
    """Bfill propagates the next non-null value backward."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [None, None, 3.0, 4.0]}")))
    var result = df.bfill()
    var col_a = result["a"]
    assert_equal(col_a._col._data[List[Float64]][0], Float64(3.0))
    assert_equal(col_a._col._data[List[Float64]][1], Float64(3.0))


def test_df_bfill_no_nulls() raises:
    """Bfill on a DataFrame without nulls returns identical values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.bfill()
    assert_equal(result.shape()[0], 3)


# ------------------------------------------------------------------
# interpolate
# ------------------------------------------------------------------

def test_df_interpolate_linear() raises:
    """Interpolate with method=linear fills interior nulls by linear interpolation."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [0.0, None, 4.0]}")))
    var result = df.interpolate()
    var col_a = result["a"]
    assert_equal(col_a._col._data[List[Float64]][1], Float64(2.0))


def test_df_interpolate_no_nulls() raises:
    """Interpolate on a DataFrame without nulls returns identical values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.interpolate()
    assert_equal(result.shape()[0], 3)


def test_df_interpolate_unsupported_method() raises:
    """Interpolate raises for unsupported methods."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var raised = False
    try:
        _ = df.interpolate(method="cubic")
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# Series fillna (kept from original test suite)
# ------------------------------------------------------------------

def test_series_fillna_works() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 2.0]")))
    var filled = s.fillna(DFScalar(Float64(0.0)))
    assert_true(filled.size() == 3)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
