"""Tests for DataFrame missing-data handling."""
from std.python import Python, PythonObject
from testing import assert_true, assert_equal, TestSuite
from bison import DataFrame, Series, DFScalar


# ------------------------------------------------------------------
# isna / isnull
# ------------------------------------------------------------------

fn test_df_isna_no_nulls() raises:
    """isna() on a DataFrame without nulls returns all-False boolean columns."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var result = df.isna()
    assert_equal(result.shape()[0], 3)
    assert_equal(result.shape()[1], 1)
    var col_a = result["a"]
    for i in range(3):
        assert_true(not col_a._col._data[List[Bool]][i])


fn test_df_isna_with_nulls() raises:
    """isna() correctly marks NaN rows as True."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var result = df.isna()
    var col_a = result["a"]
    assert_true(not col_a._col._data[List[Bool]][0])
    assert_true(col_a._col._data[List[Bool]][1])
    assert_true(not col_a._col._data[List[Bool]][2])


fn test_df_isnull_alias() raises:
    """isnull() is an alias for isna()."""
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

fn test_df_notna_with_nulls() raises:
    """notna() returns True for non-null entries."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var result = df.notna()
    var col_a = result["a"]
    assert_true(col_a._col._data[List[Bool]][0])
    assert_true(not col_a._col._data[List[Bool]][1])
    assert_true(col_a._col._data[List[Bool]][2])


fn test_df_notnull_alias() raises:
    """notnull() is an alias for notna()."""
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

fn test_df_fillna_float() raises:
    """fillna(0) replaces NaN in a float column."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var result = df.fillna(PythonObject(0))
    assert_equal(result.shape()[0], 3)
    var col_a = result["a"]
    assert_equal(col_a._col._data[List[Float64]][1], Float64(0.0))


fn test_df_fillna_no_nulls() raises:
    """fillna on a DataFrame without nulls returns identical shape."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.fillna(PythonObject(99))
    assert_equal(result.shape()[0], 3)
    assert_equal(result.shape()[1], 1)


# ------------------------------------------------------------------
# dropna
# ------------------------------------------------------------------

fn test_df_dropna_any() raises:
    """dropna(how='any') removes rows that have at least one null."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var result = df.dropna()
    assert_equal(result.shape()[0], 2)


fn test_df_dropna_all() raises:
    """dropna(how='all') keeps rows that have at least one non-null value."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0], 'b': [None, None, 6.0]}")))
    var result = df.dropna(how="all")
    # Row 1: a=1.0, b=None  -> not ALL null  -> kept
    # Row 2: a=None, b=None -> ALL null       -> dropped
    # Row 3: a=3.0, b=6.0  -> not ALL null  -> kept
    assert_equal(result.shape()[0], 2)


fn test_df_dropna_no_nulls() raises:
    """dropna on a DataFrame without nulls returns all rows."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.dropna()
    assert_equal(result.shape()[0], 3)


# ------------------------------------------------------------------
# ffill
# ------------------------------------------------------------------

fn test_df_ffill() raises:
    """ffill() propagates the last non-null value forward."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, None, 4.0]}")))
    var result = df.ffill()
    var col_a = result["a"]
    assert_equal(col_a._col._data[List[Float64]][1], Float64(1.0))
    assert_equal(col_a._col._data[List[Float64]][2], Float64(1.0))
    assert_equal(col_a._col._data[List[Float64]][3], Float64(4.0))


fn test_df_ffill_no_nulls() raises:
    """ffill() on a DataFrame without nulls returns identical values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.ffill()
    assert_equal(result.shape()[0], 3)


# ------------------------------------------------------------------
# bfill
# ------------------------------------------------------------------

fn test_df_bfill() raises:
    """bfill() propagates the next non-null value backward."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [None, None, 3.0, 4.0]}")))
    var result = df.bfill()
    var col_a = result["a"]
    assert_equal(col_a._col._data[List[Float64]][0], Float64(3.0))
    assert_equal(col_a._col._data[List[Float64]][1], Float64(3.0))


fn test_df_bfill_no_nulls() raises:
    """bfill() on a DataFrame without nulls returns identical values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.bfill()
    assert_equal(result.shape()[0], 3)


# ------------------------------------------------------------------
# interpolate
# ------------------------------------------------------------------

fn test_df_interpolate_linear() raises:
    """interpolate(method='linear') fills interior nulls by linear interpolation."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [0.0, None, 4.0]}")))
    var result = df.interpolate()
    var col_a = result["a"]
    assert_equal(col_a._col._data[List[Float64]][1], Float64(2.0))


fn test_df_interpolate_no_nulls() raises:
    """interpolate() on a DataFrame without nulls returns identical values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var result = df.interpolate()
    assert_equal(result.shape()[0], 3)


fn test_df_interpolate_unsupported_method() raises:
    """interpolate() raises for unsupported methods."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var raised = False
    try:
        _ = df.interpolate(method="cubic")
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# Series fillna (previously tested as a stub)
# ------------------------------------------------------------------

fn test_series_fillna_works() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 2.0]")))
    var filled = s.fillna(DFScalar(Float64(0.0)))
    assert_true(filled.size() == 3)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
