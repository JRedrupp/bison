"""Tests for DataFrame loc / iloc / at / iat indexers (issue #5)."""
from std.python import Python, PythonObject
from std.memory import UnsafePointer
from testing import assert_equal, assert_true, assert_false, TestSuite
from bison import DataFrame, Series, DFScalar, IAtIndexer, AtIndexer, ILocIndexer, LocIndexer


# ------------------------------------------------------------------
# IAtIndexer – integer-based scalar access
# ------------------------------------------------------------------

fn test_iat_getitem_int_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20, 30]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    assert_true(iat[0, 0][Int64] == 10)
    assert_true(iat[1, 0][Int64] == 20)
    assert_true(iat[2, 0][Int64] == 30)


fn test_iat_getitem_float_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1.5, 2.5]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    assert_true(iat[0, 0][Float64] == 1.5)
    assert_true(iat[1, 0][Float64] == 2.5)


fn test_iat_getitem_bool_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'b': [True, False]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    assert_true(iat[0, 0][Bool] == True)
    assert_true(iat[1, 0][Bool] == False)


fn test_iat_getitem_negative_row() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    assert_true(iat[-1, 0][Int64] == 3)
    assert_true(iat[-3, 0][Int64] == 1)


fn test_iat_getitem_out_of_bounds_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    var raised = False
    try:
        _ = iat[5, 0]
    except:
        raised = True
    assert_true(raised)


fn test_iat_getitem_bad_col_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    var raised = False
    try:
        _ = iat[0, 99]
    except:
        raised = True
    assert_true(raised)


fn test_iat_setitem_int_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    iat[0, 0] = DFScalar(Int64(99))
    # Mutation propagates to the original DataFrame.
    assert_true(df["a"].iloc(0)[Int64] == 99)
    assert_true(df["a"].iloc(1)[Int64] == 20)


fn test_iat_setitem_float_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1.0, 2.0]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    iat[1, 0] = DFScalar(Float64(9.9))
    assert_true(df["x"].iloc(1)[Float64] == 9.9)


fn test_iat_setitem_bool_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'b': [True, True]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    iat[0, 0] = DFScalar(Bool(False))
    assert_true(df["b"].iloc(0)[Bool] == False)


fn test_iat_setitem_multiple_columns() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var iat = IAtIndexer(UnsafePointer(to=df))
    iat[0, 0] = DFScalar(Int64(10))
    iat[1, 1] = DFScalar(Int64(40))
    assert_true(df["a"].iloc(0)[Int64] == 10)
    assert_true(df["b"].iloc(1)[Int64] == 40)


# ------------------------------------------------------------------
# AtIndexer – label-based scalar access
# ------------------------------------------------------------------

fn test_at_getitem_default_int_index() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20, 30]}")))
    var at = AtIndexer(UnsafePointer(to=df))
    assert_true(at["0", "a"][Int64] == 10)
    assert_true(at["1", "a"][Int64] == 20)
    assert_true(at["2", "a"][Int64] == 30)


fn test_at_getitem_string_index() raises:
    var pd = Python.import_module("pandas")
    var py_df = pd.DataFrame(
        Python.evaluate("{'val': [100, 200]}"),
        index=Python.evaluate("['r0', 'r1']"),
    )
    var df = DataFrame(py_df)
    var at = AtIndexer(UnsafePointer(to=df))
    assert_true(at["r0", "val"][Int64] == 100)
    assert_true(at["r1", "val"][Int64] == 200)


fn test_at_getitem_missing_col_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var at = AtIndexer(UnsafePointer(to=df))
    var raised = False
    try:
        _ = at["0", "z"]
    except:
        raised = True
    assert_true(raised)


fn test_at_getitem_missing_row_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var at = AtIndexer(UnsafePointer(to=df))
    var raised = False
    try:
        _ = at["99", "a"]
    except:
        raised = True
    assert_true(raised)


fn test_at_setitem_default_index() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var at = AtIndexer(UnsafePointer(to=df))
    at["0", "a"] = DFScalar(Int64(55))
    assert_true(df["a"].iloc(0)[Int64] == 55)
    assert_true(df["a"].iloc(1)[Int64] == 2)


fn test_at_setitem_string_index() raises:
    var pd = Python.import_module("pandas")
    var py_df = pd.DataFrame(
        Python.evaluate("{'x': [10.0, 20.0]}"),
        index=Python.evaluate("['a', 'b']"),
    )
    var df = DataFrame(py_df)
    var at = AtIndexer(UnsafePointer(to=df))
    at["b", "x"] = DFScalar(Float64(99.0))
    assert_true(df["x"].iloc(1)[Float64] == 99.0)


# ------------------------------------------------------------------
# ILocIndexer – integer-position row access
# ------------------------------------------------------------------

fn test_iloc_getitem_returns_series() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var iloc = ILocIndexer(UnsafePointer(to=df))
    var row = iloc[0]
    # Row should have as many elements as columns.
    assert_equal(row.size(), 2)


fn test_iloc_getitem_row_values() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20], 'b': [30, 40]}")))
    var iloc = ILocIndexer(UnsafePointer(to=df))
    var row0 = iloc[0]
    assert_true(row0.iloc(0)[PythonObject].__int__() == 10)
    assert_true(row0.iloc(1)[PythonObject].__int__() == 30)
    var row1 = iloc[1]
    assert_true(row1.iloc(0)[PythonObject].__int__() == 20)
    assert_true(row1.iloc(1)[PythonObject].__int__() == 40)


fn test_iloc_getitem_negative_index() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var iloc = ILocIndexer(UnsafePointer(to=df))
    var last = iloc[-1]
    assert_true(last.iloc(0)[PythonObject].__int__() == 3)


fn test_iloc_getitem_out_of_bounds_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var iloc = ILocIndexer(UnsafePointer(to=df))
    var raised = False
    try:
        _ = iloc[5]
    except:
        raised = True
    assert_true(raised)


fn test_iloc_setitem_updates_dataframe() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var iloc = ILocIndexer(UnsafePointer(to=df))
    var new_row = Series(pd.Series(Python.evaluate("[99, 88]"), dtype="int64"))
    iloc[0] = new_row^
    assert_true(df["a"].iloc(0)[Int64] == 99)
    assert_true(df["b"].iloc(0)[Int64] == 88)
    # Row 1 unchanged.
    assert_true(df["a"].iloc(1)[Int64] == 2)
    assert_true(df["b"].iloc(1)[Int64] == 4)


fn test_iloc_setitem_wrong_size_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var iloc = ILocIndexer(UnsafePointer(to=df))
    # Series with only one element for a 2-column DataFrame.
    var short = Series(pd.Series(Python.evaluate("[9]"), dtype="int64"))
    var raised = False
    try:
        iloc[0] = short^
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# LocIndexer – label-based row access
# ------------------------------------------------------------------

fn test_loc_getitem_default_int_index() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20], 'b': [30, 40]}")))
    var loc = LocIndexer(UnsafePointer(to=df))
    var row0 = loc["0"]
    assert_equal(row0.size(), 2)
    assert_true(row0.iloc(0)[PythonObject].__int__() == 10)
    assert_true(row0.iloc(1)[PythonObject].__int__() == 30)


fn test_loc_getitem_string_index() raises:
    var pd = Python.import_module("pandas")
    var py_df = pd.DataFrame(
        Python.evaluate("{'val': [100, 200, 300]}"),
        index=Python.evaluate("['x', 'y', 'z']"),
    )
    var df = DataFrame(py_df)
    var loc = LocIndexer(UnsafePointer(to=df))
    var ry = loc["y"]
    assert_true(ry.iloc(0)[PythonObject].__int__() == 200)


fn test_loc_getitem_missing_label_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var loc = LocIndexer(UnsafePointer(to=df))
    var raised = False
    try:
        _ = loc["99"]
    except:
        raised = True
    assert_true(raised)


fn test_loc_setitem_updates_row() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var loc = LocIndexer(UnsafePointer(to=df))
    var new_row = Series(pd.Series(Python.evaluate("[55, 66]"), dtype="int64"))
    loc["1"] = new_row^
    assert_true(df["a"].iloc(1)[Int64] == 55)
    assert_true(df["b"].iloc(1)[Int64] == 66)
    # Row 0 unchanged.
    assert_true(df["a"].iloc(0)[Int64] == 1)
    assert_true(df["b"].iloc(0)[Int64] == 3)


fn test_loc_setitem_wrong_size_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var loc = LocIndexer(UnsafePointer(to=df))
    var short = Series(pd.Series(Python.evaluate("[9]"), dtype="int64"))
    var raised = False
    try:
        loc["0"] = short^
    except:
        raised = True
    assert_true(raised)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
