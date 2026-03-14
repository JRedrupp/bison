"""Tests for reshaping operations."""
from std.python import Python, PythonObject
from testing import assert_true, TestSuite
from bison import DataFrame, Series, SeriesScalar


fn test_sort_values_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var raised = False
    try:
        _ = df.sort_values(PythonObject("a"))
    except:
        raised = True
    assert_true(raised)


fn test_pivot_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2], 'c': [3]}")))
    var raised = False
    try:
        _ = df.pivot(index="a", columns="b", values="c")
    except:
        raised = True
    assert_true(raised)


fn test_melt_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'id': [1], 'val': [10]}")))
    var raised = False
    try:
        _ = df.melt()
    except:
        raised = True
    assert_true(raised)


fn test_transpose_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var raised = False
    try:
        _ = df.transpose()
    except:
        raised = True
    assert_true(raised)


fn test_drop_duplicates_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2]}")))
    var raised = False
    try:
        _ = df.drop_duplicates()
    except:
        raised = True
    assert_true(raised)


fn test_series_sort_values() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]"), dtype="int64"))
    var r = s.sort_values()
    assert_true(r.iloc(0)[Int64] == 1)
    assert_true(r.iloc(1)[Int64] == 2)
    assert_true(r.iloc(2)[Int64] == 3)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
