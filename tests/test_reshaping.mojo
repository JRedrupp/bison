"""Tests that reshaping stubs raise 'not implemented'."""
from python import Python, PythonObject
from testing import assert_true
from bison import DataFrame, Series


def test_sort_values_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [3, 1, 2]}))
    var raised = False
    try:
        _ = df.sort_values(PythonObject("a"))
    except:
        raised = True
    assert_true(raised)


def test_pivot_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1], "b": [2], "c": [3]}))
    var raised = False
    try:
        _ = df.pivot(index="a", columns="b", values="c")
    except:
        raised = True
    assert_true(raised)


def test_melt_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"id": [1], "val": [10]}))
    var raised = False
    try:
        _ = df.melt()
    except:
        raised = True
    assert_true(raised)


def test_transpose_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1, 2]}))
    var raised = False
    try:
        _ = df.transpose()
    except:
        raised = True
    assert_true(raised)


def test_drop_duplicates_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1, 1, 2]}))
    var raised = False
    try:
        _ = df.drop_duplicates()
    except:
        raised = True
    assert_true(raised)


def test_series_sort_values_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series([3, 1, 2]))
    var raised = False
    try:
        _ = s.sort_values()
    except:
        raised = True
    assert_true(raised)


def main() raises:
    test_sort_values_stub()
    test_pivot_stub()
    test_melt_stub()
    test_transpose_stub()
    test_drop_duplicates_stub()
    test_series_sort_values_stub()
    print("test_reshaping: all tests passed")
