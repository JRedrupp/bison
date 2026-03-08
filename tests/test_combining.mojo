"""Tests that combining stubs raise 'not implemented'."""
from python import Python, PythonObject
from testing import assert_true
from bison import DataFrame


def test_merge_stub() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame({"key": [1, 2], "a": [10, 20]}))
    var right = DataFrame(pd.DataFrame({"key": [1, 2], "b": [30, 40]}))
    var raised = False
    try:
        _ = left.merge(right, on=PythonObject("key"))
    except:
        raised = True
    assert_true(raised)


def test_join_stub() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame({"a": [1, 2]}))
    var right = DataFrame(pd.DataFrame({"b": [3, 4]}))
    var raised = False
    try:
        _ = left.join(right)
    except:
        raised = True
    assert_true(raised)


def test_append_stub() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame({"a": [1]}))
    var df2 = DataFrame(pd.DataFrame({"a": [2]}))
    var raised = False
    try:
        _ = df1.append(df2)
    except:
        raised = True
    assert_true(raised)


def main() raises:
    test_merge_stub()
    test_join_stub()
    test_append_stub()
    print("test_combining: all tests passed")
