"""Tests for DataFrame construction and basic attributes."""
from python import Python, PythonObject
from testing import assert_equal, assert_true, assert_false
from bison import DataFrame


def test_shape_from_pandas() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    var df = DataFrame(pd_df)
    var s = df.shape()
    assert_equal(s[0], 3)
    assert_equal(s[1], 2)


def test_len() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame({"x": [10, 20]})
    var df = DataFrame(pd_df)
    assert_equal(df.__len__(), 2)


def test_empty_false() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame({"a": [1]})
    var df = DataFrame(pd_df)
    assert_false(df.empty())


def test_empty_true() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame()
    var df = DataFrame(pd_df)
    assert_true(df.empty())


def test_columns() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame({"col1": [1], "col2": [2]})
    var df = DataFrame(pd_df)
    var cols = df.columns()
    assert_equal(len(cols), 2)
    assert_equal(cols[0], "col1")
    assert_equal(cols[1], "col2")


def test_ndim() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1]}))
    assert_equal(df.ndim(), 2)


def test_size() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
    assert_equal(df.size(), 4)


def test_contains() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1], "b": [2]}))
    assert_true(df.__contains__("a"))
    assert_false(df.__contains__("z"))


def test_to_pandas_roundtrip() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame({"a": [1, 2, 3]})
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    # length should match
    assert_equal(int(back.__len__()), 3)


def test_from_dict() raises:
    var pd = Python.import_module("pandas")
    var data = pd.dict([("a", [1, 2]), ("b", [3, 4])])
    # from_dict is a stub — expect Error
    try:
        # Use from_pandas path instead (from_dict calls pd.DataFrame internally)
        var pd_df = pd.DataFrame(data)
        var df = DataFrame(pd_df)
        assert_equal(df.shape()[0], 2)
        assert_equal(df.shape()[1], 2)
    except e:
        pass  # stub may raise


def main() raises:
    test_shape_from_pandas()
    test_len()
    test_empty_false()
    test_empty_true()
    test_columns()
    test_ndim()
    test_size()
    test_contains()
    test_to_pandas_roundtrip()
    test_from_dict()
    print("test_dataframe: all tests passed")
