"""Tests for from_pandas / to_pandas interop (these work at stub stage)."""
from python import Python, PythonObject
from testing import assert_equal, assert_true
from bison import DataFrame, Series


def test_df_from_pandas_preserves_shape():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]}"))
    var df = DataFrame.from_pandas(pd_df)
    assert_equal(df.shape()[0], 3)
    assert_equal(df.shape()[1], 3)


def test_df_to_pandas_identity():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'x': [10, 20]}"))
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    var testing = Python.import_module("pandas.testing")
    testing.assert_frame_equal(pd_df, back)


def test_series_from_pandas_preserves_name():
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1, 2, 3]"), name="score")
    var s = Series.from_pandas(pd_s)
    assert_equal(s.name, "score")
    assert_equal(s.__len__(), 3)


def test_series_to_pandas_identity():
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[5, 6, 7]"), name="v")
    var s = Series.from_pandas(pd_s)
    var back = s.to_pandas()
    var testing = Python.import_module("pandas.testing")
    testing.assert_series_equal(pd_s, back)


def test_df_columns_match():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'alpha': [1], 'beta': [2], 'gamma': [3]}"))
    var df = DataFrame.from_pandas(pd_df)
    var cols = df.columns()
    assert_equal(cols[0], "alpha")
    assert_equal(cols[1], "beta")
    assert_equal(cols[2], "gamma")


def main():
    test_df_from_pandas_preserves_shape()
    test_df_to_pandas_identity()
    test_series_from_pandas_preserves_name()
    test_series_to_pandas_identity()
    test_df_columns_match()
    print("test_interop: all tests passed")
