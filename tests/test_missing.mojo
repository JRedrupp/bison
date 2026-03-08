"""Tests that missing-data stubs raise 'not implemented'."""
from python import Python, PythonObject
from testing import assert_true
from bison import DataFrame, Series


def test_df_isna_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.isna()
    except:
        raised = True
    assert_true(raised)


def test_df_fillna_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.fillna(PythonObject(0))
    except:
        raised = True
    assert_true(raised)


def test_df_dropna_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.dropna()
    except:
        raised = True
    assert_true(raised)


def test_df_ffill_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.ffill()
    except:
        raised = True
    assert_true(raised)


def test_series_fillna_stub():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0]")))
    var raised = False
    try:
        _ = s.fillna(PythonObject(0))
    except:
        raised = True
    assert_true(raised)


def main():
    test_df_isna_stub()
    test_df_fillna_stub()
    test_df_dropna_stub()
    test_df_ffill_stub()
    test_series_fillna_stub()
    print("test_missing: all tests passed")
