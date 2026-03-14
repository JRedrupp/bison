"""Tests that missing-data stubs raise 'not implemented'."""
from python import Python, PythonObject
from testing import assert_true, TestSuite
from bison import DataFrame, Series, DFScalar


fn test_df_isna_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.isna()
    except:
        raised = True
    assert_true(raised)


fn test_df_fillna_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.fillna(PythonObject(0))
    except:
        raised = True
    assert_true(raised)


fn test_df_dropna_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.dropna()
    except:
        raised = True
    assert_true(raised)


fn test_df_ffill_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.ffill()
    except:
        raised = True
    assert_true(raised)


fn test_series_fillna_works() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 2.0]")))
    var filled = s.fillna(DFScalar(Float64(0.0)))
    assert_true(filled.size() == 3)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
