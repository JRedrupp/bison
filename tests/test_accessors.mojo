"""Tests that .str and .dt accessor stubs raise 'not implemented'."""
from python import Python
from testing import assert_true, TestSuite
from bison import Series


fn test_str_upper_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo', 'bar']")))
    var raised = False
    try:
        var acc = s.str()
        _ = acc.upper()
    except:
        raised = True
    assert_true(raised)


fn test_str_contains_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world']")))
    var raised = False
    try:
        var acc = s.str()
        _ = acc.contains("ell")
    except:
        raised = True
    assert_true(raised)


fn test_dt_year_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-01', '2021-06-15']"))))
    var raised = False
    try:
        var acc = s.dt()
        _ = acc.year()
    except:
        raised = True
    assert_true(raised)


fn test_dt_month_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-01']"))))
    var raised = False
    try:
        var acc = s.dt()
        _ = acc.month()
    except:
        raised = True
    assert_true(raised)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
