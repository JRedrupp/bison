"""Tests that combining stubs raise 'not implemented'."""
from python import Python, PythonObject
from testing import assert_true, TestSuite
from bison import DataFrame


fn test_merge_stub() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'a': [10, 20]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'b': [30, 40]}")))
    var raised = False
    try:
        _ = left.merge(right, on=PythonObject("key"))
    except:
        raised = True
    assert_true(raised)


fn test_join_stub() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var raised = False
    try:
        _ = left.join(right)
    except:
        raised = True
    assert_true(raised)


fn test_append_stub() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [2]}")))
    var raised = False
    try:
        _ = df1.append(df2)
    except:
        raised = True
    assert_true(raised)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
