"""Tests that groupby stubs raise 'not implemented'."""
from python import Python, PythonObject
from testing import assert_true, TestSuite
from bison import DataFrame


def test_groupby_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'grp': ['a', 'a', 'b'], 'val': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.groupby(PythonObject("grp"))
    except:
        raised = True
    assert_true(raised)


def test_groupby_sum_stub():
    """DataFrameGroupBy.sum is also a stub."""
    from bison import DataFrameGroupBy
    var pd = Python.import_module("pandas")
    var pd_gb = pd.DataFrame(Python.evaluate("{'a': [1]}")).groupby("a")
    var gb = DataFrameGroupBy(pd_gb)
    var raised = False
    try:
        _ = gb.sum()
    except:
        raised = True
    assert_true(raised)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
