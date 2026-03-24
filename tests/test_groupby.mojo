"""Tests that groupby stubs raise 'not implemented'."""
from std.python import Python
from std.testing import assert_true, TestSuite
from bison import DataFrame, DataFrameGroupBy


def test_groupby_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'grp': ['a', 'a', 'b'], 'val': [1, 2, 3]}")))
    var raised = False
    try:
        var by = List[String]()
        by.append("grp")
        _ = df.groupby(by^)
    except:
        raised = True
    assert_true(raised)


def test_groupby_sum_stub() raises:
    """DataFrameGroupBy.sum is also a stub."""
    var gb = DataFrameGroupBy()
    var raised = False
    try:
        _ = gb.sum()
    except:
        raised = True
    assert_true(raised)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
