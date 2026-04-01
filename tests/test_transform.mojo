"""Tests transformation methods: shift, diff, pct_change."""
from std.python import Python
from std.testing import assert_true, TestSuite
from bison import DataFrame, Series


# ---------------------------------------------------------------------------
# shift — implemented
# ---------------------------------------------------------------------------

def test_series_shift() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var r = s.shift()
    assert_true(r.isna().iloc(0)[Bool])
    assert_true(r.iloc(1)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 2.0)


def test_df_shift_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var raised = False
    try:
        _ = df.shift()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.shift should have raised")


# ---------------------------------------------------------------------------
# diff — implemented
# ---------------------------------------------------------------------------

def test_series_diff() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var r = s.diff()
    assert_true(r.isna().iloc(0)[Bool])
    assert_true(r.iloc(1)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 1.0)


def test_df_diff_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var raised = False
    try:
        _ = df.diff()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.diff should have raised")


# ---------------------------------------------------------------------------
# pct_change — implemented
# ---------------------------------------------------------------------------

def test_series_pct_change() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 4.0]")))
    var r = s.pct_change()
    assert_true(r.isna().iloc(0)[Bool])
    assert_true(r.iloc(1)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 1.0)


def test_df_pct_change_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 4.0]}")))
    var raised = False
    try:
        _ = df.pct_change()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.pct_change should have raised")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
