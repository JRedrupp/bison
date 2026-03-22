"""Tests transformation methods: shift, diff, pct_change."""
from std.python import Python
from testing import assert_true, TestSuite
from bison import DataFrame, Series


# ---------------------------------------------------------------------------
# shift — stub
# ---------------------------------------------------------------------------

def test_series_shift_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var raised = False
    try:
        _ = s.shift()
    except:
        raised = True
    if not raised:
        raise Error("Series.shift should have raised")


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
# diff — stub
# ---------------------------------------------------------------------------

def test_series_diff_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var raised = False
    try:
        _ = s.diff()
    except:
        raised = True
    if not raised:
        raise Error("Series.diff should have raised")


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
# pct_change — stub
# ---------------------------------------------------------------------------

def test_series_pct_change_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 4.0]")))
    var raised = False
    try:
        _ = s.pct_change()
    except:
        raised = True
    if not raised:
        raise Error("Series.pct_change should have raised")


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
