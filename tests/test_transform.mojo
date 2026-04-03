"""Tests transformation methods: shift, diff, pct_change."""
from std.python import Python
from std.testing import assert_true, TestSuite
from bison import DataFrame, Series


# ---------------------------------------------------------------------------
# shift
# ---------------------------------------------------------------------------

def test_series_shift() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var r = s.shift()
    assert_true(r.isna().iloc(0)[Bool])
    assert_true(r.iloc(1)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 2.0)


def test_df_shift_default() raises:
    """Shift(1) on a two-column DataFrame — matches pandas output."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [10.0, 20.0, 30.0]}"))
    )
    var r = df.shift()
    # Row 0 should be null in both columns
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["b"].isna().iloc(0)[Bool])
    # Row 1 gets original row 0
    assert_true(r["a"].iloc(1)[Float64] == 1.0)
    assert_true(r["b"].iloc(1)[Float64] == 10.0)
    # Row 2 gets original row 1
    assert_true(r["a"].iloc(2)[Float64] == 2.0)
    assert_true(r["b"].iloc(2)[Float64] == 20.0)


def test_df_shift_periods_2() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0, 4.0]}"))
    )
    var r = df.shift(2)
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].isna().iloc(1)[Bool])
    assert_true(r["a"].iloc(2)[Float64] == 1.0)
    assert_true(r["a"].iloc(3)[Float64] == 2.0)


def test_df_shift_negative() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}"))
    )
    var r = df.shift(-1)
    assert_true(r["a"].iloc(0)[Float64] == 2.0)
    assert_true(r["a"].iloc(1)[Float64] == 3.0)
    assert_true(r["a"].isna().iloc(2)[Bool])


def test_df_shift_axis1() raises:
    """Shift(axis=1) with periods=1: first column null, rest shift right."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0], 'b': [3.0, 4.0], 'c': [5.0, 6.0]}"))
    )
    var r = df.shift(1, axis=1)
    # Column 'a' (j=0, src=-1) is all null
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].isna().iloc(1)[Bool])
    # Column 'b' (j=1, src=0) gets column 'a' values
    assert_true(r["b"].iloc(0)[Float64] == 1.0)
    assert_true(r["b"].iloc(1)[Float64] == 2.0)
    # Column 'c' (j=2, src=1) gets column 'b' values
    assert_true(r["c"].iloc(0)[Float64] == 3.0)
    assert_true(r["c"].iloc(1)[Float64] == 4.0)


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------

def test_series_diff() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var r = s.diff()
    assert_true(r.isna().iloc(0)[Bool])
    assert_true(r.iloc(1)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 1.0)


def test_df_diff_default() raises:
    """Diff(1) on a two-column DataFrame — matches pandas output."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 3.0, 6.0], 'b': [10.0, 15.0, 21.0]}"))
    )
    var r = df.diff()
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["b"].isna().iloc(0)[Bool])
    assert_true(r["a"].iloc(1)[Float64] == 2.0)
    assert_true(r["b"].iloc(1)[Float64] == 5.0)
    assert_true(r["a"].iloc(2)[Float64] == 3.0)
    assert_true(r["b"].iloc(2)[Float64] == 6.0)


def test_df_diff_periods_2() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 4.0, 7.0]}"))
    )
    var r = df.diff(2)
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].isna().iloc(1)[Bool])
    assert_true(r["a"].iloc(2)[Float64] == 3.0)
    assert_true(r["a"].iloc(3)[Float64] == 5.0)


def test_df_diff_axis1() raises:
    """Diff(axis=1) with periods=1: first column null, rest = col[j] - col[j-1]."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0], 'b': [3.0, 5.0], 'c': [6.0, 9.0]}"))
    )
    var r = df.diff(1, axis=1)
    # Column 'a' (j=0, no source) is all null
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].isna().iloc(1)[Bool])
    # Column 'b' (j=1) = b - a
    assert_true(r["b"].iloc(0)[Float64] == 2.0)
    assert_true(r["b"].iloc(1)[Float64] == 3.0)
    # Column 'c' (j=2) = c - b
    assert_true(r["c"].iloc(0)[Float64] == 3.0)
    assert_true(r["c"].iloc(1)[Float64] == 4.0)


# ---------------------------------------------------------------------------
# pct_change
# ---------------------------------------------------------------------------

def test_series_pct_change() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 4.0]")))
    var r = s.pct_change()
    assert_true(r.isna().iloc(0)[Bool])
    assert_true(r.iloc(1)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 1.0)


def test_df_pct_change_default() raises:
    """Pct_change(1) on a two-column DataFrame — matches pandas output."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 4.0], 'b': [10.0, 20.0, 40.0]}"))
    )
    var r = df.pct_change()
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["b"].isna().iloc(0)[Bool])
    assert_true(r["a"].iloc(1)[Float64] == 1.0)
    assert_true(r["b"].iloc(1)[Float64] == 1.0)
    assert_true(r["a"].iloc(2)[Float64] == 1.0)
    assert_true(r["b"].iloc(2)[Float64] == 1.0)


def test_df_pct_change_periods_2() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0, 6.0]}"))
    )
    var r = df.pct_change(2)
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].isna().iloc(1)[Bool])
    assert_true(r["a"].iloc(2)[Float64] == 2.0)
    assert_true(r["a"].iloc(3)[Float64] == 2.0)


def test_df_pct_change_axis1() raises:
    """Pct_change(axis=1) with periods=1: first column null, rest = (cur-prev)/prev."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0], 'b': [2.0, 6.0], 'c': [4.0, 9.0]}"))
    )
    var r = df.pct_change(1, axis=1)
    # Column 'a' (j=0, no source) is all null
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].isna().iloc(1)[Bool])
    # Column 'b' (j=1) = (b - a) / a
    assert_true(r["b"].iloc(0)[Float64] == 1.0)
    assert_true(r["b"].iloc(1)[Float64] == 2.0)
    # Column 'c' (j=2) = (c - b) / b
    assert_true(r["c"].iloc(0)[Float64] == 1.0)
    assert_true(r["c"].iloc(1)[Float64] == 0.5)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
