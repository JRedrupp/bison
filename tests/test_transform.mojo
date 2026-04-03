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


def test_df_shift_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0], 'b': [3.0, 4.0]}"))
    )
    var raised = False
    try:
        _ = df.shift(1, axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.shift(axis=1) should have raised")


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


def test_df_diff_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0], 'b': [3.0, 4.0]}"))
    )
    var raised = False
    try:
        _ = df.diff(1, axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.diff(axis=1) should have raised")


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


def test_df_pct_change_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0], 'b': [3.0, 4.0]}"))
    )
    var raised = False
    try:
        _ = df.pct_change(1, axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.pct_change(axis=1) should have raised")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
