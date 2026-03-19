"""Tests for DataFrame structural operations (issue #9):
copy, round, astype, clip, insert, pop, assign, drop, where, mask,
duplicated, drop_duplicates, isin, combine_first, update.
"""
from std.python import Python
from std.collections import Dict, Optional
from testing import assert_equal, assert_true, assert_false, TestSuite
from bison import DataFrame, Series, ColumnData, DFScalar


# ------------------------------------------------------------------
# copy
# ------------------------------------------------------------------

fn test_copy_returns_equal_shape() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}")))
    var c = df.copy(True)
    assert_equal(c.shape()[0], 3)
    assert_equal(c.shape()[1], 2)


fn test_copy_values_match() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20]}")))
    var c = df.copy(True)
    assert_true(c["a"].iloc(0)[Int64] == 10)
    assert_true(c["a"].iloc(1)[Int64] == 20)


fn test_copy_is_independent() raises:
    # Modifying the copy must not change the original.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var c = df.copy(True)
    var new_s = Series(pd.Series(Python.evaluate("[99, 99]"), dtype="int64"))
    c["a"] = new_s^
    # original still has its own values
    assert_true(df["a"].iloc(0)[Int64] == 1)


fn test_copy_shallow_raises() raises:
    # copy(deep=False) must raise because shallow copy is not yet supported.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var raised = False
    try:
        _ = df.copy(False)
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# round
# ------------------------------------------------------------------

fn test_round_float() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.234, 5.678]}")))
    var r = df.round(2)
    assert_true(r["a"].iloc(0)[Float64] == 1.23)
    assert_true(r["a"].iloc(1)[Float64] == 5.68)


fn test_round_int_identity() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var r = df.round(0)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(2)[Int64] == 3)


# ------------------------------------------------------------------
# astype
# ------------------------------------------------------------------

fn test_astype_int_to_float() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var r = df.astype("float64")
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(2)[Float64] == 3.0)


fn test_astype_float_to_int() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.9, 2.1]}")))
    var r = df.astype("int64")
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)


# ------------------------------------------------------------------
# clip
# ------------------------------------------------------------------

fn test_clip_both_bounds() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 5.0, 10.0]}")))
    var r = df.clip(lower=2.0, upper=8.0)
    assert_true(r["a"].iloc(0)[Float64] == 2.0)
    assert_true(r["a"].iloc(1)[Float64] == 5.0)
    assert_true(r["a"].iloc(2)[Float64] == 8.0)


fn test_clip_lower_only() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 5.0, 10.0]}")))
    var r = df.clip(lower=3.0)
    assert_true(r["a"].iloc(0)[Float64] == 3.0)
    assert_true(r["a"].iloc(1)[Float64] == 5.0)
    assert_true(r["a"].iloc(2)[Float64] == 10.0)


fn test_clip_upper_only() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 5.0, 10.0]}")))
    var r = df.clip(upper=7.0)
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(1)[Float64] == 5.0)
    assert_true(r["a"].iloc(2)[Float64] == 7.0)


fn test_clip_no_bounds_returns_copy() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var r = df.clip()
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(1)[Float64] == 2.0)


# ------------------------------------------------------------------
# insert
# ------------------------------------------------------------------

fn test_insert_at_front() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'b': [1, 2]}")))
    var v = DFScalar(Int64(99))
    df.insert(0, "a", v^)
    assert_equal(df.shape()[1], 2)
    var cols = df.columns()
    assert_equal(cols[0], "a")
    assert_equal(cols[1], "b")
    assert_true(df["a"].iloc(0)[Int64] == 99)
    assert_true(df["a"].iloc(1)[Int64] == 99)


fn test_insert_at_end() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var v = DFScalar(Float64(3.0))
    df.insert(10, "z", v^)
    assert_equal(df.shape()[1], 2)
    var cols = df.columns()
    assert_equal(cols[1], "z")


fn test_insert_duplicate_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var v = DFScalar(Int64(0))
    var raised = False
    try:
        df.insert(0, "a", v^)
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# pop
# ------------------------------------------------------------------

fn test_pop_returns_series() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var s = df.pop("a")
    assert_true(s.iloc(0)[Int64] == 1)
    assert_true(s.iloc(1)[Int64] == 2)


fn test_pop_removes_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    _ = df.pop("a")
    assert_equal(df.shape()[1], 1)
    assert_false(df.__contains__("a"))
    assert_true(df.__contains__("b"))


fn test_pop_missing_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var raised = False
    try:
        _ = df.pop("x")
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# assign
# ------------------------------------------------------------------

fn test_assign_adds_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_s = Series(pd.Series(Python.evaluate("[10, 20]"), dtype="int64"))
    var cols = Dict[String, Series]()
    cols["b"] = new_s^
    var r = df.assign(cols^)
    assert_equal(r.shape()[1], 2)
    assert_true(r["b"].iloc(0)[Int64] == 10)
    assert_true(r["b"].iloc(1)[Int64] == 20)


fn test_assign_replaces_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_s = Series(pd.Series(Python.evaluate("[99, 99]"), dtype="int64"))
    var cols = Dict[String, Series]()
    cols["a"] = new_s^
    var r = df.assign(cols^)
    assert_equal(r.shape()[1], 1)
    assert_true(r["a"].iloc(0)[Int64] == 99)


fn test_assign_does_not_mutate_original() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_s = Series(pd.Series(Python.evaluate("[10, 20]"), dtype="int64"))
    var cols = Dict[String, Series]()
    cols["b"] = new_s^
    _ = df.assign(cols^)
    assert_equal(df.shape()[1], 1)


# ------------------------------------------------------------------
# drop (columns)
# ------------------------------------------------------------------

fn test_drop_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var labels = List[String]()
    labels.append("a")
    var r = df.drop(columns=labels^)
    assert_equal(r.shape()[1], 1)
    assert_false(r.__contains__("a"))
    assert_true(r.__contains__("b"))


fn test_drop_column_axis1() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2], 'c': [3]}")))
    var labels = List[String]()
    labels.append("b")
    var r = df.drop(labels=labels^, axis=1)
    assert_equal(r.shape()[1], 2)
    assert_false(r.__contains__("b"))


fn test_drop_missing_column_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var labels = List[String]()
    labels.append("z")
    var raised = False
    try:
        _ = df.drop(columns=labels^)
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# drop (rows, axis=0)
# ------------------------------------------------------------------

fn test_drop_rows_by_integer_label() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20, 30]}")))
    var labels = List[String]()
    labels.append("1")
    var r = df.drop(labels=labels^, axis=0)
    assert_equal(r.shape()[0], 2)
    assert_true(r["a"].iloc(0)[Int64] == 10)
    assert_true(r["a"].iloc(1)[Int64] == 30)


# ------------------------------------------------------------------
# where / mask
# ------------------------------------------------------------------

fn test_where_keeps_true_positions() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var cond = Series(pd.Series(Python.evaluate("[True, False, True]")))
    var r = df.where(cond)
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].isna().iloc(1)[Bool])
    assert_true(r["a"].iloc(2)[Float64] == 3.0)


fn test_mask_nulls_true_positions() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var cond = Series(pd.Series(Python.evaluate("[True, False, True]")))
    var r = df.mask(cond)
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].iloc(1)[Float64] == 2.0)
    assert_true(r["a"].isna().iloc(2)[Bool])


fn test_where_other_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var cond = Series(pd.Series(Python.evaluate("[True, False]")))
    var other = Optional[DFScalar](DFScalar(Float64(0.0)))
    var raised = False
    try:
        _ = df.where(cond, other)
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# duplicated
# ------------------------------------------------------------------

fn test_duplicated_keep_first() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2]}")))
    var d = df.duplicated()
    assert_false(d.iloc(0)[Bool])
    assert_true(d.iloc(1)[Bool])
    assert_false(d.iloc(2)[Bool])


fn test_duplicated_keep_last() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2]}")))
    var d = df.duplicated(keep="last")
    assert_true(d.iloc(0)[Bool])
    assert_false(d.iloc(1)[Bool])
    assert_false(d.iloc(2)[Bool])


fn test_duplicated_keep_false() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2]}")))
    var d = df.duplicated(keep="False")
    assert_true(d.iloc(0)[Bool])
    assert_true(d.iloc(1)[Bool])
    assert_false(d.iloc(2)[Bool])


fn test_duplicated_subset() raises:
    # Rows differ in 'b' but not in 'a'; subset=['a'] treats them as duplicates.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1], 'b': [10, 20]}")))
    var sub = List[String]()
    sub.append("a")
    var d = df.duplicated(subset=sub^)
    assert_false(d.iloc(0)[Bool])
    assert_true(d.iloc(1)[Bool])


# ------------------------------------------------------------------
# drop_duplicates
# ------------------------------------------------------------------

fn test_drop_duplicates_keep_first() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2]}")))
    var r = df.drop_duplicates()
    assert_equal(r.shape()[0], 2)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)


fn test_drop_duplicates_keep_last() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2]}")))
    var r = df.drop_duplicates(keep="last")
    assert_equal(r.shape()[0], 2)


fn test_drop_duplicates_no_duplicates() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var r = df.drop_duplicates()
    assert_equal(r.shape()[0], 3)


# ------------------------------------------------------------------
# isin
# ------------------------------------------------------------------

fn test_isin_int_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var vals = List[DFScalar]()
    vals.append(DFScalar(Int64(1)))
    vals.append(DFScalar(Int64(3)))
    var values = Dict[String, List[DFScalar]]()
    values["a"] = vals^
    var r = df.isin(values^)
    assert_true(r["a"].iloc(0)[Bool])
    assert_false(r["a"].iloc(1)[Bool])
    assert_true(r["a"].iloc(2)[Bool])


fn test_isin_column_not_in_dict_is_all_false() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var vals = List[DFScalar]()
    vals.append(DFScalar(Int64(1)))
    var values = Dict[String, List[DFScalar]]()
    values["a"] = vals^
    var r = df.isin(values^)
    # 'b' is not in the dict, so all False
    assert_false(r["b"].iloc(0)[Bool])
    assert_false(r["b"].iloc(1)[Bool])


# ------------------------------------------------------------------
# combine_first
# ------------------------------------------------------------------

fn test_combine_first_fills_nulls() raises:
    # self has nulls that other fills.
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10.0, 20.0, 30.0]}")))
    var r = df1.combine_first(df2)
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(1)[Float64] == 20.0)
    assert_true(r["a"].iloc(2)[Float64] == 3.0)


fn test_combine_first_appends_other_only_columns() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'b': [10, 20]}")))
    var r = df1.combine_first(df2)
    assert_equal(r.shape()[1], 2)
    assert_true(r.__contains__("a"))
    assert_true(r.__contains__("b"))


fn test_combine_first_self_wins_non_null() raises:
    # Where self is non-null, self's value is kept.
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [99.0, 99.0]}")))
    var r = df1.combine_first(df2)
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(1)[Float64] == 2.0)


# ------------------------------------------------------------------
# update
# ------------------------------------------------------------------

fn test_update_other_wins_non_null() raises:
    # other has non-null value; self gets overwritten.
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [99.0, 20.0]}")))
    df1.update(df2)
    # Row 0: df2 is non-null (99) → df1 gets 99.
    assert_true(df1["a"].iloc(0)[Float64] == 99.0)
    # Row 1: df1 was null, df2 is 20 → df1 gets 20.
    assert_true(df1["a"].iloc(1)[Float64] == 20.0)


fn test_update_ignores_other_only_columns() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'b': [99]}")))
    df1.update(df2)
    assert_equal(df1.shape()[1], 1)
    assert_false(df1.__contains__("b"))


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
