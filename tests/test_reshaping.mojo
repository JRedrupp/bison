"""Tests for reshaping operations."""
from std.python import Python, PythonObject
from std.collections import Dict, Optional
from testing import assert_true, assert_equal, TestSuite
from bison import DataFrame, Series, SeriesScalar, DFScalar


fn test_sort_values_ascending_int() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)
    assert_true(r["a"].iloc(2)[Int64] == 3)


fn test_sort_values_descending_int() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by, ascending=False)
    assert_true(r["a"].iloc(0)[Int64] == 3)
    assert_true(r["a"].iloc(1)[Int64] == 2)
    assert_true(r["a"].iloc(2)[Int64] == 1)


fn test_sort_values_multi_col() raises:
    # Sort by ['a', 'b']: primary key 'a', secondary key 'b'.
    # Input:  a=[1,1,2], b=[3,1,2]
    # Expected order: (1,1), (1,3), (2,2) → rows 1, 0, 2
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2], 'b': [3, 1, 2]}")))
    var by = List[String]()
    by.append("a")
    by.append("b")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["b"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 1)
    assert_true(r["b"].iloc(1)[Int64] == 3)
    assert_true(r["a"].iloc(2)[Int64] == 2)
    assert_true(r["b"].iloc(2)[Int64] == 2)


fn test_sort_values_null_last() raises:
    # Null should always appear at the end regardless of direction.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, None, 1]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(1)[Float64] == 3.0)
    assert_true(r["a"].isnull().iloc(2)[Bool] == True)


fn test_sort_values_preserves_columns() raises:
    # All columns must be reordered together, not just the sort key.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2], 'b': [30, 10, 20]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["b"].iloc(0)[Int64] == 10)
    assert_true(r["a"].iloc(1)[Int64] == 2)
    assert_true(r["b"].iloc(1)[Int64] == 20)
    assert_true(r["a"].iloc(2)[Int64] == 3)
    assert_true(r["b"].iloc(2)[Int64] == 30)


fn test_sort_index_ascending_default() raises:
    # Ascending sort on a default integer index is a no-op.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var r = df.sort_index()
    assert_true(r["a"].iloc(0)[Int64] == 3)
    assert_true(r["a"].iloc(1)[Int64] == 1)
    assert_true(r["a"].iloc(2)[Int64] == 2)


fn test_sort_index_descending_default() raises:
    # Descending sort on a default integer index reverses the rows.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var r = df.sort_index(ascending=False)
    assert_true(r["a"].iloc(0)[Int64] == 2)
    assert_true(r["a"].iloc(1)[Int64] == 1)
    assert_true(r["a"].iloc(2)[Int64] == 3)


fn test_sort_index_axis1_ascending() raises:
    # axis=1 sorts column labels lexicographically (ascending).
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'c': [1, 2], 'a': [3, 4], 'b': [5, 6]}")))
    var r = df.sort_index(axis=1)
    var cols = r.columns()
    assert_equal(cols[0], "a")
    assert_equal(cols[1], "b")
    assert_equal(cols[2], "c")
    # Data must follow the reordered columns.
    assert_true(r["a"].iloc(0)[Int64] == 3)
    assert_true(r["b"].iloc(0)[Int64] == 5)
    assert_true(r["c"].iloc(0)[Int64] == 1)


fn test_sort_index_axis1_descending() raises:
    # axis=1 sorts column labels lexicographically (descending).
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'c': [2], 'b': [3]}")))
    var r = df.sort_index(axis=1, ascending=False)
    var cols = r.columns()
    assert_equal(cols[0], "c")
    assert_equal(cols[1], "b")
    assert_equal(cols[2], "a")


fn test_sort_index_axis1_already_sorted() raises:
    # No-op when columns are already in ascending order.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2], 'c': [3]}")))
    var r = df.sort_index(axis=1)
    var cols = r.columns()
    assert_equal(cols[0], "a")
    assert_equal(cols[1], "b")
    assert_equal(cols[2], "c")


fn test_sort_index_axis1_single_column() raises:
    # Single-column DataFrame is unchanged.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'z': [10, 20]}")))
    var r = df.sort_index(axis=1)
    var cols = r.columns()
    assert_equal(cols[0], "z")
    assert_true(r["z"].iloc(0)[Int64] == 10)


fn test_pivot_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2], 'c': [3]}")))
    var raised = False
    try:
        _ = df.pivot(index="a", columns="b", values="c")
    except:
        raised = True
    assert_true(raised)


fn test_melt_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'id': [1], 'val': [10]}")))
    var raised = False
    try:
        _ = df.melt()
    except:
        raised = True
    assert_true(raised)


fn test_transpose_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var raised = False
    try:
        _ = df.transpose()
    except:
        raised = True
    assert_true(raised)


fn test_drop_duplicates_removes_duplicates() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2], 'b': [10, 10, 20]}")))
    var r = df.drop_duplicates()
    assert_true(r.shape()[0] == 2)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)


fn test_series_sort_values() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]"), dtype="int64"))
    var r = s.sort_values()
    assert_true(r.iloc(0)[Int64] == 1)
    assert_true(r.iloc(1)[Int64] == 2)
    assert_true(r.iloc(2)[Int64] == 3)


# ------------------------------------------------------------------
# rename
# ------------------------------------------------------------------

fn test_rename_columns() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var cols_map = Dict[String, String]()
    cols_map["a"] = "x"
    var r = df.rename(columns=Optional[Dict[String, String]](cols_map^))
    assert_equal(r.shape()[1], 1)
    assert_true(r["x"].iloc(0)[Int64] == 1)
    assert_true(r["x"].iloc(1)[Int64] == 2)


fn test_rename_columns_partial() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var cols_map = Dict[String, String]()
    cols_map["a"] = "x"
    var r = df.rename(columns=Optional[Dict[String, String]](cols_map^))
    assert_equal(r.shape()[1], 2)
    assert_true(r["x"].iloc(0)[Int64] == 1)
    assert_true(r["b"].iloc(0)[Int64] == 2)


fn test_rename_index() raises:
    # Rename index label 'i' → 'z'; sort_index ascending gives j, k, z.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20, 30]}"), index=Python.evaluate("['i', 'j', 'k']")))
    var idx_map = Dict[String, String]()
    idx_map["i"] = "z"
    var r = df.rename(index=Optional[Dict[String, String]](idx_map^))
    assert_equal(r.shape()[0], 3)
    var s = r.sort_index()
    # Sorted order: 'j'=20, 'k'=30, 'z'=10 (formerly 'i')
    assert_true(s["a"].iloc(0)[Int64] == 20)
    assert_true(s["a"].iloc(2)[Int64] == 10)


# ------------------------------------------------------------------
# reset_index
# ------------------------------------------------------------------

fn test_reset_index_drop_true() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}"), index=Python.evaluate("['x', 'y']")))
    var r = df.reset_index(drop=True)
    assert_equal(r.shape()[0], 2)
    assert_equal(r.shape()[1], 1)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)


fn test_reset_index_drop_false() raises:
    # Index promoted to a new "index" column prepended to the result.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}"), index=Python.evaluate("['x', 'y']")))
    var r = df.reset_index()  # drop=False by default
    assert_equal(r.shape()[0], 2)
    assert_equal(r.shape()[1], 2)  # "index" column + "a" column
    assert_true(r["a"].iloc(0)[Int64] == 1)


fn test_reset_index_range_index() raises:
    # On a default RangeIndex, reset_index is a copy.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var r = df.reset_index(drop=True)
    assert_equal(r.shape()[0], 3)
    assert_equal(r.shape()[1], 1)
    assert_true(r["a"].iloc(0)[Int64] == 1)


# ------------------------------------------------------------------
# set_index
# ------------------------------------------------------------------

fn test_set_index_single_drop_true() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': ['x', 'y'], 'b': [10, 20]}")))
    var keys = List[String]()
    keys.append("a")
    var r = df.set_index(keys)  # drop=True by default
    assert_equal(r.shape()[1], 1)  # "a" removed, only "b" remains
    assert_true(r["b"].iloc(0)[Int64] == 10)
    assert_true(r["b"].iloc(1)[Int64] == 20)


fn test_set_index_single_drop_false() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': ['x', 'y'], 'b': [10, 20]}")))
    var keys = List[String]()
    keys.append("a")
    var r = df.set_index(keys, drop=False)
    assert_equal(r.shape()[1], 2)  # "a" kept as column and as index
    assert_true(r["b"].iloc(0)[Int64] == 10)


fn test_set_index_multi_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var keys = List[String]()
    keys.append("a")
    keys.append("b")
    var raised = False
    try:
        _ = df.set_index(keys)
    except:
        raised = True
    assert_true(raised)


# ------------------------------------------------------------------
# rename_axis
# ------------------------------------------------------------------

fn test_rename_axis_is_copy() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var r = df.rename_axis(mapper=Optional[String]("rows"))
    assert_equal(r.shape()[0], 2)
    assert_equal(r.shape()[1], 2)
    assert_true(r["a"].iloc(0)[Int64] == 1)


# ------------------------------------------------------------------
# reindex
# ------------------------------------------------------------------

fn test_reindex_axis1_reorder() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2], 'c': [3]}")))
    var lbls = List[String]()
    lbls.append("c")
    lbls.append("a")
    lbls.append("b")
    var r = df.reindex(labels=Optional[List[String]](lbls^), axis=1)
    assert_equal(r.shape()[1], 3)
    assert_true(r["c"].iloc(0)[Int64] == 3)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["b"].iloc(0)[Int64] == 2)


fn test_reindex_axis1_fill() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var lbls = List[String]()
    lbls.append("a")
    lbls.append("x")
    var fv = Optional[DFScalar](DFScalar(Int64(99)))
    var r = df.reindex(labels=Optional[List[String]](lbls^), axis=1, fill_value=fv)
    assert_equal(r.shape()[1], 2)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["x"].iloc(0)[Int64] == 99)


fn test_reindex_axis1_null_fill() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var lbls = List[String]()
    lbls.append("a")
    lbls.append("x")
    var r = df.reindex(labels=Optional[List[String]](lbls^), axis=1)
    assert_equal(r.shape()[1], 2)
    assert_equal(r.shape()[0], 2)


fn test_reindex_axis0_reorder() raises:
    # index=['c', 'a', 'b'], values=[10, 20, 30]. Reindex to ['a', 'b', 'c'].
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20, 30]}"), index=Python.evaluate("['c', 'a', 'b']")))
    var lbls = List[String]()
    lbls.append("a")
    lbls.append("b")
    lbls.append("c")
    var r = df.reindex(labels=Optional[List[String]](lbls^))  # axis=0 default
    assert_equal(r.shape()[0], 3)
    assert_true(r["a"].iloc(0)[Int64] == 20)  # row 'a' had value 20
    assert_true(r["a"].iloc(1)[Int64] == 30)  # row 'b' had value 30
    assert_true(r["a"].iloc(2)[Int64] == 10)  # row 'c' had value 10


fn test_reindex_axis0_fill() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10]}"), index=Python.evaluate("['x']")))
    var lbls = List[String]()
    lbls.append("x")
    lbls.append("z")
    var fv = Optional[DFScalar](DFScalar(Int64(0)))
    var r = df.reindex(labels=Optional[List[String]](lbls^), fill_value=fv)
    assert_equal(r.shape()[0], 2)
    assert_true(r["a"].iloc(0)[Int64] == 10)
    assert_true(r["a"].iloc(1)[Int64] == 0)


fn test_reindex_axis0_obj_fill_value() raises:
    # Object-dtype column with axis=0 reindex and fill_value: bug fix for
    # _ReindexRowsVisitor.on_obj ignoring fill_value.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': ['x', 'y']}"), index=Python.evaluate("['r0', 'r1']")))
    var lbls = List[String]()
    lbls.append("r0")
    lbls.append("r2")  # new row — should get fill_value
    var fv = Optional[DFScalar](DFScalar(String("FILL")))
    var r = df.reindex(labels=Optional[List[String]](lbls^), fill_value=fv)
    assert_equal(r.shape()[0], 2)
    # existing row preserved
    assert_true(String(r["a"].iloc(0)[PythonObject]) == "x")
    # missing row gets fill_value, not None
    assert_true(String(r["a"].iloc(1)[PythonObject]) == "FILL")


fn test_reindex_axis0_obj_null_propagation() raises:
    # Object-dtype column: existing null rows should propagate through reindex.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [None, 'y']}"), index=Python.evaluate("['r0', 'r1']")))
    var lbls = List[String]()
    lbls.append("r1")
    lbls.append("r0")  # was null in source
    var r = df.reindex(labels=Optional[List[String]](lbls^))
    assert_equal(r.shape()[0], 2)
    # non-null row
    assert_true(String(r["a"].iloc(0)[PythonObject]) == "y")
    # row that was null in the source should still be null
    assert_true(r["a"].isna().iloc(1)[Bool] == True)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
