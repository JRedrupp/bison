"""Tests for reshaping operations."""
from std.python import Python, PythonObject
from std.collections import Dict, Optional
from std.testing import assert_true, assert_equal, TestSuite
from bison import DataFrame, Series, SeriesScalar, DFScalar


def test_sort_values_ascending_int() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)
    assert_true(r["a"].iloc(2)[Int64] == 3)


def test_sort_values_descending_int() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by, ascending=False)
    assert_true(r["a"].iloc(0)[Int64] == 3)
    assert_true(r["a"].iloc(1)[Int64] == 2)
    assert_true(r["a"].iloc(2)[Int64] == 1)


def test_sort_values_multi_col() raises:
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


def test_sort_values_null_last() raises:
    # Null should always appear at the end regardless of direction.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, None, 1]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(1)[Float64] == 3.0)
    assert_true(r["a"].isnull().iloc(2)[Bool] == True)


def test_sort_values_preserves_columns() raises:
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


def test_sort_index_ascending_default() raises:
    # Ascending sort on a default integer index is a no-op.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var r = df.sort_index()
    assert_true(r["a"].iloc(0)[Int64] == 3)
    assert_true(r["a"].iloc(1)[Int64] == 1)
    assert_true(r["a"].iloc(2)[Int64] == 2)


def test_sort_index_descending_default() raises:
    # Descending sort on a default integer index reverses the rows.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var r = df.sort_index(ascending=False)
    assert_true(r["a"].iloc(0)[Int64] == 2)
    assert_true(r["a"].iloc(1)[Int64] == 1)
    assert_true(r["a"].iloc(2)[Int64] == 3)


def test_sort_index_axis1_ascending() raises:
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


def test_sort_index_axis1_descending() raises:
    # axis=1 sorts column labels lexicographically (descending).
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'c': [2], 'b': [3]}")))
    var r = df.sort_index(axis=1, ascending=False)
    var cols = r.columns()
    assert_equal(cols[0], "c")
    assert_equal(cols[1], "b")
    assert_equal(cols[2], "a")


def test_sort_index_axis1_already_sorted() raises:
    # No-op when columns are already in ascending order.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2], 'c': [3]}")))
    var r = df.sort_index(axis=1)
    var cols = r.columns()
    assert_equal(cols[0], "a")
    assert_equal(cols[1], "b")
    assert_equal(cols[2], "c")


def test_sort_index_axis1_single_column() raises:
    # Single-column DataFrame is unchanged.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'z': [10, 20]}")))
    var r = df.sort_index(axis=1)
    var cols = r.columns()
    assert_equal(cols[0], "z")
    assert_true(r["z"].iloc(0)[Int64] == 10)


def test_pivot_basic() raises:
    # long format: rows=date, cols=city, vals=temperature
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate(
        "{'date': ['2020-01-01', '2020-01-01', '2020-01-02', '2020-01-02'],"
        " 'city': ['NYC', 'LA', 'NYC', 'LA'],"
        " 'temp': [32, 75, 28, 70]}"
    )))
    var r = df.pivot(index="date", columns="city", values="temp")
    # Two columns: LA and NYC (insertion order).
    assert_equal(r.shape()[0], 2)  # two unique dates
    assert_equal(r.shape()[1], 2)  # two unique cities
    # NYC on 2020-01-01 = 32, on 2020-01-02 = 28
    var nyc = r["NYC"]
    assert_true(Bool(nyc.iloc(0)[PythonObject] == 32))
    assert_true(Bool(nyc.iloc(1)[PythonObject] == 28))


def test_pivot_duplicate_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate(
        "{'idx': ['a', 'a'], 'col': ['x', 'x'], 'val': [1, 2]}"
    )))
    var raised = False
    try:
        _ = df.pivot(index="idx", columns="col", values="val")
    except:
        raised = True
    assert_true(raised)


def test_melt_no_id_vars() raises:
    # All columns become value_vars when id_vars is empty.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var r = df.melt()
    # 2 cols × 2 rows = 4 rows; columns: "variable" and "value"
    assert_equal(r.shape()[0], 4)
    assert_equal(r.shape()[1], 2)


def test_melt_with_id_vars() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'id': [1, 2], 'a': [10, 20], 'b': [30, 40]}")))
    var id_v = List[String]()
    id_v.append("id")
    var r = df.melt(id_vars=Optional[List[String]](id_v^))
    # 2 value cols × 2 rows = 4 rows; 3 cols (id, variable, value)
    assert_equal(r.shape()[0], 4)
    assert_equal(r.shape()[1], 3)
    # id column retains its original int64 dtype; values are [1, 2, 1, 2]
    assert_true(r["id"].iloc(0)[Int64] == 1)
    assert_true(r["id"].iloc(1)[Int64] == 2)
    assert_true(r["id"].iloc(2)[Int64] == 1)
    assert_true(r["id"].iloc(3)[Int64] == 2)


def test_transpose_shape() raises:
    # A 3-row × 2-col DataFrame transposes to 2 rows × 3 cols.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}")))
    var r = df.transpose()
    assert_equal(r.shape()[0], 2)  # 2 rows (one per original column)
    assert_equal(r.shape()[1], 3)  # 3 columns (one per original row)


def test_transpose_values() raises:
    # Values at each position are correct.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [10, 20], 'y': [30, 40]}")))
    var r = df.transpose()
    # Original col 'x' becomes row 0 in result; col 'y' becomes row 1.
    # Original row 0 (10, 30) becomes result col '0'; row 1 (20, 40) → col '1'.
    assert_true(Bool(r["0"].iloc(0)[PythonObject] == 10))  # x, row 0
    assert_true(Bool(r["0"].iloc(1)[PythonObject] == 30))  # y, row 0
    assert_true(Bool(r["1"].iloc(0)[PythonObject] == 20))  # x, row 1
    assert_true(Bool(r["1"].iloc(1)[PythonObject] == 40))  # y, row 1


def test_T_alias() raises:
    # T() is an alias for transpose().
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var r1 = df.transpose()
    var r2 = df.T()
    assert_equal(r1.shape()[0], r2.shape()[0])
    assert_equal(r1.shape()[1], r2.shape()[1])


def test_drop_duplicates_removes_duplicates() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 1, 2], 'b': [10, 10, 20]}")))
    var r = df.drop_duplicates()
    assert_true(r.shape()[0] == 2)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)


def test_series_sort_values() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]"), dtype="int64"))
    var r = s.sort_values()
    assert_true(r.iloc(0)[Int64] == 1)
    assert_true(r.iloc(1)[Int64] == 2)
    assert_true(r.iloc(2)[Int64] == 3)


# ------------------------------------------------------------------
# rename
# ------------------------------------------------------------------

def test_rename_columns() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var cols_map = Dict[String, String]()
    cols_map["a"] = "x"
    var r = df.rename(columns=Optional[Dict[String, String]](cols_map^))
    assert_equal(r.shape()[1], 1)
    assert_true(r["x"].iloc(0)[Int64] == 1)
    assert_true(r["x"].iloc(1)[Int64] == 2)


def test_rename_columns_partial() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var cols_map = Dict[String, String]()
    cols_map["a"] = "x"
    var r = df.rename(columns=Optional[Dict[String, String]](cols_map^))
    assert_equal(r.shape()[1], 2)
    assert_true(r["x"].iloc(0)[Int64] == 1)
    assert_true(r["b"].iloc(0)[Int64] == 2)


def test_rename_index() raises:
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

def test_reset_index_drop_true() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}"), index=Python.evaluate("['x', 'y']")))
    var r = df.reset_index(drop=True)
    assert_equal(r.shape()[0], 2)
    assert_equal(r.shape()[1], 1)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)


def test_reset_index_drop_false() raises:
    # Index promoted to a new "index" column prepended to the result.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}"), index=Python.evaluate("['x', 'y']")))
    var r = df.reset_index()  # drop=False by default
    assert_equal(r.shape()[0], 2)
    assert_equal(r.shape()[1], 2)  # "index" column + "a" column
    assert_true(r["a"].iloc(0)[Int64] == 1)


def test_reset_index_range_index() raises:
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

def test_set_index_single_drop_true() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': ['x', 'y'], 'b': [10, 20]}")))
    var keys = List[String]()
    keys.append("a")
    var r = df.set_index(keys)  # drop=True by default
    assert_equal(r.shape()[1], 1)  # "a" removed, only "b" remains
    assert_true(r["b"].iloc(0)[Int64] == 10)
    assert_true(r["b"].iloc(1)[Int64] == 20)


def test_set_index_single_drop_false() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': ['x', 'y'], 'b': [10, 20]}")))
    var keys = List[String]()
    keys.append("a")
    var r = df.set_index(keys, drop=False)
    assert_equal(r.shape()[1], 2)  # "a" kept as column and as index
    assert_true(r["b"].iloc(0)[Int64] == 10)


def test_set_index_multi_drop_true() raises:
    """Multi-key set_index creates a tuple-valued MultiIndex (drop=True)."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': ['x', 'y', 'z'], 'c': [10, 20, 30]}")))
    var keys = List[String]()
    keys.append("a")
    keys.append("b")
    var r = df.set_index(keys)
    # 'a' and 'b' are dropped; only 'c' remains.
    assert_equal(r.shape()[1], 1)
    assert_true(r["c"].iloc(0)[Int64] == 10)
    assert_true(r["c"].iloc(1)[Int64] == 20)
    assert_true(r["c"].iloc(2)[Int64] == 30)
    # Index entries must be Python tuples (1, 'x'), (2, 'y'), (3, 'z').
    var pd_series = r["c"].to_pandas()
    assert_true(Bool(pd_series.index[0] == pd.Index(Python.evaluate("[(1, 'x')]"))[0]))
    assert_true(Bool(pd_series.index[1] == pd.Index(Python.evaluate("[(2, 'y')]"))[0]))
    assert_true(Bool(pd_series.index[2] == pd.Index(Python.evaluate("[(3, 'z')]"))[0]))


def test_set_index_multi_drop_false() raises:
    """Multi-key set_index with drop=False keeps key columns in result."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': ['x', 'y'], 'c': [10, 20]}")))
    var keys = List[String]()
    keys.append("a")
    keys.append("b")
    var r = df.set_index(keys, drop=False)
    # 'a', 'b', and 'c' all remain.
    assert_equal(r.shape()[1], 3)
    assert_true(r["c"].iloc(0)[Int64] == 10)


def test_set_index_multi_sort_index() raises:
    """Sort_index on a MultiIndex DataFrame sorts rows by tuple comparison."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [2, 1, 1], 'b': [3, 4, 2], 'c': [100, 200, 300]}")))
    var keys = List[String]()
    keys.append("a")
    keys.append("b")
    var indexed = df.set_index(keys)
    var r = indexed.sort_index()
    # Sorted order by tuple: (1,2), (1,4), (2,3) → rows 2, 1, 0.
    assert_true(r["c"].iloc(0)[Int64] == 300)
    assert_true(r["c"].iloc(1)[Int64] == 200)
    assert_true(r["c"].iloc(2)[Int64] == 100)


def test_set_index_multi_reset_index_drop_true() raises:
    """Reset_index(drop=True) on a MultiIndex clears the index."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': ['x', 'y'], 'c': [10, 20]}")))
    var keys = List[String]()
    keys.append("a")
    keys.append("b")
    var indexed = df.set_index(keys)
    var r = indexed.reset_index(drop=True)
    # Only 'c' column, default RangeIndex.
    assert_equal(r.shape()[1], 1)
    assert_true(r["c"].iloc(0)[Int64] == 10)


def test_set_index_multi_reset_index_drop_false() raises:
    """Reset_index(drop=False) on a MultiIndex expands tuples back to columns."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': ['x', 'y'], 'c': [10, 20]}")))
    var keys = List[String]()
    keys.append("a")
    keys.append("b")
    var indexed = df.set_index(keys)
    var r = indexed.reset_index()
    # Should have 'a', 'b', 'c' — three columns.
    assert_equal(r.shape()[1], 3)
    # Values from the expanded index columns match originals.
    var pd_a = r["a"].to_pandas()
    assert_true(Bool(pd_a.iloc[0] == 1))
    assert_true(Bool(pd_a.iloc[1] == 2))
    var pd_c = r["c"].to_pandas()
    assert_true(Bool(pd_c.iloc[0] == 10))
    assert_true(Bool(pd_c.iloc[1] == 20))


def test_set_index_null_key() raises:
    """Null entries in the key column must become NaN/None in the resulting index."""
    var pd = Python.import_module("pandas")
    # "a" is a float column with a null at position 1.
    var py_df = pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0], 'b': [10, 20, 30]}"))
    var df = DataFrame(py_df)
    var keys = List[String]()
    keys.append("a")
    var r = df.set_index(keys)
    # Row 1 index entry must be None/NaN, not the typed placeholder 0.0.
    var pd_series = r["b"].to_pandas()
    assert_true(Bool(pd.isna(pd_series.index[1])))
    # Non-null rows must retain their original values.
    assert_true(Bool(pd_series.index[0] == 1.0))
    assert_true(Bool(pd_series.index[2] == 3.0))


# ------------------------------------------------------------------
# rename_axis
# ------------------------------------------------------------------

def test_rename_axis_is_copy() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var r = df.rename_axis(mapper=Optional[String]("rows"))
    assert_equal(r.shape()[0], 2)
    assert_equal(r.shape()[1], 2)
    assert_true(r["a"].iloc(0)[Int64] == 1)


# ------------------------------------------------------------------
# reindex
# ------------------------------------------------------------------

def test_reindex_axis1_reorder() raises:
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


def test_reindex_axis1_fill() raises:
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


def test_reindex_axis1_null_fill() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var lbls = List[String]()
    lbls.append("a")
    lbls.append("x")
    var r = df.reindex(labels=Optional[List[String]](lbls^), axis=1)
    assert_equal(r.shape()[1], 2)
    assert_equal(r.shape()[0], 2)


def test_reindex_axis1_null_fill_dtype_inferred() raises:
    # Missing columns should get the same dtype as existing columns,
    # not unconditionally float64 (tech debt fix).
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var lbls = List[String]()
    lbls.append("a")
    lbls.append("x")  # missing — should be int64, not float64
    var r = df.reindex(labels=Optional[List[String]](lbls^), axis=1)
    assert_equal(r.shape()[1], 2)
    # All source columns are int64, so the null column should also be int64.
    assert_equal(r["x"]._col.dtype.name, "int64")
    # The null column must be entirely null.
    assert_true(r["x"].isna().iloc(0)[Bool] == True)
    assert_true(r["x"].isna().iloc(1)[Bool] == True)


def test_reindex_axis0_reorder() raises:
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


def test_reindex_axis0_fill() raises:
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


def test_reindex_axis0_obj_fill_value() raises:
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


def test_reindex_axis0_obj_null_propagation() raises:
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


def test_stack_shape() raises:
    # A 2×3 DataFrame stacks to a Series of length 6.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4], 'c': [5, 6]}")))
    var s = df.stack()
    assert_equal(s.shape()[0], 6)


def test_stack_values() raises:
    # Values should be row-major: row0/col0, row0/col1, row1/col0, row1/col1.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [10, 20], 'y': [30, 40]}")))
    var s = df.stack()
    assert_equal(s.shape()[0], 4)
    assert_true(Bool(s.iloc(0)[PythonObject] == 10))
    assert_true(Bool(s.iloc(1)[PythonObject] == 30))
    assert_true(Bool(s.iloc(2)[PythonObject] == 20))
    assert_true(Bool(s.iloc(3)[PythonObject] == 40))


def test_explode_basic() raises:
    # Each list element becomes its own row; other columns repeat.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [[10, 11], [20]]}")))
    var r = df.explode("b")
    # Row 0 has 2 elements, row 1 has 1 → 3 output rows.
    assert_equal(r.shape()[0], 3)
    assert_equal(r.shape()[1], 2)
    # 'a' retains its int64 dtype; values are repeated by source row.
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 1)
    assert_true(r["a"].iloc(2)[Int64] == 2)
    # 'b' values are individual elements.
    assert_true(Bool(r["b"].iloc(0)[PythonObject] == 10))
    assert_true(Bool(r["b"].iloc(1)[PythonObject] == 11))
    assert_true(Bool(r["b"].iloc(2)[PythonObject] == 20))


def test_explode_missing_column_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var raised = False
    try:
        _ = df.explode("zzz")
    except:
        raised = True
    assert_true(raised)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
