"""Tests for DataFrame construction and basic attributes."""
from std.python import Python, PythonObject
from std.collections import Dict
from std.testing import assert_equal, assert_true, assert_false, TestSuite
from bison import DataFrame, ColumnData, DFScalar, Series


def test_shape_from_pandas() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var s = df.shape()
    assert_equal(s[0], 3)
    assert_equal(s[1], 2)


def test_len() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'x': [10, 20]}"))
    var df = DataFrame(pd_df)
    assert_equal(df.__len__(), 2)


def test_empty_false() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1]}"))
    var df = DataFrame(pd_df)
    assert_false(df.empty())


def test_empty_true() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame()
    var df = DataFrame(pd_df)
    assert_true(df.empty())


def test_columns() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'col1': [1], 'col2': [2]}"))
    var df = DataFrame(pd_df)
    var cols = df.columns()
    assert_equal(len(cols), 2)
    assert_equal(cols[0], "col1")
    assert_equal(cols[1], "col2")


def test_ndim() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    assert_equal(df.ndim(), 2)


def test_size() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    assert_equal(df.size(), 4)


def test_contains() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    assert_true(df.__contains__("a"))
    assert_false(df.__contains__("z"))


def test_to_pandas_roundtrip() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}"))
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    # length should match
    assert_equal(back.__len__(), 3)


def test_from_dict() raises:
    var d = Dict[String, ColumnData]()
    var col_a = List[Int64]()
    col_a.append(1)
    col_a.append(2)
    var col_b = List[Int64]()
    col_b.append(3)
    col_b.append(4)
    d["a"] = ColumnData(col_a^)
    d["b"] = ColumnData(col_b^)
    var df = DataFrame.from_dict(d)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)
    assert_equal(df.columns()[0], "a")
    assert_equal(df.columns()[1], "b")


# ------------------------------------------------------------------
# Selection / indexing
# ------------------------------------------------------------------


def test_getitem_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}")))
    var s = df["a"]
    assert_equal(s.size(), 3)


def test_getitem_bool_mask_basic() raises:
    """Boolean mask df[boolean_series] returns only rows where mask is True."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var mask = df["a"].__gt__(3.0)
    var result = df[mask]
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 1)


def test_getitem_bool_mask_pattern() raises:
    """The df[df['a'] > threshold] pattern is the canonical pandas filter."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}")))
    var result = df[df["a"].__gt__(1.5)]
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 2)
    # Rows 1 and 2 survive; first value of 'b' should be 5.0
    assert_true(result["b"].iloc(0)[Float64] == 5.0)
    assert_true(result["b"].iloc(1)[Float64] == 6.0)


def test_getitem_bool_mask_none_pass() raises:
    """All-False mask returns empty DataFrame."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var mask = df["a"].__gt__(100.0)
    var result = df[mask]
    assert_equal(result.shape()[0], 0)
    assert_equal(result.shape()[1], 1)


def test_getitem_bool_mask_all_pass() raises:
    """All-True mask returns all rows."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var mask = df["a"].__gt__(0.0)
    var result = df[mask]
    assert_equal(result.shape()[0], 3)


def test_getitem_bool_mask_length_mismatch_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var mask = Series(pd.Series(Python.evaluate("[True, False]")))
    var raised = False
    try:
        _ = df[mask]
    except:
        raised = True
    assert_true(raised)


def test_getitem_bool_mask_string_eq() raises:
    """Boolean mask created via string equality filters correctly."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'cat': ['a', 'b', 'a', 'c'], 'val': [1, 2, 3, 4]}")))
    var mask = df["cat"].__eq__(String("a"))
    var result = df[mask]
    assert_equal(result.shape()[0], 2)
    assert_equal(result["val"].iloc(0)[Int64], Int64(1))
    assert_equal(result["val"].iloc(1)[Int64], Int64(3))


def test_getitem_bool_mask_mixed_dtype_take_path() raises:
    """Boolean mask keeps row order across mixed AnyArray-backed dtypes."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(
            Python.evaluate(
                "{'i': [10, 20, 30, 40, 50], 'f': [1.0, 2.0, 3.0, 4.0, 5.0], 'b': [True, False, True, False, True], 's': ['a', 'b', 'c', 'd', 'e'], 'sel': [0, 1, 0, 1, 1]}"
            )
        )
    )
    var result = df[df["sel"].__gt__(0.0)]
    assert_equal(result.shape()[0], 3)
    assert_equal(result["i"].iloc(0)[Int64], Int64(20))
    assert_equal(result["i"].iloc(1)[Int64], Int64(40))
    assert_equal(result["i"].iloc(2)[Int64], Int64(50))
    assert_true(result["f"].iloc(0)[Float64] == 2.0)
    assert_true(result["f"].iloc(1)[Float64] == 4.0)
    assert_true(result["f"].iloc(2)[Float64] == 5.0)
    assert_equal(result["b"].iloc(0)[Bool], Bool(False))
    assert_equal(result["b"].iloc(1)[Bool], Bool(False))
    assert_equal(result["b"].iloc(2)[Bool], Bool(True))
    assert_equal(result["s"].iloc(0)[String], "b")
    assert_equal(result["s"].iloc(1)[String], "d")
    assert_equal(result["s"].iloc(2)[String], "e")


def test_getitem_missing_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var raised = False
    try:
        _ = df["z"]
    except:
        raised = True
    assert_true(raised)


def test_setitem_new_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_series = Series(pd.Series(Python.evaluate("[10, 20]"), name="b"))
    df["b"] = new_series
    assert_equal(df.shape()[1], 2)
    assert_true(df.__contains__("b"))


def test_setitem_replace_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_series = Series(pd.Series(Python.evaluate("[99, 98]"), name="a"))
    df["a"] = new_series
    assert_equal(df.shape()[1], 1)
    var s = df["a"]
    assert_equal(s.size(), 2)


def test_get_existing_key() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var result = df.get("a")
    assert_true(result.__bool__())
    assert_equal(result.value().size(), 3)


def test_get_missing_key_default_none() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var result = df.get("z")
    assert_false(result.__bool__())


def test_head_basic() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var h = df.head(3)
    assert_equal(h.shape()[0], 3)
    assert_equal(h.shape()[1], 1)


def test_head_larger_than_rows() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var h = df.head(10)
    assert_equal(h.shape()[0], 2)


def test_tail_basic() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var t = df.tail(2)
    assert_equal(t.shape()[0], 2)
    assert_equal(t.shape()[1], 1)


def test_tail_larger_than_rows() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var t = df.tail(10)
    assert_equal(t.shape()[0], 2)


def test_head_tail_values() raises:
    """Verify head/tail return the correct rows using native sum aggregation."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20, 30, 40, 50]}")))
    # head(2) picks [10, 20] → sum == 30
    var h = df.head(2)
    assert_true(h.sum().sum() == 30.0)
    # tail(2) picks [40, 50] → sum == 90
    var t = df.tail(2)
    assert_true(t.sum().sum() == 90.0)


def test_sample_n() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var s = df.sample(3, random_state=42)
    assert_equal(s.shape()[0], 3)
    assert_equal(s.shape()[1], 1)


def test_sample_n_larger_than_rows() raises:
    # Without replace, n is capped at nrows.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var s = df.sample(10, random_state=0)
    assert_equal(s.shape()[0], 2)


def test_sample_replace() raises:
    # With replace=True, n may exceed nrows and rows may repeat.
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var s = df.sample(10, replace=True, random_state=42)
    assert_equal(s.shape()[0], 10)
    assert_equal(s.shape()[1], 1)


def test_filter_items() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2], 'c': [3]}")))
    var items = List[String]()
    items.append("a")
    items.append("c")
    var result = df.filter(items=items^)
    assert_equal(result.shape()[1], 2)
    assert_true(result.__contains__("a"))
    assert_true(result.__contains__("c"))
    assert_false(result.__contains__("b"))


def test_filter_like() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'foo_1': [1], 'foo_2': [2], 'bar': [3]}")))
    var result = df.filter(like="foo")
    assert_equal(result.shape()[1], 2)
    assert_true(result.__contains__("foo_1"))
    assert_true(result.__contains__("foo_2"))
    assert_false(result.__contains__("bar"))


def test_filter_regex() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a1': [1], 'b2': [2], 'c1': [3]}")))
    var result = df.filter(regex=".*1$")
    assert_equal(result.shape()[1], 2)
    assert_true(result.__contains__("a1"))
    assert_true(result.__contains__("c1"))
    assert_false(result.__contains__("b2"))


def test_filter_no_args_keeps_all() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var result = df.filter()
    assert_equal(result.shape()[1], 2)


def test_filter_axis0_items() raises:
    var pd = Python.import_module("pandas")
    # DataFrame with a string index
    var pdf = pd.DataFrame(
        Python.evaluate("{'val': [10, 20, 30]}"),
        index=Python.evaluate("['x', 'y', 'z']"),
    )
    var df = DataFrame(pdf)
    var items = List[String]()
    items.append("x")
    items.append("z")
    var result = df.filter(items=items^, axis=0)
    assert_equal(result.shape()[0], 2)


def test_filter_axis0_like() raises:
    var pd = Python.import_module("pandas")
    var pdf = pd.DataFrame(
        Python.evaluate("{'val': [1, 2, 3]}"),
        index=Python.evaluate("['foo_1', 'foo_2', 'bar']"),
    )
    var df = DataFrame(pdf)
    var result = df.filter(like="foo", axis=0)
    assert_equal(result.shape()[0], 2)


def test_filter_axis0_regex() raises:
    var pd = Python.import_module("pandas")
    var pdf = pd.DataFrame(
        Python.evaluate("{'val': [1, 2, 3]}"),
        index=Python.evaluate("['a1', 'b2', 'c1']"),
    )
    var df = DataFrame(pdf)
    var result = df.filter(regex=".*1$", axis=0)
    assert_equal(result.shape()[0], 2)


def test_select_dtypes_include() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var inc = List[String]()
    inc.append("int64")
    var result = df.select_dtypes(include=inc^)
    assert_equal(result.shape()[1], 1)
    assert_true(result.__contains__("a"))
    assert_false(result.__contains__("b"))


def test_select_dtypes_exclude() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var exc = List[String]()
    exc.append("float64")
    var result = df.select_dtypes(exclude=exc^)
    assert_equal(result.shape()[1], 1)
    assert_true(result.__contains__("a"))
    assert_false(result.__contains__("b"))


def test_sort_values_basic() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)
    assert_true(r["a"].iloc(2)[Int64] == 3)


def test_sort_values_na_last_default() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3.0, None, 1.0]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(1)[Float64] == 3.0)
    assert_true(r["a"].isna().iloc(2)[Bool])


def test_sort_values_na_first() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3.0, None, 1.0]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by, na_position="first")
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].iloc(1)[Float64] == 1.0)
    assert_true(r["a"].iloc(2)[Float64] == 3.0)


def test_sort_values_na_first_descending() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3.0, None, 1.0]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by, ascending=False, na_position="first")
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].iloc(1)[Float64] == 3.0)
    assert_true(r["a"].iloc(2)[Float64] == 1.0)


def test_sort_values_per_column_ascending() raises:
    # Sort by two columns with independent ascending flags:
    # primary key 'a' descending, secondary key 'b' ascending.
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 1, 2], 'b': [3, 1, 2]}"))
    )
    var by = List[String]()
    by.append("a")
    by.append("b")
    var asc = List[Bool]()
    asc.append(False)
    asc.append(True)
    var r = df.sort_values(by, asc)
    # Row with a=2 should come first (a descending).
    assert_true(r["a"].iloc(0)[Int64] == 2)
    # Rows with a=1 should be ordered by b ascending: b=1 then b=3.
    assert_true(r["a"].iloc(1)[Int64] == 1)
    assert_true(r["b"].iloc(1)[Int64] == 1)
    assert_true(r["a"].iloc(2)[Int64] == 1)
    assert_true(r["b"].iloc(2)[Int64] == 3)


def test_dtypes_names() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var dt = df.dtypes()
    assert_equal(dt.size(), 2)


def test_dtypes_values() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var dt = df.dtypes()
    assert_equal(dt.iloc(0)[String], "int64")
    assert_equal(dt.iloc(1)[String], "float64")


def test_memory_usage_length() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0]}")))
    var mu = df.memory_usage()
    assert_equal(mu.size(), 2)


def test_memory_usage_int64_bytes() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var mu = df.memory_usage()
    # 3 rows * 8 bytes per int64 = 24
    assert_equal(mu.iloc(0)[Int64], Int64(24))


def test_memory_usage_float64_bytes() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'b': [1.0, 2.0]}")))
    var mu = df.memory_usage()
    # 2 rows * 8 bytes per float64 = 16
    assert_equal(mu.iloc(0)[Int64], Int64(16))


def test_info_no_raise() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3.0, 4.0]}")))
    df.info()


def test_items_count() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var cols = df.items()
    assert_equal(len(cols), 2)


def test_items_names() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1], 'y': [2]}")))
    var cols = df.items()
    assert_equal(cols[0].name.value(), "x")
    assert_equal(cols[1].name.value(), "y")


def test_items_values() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20]}")))
    var cols = df.items()
    assert_equal(cols[0].iloc(0)[Int64], Int64(10))
    assert_equal(cols[0].iloc(1)[Int64], Int64(20))


def test_iterrows_count() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var rows = df.iterrows()
    assert_equal(len(rows), 3)


def test_iterrows_values() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20], 'b': [30, 40]}")))
    var rows = df.iterrows()
    # Row 0: a=10, b=30
    assert_equal(rows[0].size(), 2)
    assert_equal(rows[1].size(), 2)


def test_itertuples_with_index() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var tuples = df.itertuples()
    # With index=True (default): each row has index value + column values
    assert_equal(len(tuples), 2)
    assert_equal(tuples[0].size(), 2)  # Index + a


def test_itertuples_without_index() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var tuples = df.itertuples(index=False)
    assert_equal(len(tuples), 2)
    assert_equal(tuples[0].size(), 2)  # a + b only (no index)


def test_itertuples_name() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var tuples = df.itertuples(index=True, name="Row")
    assert_equal(tuples[0].name.value(), "Row")


def test_from_records_basic() raises:
    var row0: Dict[String, DFScalar] = {"a": DFScalar(1), "b": DFScalar(10)}
    var row1: Dict[String, DFScalar] = {"a": DFScalar(2), "b": DFScalar(20)}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    records.append(row1^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)
    assert_equal(df.columns()[0], "a")
    assert_equal(df.columns()[1], "b")


def test_from_records_empty() raises:
    var records = List[Dict[String, DFScalar]]()
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 0)
    assert_equal(df.shape()[1], 0)


def test_from_records_columns_param() raises:
    var row0: Dict[String, DFScalar] = {"a": DFScalar(1), "b": DFScalar(2), "c": DFScalar(3)}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    var cols = List[String]()
    cols.append("a")
    cols.append("c")
    var df = DataFrame.from_records(records, cols^)
    assert_equal(df.shape()[1], 2)
    assert_equal(df.columns()[0], "a")
    assert_equal(df.columns()[1], "c")


def test_from_records_mixed_types() raises:
    var row0: Dict[String, DFScalar] = {"i": DFScalar(42), "s": DFScalar(String("hello"))}
    var row1: Dict[String, DFScalar] = {"i": DFScalar(7), "s": DFScalar(String("world"))}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    records.append(row1^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)


def test_from_records_int_float_mixed() raises:
    # First row has Int64, second row has Float64 — column should be promoted to float64
    var row0: Dict[String, DFScalar] = {"x": DFScalar(1)}
    var row1: Dict[String, DFScalar] = {"x": Float64(2.5)}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    records.append(row1^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 1)
    # Verify the column is promoted to float64
    assert_equal(df.dtypes().iloc(0)[String], "float64")
    var pd_df = df.to_pandas()
    assert_true(Bool(pd_df["x"][0] == 1.0))
    assert_true(Bool(pd_df["x"][1] == 2.5))


def test_from_records_bool_int_mixed() raises:
    # First row has Bool, second row has Int64 — column should be promoted to int64
    var row0: Dict[String, DFScalar] = {"y": DFScalar(True)}
    var row1: Dict[String, DFScalar] = {"y": DFScalar(42)}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    records.append(row1^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 1)
    # Verify the column is promoted to int64
    assert_equal(df.dtypes().iloc(0)[String], "int64")
    var pd_df = df.to_pandas()
    assert_true(Bool(pd_df["y"][0] == 1))
    assert_true(Bool(pd_df["y"][1] == 42))


def test_from_records_missing_key() raises:
    var row0: Dict[String, DFScalar] = {"a": DFScalar(1), "b": DFScalar(10)}
    var row1: Dict[String, DFScalar] = {"a": DFScalar(2)}
    # "b" is missing in row1
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    records.append(row1^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)
    # verify via pandas that the missing value is NaN
    var pd_df = df.to_pandas()
    assert_true(Bool(pd_df["b"].isna()[1]))


def test_from_records_column_order_deterministic() raises:
    # Column names should be sorted alphabetically when `columns` is not provided,
    # so the result is deterministic regardless of Dict iteration order.
    var row0: Dict[String, DFScalar] = {"z": DFScalar(3), "a": DFScalar(1), "m": DFScalar(2)}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[1], 3)
    assert_equal(df.columns()[0], "a")
    assert_equal(df.columns()[1], "m")
    assert_equal(df.columns()[2], "z")


def test_from_records_bool_with_nulls() raises:
    # Bool column containing a null should round-trip through to_pandas() without error.
    # Pandas cannot represent NaN in a bool dtype column, so bison must promote to object.
    var row0: Dict[String, DFScalar] = {"flag": DFScalar(True)}
    var row1: Dict[String, DFScalar] = {}  # "flag" is missing → null
    var row2: Dict[String, DFScalar] = {"flag": DFScalar(False)}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    records.append(row1^)
    records.append(row2^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 3)
    assert_equal(df.shape()[1], 1)
    # to_pandas() must succeed (previously raised with bool dtype + NaN)
    var pd_df = df.to_pandas()
    assert_true(Bool(pd_df["flag"].isna()[1]))
    assert_true(Bool(pd_df["flag"][0] == True))
    assert_true(Bool(pd_df["flag"][2] == False))


def test_fillna_null_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var raised = False
    try:
        _ = df.fillna(DFScalar.null())
    except e:
        raised = "null" in String(e)
    assert_true(raised)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
