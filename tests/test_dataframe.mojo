"""Tests for DataFrame construction and basic attributes."""
from std.python import Python, PythonObject
from std.collections import Dict
from testing import assert_equal, assert_true, assert_false, TestSuite
from bison import DataFrame, ColumnData, DFScalar, Series


fn test_shape_from_pandas() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var s = df.shape()
    assert_equal(s[0], 3)
    assert_equal(s[1], 2)


fn test_len() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'x': [10, 20]}"))
    var df = DataFrame(pd_df)
    assert_equal(df.__len__(), 2)


fn test_empty_false() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1]}"))
    var df = DataFrame(pd_df)
    assert_false(df.empty())


fn test_empty_true() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame()
    var df = DataFrame(pd_df)
    assert_true(df.empty())


fn test_columns() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'col1': [1], 'col2': [2]}"))
    var df = DataFrame(pd_df)
    var cols = df.columns()
    assert_equal(len(cols), 2)
    assert_equal(cols[0], "col1")
    assert_equal(cols[1], "col2")


fn test_ndim() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    assert_equal(df.ndim(), 2)


fn test_size() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    assert_equal(df.size(), 4)


fn test_contains() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    assert_true(df.__contains__("a"))
    assert_false(df.__contains__("z"))


fn test_to_pandas_roundtrip() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}"))
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    # length should match
    assert_equal(back.__len__(), 3)


fn test_from_dict() raises:
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


fn test_getitem_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}")))
    var s = df["a"]
    assert_equal(s.size(), 3)


fn test_getitem_missing_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var raised = False
    try:
        _ = df["z"]
    except:
        raised = True
    assert_true(raised)


fn test_setitem_new_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_series = Series(pd.Series(Python.evaluate("[10, 20]"), name="b"))
    df["b"] = new_series
    assert_equal(df.shape()[1], 2)
    assert_true(df.__contains__("b"))


fn test_setitem_replace_column() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_series = Series(pd.Series(Python.evaluate("[99, 98]"), name="a"))
    df["a"] = new_series
    assert_equal(df.shape()[1], 1)
    var s = df["a"]
    assert_equal(s.size(), 2)


fn test_get_existing_key() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var result = df.get("a")
    assert_true(result.__bool__())
    assert_equal(result.value().size(), 3)


fn test_get_missing_key_default_none() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var result = df.get("z")
    assert_false(result.__bool__())


fn test_head_basic() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var h = df.head(3)
    assert_equal(h.shape()[0], 3)
    assert_equal(h.shape()[1], 1)


fn test_head_larger_than_rows() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var h = df.head(10)
    assert_equal(h.shape()[0], 2)


fn test_tail_basic() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var t = df.tail(2)
    assert_equal(t.shape()[0], 2)
    assert_equal(t.shape()[1], 1)


fn test_tail_larger_than_rows() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var t = df.tail(10)
    assert_equal(t.shape()[0], 2)


fn test_head_tail_values() raises:
    """Verify head/tail return the correct rows using native sum aggregation."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20, 30, 40, 50]}")))
    # head(2) picks [10, 20] → sum == 30
    var h = df.head(2)
    assert_true(h.sum().sum() == 30.0)
    # tail(2) picks [40, 50] → sum == 90
    var t = df.tail(2)
    assert_true(t.sum().sum() == 90.0)


fn test_sample_n() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var s = df.sample(3, random_state=42)
    assert_equal(s.shape()[0], 3)
    assert_equal(s.shape()[1], 1)


fn test_sample_n_larger_than_rows() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var s = df.sample(10, random_state=0)
    assert_equal(s.shape()[0], 2)


fn test_filter_items() raises:
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


fn test_filter_like() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'foo_1': [1], 'foo_2': [2], 'bar': [3]}")))
    var result = df.filter(like="foo")
    assert_equal(result.shape()[1], 2)
    assert_true(result.__contains__("foo_1"))
    assert_true(result.__contains__("foo_2"))
    assert_false(result.__contains__("bar"))


fn test_filter_regex() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a1': [1], 'b2': [2], 'c1': [3]}")))
    var result = df.filter(regex=".*1$")
    assert_equal(result.shape()[1], 2)
    assert_true(result.__contains__("a1"))
    assert_true(result.__contains__("c1"))
    assert_false(result.__contains__("b2"))


fn test_filter_no_args_keeps_all() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var result = df.filter()
    assert_equal(result.shape()[1], 2)


fn test_select_dtypes_include() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var inc = List[String]()
    inc.append("int64")
    var result = df.select_dtypes(include=inc^)
    assert_equal(result.shape()[1], 1)
    assert_true(result.__contains__("a"))
    assert_false(result.__contains__("b"))


fn test_select_dtypes_exclude() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var exc = List[String]()
    exc.append("float64")
    var result = df.select_dtypes(exclude=exc^)
    assert_equal(result.shape()[1], 1)
    assert_true(result.__contains__("a"))
    assert_false(result.__contains__("b"))


fn test_sort_values_basic() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Int64] == 1)
    assert_true(r["a"].iloc(1)[Int64] == 2)
    assert_true(r["a"].iloc(2)[Int64] == 3)


fn test_sort_values_na_last_default() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3.0, None, 1.0]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by)
    assert_true(r["a"].iloc(0)[Float64] == 1.0)
    assert_true(r["a"].iloc(1)[Float64] == 3.0)
    assert_true(r["a"].isna().iloc(2)[Bool])


fn test_sort_values_na_first() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3.0, None, 1.0]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by, na_position="first")
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].iloc(1)[Float64] == 1.0)
    assert_true(r["a"].iloc(2)[Float64] == 3.0)


fn test_sort_values_na_first_descending() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3.0, None, 1.0]}")))
    var by = List[String]()
    by.append("a")
    var r = df.sort_values(by, ascending=False, na_position="first")
    assert_true(r["a"].isna().iloc(0)[Bool])
    assert_true(r["a"].iloc(1)[Float64] == 3.0)
    assert_true(r["a"].iloc(2)[Float64] == 1.0)


fn test_dtypes_names() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var dt = df.dtypes()
    assert_equal(dt.size(), 2)


fn test_dtypes_values() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var dt = df.dtypes()
    assert_equal(dt.iloc(0)[String], "int64")
    assert_equal(dt.iloc(1)[String], "float64")


fn test_memory_usage_length() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0]}")))
    var mu = df.memory_usage()
    assert_equal(mu.size(), 2)


fn test_memory_usage_int64_bytes() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var mu = df.memory_usage()
    # 3 rows * 8 bytes per int64 = 24
    assert_equal(mu.iloc(0)[Int64], Int64(24))


fn test_memory_usage_float64_bytes() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'b': [1.0, 2.0]}")))
    var mu = df.memory_usage()
    # 2 rows * 8 bytes per float64 = 16
    assert_equal(mu.iloc(0)[Int64], Int64(16))


fn test_info_no_raise() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3.0, 4.0]}")))
    df.info()


fn test_items_count() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var cols = df.items()
    assert_equal(len(cols), 2)


fn test_items_names() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1], 'y': [2]}")))
    var cols = df.items()
    assert_equal(cols[0].name, "x")
    assert_equal(cols[1].name, "y")


fn test_items_values() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20]}")))
    var cols = df.items()
    assert_equal(cols[0].iloc(0)[Int64], Int64(10))
    assert_equal(cols[0].iloc(1)[Int64], Int64(20))


fn test_iterrows_count() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var rows = df.iterrows()
    assert_equal(len(rows), 3)


fn test_iterrows_values() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20], 'b': [30, 40]}")))
    var rows = df.iterrows()
    # Row 0: a=10, b=30
    assert_equal(rows[0].size(), 2)
    assert_equal(rows[1].size(), 2)


fn test_itertuples_with_index() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var tuples = df.itertuples()
    # With index=True (default): each row has index value + column values
    assert_equal(len(tuples), 2)
    assert_equal(tuples[0].size(), 2)  # Index + a


fn test_itertuples_without_index() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var tuples = df.itertuples(index=False)
    assert_equal(len(tuples), 2)
    assert_equal(tuples[0].size(), 2)  # a + b only (no index)


fn test_itertuples_name() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var tuples = df.itertuples(index=True, name="Row")
    assert_equal(tuples[0].name, "Row")


fn test_from_records_basic() raises:
    var row0: Dict[String, DFScalar] = {"a": DFScalar(Int64(1)), "b": DFScalar(Int64(10))}
    var row1: Dict[String, DFScalar] = {"a": DFScalar(Int64(2)), "b": DFScalar(Int64(20))}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    records.append(row1^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)
    assert_equal(df.columns()[0], "a")
    assert_equal(df.columns()[1], "b")


fn test_from_records_empty() raises:
    var records = List[Dict[String, DFScalar]]()
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 0)
    assert_equal(df.shape()[1], 0)


fn test_from_records_columns_param() raises:
    var row0: Dict[String, DFScalar] = {"a": DFScalar(Int64(1)), "b": DFScalar(Int64(2)), "c": DFScalar(Int64(3))}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    var cols = List[String]()
    cols.append("a")
    cols.append("c")
    var df = DataFrame.from_records(records, cols^)
    assert_equal(df.shape()[1], 2)
    assert_equal(df.columns()[0], "a")
    assert_equal(df.columns()[1], "c")


fn test_from_records_mixed_types() raises:
    var row0: Dict[String, DFScalar] = {"i": DFScalar(Int64(42)), "s": DFScalar(String("hello"))}
    var row1: Dict[String, DFScalar] = {"i": DFScalar(Int64(7)), "s": DFScalar(String("world"))}
    var records = List[Dict[String, DFScalar]]()
    records.append(row0^)
    records.append(row1^)
    var df = DataFrame.from_records(records)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)


fn test_from_records_int_float_mixed() raises:
    # First row has Int64, second row has Float64 — column should be promoted to float64
    var row0: Dict[String, DFScalar] = {"x": DFScalar(Int64(1))}
    var row1: Dict[String, DFScalar] = {"x": DFScalar(Float64(2.5))}
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


fn test_from_records_bool_int_mixed() raises:
    # First row has Bool, second row has Int64 — column should be promoted to int64
    var row0: Dict[String, DFScalar] = {"y": DFScalar(True)}
    var row1: Dict[String, DFScalar] = {"y": DFScalar(Int64(42))}
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


fn test_from_records_missing_key() raises:
    var row0: Dict[String, DFScalar] = {"a": DFScalar(Int64(1)), "b": DFScalar(Int64(10))}
    var row1: Dict[String, DFScalar] = {"a": DFScalar(Int64(2))}
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


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
