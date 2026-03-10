"""Tests for DataFrame construction and basic attributes."""
from python import Python, PythonObject
from collections import Dict
from testing import assert_equal, assert_true, assert_false, TestSuite
from bison import DataFrame, ColumnData, Series


def test_shape_from_pandas():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var s = df.shape()
    assert_equal(s[0], 3)
    assert_equal(s[1], 2)


def test_len():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'x': [10, 20]}"))
    var df = DataFrame(pd_df)
    assert_equal(df.__len__(), 2)


def test_empty_false():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1]}"))
    var df = DataFrame(pd_df)
    assert_false(df.empty())


def test_empty_true():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame()
    var df = DataFrame(pd_df)
    assert_true(df.empty())


def test_columns():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'col1': [1], 'col2': [2]}"))
    var df = DataFrame(pd_df)
    var cols = df.columns()
    assert_equal(len(cols), 2)
    assert_equal(cols[0], "col1")
    assert_equal(cols[1], "col2")


def test_ndim():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    assert_equal(df.ndim(), 2)


def test_size():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    assert_equal(df.size(), 4)


def test_contains():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    assert_true(df.__contains__("a"))
    assert_false(df.__contains__("z"))


def test_to_pandas_roundtrip():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}"))
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    # length should match
    assert_equal(back.__len__(), 3)


def test_from_dict():
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


def test_getitem_column():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}")))
    var s = df["a"]
    assert_equal(s.size(), 3)


def test_getitem_missing_raises():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var raised = False
    try:
        _ = df["z"]
    except:
        raised = True
    assert_true(raised)


def test_setitem_new_column():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_series = Series(pd.Series(Python.evaluate("[10, 20]"), name="b"))
    df["b"] = new_series
    assert_equal(df.shape()[1], 2)
    assert_true(df.__contains__("b"))


def test_setitem_replace_column():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var new_series = Series(pd.Series(Python.evaluate("[99, 98]"), name="a"))
    df["a"] = new_series
    assert_equal(df.shape()[1], 1)
    var s = df["a"]
    assert_equal(s.size(), 2)


def test_get_existing_key():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var result = df.get("a")
    assert_true(result.__bool__())
    assert_equal(result.value().size(), 3)


def test_get_missing_key_default_none():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var result = df.get("z")
    assert_false(result.__bool__())


def test_head_basic():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var h = df.head(3)
    assert_equal(h.shape()[0], 3)
    assert_equal(h.shape()[1], 1)


def test_head_larger_than_rows():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var h = df.head(10)
    assert_equal(h.shape()[0], 2)


def test_tail_basic():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var t = df.tail(2)
    assert_equal(t.shape()[0], 2)
    assert_equal(t.shape()[1], 1)


def test_tail_larger_than_rows():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var t = df.tail(10)
    assert_equal(t.shape()[0], 2)


def test_head_tail_values():
    """Verify head/tail return the correct rows using native sum aggregation."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20, 30, 40, 50]}")))
    # head(2) picks [10, 20] → sum == 30
    var h = df.head(2)
    assert_true(h.sum().sum() == 30.0)
    # tail(2) picks [40, 50] → sum == 90
    var t = df.tail(2)
    assert_true(t.sum().sum() == 90.0)


def test_sample_n():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}")))
    var s = df.sample(3, random_state=42)
    assert_equal(s.shape()[0], 3)
    assert_equal(s.shape()[1], 1)


def test_sample_n_larger_than_rows():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var s = df.sample(10, random_state=0)
    assert_equal(s.shape()[0], 2)


def test_filter_items():
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


def test_filter_like():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'foo_1': [1], 'foo_2': [2], 'bar': [3]}")))
    var result = df.filter(like="foo")
    assert_equal(result.shape()[1], 2)
    assert_true(result.__contains__("foo_1"))
    assert_true(result.__contains__("foo_2"))
    assert_false(result.__contains__("bar"))


def test_filter_regex():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a1': [1], 'b2': [2], 'c1': [3]}")))
    var result = df.filter(regex=".*1$")
    assert_equal(result.shape()[1], 2)
    assert_true(result.__contains__("a1"))
    assert_true(result.__contains__("c1"))
    assert_false(result.__contains__("b2"))


def test_filter_no_args_keeps_all():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var result = df.filter()
    assert_equal(result.shape()[1], 2)


def test_select_dtypes_include():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var inc = List[String]()
    inc.append("int64")
    var result = df.select_dtypes(include=inc^)
    assert_equal(result.shape()[1], 1)
    assert_true(result.__contains__("a"))
    assert_false(result.__contains__("b"))


def test_select_dtypes_exclude():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [1.0, 2.0]}")))
    var exc = List[String]()
    exc.append("float64")
    var result = df.select_dtypes(exclude=exc^)
    assert_equal(result.shape()[1], 1)
    assert_true(result.__contains__("a"))
    assert_false(result.__contains__("b"))


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
