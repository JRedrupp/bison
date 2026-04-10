"""Tests for DataFrame.groupby(), DataFrameGroupBy, and SeriesGroupBy."""
from std.python import Python, PythonObject
from std.collections import Dict
from std.testing import assert_equal, TestSuite
from bison import DataFrame, Series, ColumnData, Column, NullMask
from bison.dtypes import int64 as _bison_int64, float64 as _bison_float64
from _helpers import assert_frame_equal, assert_series_equal


def _make_pd_df() raises -> PythonObject:
    var pd = Python.import_module("pandas")
    return pd.DataFrame(
        Python.evaluate(
            "{'grp': ['a', 'a', 'b', 'b'], 'val': [1, 2, 3, 4],"
            " 'x': [10.0, 20.0, 30.0, 40.0]}"
        )
    )


# ------------------------------------------------------------------
# DataFrameGroupBy tests
# ------------------------------------------------------------------


def test_dataframegroupby_sum() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).sum().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").sum())


def test_dataframegroupby_mean() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).mean().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").mean())


def test_dataframegroupby_min() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).min().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").min())


def test_dataframegroupby_max() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).max().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").max())


def test_dataframegroupby_count() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).count().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").count())


def test_dataframegroupby_nunique() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).nunique().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").nunique())


def test_dataframegroupby_first() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).first().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").first())


def test_dataframegroupby_last() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).last().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").last())


def test_dataframegroupby_size() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).size().to_pandas()
    assert_series_equal(
        result, pd_df.groupby("grp").size()
    )


def test_dataframegroupby_std() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).std().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").std())


def test_dataframegroupby_var() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).var().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").var())


def test_dataframegroupby_agg() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).agg("sum").to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").agg("sum"))


def test_dataframegroupby_transform() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("sum").to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").transform("sum"))


def test_dataframegroupby_transform_dropna() raises:
    """Transform() with dropna=True must not raise when key column has nulls."""
    var pd = Python.import_module("pandas")
    # Build a DataFrame with one null in the groupby key column.
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'grp': ['a', None, 'b', 'b'], 'val': [1.0, 2.0, 3.0, 4.0]}"
        )
    )
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    # dropna=True (the default) — the null-keyed row should become NaN in the output.
    var result = df.groupby(by).transform("sum").to_pandas()
    var expected = pd_df.groupby("grp", dropna=True).transform("sum")
    assert_frame_equal(result, expected, check_dtype=False)


def test_dataframegroupby_transform_std() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("std").to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").transform("std"), check_dtype=False)


def test_dataframegroupby_transform_var() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("var").to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").transform("var"), check_dtype=False)


def test_dataframegroupby_transform_count() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("count").to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").transform("count"), check_dtype=False)


def test_dataframegroupby_transform_first() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("first").to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").transform("first"), check_dtype=False)


def test_dataframegroupby_transform_last() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("last").to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").transform("last"), check_dtype=False)


# ------------------------------------------------------------------
# SeriesGroupBy tests
# ------------------------------------------------------------------
# Series.groupby takes element-wise group labels as the `by` list.
# We pass ["a","a","b","b"] as raw labels and compare against the same
# labels in Python to avoid index-name discrepancies from named grouping.


def _pd_labels() raises -> PythonObject:
    return Python.evaluate("['a', 'a', 'b', 'b']")


def _mojo_labels() -> List[String]:
    var by = List[String]()
    by.append("a")
    by.append("a")
    by.append("b")
    by.append("b")
    return by^


def test_seriesgroupby_sum() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).sum().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).sum()
    )


def test_seriesgroupby_mean() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).mean().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).mean()
    )


def test_seriesgroupby_min() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).min().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).min()
    )


def test_seriesgroupby_max() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).max().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).max()
    )


def test_seriesgroupby_count() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).count().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).count()
    )


def test_seriesgroupby_size() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).size().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).size()
    )


def test_seriesgroupby_agg() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).agg("sum").to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).agg("sum")
    )


def test_seriesgroupby_nunique() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).nunique().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).nunique()
    )


def test_seriesgroupby_std() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).std().to_pandas()
    # check_dtype=False: native returns Float64; pandas returns Float64 too,
    # but check_dtype ensures no accidental int comparison
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).std(), check_dtype=False
    )


def test_seriesgroupby_var() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).var().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).var(), check_dtype=False
    )


def test_seriesgroupby_first() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).first().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).first()
    )


def test_seriesgroupby_last() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).last().to_pandas()
    assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).last()
    )


def test_seriesgroupby_transform() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("sum").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("sum"),
    )


def test_seriesgroupby_transform_mean() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("mean").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("mean"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_min() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("min").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("min"),
    )


def test_seriesgroupby_transform_max() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("max").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("max"),
    )


def test_seriesgroupby_transform_std() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("std").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("std"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_var() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("var").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("var"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_count() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("count").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("count"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_size() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("size").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("size"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_first() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("first").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("first"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_last() raises:
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("last").to_pandas()
    assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("last"),
        check_dtype=False,
    )


def test_dataframegroupby_int_key_natural_sort() raises:
    """Groupby with Int64 key column must order groups numerically, not lexicographically.

    Keys 1, 2, 10: lex sort produces "1","10","2"; natural sort produces 1,2,10.
    The bug is visible because iloc[1].val would be 300 (group 10) under lex order
    but 60 (group 2) under natural order.
    """
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'grp': [10, 1, 2, 10, 1, 2], 'val': [100, 10, 20, 200, 30, 40]}"
        )
    )
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).sum().to_pandas()
    assert_frame_equal(result, pd_df.groupby("grp").sum())


def test_dataframegroupby_float_key_natural_sort() raises:
    """Groupby with Float64 key column must order groups numerically, not lexicographically.

    Keys 1.5, 2.0, 10.5: lex sort produces "1.5","10.5","2.0"; natural gives 1.5,2.0,10.5.
    The result index must be a float64 pandas Index, matching the pandas reference exactly.
    """
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'grp': [10.5, 1.5, 2.0, 10.5, 1.5, 2.0],"
            " 'val': [100.0, 10.0, 20.0, 200.0, 30.0, 40.0]}"
        )
    )
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).sum().to_pandas()
    assert_frame_equal(
        result,
        pd_df.groupby("grp").sum(),
    )


def test_dataframegroupby_multikey_numeric_secondary_sort() raises:
    """Multi-key groupby must sort later numeric keys numerically within each prefix."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'grp1': ['a', 'a', 'a', 'b', 'b', 'b'],"
            " 'grp2': [10, 1, 2, 10, 1, 2],"
            " 'val': [100, 10, 20, 200, 30, 40]}"
        )
    )
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).sum().to_pandas()
    assert_frame_equal(
        result,
        pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).sum(),
    )


def test_seriesgroupby_dropna_sum() raises:
    """Dropna=True must exclude null-labelled rows from all groups."""
    var pd = Python.import_module("pandas")
    # Series with 4 rows; row 1 has a null label.
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]"), name="val")
    var s = Series(pd_s, "val")
    var by = List[String]()
    by.append("a")
    by.append("a")  # string value is ignored because null_mask marks this row as null
    by.append("b")
    by.append("b")
    var null_mask = NullMask()
    null_mask.append_valid()
    null_mask.append_null()  # row 1 is null-labelled
    null_mask.append_valid()
    null_mask.append_valid()
    # dropna=True (the default): null-labelled row should be excluded.
    var result = s.groupby(by, dropna=True, by_null_mask=null_mask).sum()
    var result_pd = result.to_pandas()
    var py_labels = Python.evaluate("['a', None, 'b', 'b']")
    var expected = pd_s.groupby(py_labels, dropna=True).sum()
    assert_series_equal(result_pd, expected)


def test_seriesgroupby_dropna_transform_sum() raises:
    """Transform('sum') with dropna=True must emit NaN for null-labelled rows."""
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]"), name="val")
    var s = Series(pd_s, "val")
    var by = List[String]()
    by.append("a")
    by.append("a")
    by.append("b")
    by.append("b")
    var null_mask = NullMask()
    null_mask.append_valid()
    null_mask.append_null()
    null_mask.append_valid()
    null_mask.append_valid()
    var result = s.groupby(by, dropna=True, by_null_mask=null_mask).transform(
        "sum"
    )
    var result_pd = result.to_pandas()
    var py_labels = Python.evaluate("['a', None, 'b', 'b']")
    var expected = pd_s.groupby(py_labels, dropna=True).transform("sum")
    assert_series_equal(result_pd, expected, check_dtype=False)


def _make_pd_df_multi() raises -> PythonObject:
    """DataFrame with two groupby key columns for multi-key tests."""
    var pd = Python.import_module("pandas")
    return pd.DataFrame(
        Python.evaluate(
            "{'grp1': ['a', 'a', 'b', 'b'], 'grp2': ['x', 'y', 'x', 'y'],"
            " 'val': [1, 2, 3, 4], 'x': [10.0, 20.0, 30.0, 40.0]}"
        )
    )


# ------------------------------------------------------------------
# Multi-key groupby tests
# ------------------------------------------------------------------


def test_dataframegroupby_multikey_sum() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).sum().to_pandas()
    assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).sum())


def test_dataframegroupby_multikey_mean() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).mean().to_pandas()
    assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).mean())


def test_dataframegroupby_multikey_min() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).min().to_pandas()
    assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).min())


def test_dataframegroupby_multikey_max() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).max().to_pandas()
    assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).max())


def test_dataframegroupby_multikey_count() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).count().to_pandas()
    assert_frame_equal(
        result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).count()
    )


def test_dataframegroupby_multikey_nunique() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).nunique().to_pandas()
    assert_frame_equal(
        result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).nunique()
    )


def test_dataframegroupby_multikey_first() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).first().to_pandas()
    assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).first())


def test_dataframegroupby_multikey_last() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).last().to_pandas()
    assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).last())


def test_dataframegroupby_multikey_size() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).size().to_pandas()
    assert_series_equal(
        result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).size()
    )


# ------------------------------------------------------------------
# as_index=False tests
# ------------------------------------------------------------------


def test_dataframegroupby_as_index_false_sum() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by, as_index=False).sum().to_pandas()
    assert_frame_equal(
        result, pd_df.groupby("grp", as_index=False).sum(), check_dtype=False
    )


def test_dataframegroupby_as_index_false_mean() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by, as_index=False).mean().to_pandas()
    assert_frame_equal(
        result,
        pd_df.groupby("grp", as_index=False).mean(),
        check_dtype=False,
    )


def test_dataframegroupby_as_index_false_count() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by, as_index=False).count().to_pandas()
    assert_frame_equal(
        result,
        pd_df.groupby("grp", as_index=False).count(),
        check_dtype=False,
    )


def test_dataframegroupby_multikey_as_index_false_sum() raises:
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by, as_index=False).sum().to_pandas()
    assert_frame_equal(
        result,
        pd_df.groupby(
            Python.evaluate("['grp1', 'grp2']"), as_index=False
        ).sum(),
        check_dtype=False,
    )


# ------------------------------------------------------------------
# Composite key delimiter collision tests (issue #506)
# ------------------------------------------------------------------


def test_groupby_multikey_delimiter_in_value() raises:
    """Groupby with two string key columns where values contain the pipe
    delimiter must produce the same number of groups as pandas.

    Without collision-safe serialisation, ("a|b", "c") and ("a", "b|c")
    both serialise to "a|b|c" and get merged into one group.
    """
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'k1': ['a|b', 'a'], 'k2': ['c', 'b|c'], 'val': [10, 20]}"
        )
    )
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("k1")
    by.append("k2")
    var result = df.groupby(by).sum()
    # pandas sees two distinct groups: ("a|b", "c") and ("a", "b|c")
    assert_equal(result.shape()[0], 2)
    var result_pd = result.to_pandas()
    var expected = pd_df.groupby(Python.evaluate("['k1', 'k2']")).sum()
    assert_frame_equal(result_pd, expected)


def test_groupby_multikey_delimiter_only_values() raises:
    """Edge case: key values that are exactly the delimiter character."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'k1': ['|', 'a'], 'k2': ['a', '|'], 'val': [1, 2]}"
        )
    )
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("k1")
    by.append("k2")
    var result = df.groupby(by).sum()
    assert_equal(result.shape()[0], 2)
    var result_pd = result.to_pandas()
    var expected = pd_df.groupby(Python.evaluate("['k1', 'k2']")).sum()
    assert_frame_equal(result_pd, expected)


# ------------------------------------------------------------------
# Marrow hash-aggregate fast-path tests
# ------------------------------------------------------------------


def _make_native_df() raises -> DataFrame:
    """Create a DataFrame with native int64 key column (not PythonObject).

    This triggers the marrow groupby fast path since the key column is
    List[Int64], not List[PythonObject].
    """
    var d = Dict[String, ColumnData]()
    var grp = List[Int64]()
    grp.append(1)
    grp.append(1)
    grp.append(2)
    grp.append(2)
    d["grp"] = ColumnData(grp^)
    var val = List[Int64]()
    val.append(10)
    val.append(20)
    val.append(30)
    val.append(40)
    d["val"] = ColumnData(val^)
    var x = List[Float64]()
    x.append(1.5)
    x.append(2.5)
    x.append(3.5)
    x.append(4.5)
    d["x"] = ColumnData(x^)
    return DataFrame.from_dict(d)


def _native_to_pandas() raises -> PythonObject:
    """Create the equivalent pandas DataFrame for comparison."""
    var pd = Python.import_module("pandas")
    return pd.DataFrame(
        Python.evaluate(
            "{'grp': [1, 1, 2, 2], 'val': [10, 20, 30, 40],"
            " 'x': [1.5, 2.5, 3.5, 4.5]}"
        )
    )


def test_marrow_sum() raises:
    """Marrow fast path: sum with int64 key preserves int64 for int columns."""
    var df = _make_native_df()
    var pd_df = _native_to_pandas()
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).sum().to_pandas()
    var expected = pd_df.groupby("grp").sum()
    assert_frame_equal(result, expected)


def test_marrow_mean() raises:
    """Marrow fast path: mean with int64 key returns float64."""
    var df = _make_native_df()
    var pd_df = _native_to_pandas()
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).mean().to_pandas()
    var expected = pd_df.groupby("grp").mean()
    assert_frame_equal(result, expected)


def test_marrow_min() raises:
    """Marrow fast path: min with int64 key preserves int64 for int columns."""
    var df = _make_native_df()
    var pd_df = _native_to_pandas()
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).min().to_pandas()
    var expected = pd_df.groupby("grp").min()
    assert_frame_equal(result, expected)


def test_marrow_max() raises:
    """Marrow fast path: max with int64 key preserves int64 for int columns."""
    var df = _make_native_df()
    var pd_df = _native_to_pandas()
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).max().to_pandas()
    var expected = pd_df.groupby("grp").max()
    assert_frame_equal(result, expected)


def test_marrow_count() raises:
    """Marrow fast path: count with int64 key returns int64."""
    var df = _make_native_df()
    var pd_df = _native_to_pandas()
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).count().to_pandas()
    var expected = pd_df.groupby("grp").count()
    assert_frame_equal(result, expected)


def test_marrow_float_key() raises:
    """Marrow fast path: groupby on a float64 key column."""
    var d = Dict[String, ColumnData]()
    var grp = List[Float64]()
    grp.append(1.0)
    grp.append(1.0)
    grp.append(2.0)
    grp.append(2.0)
    d["grp"] = ColumnData(grp^)
    var val = List[Int64]()
    val.append(10)
    val.append(20)
    val.append(30)
    val.append(40)
    d["val"] = ColumnData(val^)
    var df = DataFrame.from_dict(d)
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'grp': [1.0, 1.0, 2.0, 2.0], 'val': [10, 20, 30, 40]}"
        )
    )
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).sum().to_pandas()
    var expected = pd_df.groupby("grp").sum()
    assert_frame_equal(result, expected)


def test_marrow_string_key() raises:
    """Marrow fast path: groupby on a native List[String] key column."""
    var d = Dict[String, ColumnData]()
    var grp = List[String]()
    grp.append("a")
    grp.append("a")
    grp.append("b")
    grp.append("b")
    d["grp"] = ColumnData(grp^)
    var val = List[Int64]()
    val.append(10)
    val.append(20)
    val.append(30)
    val.append(40)
    d["val"] = ColumnData(val^)
    var df = DataFrame.from_dict(d)
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'grp': ['a', 'a', 'b', 'b'], 'val': [10, 20, 30, 40]}"
        )
    )
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).sum().to_pandas()
    var expected = pd_df.groupby("grp").sum()
    assert_frame_equal(result, expected)


def test_marrow_unsorted_keys() raises:
    """Marrow fast path: keys not in order are sorted correctly."""
    var d = Dict[String, ColumnData]()
    var grp = List[Int64]()
    grp.append(3)
    grp.append(1)
    grp.append(2)
    grp.append(1)
    grp.append(3)
    grp.append(2)
    d["grp"] = ColumnData(grp^)
    var val = List[Int64]()
    val.append(10)
    val.append(20)
    val.append(30)
    val.append(40)
    val.append(50)
    val.append(60)
    d["val"] = ColumnData(val^)
    var df = DataFrame.from_dict(d)
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'grp': [3, 1, 2, 1, 3, 2], 'val': [10, 20, 30, 40, 50, 60]}"
        )
    )
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).sum().to_pandas()
    var expected = pd_df.groupby("grp").sum()
    assert_frame_equal(result, expected)


def test_marrow_fallback_multikey() raises:
    """Multi-key groupby should fall back to existing path, not marrow."""
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    by.append("val")
    var result = df.groupby(by).sum().to_pandas()
    var expected = pd_df.groupby(Python.evaluate("['grp', 'val']")).sum()
    assert_frame_equal(result, expected)


def test_marrow_fallback_as_index_false() raises:
    """The as_index=False option should fall back to existing path."""
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by, as_index=False).sum().to_pandas()
    var expected = pd_df.groupby("grp", as_index=False).sum()
    assert_frame_equal(result, expected, check_dtype=False)


def test_marrow_series_sum() raises:
    """Marrow fast path for SeriesGroupBy.sum()."""
    var vals = List[Int64]()
    vals.append(10)
    vals.append(20)
    vals.append(30)
    vals.append(40)
    var s = Series(Column("val", ColumnData(vals^), _bison_int64))
    var labels = List[String]()
    labels.append("a")
    labels.append("a")
    labels.append("b")
    labels.append("b")
    var result = s.groupby(labels).sum()
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(
        Python.evaluate("[10, 20, 30, 40]"), name="val"
    )
    var expected = pd_s.groupby(Python.evaluate("['a', 'a', 'b', 'b']")).sum()
    assert_series_equal(result.to_pandas(), expected)


def test_marrow_series_mean() raises:
    """Marrow fast path for SeriesGroupBy.mean()."""
    var vals = List[Float64]()
    vals.append(1.0)
    vals.append(3.0)
    vals.append(5.0)
    vals.append(7.0)
    var s = Series(Column("val", ColumnData(vals^), _bison_float64))
    var labels = List[String]()
    labels.append("a")
    labels.append("a")
    labels.append("b")
    labels.append("b")
    var result = s.groupby(labels).mean()
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(
        Python.evaluate("[1.0, 3.0, 5.0, 7.0]"), name="val"
    )
    var expected = pd_s.groupby(Python.evaluate("['a', 'a', 'b', 'b']")).mean()
    assert_series_equal(result.to_pandas(), expected)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
