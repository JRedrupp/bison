"""Tests for DataFrame.groupby(), DataFrameGroupBy, and SeriesGroupBy."""
from std.python import Python, PythonObject
from std.testing import TestSuite
from bison import DataFrame, Series


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
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).sum().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").sum())


def test_dataframegroupby_mean() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).mean().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").mean())


def test_dataframegroupby_min() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).min().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").min())


def test_dataframegroupby_max() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).max().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").max())


def test_dataframegroupby_count() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).count().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").count())


def test_dataframegroupby_nunique() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).nunique().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").nunique())


def test_dataframegroupby_first() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).first().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").first())


def test_dataframegroupby_last() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).last().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").last())


def test_dataframegroupby_size() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).size().to_pandas()
    testing.assert_series_equal(
        result, pd_df.groupby("grp").size()
    )


def test_dataframegroupby_std() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).std().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").std())


def test_dataframegroupby_var() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).var().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").var())


def test_dataframegroupby_agg() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).agg("sum").to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").agg("sum"))


def test_dataframegroupby_transform() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("sum").to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").transform("sum"))


def test_dataframegroupby_transform_dropna() raises:
    """transform() with dropna=True must not raise when key column has nulls."""
    var testing = Python.import_module("pandas.testing")
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
    testing.assert_frame_equal(result, expected, check_dtype=False)


def test_dataframegroupby_transform_std() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("std").to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").transform("std"), check_dtype=False)


def test_dataframegroupby_transform_var() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("var").to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").transform("var"), check_dtype=False)


def test_dataframegroupby_transform_count() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("count").to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").transform("count"), check_dtype=False)


def test_dataframegroupby_transform_first() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("first").to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").transform("first"), check_dtype=False)


def test_dataframegroupby_transform_last() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by).transform("last").to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby("grp").transform("last"), check_dtype=False)


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
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).sum().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).sum()
    )


def test_seriesgroupby_mean() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).mean().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).mean()
    )


def test_seriesgroupby_min() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).min().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).min()
    )


def test_seriesgroupby_max() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).max().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).max()
    )


def test_seriesgroupby_count() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).count().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).count()
    )


def test_seriesgroupby_size() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).size().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).size()
    )


def test_seriesgroupby_agg() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).agg("sum").to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).agg("sum")
    )


def test_seriesgroupby_nunique() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).nunique().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).nunique()
    )


def test_seriesgroupby_std() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).std().to_pandas()
    # check_dtype=False: native returns Float64; pandas returns Float64 too,
    # but check_dtype ensures no accidental int comparison
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).std(), check_dtype=False
    )


def test_seriesgroupby_var() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).var().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).var(), check_dtype=False
    )


def test_seriesgroupby_first() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).first().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).first()
    )


def test_seriesgroupby_last() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).last().to_pandas()
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).last()
    )


def test_seriesgroupby_transform() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("sum").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("sum"),
    )


def test_seriesgroupby_transform_mean() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("mean").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("mean"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_min() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("min").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("min"),
    )


def test_seriesgroupby_transform_max() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("max").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("max"),
    )


def test_seriesgroupby_transform_std() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("std").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("std"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_var() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("var").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("var"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_count() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("count").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("count"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_size() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("size").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("size"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_first() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("first").to_pandas()
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("first"),
        check_dtype=False,
    )


def test_seriesgroupby_transform_last() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).transform("last").to_pandas()
    testing.assert_series_equal(
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
    var testing = Python.import_module("pandas.testing")
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
    testing.assert_frame_equal(result, pd_df.groupby("grp").sum())


def test_dataframegroupby_float_key_natural_sort() raises:
    """Groupby with Float64 key column must order groups numerically, not lexicographically.

    Keys 1.5, 2.0, 10.5: lex sort produces "1.5","10.5","2.0"; natural gives 1.5,2.0,10.5.
    The result index must be a float64 pandas Index, matching the pandas reference exactly.
    """
    var testing = Python.import_module("pandas.testing")
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
    testing.assert_frame_equal(
        result,
        pd_df.groupby("grp").sum(),
    )


def test_seriesgroupby_dropna_sum() raises:
    """Dropna=True must exclude null-labelled rows from all groups."""
    var testing = Python.import_module("pandas.testing")
    var pd = Python.import_module("pandas")
    # Series with 4 rows; row 1 has a null label.
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]"), name="val")
    var s = Series(pd_s, "val")
    var by = List[String]()
    by.append("a")
    by.append("a")  # string value is ignored because null_mask marks this row as null
    by.append("b")
    by.append("b")
    var null_mask = List[Bool]()
    null_mask.append(False)
    null_mask.append(True)  # row 1 is null-labelled
    null_mask.append(False)
    null_mask.append(False)
    # dropna=True (the default): null-labelled row should be excluded.
    var result = s.groupby(by, dropna=True, by_null_mask=null_mask).sum()
    var result_pd = result.to_pandas()
    var py_labels = Python.evaluate("['a', None, 'b', 'b']")
    var expected = pd_s.groupby(py_labels, dropna=True).sum()
    testing.assert_series_equal(result_pd, expected)


def test_seriesgroupby_dropna_transform_sum() raises:
    """Transform('sum') with dropna=True must emit NaN for null-labelled rows."""
    var testing = Python.import_module("pandas.testing")
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]"), name="val")
    var s = Series(pd_s, "val")
    var by = List[String]()
    by.append("a")
    by.append("a")
    by.append("b")
    by.append("b")
    var null_mask = List[Bool]()
    null_mask.append(False)
    null_mask.append(True)
    null_mask.append(False)
    null_mask.append(False)
    var result = s.groupby(by, dropna=True, by_null_mask=null_mask).transform(
        "sum"
    )
    var result_pd = result.to_pandas()
    var py_labels = Python.evaluate("['a', None, 'b', 'b']")
    var expected = pd_s.groupby(py_labels, dropna=True).transform("sum")
    testing.assert_series_equal(result_pd, expected, check_dtype=False)


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
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).sum().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).sum())


def test_dataframegroupby_multikey_mean() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).mean().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).mean())


def test_dataframegroupby_multikey_min() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).min().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).min())


def test_dataframegroupby_multikey_max() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).max().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).max())


def test_dataframegroupby_multikey_count() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).count().to_pandas()
    testing.assert_frame_equal(
        result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).count()
    )


def test_dataframegroupby_multikey_nunique() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).nunique().to_pandas()
    testing.assert_frame_equal(
        result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).nunique()
    )


def test_dataframegroupby_multikey_first() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).first().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).first())


def test_dataframegroupby_multikey_last() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).last().to_pandas()
    testing.assert_frame_equal(result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).last())


def test_dataframegroupby_multikey_size() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by).size().to_pandas()
    testing.assert_series_equal(
        result, pd_df.groupby(Python.evaluate("['grp1', 'grp2']")).size()
    )


# ------------------------------------------------------------------
# as_index=False tests
# ------------------------------------------------------------------


def test_dataframegroupby_as_index_false_sum() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by, as_index=False).sum().to_pandas()
    testing.assert_frame_equal(
        result, pd_df.groupby("grp", as_index=False).sum(), check_dtype=False
    )


def test_dataframegroupby_as_index_false_mean() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by, as_index=False).mean().to_pandas()
    testing.assert_frame_equal(
        result,
        pd_df.groupby("grp", as_index=False).mean(),
        check_dtype=False,
    )


def test_dataframegroupby_as_index_false_count() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp")
    var result = df.groupby(by, as_index=False).count().to_pandas()
    testing.assert_frame_equal(
        result,
        pd_df.groupby("grp", as_index=False).count(),
        check_dtype=False,
    )


def test_dataframegroupby_multikey_as_index_false_sum() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df_multi()
    var df = DataFrame(pd_df)
    var by = List[String]()
    by.append("grp1")
    by.append("grp2")
    var result = df.groupby(by, as_index=False).sum().to_pandas()
    testing.assert_frame_equal(
        result,
        pd_df.groupby(
            Python.evaluate("['grp1', 'grp2']"), as_index=False
        ).sum(),
        check_dtype=False,
    )


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
