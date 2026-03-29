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
    # check_names=False: Series.from_pandas converts None name to "None" string
    testing.assert_series_equal(
        result, pd_df.groupby("grp").size(), check_names=False
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
    # check_dtype=False: native returns Float64; pandas returns Int64
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).sum(), check_dtype=False
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
    # check_dtype=False: native returns Float64; pandas returns Int64
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).min(), check_dtype=False
    )


def test_seriesgroupby_max() raises:
    var testing = Python.import_module("pandas.testing")
    var pd_df = _make_pd_df()
    var s = Series(pd_df["val"], "val")
    var result = s.groupby(_mojo_labels()).max().to_pandas()
    # check_dtype=False: native returns Float64; pandas returns Int64
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).max(), check_dtype=False
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
    # check_dtype=False: dispatches to sum() which returns Float64
    testing.assert_series_equal(
        result, pd_df["val"].groupby(_pd_labels()).agg("sum"), check_dtype=False
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
    # check_dtype=False: native returns Float64; pandas returns Int64
    testing.assert_series_equal(
        result,
        pd_df["val"].groupby(_pd_labels()).transform("sum"),
        check_dtype=False,
    )


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
