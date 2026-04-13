"""Tests for DataFrame.rolling(), .expanding(), .ewm() and their Series variants."""
from std.python import Python, PythonObject
from std.testing import assert_equal, TestSuite
from bison import DataFrame, Series


def _make_pd_df() raises -> PythonObject:
    var pd = Python.import_module("pandas")
    return pd.DataFrame(
        Python.evaluate(
            "{'a': [1.0, 2.0, 3.0, 4.0, 5.0], 'b': [10, 20, 30, 40, 50]}"
        )
    )


def _assert_frame_close(
    left: PythonObject, right: PythonObject
) raises:
    """Assert two pandas DataFrames are close (allows floating-point tolerance)."""
    var pd = Python.import_module("pandas")
    pd.testing.assert_frame_equal(left, right, check_dtype=False)


def _assert_series_close(
    left: PythonObject, right: PythonObject
) raises:
    """Assert two pandas Series are close (allows floating-point tolerance)."""
    var pd = Python.import_module("pandas")
    pd.testing.assert_series_equal(left, right, check_dtype=False)


# ------------------------------------------------------------------
# DataFrame.rolling() tests
# ------------------------------------------------------------------


def test_rolling_mean() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(3).mean().to_pandas()
    var expected = pd_df.rolling(3).mean()
    _assert_frame_close(result, expected)


def test_rolling_sum() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(3).sum().to_pandas()
    var expected = pd_df.rolling(3).sum()
    _assert_frame_close(result, expected)


def test_rolling_min() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(3).min().to_pandas()
    var expected = pd_df.rolling(3).min()
    _assert_frame_close(result, expected)


def test_rolling_max() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(3).max().to_pandas()
    var expected = pd_df.rolling(3).max()
    _assert_frame_close(result, expected)


def test_rolling_count() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(3).count().to_pandas()
    var expected = pd_df.rolling(3).count()
    _assert_frame_close(result, expected)


def test_rolling_std() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(3).std().to_pandas()
    var expected = pd_df.rolling(3).std()
    _assert_frame_close(result, expected)


def test_rolling_var() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(3).var().to_pandas()
    var expected = pd_df.rolling(3).var()
    _assert_frame_close(result, expected)


def test_rolling_window_2() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(2).mean().to_pandas()
    var expected = pd_df.rolling(2).mean()
    _assert_frame_close(result, expected)


def test_rolling_min_periods() raises:
    var pd = Python.import_module("pandas")
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(3, min_periods=1).mean().to_pandas()
    var expected = pd_df.rolling(3, min_periods=1).mean()
    _assert_frame_close(result, expected)


# ------------------------------------------------------------------
# Series.rolling() tests
# ------------------------------------------------------------------


def test_series_rolling_mean() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]"), name="x")
    var s = Series(pd_s, "x")
    var result = s.rolling(3).mean().to_pandas()
    var expected = pd_s.rolling(3).mean()
    _assert_series_close(result, expected)


def test_series_rolling_sum() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]"), name="x")
    var s = Series(pd_s, "x")
    var result = s.rolling(3).sum().to_pandas()
    var expected = pd_s.rolling(3).sum()
    _assert_series_close(result, expected)


def test_series_rolling_min() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[5.0, 3.0, 1.0, 4.0, 2.0]"), name="x")
    var s = Series(pd_s, "x")
    var result = s.rolling(3).min().to_pandas()
    var expected = pd_s.rolling(3).min()
    _assert_series_close(result, expected)


def test_series_rolling_max() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 3.0, 5.0, 2.0, 4.0]"), name="x")
    var s = Series(pd_s, "x")
    var result = s.rolling(3).max().to_pandas()
    var expected = pd_s.rolling(3).max()
    _assert_series_close(result, expected)


def test_series_rolling_std() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]"), name="x")
    var s = Series(pd_s, "x")
    var result = s.rolling(3).std().to_pandas()
    var expected = pd_s.rolling(3).std()
    _assert_series_close(result, expected)


# ------------------------------------------------------------------
# DataFrame.expanding() tests
# ------------------------------------------------------------------


def test_expanding_mean() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.expanding().mean().to_pandas()
    var expected = pd_df.expanding().mean()
    _assert_frame_close(result, expected)


def test_expanding_sum() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.expanding().sum().to_pandas()
    var expected = pd_df.expanding().sum()
    _assert_frame_close(result, expected)


def test_expanding_min() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.expanding().min().to_pandas()
    var expected = pd_df.expanding().min()
    _assert_frame_close(result, expected)


def test_expanding_max() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.expanding().max().to_pandas()
    var expected = pd_df.expanding().max()
    _assert_frame_close(result, expected)


def test_expanding_count() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.expanding().count().to_pandas()
    var expected = pd_df.expanding().count()
    _assert_frame_close(result, expected)


def test_expanding_std() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.expanding().std().to_pandas()
    var expected = pd_df.expanding().std()
    _assert_frame_close(result, expected)


def test_expanding_var() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.expanding().var().to_pandas()
    var expected = pd_df.expanding().var()
    _assert_frame_close(result, expected)


def test_expanding_min_periods() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.expanding(min_periods=3).mean().to_pandas()
    var expected = pd_df.expanding(min_periods=3).mean()
    _assert_frame_close(result, expected)


# ------------------------------------------------------------------
# Series.expanding() tests
# ------------------------------------------------------------------


def test_series_expanding_mean() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]"), name="x")
    var s = Series(pd_s, "x")
    var result = s.expanding().mean().to_pandas()
    var expected = pd_s.expanding().mean()
    _assert_series_close(result, expected)


def test_series_expanding_sum() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]"), name="x")
    var s = Series(pd_s, "x")
    var result = s.expanding().sum().to_pandas()
    var expected = pd_s.expanding().sum()
    _assert_series_close(result, expected)


# ------------------------------------------------------------------
# DataFrame.ewm() tests
# ------------------------------------------------------------------


def test_ewm_mean_span() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.ewm(span=3.0).mean().to_pandas()
    var expected = pd_df.ewm(span=3).mean()
    _assert_frame_close(result, expected)


def test_ewm_mean_com() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.ewm(com=1.0).mean().to_pandas()
    var expected = pd_df.ewm(com=1.0).mean()
    _assert_frame_close(result, expected)


def test_ewm_mean_alpha() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.ewm(alpha=0.5).mean().to_pandas()
    var expected = pd_df.ewm(alpha=0.5).mean()
    _assert_frame_close(result, expected)


# ------------------------------------------------------------------
# Series.ewm() tests
# ------------------------------------------------------------------


def test_series_ewm_mean() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]"), name="x")
    var s = Series(pd_s, "x")
    var result = s.ewm(span=3.0).mean().to_pandas()
    var expected = pd_s.ewm(span=3).mean()
    _assert_series_close(result, expected)


# ------------------------------------------------------------------
# Edge cases
# ------------------------------------------------------------------


def test_rolling_window_equals_len() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(5).mean().to_pandas()
    var expected = pd_df.rolling(5).mean()
    _assert_frame_close(result, expected)


def test_rolling_window_exceeds_len() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(10).mean().to_pandas()
    var expected = pd_df.rolling(10).mean()
    _assert_frame_close(result, expected)


def test_rolling_window_1() raises:
    var pd_df = _make_pd_df()
    var df = DataFrame(pd_df)
    var result = df.rolling(1).mean().to_pandas()
    var expected = pd_df.rolling(1).mean()
    _assert_frame_close(result, expected)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
