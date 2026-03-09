"""Tests aggregation methods."""
from python import Python
from testing import assert_true, TestSuite
from math import isnan
from bison import DataFrame, Series


# ---------------------------------------------------------------------------
# sum
# ---------------------------------------------------------------------------

def test_series_sum_int():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.sum() == 6.0)


def test_series_sum_float():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.5, 2.5]")))
    assert_true(s.sum() == 4.0)


def test_series_sum_skipna_flag():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0]")))
    # No nulls: skipna=False still returns the full sum.
    assert_true(s.sum(skipna=False) == 3.0)


def test_series_sum_skipna_true_nan():
    """NaN values are skipped when skipna=True (default); returns sum of the rest."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(s.sum(skipna=True) == 4.0)


def test_series_sum_skipna_false_nan():
    """Returns NaN when any element is null/NaN and skipna=False."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    var result = s.sum(skipna=False)
    assert_true(isnan(result))


def test_df_sum():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [1.5, 2.5, 3.5]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.sum().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 6.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 7.5)


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------

def test_series_count_no_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.count() == 3)


def test_series_count_with_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(s.count() == 2)


def test_df_count():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [1.0, float('nan'), 3.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.count().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 2.0)


# ---------------------------------------------------------------------------
# mean
# ---------------------------------------------------------------------------

def test_series_mean_int():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.mean() == 2.0)


def test_series_mean_float():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    assert_true(s.mean() == 2.0)


def test_series_mean_skipna_true():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(s.mean(skipna=True) == 2.0)


def test_series_mean_skipna_false_nan():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(isnan(s.mean(skipna=False)))


def test_df_mean():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.mean().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 5.0)


# ---------------------------------------------------------------------------
# min / max
# ---------------------------------------------------------------------------

def test_series_min_int():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    assert_true(s.min() == 1.0)


def test_series_max_int():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    assert_true(s.max() == 3.0)


def test_series_min_skipna():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(s.min(skipna=True) == 1.0)
    assert_true(isnan(s.min(skipna=False)))


def test_df_min():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3.0, 1.0, 2.0], 'b': [10.0, 5.0, 8.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.min().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 1.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 5.0)


def test_df_max():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3.0, 1.0, 2.0], 'b': [10.0, 5.0, 8.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.max().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 10.0)


# ---------------------------------------------------------------------------
# std / var
# ---------------------------------------------------------------------------

def test_series_std():
    var pd = Python.import_module("pandas")
    # [1, 2, 3]: mean=2, sum_sq_dev=2, var(ddof=1)=1.0, std=1.0
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var result = s.std()
    assert_true(result - 1.0 < 1e-10 and 1.0 - result < 1e-10)


def test_series_var():
    var pd = Python.import_module("pandas")
    # [1, 2, 3]: mean=2, sum_sq_dev=2, var(ddof=1)=1.0
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var result = s.var()
    assert_true(result - 1.0 < 1e-10 and 1.0 - result < 1e-10)


def test_series_std_single_element():
    """std of a single-element series with ddof=1 should be NaN."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[5.0]")))
    assert_true(isnan(s.std()))


def test_df_std():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}"))
    var df = DataFrame(pd_df)
    var result = df.std()
    var result_pd = result.to_pandas()
    assert_true(abs(Float64(String(result_pd.iloc[0])) - 1.0) < 1e-10)


# ---------------------------------------------------------------------------
# nunique
# ---------------------------------------------------------------------------

def test_series_nunique_int():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 1, 2, 3, 3]")))
    assert_true(s.nunique() == 3)


def test_series_nunique_with_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 2.0, 1.0]")))
    # nulls excluded from unique count
    assert_true(s.nunique() == 2)


def test_df_nunique():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 1, 2], 'b': [1.0, 2.0, 3.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.nunique().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 3.0)


# ---------------------------------------------------------------------------
# median / quantile
# ---------------------------------------------------------------------------

def test_series_median_odd():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    assert_true(s.median() == 2.0)


def test_series_median_even():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    assert_true(s.median() == 2.5)


def test_series_median_skipna_false():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(isnan(s.median(skipna=False)))


def test_series_quantile_25():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    assert_true(s.quantile(0.25) == 1.75)


def test_series_quantile_75():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    assert_true(s.quantile(0.75) == 3.25)


def test_df_median():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.median().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 5.0)


# ---------------------------------------------------------------------------
# describe (still a stub)
# ---------------------------------------------------------------------------

def test_df_describe_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.describe()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.describe should have raised")


# ---------------------------------------------------------------------------
# value_counts (still a stub)
# ---------------------------------------------------------------------------

def test_series_value_counts_stub():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['a', 'b', 'a']")))
    var raised = False
    try:
        _ = s.value_counts()
    except:
        raised = True
    if not raised:
        raise Error("Series.value_counts should have raised")


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
