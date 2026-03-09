"""Tests aggregation methods (sum implemented; others are stubs)."""
from python import Python
from testing import assert_true
from math import isnan
from bison import DataFrame, Series


def _raises(name: String, raised: Bool):
    if not raised:
        raise Error(name + " should have raised")


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


def test_df_mean_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.mean()
    except:
        raised = True
    _raises("DataFrame.mean", raised)


def test_df_describe_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.describe()
    except:
        raised = True
    _raises("DataFrame.describe", raised)


def test_series_mean_stub():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var raised = False
    try:
        _ = s.mean()
    except:
        raised = True
    _raises("Series.mean", raised)


def test_series_value_counts_stub():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['a', 'b', 'a']")))
    var raised = False
    try:
        _ = s.value_counts()
    except:
        raised = True
    _raises("Series.value_counts", raised)


def main():
    test_series_sum_int()
    test_series_sum_float()
    test_series_sum_skipna_flag()
    test_series_sum_skipna_true_nan()
    test_series_sum_skipna_false_nan()
    test_df_sum()
    test_df_mean_stub()
    test_df_describe_stub()
    test_series_mean_stub()
    test_series_value_counts_stub()
    print("test_aggregation: all tests passed")
