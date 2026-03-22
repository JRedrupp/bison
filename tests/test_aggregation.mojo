"""Tests aggregation methods."""
from std.python import Python
from testing import assert_true, TestSuite
from math import isnan
from bison import DataFrame, Series


# ---------------------------------------------------------------------------
# sum
# ---------------------------------------------------------------------------

def test_series_sum_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.sum() == 6.0)


def test_series_sum_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.5, 2.5]")))
    assert_true(s.sum() == 4.0)


def test_series_sum_skipna_flag() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0]")))
    # No nulls: skipna=False still returns the full sum.
    assert_true(s.sum(skipna=False) == 3.0)


def test_series_sum_skipna_true_nan() raises:
    """NaN values are skipped when skipna=True (default); returns sum of the rest."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(s.sum(skipna=True) == 4.0)


def test_series_sum_skipna_false_nan() raises:
    """Returns NaN when any element is null/NaN and skipna=False."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    var result = s.sum(skipna=False)
    assert_true(isnan(result))


def test_df_sum() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [1.5, 2.5, 3.5]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.sum().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 6.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 7.5)


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------

def test_series_count_no_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.count() == 3)


def test_series_count_with_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(s.count() == 2)


def test_df_count() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [1.0, float('nan'), 3.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.count().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 2.0)


# ---------------------------------------------------------------------------
# mean
# ---------------------------------------------------------------------------

def test_series_mean_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.mean() == 2.0)


def test_series_mean_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    assert_true(s.mean() == 2.0)


def test_series_mean_skipna_true() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(s.mean(skipna=True) == 2.0)


def test_series_mean_skipna_false_nan() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(isnan(s.mean(skipna=False)))


def test_df_mean() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.mean().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 5.0)


# ---------------------------------------------------------------------------
# min / max
# ---------------------------------------------------------------------------

def test_series_min_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    assert_true(s.min() == 1.0)


def test_series_max_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    assert_true(s.max() == 3.0)


def test_series_min_skipna() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(s.min(skipna=True) == 1.0)
    assert_true(isnan(s.min(skipna=False)))


def test_df_min() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3.0, 1.0, 2.0], 'b': [10.0, 5.0, 8.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.min().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 1.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 5.0)


def test_df_max() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3.0, 1.0, 2.0], 'b': [10.0, 5.0, 8.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.max().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 10.0)


# ---------------------------------------------------------------------------
# std / var
# ---------------------------------------------------------------------------

def test_series_std() raises:
    var pd = Python.import_module("pandas")
    # [1, 2, 3]: mean=2, sum_sq_dev=2, var(ddof=1)=1.0, std=1.0
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var result = s.std()
    assert_true(result - 1.0 < 1e-10 and 1.0 - result < 1e-10)


def test_series_var() raises:
    var pd = Python.import_module("pandas")
    # [1, 2, 3]: mean=2, sum_sq_dev=2, var(ddof=1)=1.0
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var result = s.var()
    assert_true(result - 1.0 < 1e-10 and 1.0 - result < 1e-10)


def test_series_std_single_element() raises:
    """std of a single-element series with ddof=1 should be NaN."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[5.0]")))
    assert_true(isnan(s.std()))


def test_df_std() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}"))
    var df = DataFrame(pd_df)
    var result = df.std()
    var result_pd = result.to_pandas()
    assert_true(abs(Float64(String(result_pd.iloc[0])) - 1.0) < 1e-10)


# ---------------------------------------------------------------------------
# nunique
# ---------------------------------------------------------------------------

def test_series_nunique_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 1, 2, 3, 3]")))
    assert_true(s.nunique() == 3)


def test_series_nunique_with_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 2.0, 1.0]")))
    # nulls excluded from unique count
    assert_true(s.nunique() == 2)


def test_df_nunique() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 1, 2], 'b': [1.0, 2.0, 3.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.nunique().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 3.0)


# ---------------------------------------------------------------------------
# median / quantile
# ---------------------------------------------------------------------------

def test_series_median_odd() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    assert_true(s.median() == 2.0)


def test_series_median_even() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    assert_true(s.median() == 2.5)


def test_series_median_skipna_false() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    assert_true(isnan(s.median(skipna=False)))


def test_series_quantile_25() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    assert_true(s.quantile(0.25) == 1.75)


def test_series_quantile_75() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    assert_true(s.quantile(0.75) == 3.25)


def test_df_median() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.median().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 5.0)


# ---------------------------------------------------------------------------
# describe (still a stub)
# ---------------------------------------------------------------------------

def test_df_describe_stub() raises:
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
# value_counts
# ---------------------------------------------------------------------------

def test_series_value_counts() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 2, 3, 3, 3]")))
    var vc = s.value_counts()
    # 3 unique values; first element should have the highest count (3)
    assert_true(vc.size() == 3)
    assert_true(vc.to_pandas().iloc[0] == 3)


# ---------------------------------------------------------------------------
# cumsum
# ---------------------------------------------------------------------------

def test_series_cumsum_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var result_pd = s.cumsum().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 1.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[2])) == 6.0)


def test_series_cumsum_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var result_pd = s.cumsum().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 1.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[2])) == 6.0)


def test_series_cumsum_skipna_true() raises:
    """Null elements produce NaN but do not break the running sum."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    var result_pd = s.cumsum().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 1.0)
    assert_true(isnan(Float64(String(result_pd.iloc[1]))))
    assert_true(Float64(String(result_pd.iloc[2])) == 4.0)


def test_series_cumsum_skipna_false() raises:
    """Once a null is encountered, all subsequent values are NaN."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, float('nan'), 3.0]")))
    var result_pd = s.cumsum(skipna=False).to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 1.0)
    assert_true(isnan(Float64(String(result_pd.iloc[1]))))
    assert_true(isnan(Float64(String(result_pd.iloc[2]))))


def test_df_cumsum() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.cumsum().to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == 1.0)
    assert_true(Float64(String(result_pd["a"].iloc[1])) == 3.0)
    assert_true(Float64(String(result_pd["a"].iloc[2])) == 6.0)
    assert_true(Float64(String(result_pd["b"].iloc[0])) == 4.0)
    assert_true(Float64(String(result_pd["b"].iloc[2])) == 15.0)


# ---------------------------------------------------------------------------
# cumprod
# ---------------------------------------------------------------------------

def test_series_cumprod_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var result_pd = s.cumprod().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 1.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 2.0)
    assert_true(Float64(String(result_pd.iloc[2])) == 6.0)


def test_series_cumprod_skipna_true() raises:
    """Null elements produce NaN but do not break the running product."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[2.0, float('nan'), 3.0]")))
    var result_pd = s.cumprod().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 2.0)
    assert_true(isnan(Float64(String(result_pd.iloc[1]))))
    assert_true(Float64(String(result_pd.iloc[2])) == 6.0)


def test_df_cumprod() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 4]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.cumprod().to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == 1.0)
    assert_true(Float64(String(result_pd["a"].iloc[1])) == 2.0)
    assert_true(Float64(String(result_pd["a"].iloc[2])) == 8.0)


# ---------------------------------------------------------------------------
# cummin
# ---------------------------------------------------------------------------

def test_series_cummin_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    var result_pd = s.cummin().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 1.0)
    assert_true(Float64(String(result_pd.iloc[2])) == 1.0)


def test_series_cummin_skipna_true() raises:
    """Null elements produce NaN but do not break the running minimum."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, float('nan'), 1.0]")))
    var result_pd = s.cummin().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(isnan(Float64(String(result_pd.iloc[1]))))
    assert_true(Float64(String(result_pd.iloc[2])) == 1.0)


def test_df_cummin() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3.0, 1.0, 2.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.cummin().to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd["a"].iloc[1])) == 1.0)
    assert_true(Float64(String(result_pd["a"].iloc[2])) == 1.0)


# ---------------------------------------------------------------------------
# cummax
# ---------------------------------------------------------------------------

def test_series_cummax_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 5]")))
    var result_pd = s.cummax().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[2])) == 5.0)


def test_series_cummax_skipna_true() raises:
    """Null elements produce NaN but do not break the running maximum."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, float('nan'), 5.0]")))
    var result_pd = s.cummax().to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(isnan(Float64(String(result_pd.iloc[1]))))
    assert_true(Float64(String(result_pd.iloc[2])) == 5.0)


def test_df_cummax() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3.0, 1.0, 5.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.cummax().to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd["a"].iloc[1])) == 3.0)
    assert_true(Float64(String(result_pd["a"].iloc[2])) == 5.0)


# ---------------------------------------------------------------------------
# axis=1 stubs — all must raise with "not implemented"
# ---------------------------------------------------------------------------

def test_df_sum_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.sum(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.sum axis=1 should have raised")


def test_df_mean_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.mean(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.mean axis=1 should have raised")


def test_df_median_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.median(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.median axis=1 should have raised")


def test_df_min_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.min(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.min axis=1 should have raised")


def test_df_max_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.max(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.max axis=1 should have raised")


def test_df_std_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.std(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.std axis=1 should have raised")


def test_df_var_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.var(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.var axis=1 should have raised")


def test_df_count_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.count(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.count axis=1 should have raised")


def test_df_nunique_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.nunique(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.nunique axis=1 should have raised")


def test_df_quantile_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.quantile(0.5, axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.quantile axis=1 should have raised")


def test_df_cumsum_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.cumsum(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.cumsum axis=1 should have raised")


def test_df_cumprod_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.cumprod(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.cumprod axis=1 should have raised")


def test_df_cummin_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.cummin(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.cummin axis=1 should have raised")


def test_df_cummax_axis1_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var raised = False
    try:
        _ = df.cummax(axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.cummax axis=1 should have raised")


# ---------------------------------------------------------------------------
# sem — stub
# ---------------------------------------------------------------------------

def test_series_sem_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var raised = False
    try:
        _ = s.sem()
    except:
        raised = True
    if not raised:
        raise Error("Series.sem should have raised")


def test_df_sem_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var raised = False
    try:
        _ = df.sem()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.sem should have raised")


# ---------------------------------------------------------------------------
# skew — stub
# ---------------------------------------------------------------------------

def test_series_skew_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var raised = False
    try:
        _ = s.skew()
    except:
        raised = True
    if not raised:
        raise Error("Series.skew should have raised")


def test_df_skew_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var raised = False
    try:
        _ = df.skew()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.skew should have raised")


# ---------------------------------------------------------------------------
# kurt — stub
# ---------------------------------------------------------------------------

def test_series_kurt_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var raised = False
    try:
        _ = s.kurt()
    except:
        raised = True
    if not raised:
        raise Error("Series.kurt should have raised")


def test_df_kurt_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var raised = False
    try:
        _ = df.kurt()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.kurt should have raised")


# ---------------------------------------------------------------------------
# idxmin / idxmax — stubs
# ---------------------------------------------------------------------------

def test_series_idxmin_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, 1.0, 2.0]")))
    var raised = False
    try:
        _ = s.idxmin()
    except:
        raised = True
    if not raised:
        raise Error("Series.idxmin should have raised")


def test_series_idxmax_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, 1.0, 2.0]")))
    var raised = False
    try:
        _ = s.idxmax()
    except:
        raised = True
    if not raised:
        raise Error("Series.idxmax should have raised")


def test_df_idxmin_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3.0, 1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.idxmin()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.idxmin should have raised")


def test_df_idxmax_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3.0, 1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.idxmax()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.idxmax should have raised")


# ---------------------------------------------------------------------------
# corr / cov — stubs
# ---------------------------------------------------------------------------

def test_series_corr_raises() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[4.0, 5.0, 6.0]")))
    var raised = False
    try:
        _ = s1.corr(s2)
    except:
        raised = True
    if not raised:
        raise Error("Series.corr should have raised")


def test_series_cov_raises() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[4.0, 5.0, 6.0]")))
    var raised = False
    try:
        _ = s1.cov(s2)
    except:
        raised = True
    if not raised:
        raise Error("Series.cov should have raised")


def test_df_corr_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}")))
    var raised = False
    try:
        _ = df.corr()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.corr should have raised")


def test_df_cov_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}")))
    var raised = False
    try:
        _ = df.cov()
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.cov should have raised")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
