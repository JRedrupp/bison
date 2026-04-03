"""Tests functional methods: abs, agg, aggregate, apply, transform, eval, query, pipe, applymap."""
from std.python import Python
from std.testing import assert_true, TestSuite
from bison import DataFrame, Series
from _helpers import assert_frame_equal, assert_series_equal


# ---------------------------------------------------------------------------
# abs
# ---------------------------------------------------------------------------

def test_df_abs() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [-1, -2, 3], 'b': [4.0, -5.0, -6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.abs().to_pandas()
    var expected_pd = pd_df.abs()
    assert_frame_equal(result_pd, expected_pd)


# ---------------------------------------------------------------------------
# agg / aggregate
# ---------------------------------------------------------------------------

def test_df_agg_sum() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.agg("sum").to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 6.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 15.0)


def test_df_agg_mean() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.agg("mean").to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 5.0)


def test_df_agg_min() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.agg("min").to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 1.0)


def test_df_agg_max() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3, 1, 2]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.agg("max").to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)


def test_df_agg_count() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.agg("count").to_pandas()
    assert_true(Float64(String(result_pd.iloc[0])) == 3.0)
    assert_true(Float64(String(result_pd.iloc[1])) == 3.0)


def test_df_agg_unknown_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.agg("foo")
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.agg with unknown func should have raised")


def test_df_aggregate_delegates() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var agg_pd = df.agg("sum").to_pandas()
    var aggregate_pd = df.aggregate("sum").to_pandas()
    assert_series_equal(agg_pd, aggregate_pd)


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------

def test_df_apply_axis0_raises_redirect() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    var msg = String("")
    try:
        _ = df.apply("sum", axis=0)
    except e:
        raised = True
        msg = String(e)
    if not raised:
        raise Error("DataFrame.apply axis=0 should have raised")
    assert_true("not implemented" in msg)


def test_df_apply_axis1_not_implemented() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.apply("sum", axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.apply axis=1 should have raised")


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------

def test_df_transform_abs() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [-1, -2, 3], 'b': [4.0, -5.0, -6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("abs").to_pandas()
    var expected_pd = pd_df.abs()
    assert_frame_equal(result_pd, expected_pd)


def test_df_transform_unknown_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var raised = False
    try:
        _ = df.transform("log")
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.transform with unsupported func should have raised")


def test_df_transform_cumsum() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("cumsum").to_pandas()
    var expected_pd = pd_df.transform("cumsum")
    assert_frame_equal(result_pd, expected_pd)


def test_df_transform_cumprod() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("cumprod").to_pandas()
    var expected_pd = pd_df.transform("cumprod")
    assert_frame_equal(result_pd, expected_pd)


def test_df_transform_cummin() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3, 1, 2], 'b': [6, 4, 5]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("cummin").to_pandas()
    var expected_pd = pd_df.transform("cummin")
    assert_frame_equal(result_pd, expected_pd)


def test_df_transform_cummax() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 3, 2], 'b': [4, 6, 5]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("cummax").to_pandas()
    var expected_pd = pd_df.transform("cummax")
    assert_frame_equal(result_pd, expected_pd)


# ---------------------------------------------------------------------------
# eval
# ---------------------------------------------------------------------------

def test_df_eval_simple() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [10, 20, 30]}"))
    var df = DataFrame(pd_df)
    var result = df.eval("a + b")
    # Expected: element-wise sum a+b = [11, 22, 33]
    assert_true(Float64(String(result.to_pandas().iloc[0])) == 11.0)
    assert_true(Float64(String(result.to_pandas().iloc[1])) == 22.0)
    assert_true(Float64(String(result.to_pandas().iloc[2])) == 33.0)


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------

def test_df_query_simple() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4], 'b': [10, 20, 30, 40]}"))
    var df = DataFrame(pd_df)
    var result = df.query("a > 2")
    # Expected: rows where a > 2, i.e., a=[3,4] and b=[30,40]
    assert_true(result.shape()[0] == 2)
    assert_true(result.shape()[1] == 2)
    var a_col_pd = result["a"].to_pandas()
    assert_true(Float64(String(a_col_pd.iloc[0])) == 3.0)
    assert_true(Float64(String(a_col_pd.iloc[1])) == 4.0)


# ---------------------------------------------------------------------------
# pipe / applymap — still stubs
# ---------------------------------------------------------------------------

def test_df_pipe_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.pipe("some_fn")
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.pipe should have raised")


def test_df_applymap_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.applymap("str")
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.applymap should have raised")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
