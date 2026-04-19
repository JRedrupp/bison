"""Tests functional methods: abs, agg, aggregate, apply, transform, eval, query, pipe, applymap."""
from std.python import Python
from std.testing import assert_true, TestSuite
from bison import DataFrame, Series, FloatTransformFn


# ---------------------------------------------------------------------------
# Compile-time helper functions for apply/applymap tests
# ---------------------------------------------------------------------------

def _double(v: Float64) -> Float64:
    return v * 2.0


def _square(v: Float64) -> Float64:
    return v * v


def _pipe_abs(d: DataFrame) raises -> DataFrame:
    return d.abs()


# ---------------------------------------------------------------------------
# abs
# ---------------------------------------------------------------------------

def test_df_abs() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [-1, -2, 3], 'b': [4.0, -5.0, -6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.abs().to_pandas()
    var expected_pd = pd_df.abs()
    assert_true(String(result_pd.equals(expected_pd)) == "True")


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
    assert_true(String(agg_pd.equals(aggregate_pd)) == "True")


# ---------------------------------------------------------------------------
# apply — string overload
# ---------------------------------------------------------------------------

def test_df_apply_sum_axis0() raises:
    """Verify apply('sum', axis=0) matches agg('sum')."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var apply_pd = df.apply("sum", axis=0).to_pandas()
    var agg_pd = df.agg("sum").to_pandas()
    assert_true(String(apply_pd.equals(agg_pd)) == "True")


def test_df_apply_mean_axis0() raises:
    """Verify apply('mean', axis=0) matches agg('mean')."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}")
    )
    var df = DataFrame(pd_df)
    var apply_pd = df.apply("mean", axis=0).to_pandas()
    var agg_pd = df.agg("mean").to_pandas()
    assert_true(String(apply_pd.equals(agg_pd)) == "True")


def test_df_apply_sum_axis1() raises:
    """Verify apply('sum', axis=1) matches sum(axis=1)."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var apply_pd = df.apply("sum", axis=1).to_pandas()
    var sum_pd = df.sum(axis=1).to_pandas()
    assert_true(Float64(String(apply_pd.iloc[0])) == Float64(String(sum_pd.iloc[0])))
    assert_true(Float64(String(apply_pd.iloc[1])) == Float64(String(sum_pd.iloc[1])))
    assert_true(Float64(String(apply_pd.iloc[2])) == Float64(String(sum_pd.iloc[2])))


def test_df_apply_min_axis1() raises:
    """Verify apply('min', axis=1) matches min(axis=1)."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3, 1, 2], 'b': [6, 4, 5]}"))
    var df = DataFrame(pd_df)
    var apply_pd = df.apply("min", axis=1).to_pandas()
    var min_pd = df.min(axis=1).to_pandas()
    assert_true(Float64(String(apply_pd.iloc[0])) == Float64(String(min_pd.iloc[0])))
    assert_true(Float64(String(apply_pd.iloc[1])) == Float64(String(min_pd.iloc[1])))


def test_df_apply_unknown_raises() raises:
    """Unknown func string should raise a helpful error."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.apply("foo", axis=1)
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.apply with unknown func should have raised")


# ---------------------------------------------------------------------------
# apply — compile-time overload
# ---------------------------------------------------------------------------

def test_df_apply_comptime_double() raises:
    """Verify apply[_double]() doubles all numeric values."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0], 'b': [4.0, 5.0, 6.0]}"))
    var df = DataFrame(pd_df)
    var result = df.apply[_double]()
    var result_pd = result.to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd["a"].iloc[1])) == 4.0)
    assert_true(Float64(String(result_pd["a"].iloc[2])) == 6.0)
    assert_true(Float64(String(result_pd["b"].iloc[0])) == 8.0)
    assert_true(Float64(String(result_pd["b"].iloc[1])) == 10.0)
    assert_true(Float64(String(result_pd["b"].iloc[2])) == 12.0)


def test_df_apply_comptime_square() raises:
    """Verify apply[_square]() squares all numeric values."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'x': [2.0, 3.0, 4.0]}"))
    var df = DataFrame(pd_df)
    var result = df.apply[_square]()
    var result_pd = result.to_pandas()
    assert_true(Float64(String(result_pd["x"].iloc[0])) == 4.0)
    assert_true(Float64(String(result_pd["x"].iloc[1])) == 9.0)
    assert_true(Float64(String(result_pd["x"].iloc[2])) == 16.0)


# ---------------------------------------------------------------------------
# applymap — string overload
# ---------------------------------------------------------------------------

def test_df_applymap_abs() raises:
    """Verify applymap('abs') matches abs()."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [-1, -2, 3], 'b': [4.0, -5.0, -6.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.applymap("abs").to_pandas()
    var expected_pd = df.abs().to_pandas()
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_applymap_unknown_raises() raises:
    """Unknown func string should raise a helpful error."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.applymap("foo")
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.applymap with unknown func should have raised")


# ---------------------------------------------------------------------------
# applymap — compile-time overload
# ---------------------------------------------------------------------------

def test_df_applymap_comptime() raises:
    """Verify applymap[_double]() works element-wise."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0], 'b': [3.0, 4.0]}"))
    var df = DataFrame(pd_df)
    var result = df.applymap[_double]()
    var result_pd = result.to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == 2.0)
    assert_true(Float64(String(result_pd["b"].iloc[1])) == 8.0)


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------

def test_df_transform_abs() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [-1, -2, 3], 'b': [4.0, -5.0, -6.0]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("abs").to_pandas()
    var expected_pd = pd_df.abs()
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_transform_unknown_raises() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0, 3.0]}")))
    var raised = False
    try:
        _ = df.transform("sin")
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
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_transform_cumprod() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("cumprod").to_pandas()
    var expected_pd = pd_df.transform("cumprod")
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_transform_cummin() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [3, 1, 2], 'b': [6, 4, 5]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("cummin").to_pandas()
    var expected_pd = pd_df.transform("cummin")
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_transform_cummax() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 3, 2], 'b': [4, 6, 5]}"))
    var df = DataFrame(pd_df)
    var result_pd = df.transform("cummax").to_pandas()
    var expected_pd = pd_df.transform("cummax")
    assert_true(String(result_pd.equals(expected_pd)) == "True")


# ---------------------------------------------------------------------------
# pipe — string overload
# ---------------------------------------------------------------------------

def test_df_pipe_abs() raises:
    """Verify pipe('abs') matches abs()."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [-1, -2, 3], 'b': [4.0, -5.0, -6.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.pipe("abs").to_pandas()
    var expected_pd = df.abs().to_pandas()
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_pipe_unknown_raises() raises:
    """Unknown func string should raise a helpful error."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.pipe("some_fn")
    except:
        raised = True
    if not raised:
        raise Error("DataFrame.pipe should have raised")


# ---------------------------------------------------------------------------
# pipe — compile-time overload
# ---------------------------------------------------------------------------

def test_df_pipe_comptime() raises:
    """Verify pipe[F]() with a compile-time function transforms the DataFrame."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [-1.0, -2.0, 3.0], 'b': [4.0, -5.0, -6.0]}")
    )
    var df = DataFrame(pd_df)
    var result = df.pipe[_pipe_abs]()
    var result_pd = result.to_pandas()
    var expected_pd = df.abs().to_pandas()
    assert_true(String(result_pd.equals(expected_pd)) == "True")


# ---------------------------------------------------------------------------
# applymap — new element-wise math operations (#606)
# ---------------------------------------------------------------------------

def test_df_applymap_sqrt() raises:
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, 4.0, 9.0], 'b': [16.0, 25.0, 36.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.applymap("sqrt").to_pandas()
    var expected_pd = pd_df.apply(np.sqrt)
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_applymap_exp() raises:
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [0.0, 1.0, 2.0], 'b': [0.0, -1.0, 0.5]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.applymap("exp").to_pandas()
    var expected_pd = pd_df.apply(np.exp)
    for col_name in ["a", "b"]:
        assert_true(
            String(
                np.allclose(result_pd[col_name].values, expected_pd[col_name].values)
            )
            == "True"
        )


def test_df_applymap_log() raises:
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, 2.718281828459045, 10.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.applymap("log").to_pandas()
    var expected_pd = pd_df.apply(np.log)
    assert_true(
        String(
            np.allclose(result_pd["a"].values, expected_pd["a"].values)
        )
        == "True"
    )


def test_df_applymap_log10() raises:
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, 10.0, 100.0], 'b': [1000.0, 0.1, 0.01]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.applymap("log10").to_pandas()
    var expected_pd = pd_df.apply(np.log10)
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_applymap_ceil() raises:
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.1, 2.5, 3.9], 'b': [-1.1, -2.5, -3.9]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.applymap("ceil").to_pandas()
    var expected_pd = pd_df.apply(np.ceil)
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_applymap_floor() raises:
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.1, 2.5, 3.9], 'b': [-1.1, -2.5, -3.9]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.applymap("floor").to_pandas()
    var expected_pd = pd_df.apply(np.floor)
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_applymap_neg() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, -2.0, 3.0], 'b': [-4.0, 5.0, -6.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.applymap("neg").to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == -1.0)
    assert_true(Float64(String(result_pd["a"].iloc[1])) == 2.0)
    assert_true(Float64(String(result_pd["b"].iloc[2])) == 6.0)


def test_df_applymap_negate_alias() raises:
    """Verify applymap('negate') is an alias for applymap('neg')."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, -2.0, 3.0]}")
    )
    var df = DataFrame(pd_df)
    var result_neg = df.applymap("neg").to_pandas()
    var result_negate = df.applymap("negate").to_pandas()
    assert_true(String(result_neg.equals(result_negate)) == "True")


# ---------------------------------------------------------------------------
# transform — new element-wise math operations (#606)
# ---------------------------------------------------------------------------

def test_df_transform_sqrt() raises:
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, 4.0, 9.0], 'b': [16.0, 25.0, 36.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.transform("sqrt").to_pandas()
    var expected_pd = pd_df.apply(np.sqrt)
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_transform_neg() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, -2.0, 3.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.transform("neg").to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == -1.0)
    assert_true(Float64(String(result_pd["a"].iloc[1])) == 2.0)
    assert_true(Float64(String(result_pd["a"].iloc[2])) == -3.0)


# ---------------------------------------------------------------------------
# pipe — new element-wise math operations (#606)
# ---------------------------------------------------------------------------

def test_df_pipe_sqrt() raises:
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, 4.0, 9.0], 'b': [16.0, 25.0, 36.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.pipe("sqrt").to_pandas()
    var expected_pd = pd_df.apply(np.sqrt)
    assert_true(String(result_pd.equals(expected_pd)) == "True")


def test_df_pipe_neg() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, -2.0, 3.0]}")
    )
    var df = DataFrame(pd_df)
    var result_pd = df.pipe("neg").to_pandas()
    assert_true(Float64(String(result_pd["a"].iloc[0])) == -1.0)
    assert_true(Float64(String(result_pd["a"].iloc[1])) == 2.0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
