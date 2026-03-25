"""Tests for Series construction, attributes, and aggregations."""
from std.python import Python, PythonObject
from std.testing import assert_equal, assert_true, assert_false, TestSuite
from bison import Series, SeriesScalar, DFScalar, FloatTransformFn, DataFrame


def test_from_pandas() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1, 2, 3]"), name="vals")
    var s = Series.from_pandas(pd_s)
    assert_equal(s.name, "vals")
    assert_equal(s.__len__(), 3)


def test_size() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]")))
    assert_equal(s.size(), 3)


def test_empty_false() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1]")))
    assert_false(s.empty())


def test_empty_true() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[]"), dtype="float64"))
    assert_true(s.empty())


def test_shape() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4]")))
    var sh = s.shape()
    assert_equal(sh[0], 4)


def test_to_pandas_roundtrip() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[7, 8, 9]"))
    var s = Series(pd_s)
    var back = s.to_pandas()
    assert_equal(back.__len__(), 3)


def test_sum() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.sum() == 6.0)


def test_mean() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.mean() == 2.0)


def test_median() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.median() == 2.0)


def test_min() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    assert_true(s.min() == 1.0)


def test_max() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    assert_true(s.max() == 3.0)


def test_std() raises:
    var pd = Python.import_module("pandas")
    # std([1, 3, 5], ddof=1) == 2.0
    var s = Series(pd.Series(Python.evaluate("[1, 3, 5]")))
    assert_true(s.std() == 2.0)


def test_var() raises:
    var pd = Python.import_module("pandas")
    # var([1, 3, 5], ddof=1) == 4.0
    var s = Series(pd.Series(Python.evaluate("[1, 3, 5]")))
    assert_true(s.var() == 4.0)


def test_count() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_equal(s.count(), 3)


def test_nunique() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 2, 3, 3, 3]")))
    assert_equal(s.nunique(), 3)


def test_quantile() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    assert_true(s.quantile(0.5) == 3.0)


def test_quantile_skipna_true_with_nulls() raises:
    var pd = Python.import_module("pandas")
    # [1.0, None, 3.0] with skipna=True → quantile(0.5) of [1.0, 3.0] == 2.0
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    assert_true(s.quantile(0.5, skipna=True) == 2.0)


def test_quantile_skipna_false_with_nulls() raises:
    var pd = Python.import_module("pandas")
    # [1.0, None, 3.0] with skipna=False → NaN
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var result = s.quantile(0.5, skipna=False)
    # NaN != NaN is the standard IEEE test
    assert_true(result != result)


def test_quantile_skipna_false_no_nulls() raises:
    var pd = Python.import_module("pandas")
    # No nulls: skipna=False should still return the correct quantile
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    assert_true(s.quantile(0.5, skipna=False) == 3.0)


def test_describe() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    var d = s.describe()
    assert_equal(d.size(), 8)


def test_value_counts() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 2, 3, 3, 3]")))
    var vc = s.value_counts()
    assert_equal(vc.size(), 3)


def test_head() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    var h = s.head(3)
    assert_equal(h.size(), 3)
    assert_true(Float64(String(h.to_pandas().iloc[0])) == 1.0)
    assert_true(Float64(String(h.to_pandas().iloc[2])) == 3.0)


def test_head_default() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5, 6, 7]")))
    var h = s.head()
    assert_equal(h.size(), 5)


def test_head_clamps() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2]")))
    var h = s.head(10)
    assert_equal(h.size(), 2)


def test_tail() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    var t = s.tail(3)
    assert_equal(t.size(), 3)
    assert_true(Float64(String(t.to_pandas().iloc[0])) == 3.0)
    assert_true(Float64(String(t.to_pandas().iloc[2])) == 5.0)


def test_tail_default() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5, 6, 7]")))
    var t = s.tail()
    assert_equal(t.size(), 5)


def test_tail_clamps() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2]")))
    var t = s.tail(10)
    assert_equal(t.size(), 2)


def test_iloc() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]")))
    assert_equal(s.iloc(0)[Int64], 10)
    assert_equal(s.iloc(1)[Int64], 20)
    assert_equal(s.iloc(2)[Int64], 30)


def test_series_scalar_int_coercion() raises:
    # A plain Int must coerce to SeriesScalar as an Int64 arm (mirrors DFScalar fix).
    var ss: SeriesScalar = 42
    assert_true(ss.isa[Int64]())
    assert_equal(ss[Int64], 42)


def test_iloc_negative() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]")))
    assert_equal(s.iloc(-1)[Int64], 30)
    assert_equal(s.iloc(-3)[Int64], 10)


def test_iloc_out_of_bounds() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var raised = False
    try:
        _ = s.iloc(5)
    except:
        raised = True
    assert_true(raised)


def test_at() raises:
    var pd = Python.import_module("pandas")
    var idx = Python.evaluate("['a', 'b', 'c']")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]"), index=idx))
    assert_equal(s.at("a")[Int64], 10)
    assert_equal(s.at("b")[Int64], 20)
    assert_equal(s.at("c")[Int64], 30)


def test_at_missing_label() raises:
    var pd = Python.import_module("pandas")
    var idx = Python.evaluate("['x', 'y']")
    var s = Series(pd.Series(Python.evaluate("[1, 2]"), index=idx))
    var raised = False
    try:
        _ = s.at("z")
    except:
        raised = True
    assert_true(raised)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
