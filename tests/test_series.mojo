"""Tests for Series construction and basic attributes."""
from std.python import Python, PythonObject
from testing import assert_equal, assert_true, assert_false, TestSuite
from bison import Series, SeriesScalar, DFScalar, FloatTransformFn


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


def test_add() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.add(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 5.0)
    assert_true(Float64(String(rp.iloc[1])) == 7.0)
    assert_true(Float64(String(rp.iloc[2])) == 9.0)


def test_sub() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.sub(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == -3.0)
    assert_true(Float64(String(rp.iloc[1])) == -3.0)
    assert_true(Float64(String(rp.iloc[2])) == -3.0)


def test_mul() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.mul(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 4.0)
    assert_true(Float64(String(rp.iloc[1])) == 10.0)
    assert_true(Float64(String(rp.iloc[2])) == 18.0)


def test_div() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[4.0, 5.0, 6.0]")))
    var rp = s1.div(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 0.25)
    assert_true(Float64(String(rp.iloc[1])) == 0.4)
    assert_true(Float64(String(rp.iloc[2])) == 0.5)


def test_floordiv() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.floordiv(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 0.0)
    assert_true(Float64(String(rp.iloc[1])) == 0.0)
    assert_true(Float64(String(rp.iloc[2])) == 0.0)


def test_mod() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[10, 7, 9]")))
    var s2 = Series(pd.Series(Python.evaluate("[3, 4, 5]")))
    var rp = s1.mod(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_pow() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[2, 3, 4]")))
    var s2 = Series(pd.Series(Python.evaluate("[3, 2, 1]")))
    var rp = s1.pow(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 8.0)
    assert_true(Float64(String(rp.iloc[1])) == 9.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_radd() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.radd(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 5.0)
    assert_true(Float64(String(rp.iloc[1])) == 7.0)
    assert_true(Float64(String(rp.iloc[2])) == 9.0)


def test_rsub() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.rsub(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 3.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 3.0)


def test_rmul() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.rmul(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 4.0)
    assert_true(Float64(String(rp.iloc[1])) == 10.0)
    assert_true(Float64(String(rp.iloc[2])) == 18.0)


def test_rdiv() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[4.0, 5.0, 6.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var rp = s1.rdiv(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 0.25)
    assert_true(Float64(String(rp.iloc[1])) == 0.4)
    assert_true(Float64(String(rp.iloc[2])) == 0.5)


def test_rfloordiv() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var s2 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var rp = s1.rfloordiv(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 0.0)
    assert_true(Float64(String(rp.iloc[1])) == 0.0)
    assert_true(Float64(String(rp.iloc[2])) == 0.0)


def test_rmod() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[3, 4, 5]")))
    var s2 = Series(pd.Series(Python.evaluate("[10, 7, 9]")))
    var rp = s1.rmod(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_rpow() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[3, 2, 1]")))
    var s2 = Series(pd.Series(Python.evaluate("[2, 3, 4]")))
    var rp = s1.rpow(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 8.0)
    assert_true(Float64(String(rp.iloc[1])) == 9.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_add_length_mismatch() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2]")))
    var s2 = Series(pd.Series(Python.evaluate("[1]")))
    var raised = False
    try:
        _ = s1.add(s2)
    except:
        raised = True
    assert_true(raised)


def test_isna() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var na = s.isna()
    assert_equal(na.size(), 3)
    assert_false(na.iloc(0)[Bool])
    assert_true(na.iloc(1)[Bool])
    assert_false(na.iloc(2)[Bool])


def test_isna_no_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var na = s.isna()
    assert_false(na.iloc(0)[Bool])
    assert_false(na.iloc(1)[Bool])
    assert_false(na.iloc(2)[Bool])


def test_isnull() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var na = s.isnull()
    assert_false(na.iloc(0)[Bool])
    assert_true(na.iloc(1)[Bool])
    assert_false(na.iloc(2)[Bool])


def test_notna() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var na = s.notna()
    assert_equal(na.size(), 3)
    assert_true(na.iloc(0)[Bool])
    assert_false(na.iloc(1)[Bool])
    assert_true(na.iloc(2)[Bool])


def test_notnull() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var na = s.notnull()
    assert_true(na.iloc(0)[Bool])
    assert_false(na.iloc(1)[Bool])
    assert_true(na.iloc(2)[Bool])


def test_fillna() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var filled = s.fillna(DFScalar(Float64(0.0)))
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 0.0)
    assert_true(Float64(String(rp.iloc[2])) == 3.0)


def test_fillna_no_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var filled = s.fillna(DFScalar(Float64(0.0)))
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 2.0)


def test_fillna_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, None, 3]"), dtype="float64"))
    var filled = s.fillna(DFScalar(Float64(9.0)))
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 9.0)


def test_dropna() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var dropped = s.dropna()
    assert_equal(dropped.size(), 2)
    var rp = dropped.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)


def test_dropna_no_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var dropped = s.dropna()
    assert_equal(dropped.size(), 3)


def test_dropna_all_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[None, None]"), dtype="float64"))
    var dropped = s.dropna()
    assert_equal(dropped.size(), 0)


def test_ffill() raises:
    var pd = Python.import_module("pandas")
    # [1.0, NaN, 3.0] → ffill → [1.0, 1.0, 3.0]
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var filled = s.ffill()
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 1.0)
    assert_true(Float64(String(rp.iloc[2])) == 3.0)


def test_ffill_leading_null() raises:
    var pd = Python.import_module("pandas")
    # [NaN, 2.0, NaN] → ffill → [NaN, 2.0, 2.0]
    var s = Series(pd.Series(Python.evaluate("[None, 2.0, None]")))
    var filled = s.ffill()
    # leading null stays null, trailing null filled
    assert_true(filled.isna().iloc(0)[Bool])
    assert_false(filled.isna().iloc(1)[Bool])
    assert_false(filled.isna().iloc(2)[Bool])
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 2.0)
    assert_true(Float64(String(rp.iloc[2])) == 2.0)


def test_ffill_no_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var filled = s.ffill()
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 2.0)


def test_bfill() raises:
    var pd = Python.import_module("pandas")
    # [1.0, NaN, 3.0] → bfill → [1.0, 3.0, 3.0]
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var filled = s.bfill()
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 3.0)


def test_bfill_trailing_null() raises:
    var pd = Python.import_module("pandas")
    # [NaN, 2.0, NaN] → bfill → [2.0, 2.0, NaN]
    var s = Series(pd.Series(Python.evaluate("[None, 2.0, None]")))
    var filled = s.bfill()
    assert_false(filled.isna().iloc(0)[Bool])
    assert_false(filled.isna().iloc(1)[Bool])
    assert_true(filled.isna().iloc(2)[Bool])
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 2.0)


def test_bfill_no_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var filled = s.bfill()
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 2.0)


def test_eq() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[1, 0, 3]")))
    var rp = s1.eq(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == True)
    assert_true(Bool(rp.iloc[1]) == False)
    assert_true(Bool(rp.iloc[2]) == True)


def test_ne() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[1, 0, 3]")))
    var rp = s1.ne(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == False)
    assert_true(Bool(rp.iloc[1]) == True)
    assert_true(Bool(rp.iloc[2]) == False)


def test_lt() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[2, 2, 2]")))
    var rp = s1.lt(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == True)
    assert_true(Bool(rp.iloc[1]) == False)
    assert_true(Bool(rp.iloc[2]) == False)


def test_le() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[2, 2, 2]")))
    var rp = s1.le(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == True)
    assert_true(Bool(rp.iloc[1]) == True)
    assert_true(Bool(rp.iloc[2]) == False)


def test_gt() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[2, 2, 2]")))
    var rp = s1.gt(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == False)
    assert_true(Bool(rp.iloc[1]) == False)
    assert_true(Bool(rp.iloc[2]) == True)


def test_ge() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[2, 2, 2]")))
    var rp = s1.ge(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == False)
    assert_true(Bool(rp.iloc[1]) == True)
    assert_true(Bool(rp.iloc[2]) == True)


def test_eq_null_propagation() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var result = s1.eq(s2)
    assert_true(result.isna().iloc(1)[Bool])
    assert_false(result.isna().iloc(0)[Bool])
    assert_false(result.isna().iloc(2)[Bool])


# Bool column comparison tests — exercises the direct Bool arm in _cmp_op
def test_bool_eq() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[True, False, True, False]"), dtype="bool"))
    var s2 = Series(pd.Series(Python.evaluate("[True, True, False, False]"), dtype="bool"))
    var rp = s1.eq(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == True)
    assert_true(Bool(rp.iloc[1]) == False)
    assert_true(Bool(rp.iloc[2]) == False)
    assert_true(Bool(rp.iloc[3]) == True)


def test_bool_ne() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[True, False, True, False]"), dtype="bool"))
    var s2 = Series(pd.Series(Python.evaluate("[True, True, False, False]"), dtype="bool"))
    var rp = s1.ne(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == False)
    assert_true(Bool(rp.iloc[1]) == True)
    assert_true(Bool(rp.iloc[2]) == True)
    assert_true(Bool(rp.iloc[3]) == False)


def test_bool_lt() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[False, False, True, True]"), dtype="bool"))
    var s2 = Series(pd.Series(Python.evaluate("[False, True, False, True]"), dtype="bool"))
    var rp = s1.lt(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == False)
    assert_true(Bool(rp.iloc[1]) == True)
    assert_true(Bool(rp.iloc[2]) == False)
    assert_true(Bool(rp.iloc[3]) == False)


def test_bool_le() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[False, False, True, True]"), dtype="bool"))
    var s2 = Series(pd.Series(Python.evaluate("[False, True, False, True]"), dtype="bool"))
    var rp = s1.le(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == True)
    assert_true(Bool(rp.iloc[1]) == True)
    assert_true(Bool(rp.iloc[2]) == False)
    assert_true(Bool(rp.iloc[3]) == True)


def test_bool_gt() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[False, False, True, True]"), dtype="bool"))
    var s2 = Series(pd.Series(Python.evaluate("[False, True, False, True]"), dtype="bool"))
    var rp = s1.gt(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == False)
    assert_true(Bool(rp.iloc[1]) == False)
    assert_true(Bool(rp.iloc[2]) == True)
    assert_true(Bool(rp.iloc[3]) == False)


def test_bool_ge() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[False, False, True, True]"), dtype="bool"))
    var s2 = Series(pd.Series(Python.evaluate("[False, True, False, True]"), dtype="bool"))
    var rp = s1.ge(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == True)
    assert_true(Bool(rp.iloc[1]) == False)
    assert_true(Bool(rp.iloc[2]) == True)
    assert_true(Bool(rp.iloc[3]) == True)


def test_copy() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var c = s.copy()
    assert_true(c.iloc(0)[Int64] == 1)
    assert_true(c.iloc(2)[Int64] == 3)


def test_rename() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")), "original")
    var r = s.rename("renamed")
    assert_true(r.name == "renamed")
    assert_true(s.name == "original")


def test_abs_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[-1.0, -2.5, 3.0, -4.0]")))
    var r = s.abs()
    assert_true(r.iloc(0)[Float64] == 1.0)
    assert_true(r.iloc(1)[Float64] == 2.5)
    assert_true(r.iloc(2)[Float64] == 3.0)


def test_abs_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[-3, -1, 0, 2]"), dtype="int64"))
    var r = s.abs()
    assert_true(r.iloc(0)[Int64] == 3)
    assert_true(r.iloc(2)[Int64] == 0)
    assert_true(r.iloc(3)[Int64] == 2)


def test_abs_null_propagation() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[-1.0, None, 3.0]")))
    var result = s.abs()
    assert_true(result.isna().iloc(1)[Bool])
    assert_false(result.isna().iloc(0)[Bool])


def test_round_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.123, 2.567, 3.001]")))
    var r = s.round(2)
    assert_true(r.iloc(0)[Float64] == 1.12)
    assert_true(r.iloc(1)[Float64] == 2.57)


def test_round_int_identity() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var r = s.round(0)
    assert_true(r.iloc(0)[Int64] == 1)
    assert_true(r.iloc(2)[Int64] == 3)


def test_round_null_propagation() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.5, None, 3.7]")))
    var result = s.round(0)
    assert_true(result.isna().iloc(1)[Bool])
    assert_false(result.isna().iloc(0)[Bool])


def test_round_bankers() raises:
    """Verify banker's rounding (round-half-to-even) at exact half-way points."""
    var pd = Python.import_module("pandas")
    # 0.5 → 0 (even), 1.5 → 2 (even), 2.5 → 2 (even), 3.5 → 4 (even)
    var s = Series(pd.Series(Python.evaluate("[0.5, 1.5, 2.5, 3.5]")))
    var r = s.round(0)
    assert_true(r.iloc(0)[Float64] == 0.0)
    assert_true(r.iloc(1)[Float64] == 2.0)
    assert_true(r.iloc(2)[Float64] == 2.0)
    assert_true(r.iloc(3)[Float64] == 4.0)
    # Negative half-way points: -0.5 → 0, -1.5 → -2
    var s2 = Series(pd.Series(Python.evaluate("[-0.5, -1.5, -2.5]")))
    var r2 = s2.round(0)
    assert_true(r2.iloc(0)[Float64] == 0.0)
    assert_true(r2.iloc(1)[Float64] == -2.0)
    assert_true(r2.iloc(2)[Float64] == -2.0)


def test_clip_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[0.0, 5.0, 10.0, 15.0]")))
    var r = s.clip(Float64(2.0), Float64(12.0))
    assert_true(r.iloc(0)[Float64] == 2.0)
    assert_true(r.iloc(1)[Float64] == 5.0)
    assert_true(r.iloc(2)[Float64] == 10.0)
    assert_true(r.iloc(3)[Float64] == 12.0)


def test_clip_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 5, 10, 20]"), dtype="int64"))
    var r = s.clip(Float64(3.0), Float64(15.0))
    assert_true(r.iloc(0)[Int64] == 3)
    assert_true(r.iloc(1)[Int64] == 5)
    assert_true(r.iloc(3)[Int64] == 15)


def test_clip_null_propagation() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 10.0]")))
    var result = s.clip(Float64(2.0), Float64(8.0))
    assert_true(result.isna().iloc(1)[Bool])
    assert_false(result.isna().iloc(0)[Bool])


def test_clip_lower_only() raises:
    # When only a lower bound is provided the upper side must not be clipped.
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[0.0, 5.0, 1000.0]")))
    var r = s.clip(lower=Float64(3.0))
    assert_true(r.iloc(0)[Float64] == 3.0)
    assert_true(r.iloc(1)[Float64] == 5.0)
    # 1000.0 must remain unchanged — sentinel ±1e308 would wrongly clip this.
    assert_true(r.iloc(2)[Float64] == 1000.0)


def test_clip_upper_only() raises:
    # When only an upper bound is provided the lower side must not be clipped.
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[0.0, 5.0, 10.0]")))
    var r = s.clip(upper=Float64(7.0))
    # 0.0 must remain unchanged.
    assert_true(r.iloc(0)[Float64] == 0.0)
    assert_true(r.iloc(1)[Float64] == 5.0)
    assert_true(r.iloc(2)[Float64] == 7.0)


def _double(v: Float64) -> Float64:
    return v * 2.0

def _identity(v: Float64) -> Float64:
    return v

def _negate(v: Float64) -> Float64:
    return -v

def test_apply() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var r = s.apply[_double]()
    assert_true(r.iloc(0)[Float64] == 2.0)
    assert_true(r.iloc(1)[Float64] == 4.0)
    assert_true(r.iloc(2)[Float64] == 6.0)


def test_apply_null_propagation() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var result = s.apply[_identity]()
    assert_true(result.isna().iloc(1)[Bool])
    assert_false(result.isna().iloc(0)[Bool])


def test_map() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var r = s.map[_negate]()
    assert_true(r.iloc(0)[Float64] == -1.0)
    assert_true(r.iloc(1)[Float64] == -2.0)
    assert_true(r.iloc(2)[Float64] == -3.0)


def test_isin_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4]"), dtype="int64"))
    var vals = List[Int64]()
    vals.append(2)
    vals.append(4)
    var r = s.isin(vals)
    assert_false(r.iloc(0)[Bool])
    assert_true(r.iloc(1)[Bool])
    assert_false(r.iloc(2)[Bool])
    assert_true(r.iloc(3)[Bool])


def test_isin_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    var vals = List[Float64]()
    vals.append(1.0)
    vals.append(3.0)
    var r = s.isin(vals)
    assert_true(r.iloc(0)[Bool])
    assert_false(r.iloc(1)[Bool])
    assert_true(r.iloc(2)[Bool])
    assert_false(r.iloc(3)[Bool])


def test_isin_str() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate('["a", "b", "c", "d"]'), dtype="string"))
    var vals = List[String]()
    vals.append("a")
    vals.append("c")
    var r = s.isin(vals)
    assert_true(r.iloc(0)[Bool])
    assert_false(r.iloc(1)[Bool])
    assert_true(r.iloc(2)[Bool])
    assert_false(r.iloc(3)[Bool])


def test_isin_bool() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[True, False, True, False]"), dtype="bool"))
    var vals = List[Bool]()
    vals.append(True)
    var r = s.isin(vals)
    assert_true(r.iloc(0)[Bool])
    assert_false(r.iloc(1)[Bool])
    assert_true(r.iloc(2)[Bool])
    assert_false(r.iloc(3)[Bool])


def test_between() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]")))
    var r = s.between(Float64(2.0), Float64(4.0))
    assert_false(r.iloc(0)[Bool])
    assert_true(r.iloc(1)[Bool])
    assert_true(r.iloc(2)[Bool])
    assert_true(r.iloc(3)[Bool])
    assert_false(r.iloc(4)[Bool])


def test_where() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    var cond = Series(pd.Series(Python.evaluate("[True, False, True, False]"), dtype="bool"))
    var result = s.where(cond)
    assert_true(result.iloc(0)[Float64] == 1.0)
    assert_true(result.isna().iloc(1)[Bool])
    assert_true(result.iloc(2)[Float64] == 3.0)
    assert_true(result.isna().iloc(3)[Bool])


def test_where_null_propagation() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var cond = Series(pd.Series(Python.evaluate("[True, True, False]"), dtype="bool"))
    var result = s.where(cond)
    assert_false(result.isna().iloc(0)[Bool])
    assert_true(result.isna().iloc(1)[Bool])
    assert_true(result.isna().iloc(2)[Bool])


def test_mask() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0]")))
    var cond = Series(pd.Series(Python.evaluate("[True, False, True, False]"), dtype="bool"))
    var result = s.mask(cond)
    assert_true(result.isna().iloc(0)[Bool])
    assert_true(result.iloc(1)[Float64] == 2.0)
    assert_true(result.isna().iloc(2)[Bool])
    assert_true(result.iloc(3)[Float64] == 4.0)


def test_mask_null_propagation() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var cond = Series(pd.Series(Python.evaluate("[False, False, True]"), dtype="bool"))
    var result = s.mask(cond)
    assert_false(result.isna().iloc(0)[Bool])
    assert_true(result.isna().iloc(1)[Bool])
    assert_true(result.isna().iloc(2)[Bool])


def test_unique_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2, 1, 3]"), dtype="int64"))
    var r = s.unique()
    assert_true(r.size() == 3)
    assert_true(r.iloc(0)[Int64] == 3)
    assert_true(r.iloc(1)[Int64] == 1)
    assert_true(r.iloc(2)[Int64] == 2)


def test_unique_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 1.0, 3.0]")))
    var r = s.unique()
    assert_true(r.size() == 3)
    assert_true(r.iloc(0)[Float64] == 1.0)
    assert_true(r.iloc(1)[Float64] == 2.0)
    assert_true(r.iloc(2)[Float64] == 3.0)


def test_astype() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var r = s.astype("float64")
    assert_true(r.iloc(0)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 3.0)


def test_astype_null_propagation() raises:
    var pd = Python.import_module("pandas")
    # Float64 → Int64: null at index 1 must propagate
    var sf = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var ri = sf.astype("int64")
    assert_false(ri.isna().iloc(0)[Bool])
    assert_true(ri.isna().iloc(1)[Bool])
    assert_false(ri.isna().iloc(2)[Bool])
    # Float64 → Bool: null at index 1 must propagate
    var rb = sf.astype("bool")
    assert_false(rb.isna().iloc(0)[Bool])
    assert_true(rb.isna().iloc(1)[Bool])
    assert_false(rb.isna().iloc(2)[Bool])


def test_reset_index() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]"), index=Python.evaluate("[5, 6, 7]")))
    var r = s.reset_index(drop=True)
    assert_true(r.iloc(0)[Int64] == 10)
    assert_true(r.iloc(2)[Int64] == 30)


# ------------------------------------------------------------------
# Sorting
# ------------------------------------------------------------------

def test_sort_values_ascending_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]"), dtype="int64"))
    var r = s.sort_values()
    assert_true(r.iloc(0)[Int64] == 1)
    assert_true(r.iloc(1)[Int64] == 2)
    assert_true(r.iloc(2)[Int64] == 3)


def test_sort_values_descending_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]"), dtype="int64"))
    var r = s.sort_values(ascending=False)
    assert_true(r.iloc(0)[Int64] == 3)
    assert_true(r.iloc(1)[Int64] == 2)
    assert_true(r.iloc(2)[Int64] == 1)


def test_sort_values_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, 1.0, 2.0]")))
    var r = s.sort_values()
    assert_true(r.iloc(0)[Float64] == 1.0)
    assert_true(r.iloc(1)[Float64] == 2.0)
    assert_true(r.iloc(2)[Float64] == 3.0)


def test_sort_values_string() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['c', 'a', 'b']"), dtype="string"))
    var r = s.sort_values()
    assert_true(r.iloc(0)[String] == "a")
    assert_true(r.iloc(1)[String] == "b")
    assert_true(r.iloc(2)[String] == "c")


def test_sort_values_null_last() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, None, 1.0]")))
    var r = s.sort_values()
    assert_true(r.iloc(0)[Float64] == 1.0)
    assert_true(r.iloc(1)[Float64] == 3.0)
    assert_true(r.isna().iloc(2)[Bool])


def test_sort_values_null_first() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, None, 1.0]")))
    var r = s.sort_values(na_position="first")
    assert_true(r.isna().iloc(0)[Bool])
    assert_true(r.iloc(1)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 3.0)


def test_sort_values_null_first_descending() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, None, 1.0]")))
    var r = s.sort_values(ascending=False, na_position="first")
    assert_true(r.isna().iloc(0)[Bool])
    assert_true(r.iloc(1)[Float64] == 3.0)
    assert_true(r.iloc(2)[Float64] == 1.0)


def test_sort_values_preserves_index() raises:
    var pd = Python.import_module("pandas")
    # s: label 'b'->3, 'a'->1, 'c'->2.  Sorted by value: 1,2,3 → labels a,c,b.
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]"), dtype="int64", index=Python.evaluate("['b', 'a', 'c']")))
    var r = s.sort_values()
    assert_true(r.iloc(0)[Int64] == 1)
    assert_true(r.iloc(1)[Int64] == 2)
    assert_true(r.iloc(2)[Int64] == 3)
    # index labels must follow their data row
    assert_true(r.at("a")[Int64] == 1)
    assert_true(r.at("c")[Int64] == 2)
    assert_true(r.at("b")[Int64] == 3)


def test_sort_index_ascending_default() raises:
    var pd = Python.import_module("pandas")
    # Default RangeIndex — ascending is a no-op.
    var s = Series(pd.Series(Python.evaluate("[30, 10, 20]"), dtype="int64"))
    var r = s.sort_index()
    assert_true(r.iloc(0)[Int64] == 30)
    assert_true(r.iloc(1)[Int64] == 10)
    assert_true(r.iloc(2)[Int64] == 20)


def test_sort_index_descending_default() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[30, 10, 20]"), dtype="int64"))
    var r = s.sort_index(ascending=False)
    assert_true(r.iloc(0)[Int64] == 20)
    assert_true(r.iloc(1)[Int64] == 10)
    assert_true(r.iloc(2)[Int64] == 30)


def test_sort_index_custom() raises:
    var pd = Python.import_module("pandas")
    # index labels [2, 0, 1] → sorted ascending → [0, 1, 2] → data [10, 20, 30]
    var s = Series(pd.Series(Python.evaluate("[30, 10, 20]"), dtype="int64", index=Python.evaluate("[2, 0, 1]")))
    var r = s.sort_index()
    assert_true(r.iloc(0)[Int64] == 10)
    assert_true(r.iloc(1)[Int64] == 20)
    assert_true(r.iloc(2)[Int64] == 30)


def test_sort_index_custom_descending() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[30, 10, 20]"), dtype="int64", index=Python.evaluate("[2, 0, 1]")))
    var r = s.sort_index(ascending=False)
    assert_true(r.iloc(0)[Int64] == 30)
    assert_true(r.iloc(1)[Int64] == 20)
    assert_true(r.iloc(2)[Int64] == 10)


def test_argsort_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]"), dtype="int64"))
    var r = s.argsort()
    # perm: smallest=1(pos1), next=2(pos2), largest=3(pos0) → [1, 2, 0]
    assert_true(r.iloc(0)[Int64] == 1)
    assert_true(r.iloc(1)[Int64] == 2)
    assert_true(r.iloc(2)[Int64] == 0)


def test_argsort_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, 1.0, 2.0]")))
    var r = s.argsort()
    assert_true(r.iloc(0)[Int64] == 1)
    assert_true(r.iloc(1)[Int64] == 2)
    assert_true(r.iloc(2)[Int64] == 0)


def test_argsort_with_null() raises:
    var pd = Python.import_module("pandas")
    # s=[3.0, None, 1.0]: perm=[2,0,1]; perm[2]=1 is null → NaN at result pos 2
    var s = Series(pd.Series(Python.evaluate("[3.0, None, 1.0]")))
    var r = s.argsort()
    assert_true(r.iloc(0)[Float64] == 2.0)
    assert_true(r.iloc(1)[Float64] == 0.0)
    assert_true(r.isna().iloc(2)[Bool])


def test_rank_no_ties() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, 1.0, 2.0]")))
    var r = s.rank()
    assert_true(r.iloc(0)[Float64] == 3.0)
    assert_true(r.iloc(1)[Float64] == 1.0)
    assert_true(r.iloc(2)[Float64] == 2.0)


def test_rank_with_ties() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 1.0, 2.0]")))
    var r = s.rank()
    # Tied at 1st/2nd → average rank = (1+2)/2 = 1.5; unique 3rd → 3.0
    assert_true(r.iloc(0)[Float64] == 1.5)
    assert_true(r.iloc(1)[Float64] == 1.5)
    assert_true(r.iloc(2)[Float64] == 3.0)


def test_rank_with_null() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, None, 1.0]")))
    var r = s.rank()
    assert_true(r.iloc(0)[Float64] == 2.0)
    assert_true(r.isna().iloc(1)[Bool])
    assert_true(r.iloc(2)[Float64] == 1.0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
