"""Tests for Series construction and basic attributes."""
from python import Python, PythonObject
from testing import assert_equal, assert_true, assert_false, TestSuite
from bison import Series, SeriesScalar, DFScalar


def test_from_pandas():
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1, 2, 3]"), name="vals")
    var s = Series.from_pandas(pd_s)
    assert_equal(s.name, "vals")
    assert_equal(s.__len__(), 3)


def test_size():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]")))
    assert_equal(s.size(), 3)


def test_empty_false():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1]")))
    assert_false(s.empty())


def test_empty_true():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[]"), dtype="float64"))
    assert_true(s.empty())


def test_shape():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4]")))
    var sh = s.shape()
    assert_equal(sh[0], 4)


def test_to_pandas_roundtrip():
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[7, 8, 9]"))
    var s = Series(pd_s)
    var back = s.to_pandas()
    assert_equal(back.__len__(), 3)


def test_sum():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.sum() == 6.0)


def test_mean():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.mean() == 2.0)


def test_median():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_true(s.median() == 2.0)


def test_min():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    assert_true(s.min() == 1.0)


def test_max():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3, 1, 2]")))
    assert_true(s.max() == 3.0)


def test_std():
    var pd = Python.import_module("pandas")
    # std([1, 3, 5], ddof=1) == 2.0
    var s = Series(pd.Series(Python.evaluate("[1, 3, 5]")))
    assert_true(s.std() == 2.0)


def test_var():
    var pd = Python.import_module("pandas")
    # var([1, 3, 5], ddof=1) == 4.0
    var s = Series(pd.Series(Python.evaluate("[1, 3, 5]")))
    assert_true(s.var() == 4.0)


def test_count():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    assert_equal(s.count(), 3)


def test_nunique():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 2, 3, 3, 3]")))
    assert_equal(s.nunique(), 3)


def test_quantile():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    assert_true(s.quantile(0.5) == 3.0)


def test_describe():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    var d = s.describe()
    assert_equal(d.size(), 8)


def test_value_counts():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 2, 3, 3, 3]")))
    var vc = s.value_counts()
    assert_equal(vc.size(), 3)


def test_head():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    var h = s.head(3)
    assert_equal(h.size(), 3)
    assert_true(Float64(String(h.to_pandas().iloc[0])) == 1.0)
    assert_true(Float64(String(h.to_pandas().iloc[2])) == 3.0)


def test_head_default():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5, 6, 7]")))
    var h = s.head()
    assert_equal(h.size(), 5)


def test_head_clamps():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2]")))
    var h = s.head(10)
    assert_equal(h.size(), 2)


def test_tail():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    var t = s.tail(3)
    assert_equal(t.size(), 3)
    assert_true(Float64(String(t.to_pandas().iloc[0])) == 3.0)
    assert_true(Float64(String(t.to_pandas().iloc[2])) == 5.0)


def test_tail_default():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5, 6, 7]")))
    var t = s.tail()
    assert_equal(t.size(), 5)


def test_tail_clamps():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2]")))
    var t = s.tail(10)
    assert_equal(t.size(), 2)


def test_iloc():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]")))
    assert_equal(s.iloc(0)[Int64], 10)
    assert_equal(s.iloc(1)[Int64], 20)
    assert_equal(s.iloc(2)[Int64], 30)


def test_iloc_negative():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]")))
    assert_equal(s.iloc(-1)[Int64], 30)
    assert_equal(s.iloc(-3)[Int64], 10)


def test_iloc_out_of_bounds():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var raised = False
    try:
        _ = s.iloc(5)
    except:
        raised = True
    assert_true(raised)


def test_at():
    var pd = Python.import_module("pandas")
    var idx = Python.evaluate("['a', 'b', 'c']")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]"), index=idx))
    assert_equal(s.at("a")[Int64], 10)
    assert_equal(s.at("b")[Int64], 20)
    assert_equal(s.at("c")[Int64], 30)


def test_at_missing_label():
    var pd = Python.import_module("pandas")
    var idx = Python.evaluate("['x', 'y']")
    var s = Series(pd.Series(Python.evaluate("[1, 2]"), index=idx))
    var raised = False
    try:
        _ = s.at("z")
    except:
        raised = True
    assert_true(raised)


def test_add():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.add(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 5.0)
    assert_true(Float64(String(rp.iloc[1])) == 7.0)
    assert_true(Float64(String(rp.iloc[2])) == 9.0)


def test_sub():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.sub(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == -3.0)
    assert_true(Float64(String(rp.iloc[1])) == -3.0)
    assert_true(Float64(String(rp.iloc[2])) == -3.0)


def test_mul():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.mul(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 4.0)
    assert_true(Float64(String(rp.iloc[1])) == 10.0)
    assert_true(Float64(String(rp.iloc[2])) == 18.0)


def test_div():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[4.0, 5.0, 6.0]")))
    var rp = s1.div(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 0.25)
    assert_true(Float64(String(rp.iloc[1])) == 0.4)
    assert_true(Float64(String(rp.iloc[2])) == 0.5)


def test_floordiv():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.floordiv(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 0.0)
    assert_true(Float64(String(rp.iloc[1])) == 0.0)
    assert_true(Float64(String(rp.iloc[2])) == 0.0)


def test_mod():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[10, 7, 9]")))
    var s2 = Series(pd.Series(Python.evaluate("[3, 4, 5]")))
    var rp = s1.mod(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_pow():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[2, 3, 4]")))
    var s2 = Series(pd.Series(Python.evaluate("[3, 2, 1]")))
    var rp = s1.pow(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 8.0)
    assert_true(Float64(String(rp.iloc[1])) == 9.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_radd():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.radd(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 5.0)
    assert_true(Float64(String(rp.iloc[1])) == 7.0)
    assert_true(Float64(String(rp.iloc[2])) == 9.0)


def test_rsub():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.rsub(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 3.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 3.0)


def test_rmul():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var rp = s1.rmul(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 4.0)
    assert_true(Float64(String(rp.iloc[1])) == 10.0)
    assert_true(Float64(String(rp.iloc[2])) == 18.0)


def test_rdiv():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[4.0, 5.0, 6.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var rp = s1.rdiv(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 0.25)
    assert_true(Float64(String(rp.iloc[1])) == 0.4)
    assert_true(Float64(String(rp.iloc[2])) == 0.5)


def test_rfloordiv():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[4, 5, 6]")))
    var s2 = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var rp = s1.rfloordiv(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 0.0)
    assert_true(Float64(String(rp.iloc[1])) == 0.0)
    assert_true(Float64(String(rp.iloc[2])) == 0.0)


def test_rmod():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[3, 4, 5]")))
    var s2 = Series(pd.Series(Python.evaluate("[10, 7, 9]")))
    var rp = s1.rmod(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_rpow():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[3, 2, 1]")))
    var s2 = Series(pd.Series(Python.evaluate("[2, 3, 4]")))
    var rp = s1.rpow(s2).to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 8.0)
    assert_true(Float64(String(rp.iloc[1])) == 9.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_add_length_mismatch():
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2]")))
    var s2 = Series(pd.Series(Python.evaluate("[1]")))
    var raised = False
    try:
        _ = s1.add(s2)
    except:
        raised = True
    assert_true(raised)


def test_isna():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var na = s.isna()
    assert_equal(na.size(), 3)
    assert_false(na.iloc(0)[Bool])
    assert_true(na.iloc(1)[Bool])
    assert_false(na.iloc(2)[Bool])


def test_isna_no_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var na = s.isna()
    assert_false(na.iloc(0)[Bool])
    assert_false(na.iloc(1)[Bool])
    assert_false(na.iloc(2)[Bool])


def test_isnull():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var na = s.isnull()
    assert_false(na.iloc(0)[Bool])
    assert_true(na.iloc(1)[Bool])
    assert_false(na.iloc(2)[Bool])


def test_notna():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var na = s.notna()
    assert_equal(na.size(), 3)
    assert_true(na.iloc(0)[Bool])
    assert_false(na.iloc(1)[Bool])
    assert_true(na.iloc(2)[Bool])


def test_notnull():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var na = s.notnull()
    assert_true(na.iloc(0)[Bool])
    assert_false(na.iloc(1)[Bool])
    assert_true(na.iloc(2)[Bool])


def test_fillna():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var filled = s.fillna(DFScalar(Float64(0.0)))
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 0.0)
    assert_true(Float64(String(rp.iloc[2])) == 3.0)


def test_fillna_no_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var filled = s.fillna(DFScalar(Float64(0.0)))
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 2.0)


def test_fillna_int():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, None, 3]"), dtype="float64"))
    var filled = s.fillna(DFScalar(Float64(9.0)))
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 9.0)


def test_dropna():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var dropped = s.dropna()
    assert_equal(dropped.size(), 2)
    var rp = dropped.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)


def test_dropna_no_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var dropped = s.dropna()
    assert_equal(dropped.size(), 3)


def test_dropna_all_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[None, None]"), dtype="float64"))
    var dropped = s.dropna()
    assert_equal(dropped.size(), 0)


def test_ffill():
    var pd = Python.import_module("pandas")
    # [1.0, NaN, 3.0] → ffill → [1.0, 1.0, 3.0]
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var filled = s.ffill()
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 1.0)
    assert_true(Float64(String(rp.iloc[2])) == 3.0)


def test_ffill_leading_null():
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


def test_ffill_no_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var filled = s.ffill()
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 2.0)


def test_bfill():
    var pd = Python.import_module("pandas")
    # [1.0, NaN, 3.0] → bfill → [1.0, 3.0, 3.0]
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var filled = s.bfill()
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 3.0)


def test_bfill_trailing_null():
    var pd = Python.import_module("pandas")
    # [NaN, 2.0, NaN] → bfill → [2.0, 2.0, NaN]
    var s = Series(pd.Series(Python.evaluate("[None, 2.0, None]")))
    var filled = s.bfill()
    assert_false(filled.isna().iloc(0)[Bool])
    assert_false(filled.isna().iloc(1)[Bool])
    assert_true(filled.isna().iloc(2)[Bool])
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[0])) == 2.0)


def test_bfill_no_nulls():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var filled = s.bfill()
    assert_equal(filled.size(), 3)
    var rp = filled.to_pandas()
    assert_true(Float64(String(rp.iloc[1])) == 2.0)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
