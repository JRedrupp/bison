"""Tests for Series arithmetic operations, NA handling, and comparisons."""
from std.python import Python
from std.testing import assert_equal, assert_true, assert_false, TestSuite
from bison import Series, DFScalar


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


def test_int64_dtype_preserved_add() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var s2 = Series(pd.Series(Python.evaluate("[4, 5, 6]"), dtype="int64"))
    var rp = s1.add(s2).to_pandas()
    assert_equal(String(rp.dtype), "int64")
    assert_true(Float64(String(rp.iloc[0])) == 5.0)
    assert_true(Float64(String(rp.iloc[1])) == 7.0)
    assert_true(Float64(String(rp.iloc[2])) == 9.0)


def test_int64_dtype_preserved_sub() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[4, 5, 6]"), dtype="int64"))
    var s2 = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var rp = s1.sub(s2).to_pandas()
    assert_equal(String(rp.dtype), "int64")
    assert_true(Float64(String(rp.iloc[0])) == 3.0)


def test_int64_dtype_preserved_mul() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[2, 3, 4]"), dtype="int64"))
    var s2 = Series(pd.Series(Python.evaluate("[3, 2, 1]"), dtype="int64"))
    var rp = s1.mul(s2).to_pandas()
    assert_equal(String(rp.dtype), "int64")
    assert_true(Float64(String(rp.iloc[0])) == 6.0)
    assert_true(Float64(String(rp.iloc[1])) == 6.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_int64_div_yields_float64() raises:
    # True division always returns float64 even for int64 inputs (pandas behaviour).
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[4, 6, 9]"), dtype="int64"))
    var s2 = Series(pd.Series(Python.evaluate("[2, 3, 3]"), dtype="int64"))
    var rp = s1.div(s2).to_pandas()
    assert_equal(String(rp.dtype), "float64")
    assert_true(Float64(String(rp.iloc[0])) == 2.0)


def test_int64_dtype_preserved_floordiv() raises:
    var pd = Python.import_module("pandas")
    # Python floor division: 7//2=3, -7//2=-4, 7//-2=-4
    var s1 = Series(pd.Series(Python.evaluate("[7, -7, 7]"), dtype="int64"))
    var s2 = Series(pd.Series(Python.evaluate("[2,  2, -2]"), dtype="int64"))
    var rp = s1.floordiv(s2).to_pandas()
    assert_equal(String(rp.dtype), "int64")
    assert_true(Float64(String(rp.iloc[0])) == 3.0)
    assert_true(Float64(String(rp.iloc[1])) == -4.0)
    assert_true(Float64(String(rp.iloc[2])) == -4.0)


def test_int64_dtype_preserved_mod() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[10, 7, 9]"), dtype="int64"))
    var s2 = Series(pd.Series(Python.evaluate("[3, 4, 5]"), dtype="int64"))
    var rp = s1.mod(s2).to_pandas()
    assert_equal(String(rp.dtype), "int64")
    assert_true(Float64(String(rp.iloc[0])) == 1.0)
    assert_true(Float64(String(rp.iloc[1])) == 3.0)
    assert_true(Float64(String(rp.iloc[2])) == 4.0)


def test_int64_dtype_preserved_pow() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[2, 3, 4]"), dtype="int64"))
    var s2 = Series(pd.Series(Python.evaluate("[3, 2, 1]"), dtype="int64"))
    var rp = s1.pow(s2).to_pandas()
    assert_equal(String(rp.dtype), "int64")
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


def test_fillna_null_raises() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var raised = False
    try:
        _ = s.fillna(DFScalar.null())
    except e:
        raised = "null" in String(e)
    assert_true(raised)


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
    assert_true(Float64(String(rp.iloc[1])) == 2.0)


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


def test_sem() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]")))
    var result = s.sem()
    var expected = Float64(String(pd.Series(Python.evaluate("[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]")).sem()))
    assert_true(result > expected - 1e-9 and result < expected + 1e-9)


def test_sem_with_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0, 5.0]")))
    var result = s.sem()
    var expected = Float64(String(pd.Series(Python.evaluate("[1.0, None, 3.0, 5.0]")).sem()))
    assert_true(result > expected - 1e-9 and result < expected + 1e-9)


def test_skew() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 100.0]")))
    var result = s.skew()
    var expected = Float64(String(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 100.0]")).skew()))
    assert_true(result > expected - 1e-9 and result < expected + 1e-9)


def test_skew_symmetric() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]")))
    var result = s.skew()
    assert_true(result > -1e-9 and result < 1e-9)


def test_kurt() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 100.0]")))
    var result = s.kurt()
    var expected = Float64(String(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 100.0]")).kurt()))
    assert_true(result > expected - 1e-9 and result < expected + 1e-9)


def test_idxmin() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, 1.0, 4.0, 1.5, 9.0]")))
    var result = s.idxmin()
    assert_equal(result, 1)


def test_idxmin_with_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[None, 1.0, 4.0, 0.5, 9.0]")))
    var result = s.idxmin()
    assert_equal(result, 3)


def test_idxmax() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[3.0, 1.0, 4.0, 1.5, 9.0]")))
    var result = s.idxmax()
    assert_equal(result, 4)


def test_idxmax_with_nulls() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[None, 1.0, 4.0, 0.5, 9.0]")))
    var result = s.idxmax()
    assert_equal(result, 4)


def test_corr() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[5.0, 4.0, 3.0, 2.0, 1.0]")))
    var result = s1.corr(s2)
    assert_true(result > -1.0 - 1e-9 and result < -1.0 + 1e-9)


def test_corr_identical() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]")))
    var result = s1.corr(s2)
    assert_true(result > 1.0 - 1e-9 and result < 1.0 + 1e-9)


def test_corr_with_nulls() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, None, 3.0, 4.0, 5.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[5.0, 4.0, 3.0, None, 1.0]")))
    var result = s1.corr(s2)
    var ps1 = pd.Series(Python.evaluate("[1.0, None, 3.0, 4.0, 5.0]"))
    var ps2 = pd.Series(Python.evaluate("[5.0, 4.0, 3.0, None, 1.0]"))
    var expected = Float64(String(ps1.corr(ps2)))
    assert_true(result > expected - 1e-9 and result < expected + 1e-9)


def test_cov() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[5.0, 4.0, 3.0, 2.0, 1.0]")))
    var result = s1.cov(s2)
    var ps1 = pd.Series(Python.evaluate("[1.0, 2.0, 3.0, 4.0, 5.0]"))
    var ps2 = pd.Series(Python.evaluate("[5.0, 4.0, 3.0, 2.0, 1.0]"))
    var expected = Float64(String(ps1.cov(ps2)))
    assert_true(result > expected - 1e-9 and result < expected + 1e-9)


def test_cov_with_nulls() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("[1.0, None, 3.0, 4.0, 5.0]")))
    var s2 = Series(pd.Series(Python.evaluate("[5.0, 4.0, 3.0, None, 1.0]")))
    var result = s1.cov(s2)
    var ps1 = pd.Series(Python.evaluate("[1.0, None, 3.0, 4.0, 5.0]"))
    var ps2 = pd.Series(Python.evaluate("[5.0, 4.0, 3.0, None, 1.0]"))
    var expected = Float64(String(ps1.cov(ps2)))
    assert_true(result > expected - 1e-9 and result < expected + 1e-9)


def test_gt_scalar() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 3.0, 5.0]")))
    var mask = s.__gt__(2.0)
    assert_equal(mask.dtype().name, "bool")
    assert_true(mask.iloc(0)[Bool] == False)
    assert_true(mask.iloc(1)[Bool] == True)
    assert_true(mask.iloc(2)[Bool] == True)


def test_lt_scalar() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 3.0, 5.0]")))
    var mask = s.__lt__(3.0)
    assert_equal(mask.dtype().name, "bool")
    assert_true(mask.iloc(0)[Bool] == True)
    assert_true(mask.iloc(1)[Bool] == False)
    assert_true(mask.iloc(2)[Bool] == False)


def test_ge_scalar() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 3.0, 5.0]")))
    var mask = s.__ge__(3.0)
    assert_true(mask.iloc(0)[Bool] == False)
    assert_true(mask.iloc(1)[Bool] == True)
    assert_true(mask.iloc(2)[Bool] == True)


def test_le_scalar() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 3.0, 5.0]")))
    var mask = s.__le__(3.0)
    assert_true(mask.iloc(0)[Bool] == True)
    assert_true(mask.iloc(1)[Bool] == True)
    assert_true(mask.iloc(2)[Bool] == False)


def test_eq_scalar_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var mask = s.__eq__(Float64(2.0))
    assert_true(mask.iloc(0)[Bool] == False)
    assert_true(mask.iloc(1)[Bool] == True)
    assert_true(mask.iloc(2)[Bool] == False)


def test_ne_scalar_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var mask = s.__ne__(Float64(2.0))
    assert_true(mask.iloc(0)[Bool] == True)
    assert_true(mask.iloc(1)[Bool] == False)
    assert_true(mask.iloc(2)[Bool] == True)


def test_eq_scalar_string() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['a', 'b', 'a']")))
    var mask = s.__eq__(String("a"))
    assert_equal(mask.dtype().name, "bool")
    assert_true(mask.iloc(0)[Bool] == True)
    assert_true(mask.iloc(1)[Bool] == False)
    assert_true(mask.iloc(2)[Bool] == True)


def test_ne_scalar_string() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['a', 'b', 'a']")))
    var mask = s.__ne__(String("a"))
    assert_true(mask.iloc(0)[Bool] == False)
    assert_true(mask.iloc(1)[Bool] == True)
    assert_true(mask.iloc(2)[Bool] == False)


def test_gt_int_series() raises:
    # Int64 column compared against a Float64 scalar (numeric promotion path)
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4, 5]")))
    var mask = s.__gt__(3.0)
    assert_true(mask.iloc(0)[Bool] == False)
    assert_true(mask.iloc(3)[Bool] == True)
    assert_true(mask.iloc(4)[Bool] == True)


def test_eq_string_series() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("['a', 'b', 'c']"), dtype="string"))
    var s2 = Series(pd.Series(Python.evaluate("['a', 'x', 'c']"), dtype="string"))
    var rp = s1.eq(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == True)
    assert_true(Bool(rp.iloc[1]) == False)
    assert_true(Bool(rp.iloc[2]) == True)


def test_ne_string_series() raises:
    var pd = Python.import_module("pandas")
    var s1 = Series(pd.Series(Python.evaluate("['a', 'b', 'c']"), dtype="string"))
    var s2 = Series(pd.Series(Python.evaluate("['a', 'x', 'c']"), dtype="string"))
    var rp = s1.ne(s2).to_pandas()
    assert_true(Bool(rp.iloc[0]) == False)
    assert_true(Bool(rp.iloc[1]) == True)
    assert_true(Bool(rp.iloc[2]) == False)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
