"""Tests for Series transformations, sorting, and selection methods."""
from std.python import Python
from std.testing import assert_equal, assert_true, assert_false, TestSuite
from bison import Series


def _double(v: Float64) -> Float64:
    return v * 2.0

def _identity(v: Float64) -> Float64:
    return v

def _negate(v: Float64) -> Float64:
    return -v


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
