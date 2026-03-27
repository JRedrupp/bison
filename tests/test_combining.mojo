"""Tests for DataFrame combining methods: merge, join, append."""
from std.python import Python, PythonObject
from std.testing import assert_true, assert_equal, TestSuite
from bison import DataFrame


def test_merge_inner_on_key() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2, 3], 'a': [10, 20, 30]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2, 4], 'b': [100, 200, 400]}")))
    var on = List[String]()
    on.append("key")
    var result = left.merge(right, on=on^)
    # inner join on key: rows with key 1 and 2 match
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 3)  # key, a, b
    var a_col = result["a"]
    assert_equal(a_col.iloc(0)[Int64], Int64(10))
    assert_equal(a_col.iloc(1)[Int64], Int64(20))


def test_merge_left_join() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'a': [10, 20]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1], 'b': [100]}")))
    var on = List[String]()
    on.append("key")
    var result = left.merge(right, how="left", on=on^)
    # left join: both left rows kept, right row 2 gets NaN for b
    assert_equal(result.shape()[0], 2)


def test_merge_outer_join() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'a': [10, 20]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [2, 3], 'b': [200, 300]}")))
    var on = List[String]()
    on.append("key")
    var result = left.merge(right, how="outer", on=on^)
    # outer join: keys 1, 2, 3
    assert_equal(result.shape()[0], 3)


def test_merge_suffixes() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'val': [10, 20]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'val': [100, 200]}")))
    var on = List[String]()
    on.append("key")
    var suf = List[String]()
    suf.append("_left")
    suf.append("_right")
    var result = left.merge(right, on=on^, suffixes=Optional[List[String]](suf^))
    var cols = result.columns()
    var found_left = False
    var found_right = False
    for i in range(len(cols)):
        if cols[i] == "val_left":
            found_left = True
        if cols[i] == "val_right":
            found_right = True
    assert_true(found_left)
    assert_true(found_right)


def test_join_basic() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var result = left.join(right)
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 2)
    var a_col = result["a"]
    var b_col = result["b"]
    assert_equal(a_col.iloc(0)[Int64], Int64(1))
    assert_equal(b_col.iloc(0)[Int64], Int64(3))


def test_join_lsuffix_rsuffix() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'x': [3, 4]}")))
    var result = left.join(right, lsuffix="_l", rsuffix="_r")
    var cols = result.columns()
    var found_l = False
    var found_r = False
    for i in range(len(cols)):
        if cols[i] == "x_l":
            found_l = True
        if cols[i] == "x_r":
            found_r = True
    assert_true(found_l)
    assert_true(found_r)


def test_append_basic() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3, 4]}")))
    var result = df1.append(df2)
    assert_equal(result.shape()[0], 4)
    assert_equal(result.shape()[1], 1)


def test_append_ignore_index() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'n': [10]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'n': [20]}")))
    var result = df1.append(df2, ignore_index=True)
    assert_equal(result.shape()[0], 2)
    var col = result["n"]
    assert_equal(col.iloc(0)[Int64], Int64(10))
    assert_equal(col.iloc(1)[Int64], Int64(20))


def test_join_how_inner_raises() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var raised = False
    try:
        _ = left.join(right, how="inner")
    except e:
        raised = True
        assert_true("not implemented" in String(e))
    if not raised:
        raise Error("join with how='inner' should have raised")


def test_join_how_outer_raises() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var raised = False
    try:
        _ = left.join(right, how="outer")
    except e:
        raised = True
        assert_true("not implemented" in String(e))
    if not raised:
        raise Error("join with how='outer' should have raised")


def test_join_how_right_raises() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var raised = False
    try:
        _ = left.join(right, how="right")
    except e:
        raised = True
        assert_true("not implemented" in String(e))
    if not raised:
        raise Error("join with how='right' should have raised")


def test_join_on_raises() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var on = List[String]()
    on.append("a")
    var raised = False
    try:
        _ = left.join(right, on=on^)
    except e:
        raised = True
        assert_true("not implemented" in String(e))
    if not raised:
        raise Error("join with on= should have raised")


def test_join_sort_raises() raises:
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var raised = False
    try:
        _ = left.join(right, sort=True)
    except e:
        raised = True
        assert_true("not implemented" in String(e))
    if not raised:
        raise Error("join with sort=True should have raised")


def test_take_with_nulls_obj_col_null_placeholder_is_none() raises:
    """take_with_nulls on a PythonObject column must emit None (not data[0])
    as the null placeholder for unmatched rows.  Regression test for #331."""
    var pd = Python.import_module("pandas")
    # Build a right frame whose 'tag' column is object-dtype so bison stores
    # it as List[PythonObject].  Only key=1 is present on the right side.
    var make_right = Python.evaluate(
        "lambda pd: pd.DataFrame({'key': [1], 'tag': pd.Series([99], dtype=object)})"
    )
    var right_pd = make_right(pd)
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'a': [10, 20]}")))
    var right = DataFrame(right_pd)
    var on = List[String]()
    on.append("key")
    var result = left.merge(right, how="left", on=on^)
    # Row 0 (key=1): matched — tag should be 99, not null.
    # Row 1 (key=2): unmatched — tag should be null, NOT 99 (which is data[0]).
    assert_equal(result.shape()[0], 2)
    # to_pandas() for List[PythonObject] columns passes raw data unconditionally
    # (no null-mask override).  With the bug, data[0]=99 is the placeholder so
    # the round-trip exposes it as 99 instead of NaN.
    var result_pd = result.to_pandas()
    var check = Python.evaluate(
        "lambda df: __import__('pandas').isna(df['tag'].iloc[1])"
    )
    assert_true(Bool(check(result_pd).__bool__()))  # fails with bug, passes after fix


def test_merge_outer_key_col_right_only_rows() raises:
    """outer join: right-only rows must carry the right key value, not NaN.

    Regression test for issue #334: the key column was built exclusively from
    take_with_nulls(out_left), so any row where out_left[r]==-1 (right-only)
    emitted null instead of the right frame's key value.
    """
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'val': [10, 20]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [2, 3], 'val': [200, 300]}")))
    var on = List[String]()
    on.append("key")
    var suf = List[String]()
    suf.append("_left")
    suf.append("_right")
    var result = left.merge(right, how="outer", on=on^, suffixes=Optional[List[String]](suf^))
    assert_equal(result.shape()[0], 3)
    # Row order: row 0 = left-only (key=1), row 1 = matched (key=2),
    # row 2 = right-only (key=3 — was null before fix).
    var result_pd = result.to_pandas()
    var key_col_pd = result_pd["key"]
    assert_true(
        not Bool(py=pd.isna(key_col_pd.iloc[2])),
        "right-only row key must not be NaN",
    )
    assert_equal(Int(py=key_col_pd.iloc[2]), 3)


def test_merge_right_key_col_right_only_rows() raises:
    """right join: right-only rows must carry the right key value, not NaN.

    Regression test for issue #334.
    """
    var pd = Python.import_module("pandas")
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'val': [10, 20]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [2, 3], 'val': [200, 300]}")))
    var on = List[String]()
    on.append("key")
    var suf = List[String]()
    suf.append("_left")
    suf.append("_right")
    var result = left.merge(right, how="right", on=on^, suffixes=Optional[List[String]](suf^))
    assert_equal(result.shape()[0], 2)
    # Row 0 = matched (key=2), row 1 = right-only (key=3 — was null before fix).
    var result_pd = result.to_pandas()
    var key_col_pd = result_pd["key"]
    assert_true(
        not Bool(py=pd.isna(key_col_pd.iloc[1])),
        "right-only row key must not be NaN",
    )
    assert_equal(Int(py=key_col_pd.iloc[1]), 3)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
