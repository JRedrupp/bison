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


def test_join_how_inner() raises:
    var pd = Python.import_module("pandas")
    # left has 3 rows, right has 2 rows — inner keeps min(3,2)=2 rows.
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [10, 20]}")))
    var result = left.join(right, how="inner")
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 2)
    var a_col = result["a"]
    var b_col = result["b"]
    assert_equal(a_col.iloc(0)[Int64], Int64(1))
    assert_equal(a_col.iloc(1)[Int64], Int64(2))
    assert_equal(b_col.iloc(0)[Int64], Int64(10))
    assert_equal(b_col.iloc(1)[Int64], Int64(20))


def test_join_how_outer() raises:
    var pd = Python.import_module("pandas")
    # left has 2 rows, right has 3 rows — outer keeps max(2,3)=3 rows.
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [10, 20, 30]}")))
    var result = left.join(right, how="outer")
    assert_equal(result.shape()[0], 3)
    assert_equal(result.shape()[1], 2)
    # Row 2: left side is null; right side has 30.
    var b_col = result["b"]
    assert_equal(b_col.iloc(2)[Int64], Int64(30))


def test_join_how_right() raises:
    var pd = Python.import_module("pandas")
    # left has 1 row, right has 3 rows — right keeps all 3 right rows.
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'b': [10, 20, 30]}")))
    var result = left.join(right, how="right")
    assert_equal(result.shape()[0], 3)
    assert_equal(result.shape()[1], 2)
    var b_col = result["b"]
    assert_equal(b_col.iloc(0)[Int64], Int64(10))
    assert_equal(b_col.iloc(1)[Int64], Int64(20))
    assert_equal(b_col.iloc(2)[Int64], Int64(30))


def test_join_on_parameter() raises:
    var pd = Python.import_module("pandas")
    # Join on column 'key': key=1 matches, key=2 does not appear in right.
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'a': [10, 20]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 3], 'b': [100, 300]}")))
    var on = List[String]()
    on.append("key")
    # Default how="left": both left rows kept, row 2 gets null for b.
    var result = left.join(right, on=on^)
    assert_equal(result.shape()[0], 2)


def test_join_sort_parameter() raises:
    var pd = Python.import_module("pandas")
    # Join on 'key' with sort=True: result sorted by key ascending.
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [2, 1], 'a': [20, 10]}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'b': [100, 200]}")))
    var on = List[String]()
    on.append("key")
    var result = left.join(right, on=on^, how="inner", sort=True)
    assert_equal(result.shape()[0], 2)
    # After sort by 'key', first row should have key=1.
    var key_col = result["key"]
    assert_equal(key_col.iloc(0)[Int64], Int64(1))
    assert_equal(key_col.iloc(1)[Int64], Int64(2))


def test_take_with_nulls_obj_col_null_placeholder_is_none() raises:
    """Take_with_nulls on a PythonObject column must emit None (not data[0])
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
    """Outer join: right-only rows must carry the right key value, not NaN.

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
    """Right join: right-only rows must carry the right key value, not NaN.

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


# ------------------------------------------------------------------
# Composite key delimiter collision tests (issue #506)
# ------------------------------------------------------------------


def test_merge_multikey_delimiter_collision() raises:
    """Multi-key merge where string values contain the pipe delimiter.

    Without collision-safe serialisation, ("a|b", "c") and ("a", "b|c")
    both serialise to "a|b|c", producing incorrect join matches.
    """
    var pd = Python.import_module("pandas")
    var left = DataFrame(
        pd.DataFrame(
            Python.evaluate(
                "{'k1': ['a|b', 'a'], 'k2': ['c', 'b|c'], 'val': [1, 2]}"
            )
        )
    )
    var right = DataFrame(
        pd.DataFrame(
            Python.evaluate(
                "{'k1': ['a|b', 'a'], 'k2': ['c', 'b|c'], 'score': [100, 200]}"
            )
        )
    )
    var on = List[String]()
    on.append("k1")
    on.append("k2")
    var result = right.merge(left, on=on^)
    # Each row should match exactly once: 2 result rows, not 4.
    assert_equal(result.shape()[0], 2)
    var result_pd = result.to_pandas()
    # Verify row 0 matched ("a|b", "c") → score=100, val=1
    assert_equal(Int(py=result_pd["score"].iloc[0]), 100)
    assert_equal(Int(py=result_pd["val"].iloc[0]), 1)
    # Verify row 1 matched ("a", "b|c") → score=200, val=2
    assert_equal(Int(py=result_pd["score"].iloc[1]), 200)
    assert_equal(Int(py=result_pd["val"].iloc[1]), 2)


def test_merge_multikey_delimiter_no_false_match() raises:
    """Keys that would collide under naive pipe-joining must NOT match."""
    var pd = Python.import_module("pandas")
    var left = DataFrame(
        pd.DataFrame(
            Python.evaluate("{'k1': ['a|b'], 'k2': ['c'], 'val': [1]}")
        )
    )
    var right = DataFrame(
        pd.DataFrame(
            Python.evaluate("{'k1': ['a'], 'k2': ['b|c'], 'score': [99]}")
        )
    )
    var on = List[String]()
    on.append("k1")
    on.append("k2")
    var result = left.merge(right, on=on^)
    # Inner join: no keys actually match, so result must be empty.
    assert_equal(result.shape()[0], 0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
