"""Tests for DataFrame combining methods: merge, join, append."""
from std.python import Python
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


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
