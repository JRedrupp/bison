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


def test_merge_outer_obj_dtype_null_placeholder() raises:
    """Regression test for issue #331: on_obj null placeholder must be None, not data[0]."""
    var pd = Python.import_module("pandas")
    # 'val' column is object dtype (mixed/string values); outer join produces unmatched rows.
    var left = DataFrame(pd.DataFrame(Python.evaluate("{'key': [1, 2], 'val': ['a', 'b']}")))
    var right = DataFrame(pd.DataFrame(Python.evaluate("{'key': [2, 3], 'extra': ['x', 'y']}")))
    var on = List[String]()
    on.append("key")
    var result = left.merge(right, how="outer", on=on^)
    # key=1 row: right side unmatched — 'extra' should be null, not 'x'
    # key=3 row: left side unmatched — 'val' should be null, not 'a'
    assert_equal(result.shape()[0], 3)
    var pd_result = result.to_pandas()
    # The 'val' null for key=3 must be pd.NA/NaN, not the string 'a'
    assert_true(Bool(py=pd.isna(pd_result.loc[2, "val"])))
    # The 'extra' null for key=1 must be pd.NA/NaN, not the string 'x'
    assert_true(Bool(py=pd.isna(pd_result.loc[0, "extra"])))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
