"""Tests for bison.concat."""
from std.python import Python, PythonObject
from std.testing import assert_true, assert_equal, TestSuite
from bison import DataFrame, concat


def test_concat_empty_list() raises:
    var result = concat(List[DataFrame]())
    assert_true(result.empty())


def test_concat_single_df() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var dfs = List[DataFrame]()
    dfs.append(df^)
    var result = concat(dfs)
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 2)


def test_concat_axis0_same_columns() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [5, 6], 'b': [7, 8]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs)
    assert_equal(result.shape()[0], 4)
    assert_equal(result.shape()[1], 2)
    # Verify values: column a should be [1, 2, 5, 6]
    var s_a = result["a"]
    assert_equal(s_a.iloc(0)[Int64], Int64(1))
    assert_equal(s_a.iloc(1)[Int64], Int64(2))
    assert_equal(s_a.iloc(2)[Int64], Int64(5))
    assert_equal(s_a.iloc(3)[Int64], Int64(6))


def test_concat_axis0_outer_join() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3], 'c': [4]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs, join="outer")
    # outer join: columns a, b, c; df1 missing c, df2 missing a
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 3)
    # b column should be [2, 3]
    var s_b = result["b"]
    assert_equal(s_b.iloc(0)[Int64], Int64(2))
    assert_equal(s_b.iloc(1)[Int64], Int64(3))


def test_concat_axis0_inner_join() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3], 'c': [4]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs, join="inner")
    # inner join: only column b
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 1)
    var cols = result.columns()
    assert_equal(cols[0], String("b"))


def test_concat_axis0_sort() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'z': [1], 'a': [2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'m': [3], 'a': [4]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs, sort=True)
    var cols = result.columns()
    # Sorted alphabetically: a, m, z
    assert_equal(cols[0], String("a"))
    assert_equal(cols[1], String("m"))
    assert_equal(cols[2], String("z"))


def test_concat_axis1() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs, axis=1)
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 2)
    var s_a = result["a"]
    var s_b = result["b"]
    assert_equal(s_a.iloc(0)[Int64], Int64(1))
    assert_equal(s_b.iloc(0)[Int64], Int64(3))


def test_concat_axis1_ignore_index() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'b': [2]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs, axis=1, ignore_index=True)
    assert_equal(result.shape()[1], 2)
    var cols = result.columns()
    assert_equal(cols[0], String("0"))
    assert_equal(cols[1], String("1"))


def test_concat_axis0_float() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1.5, 2.5]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'x': [3.5]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs)
    assert_equal(result.shape()[0], 3)
    var s_x = result["x"]
    assert_equal(s_x.iloc(2)[Float64], Float64(3.5))


def test_concat_three_dfs() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'n': [1]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'n': [2]}")))
    var df3 = DataFrame(pd.DataFrame(Python.evaluate("{'n': [3]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    dfs.append(df3^)
    var result = concat(dfs)
    assert_equal(result.shape()[0], 3)
    var s = result["n"]
    assert_equal(s.iloc(0)[Int64], Int64(1))
    assert_equal(s.iloc(1)[Int64], Int64(2))
    assert_equal(s.iloc(2)[Int64], Int64(3))


def test_concat_keys_raises() raises:
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [2]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var raised = False
    try:
        var keys = List[String]()
        keys.append("k1")
        keys.append("k2")
        _ = concat(dfs, keys=Optional[List[String]](keys^))
    except:
        raised = True
    assert_true(raised)


def test_append_dtype_mismatch_raises() raises:
    """Appending a Float64 column onto an Int64 column must raise, not silently drop data."""
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1, 2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1.5, 2.5]}")))
    var raised = False
    try:
        _ = df1.append(df2)
    except e:
        raised = "dtype mismatch" in String(e)
    assert_true(raised)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
