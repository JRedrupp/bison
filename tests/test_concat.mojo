"""Tests for bison.concat."""
from std.python import Python, PythonObject
from std.testing import assert_true, assert_equal, TestSuite
from bison import DataFrame, concat, int64, float64


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


def test_concat_keys_axis0() raises:
    """keys parameter prepends a __key__ column with one label per source row."""
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [3]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var keys = List[String]()
    keys.append("x")
    keys.append("y")
    var result = concat(dfs, keys=Optional[List[String]](keys^))
    assert_equal(result.shape()[0], 3)
    assert_equal(result.shape()[1], 2)
    var k = result["__key__"]
    assert_equal(k.iloc(0)[String], "x")
    assert_equal(k.iloc(1)[String], "x")
    assert_equal(k.iloc(2)[String], "y")


def test_concat_keys_axis1() raises:
    """keys parameter prepends a __key__ column with one label per source column."""
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'b': [3, 4]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var keys = List[String]()
    keys.append("left")
    keys.append("right")
    var result = concat(dfs, axis=1, keys=Optional[List[String]](keys^))
    assert_equal(result.shape()[1], 3)
    var k = result["__key__"]
    assert_equal(k.iloc(0)[String], "left")
    assert_equal(k.iloc(1)[String], "right")


def test_concat_keys_length_mismatch_raises() raises:
    """concat raises when len(keys) != len(objs)."""
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'a': [2]}")))
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var raised = False
    try:
        var keys = List[String]()
        keys.append("only_one")
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


def test_concat_dtype_promotion_int_float() raises:
    """Concatenating int64 and float64 columns with the same name must promote to float64."""
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1, 2]}")))
    var df2 = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1.5, 2.5]}")))
    var s1 = df1["x"]
    var s2 = df2["x"]
    assert_true(s1.dtype() == int64)
    assert_true(s2.dtype() == float64)
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs)
    assert_equal(result.shape()[0], 4)
    # All four values should be accessible as Float64.
    var s = result["x"]
    assert_equal(s.iloc(0)[Float64], Float64(1.0))
    assert_equal(s.iloc(1)[Float64], Float64(2.0))
    assert_equal(s.iloc(2)[Float64], Float64(1.5))
    assert_equal(s.iloc(3)[Float64], Float64(2.5))


def test_concat_dtype_mismatch_raises() raises:
    """Concatenating incompatible column dtypes (int64 vs string) must raise."""
    var pd = Python.import_module("pandas")
    var df1 = DataFrame(
        pd.DataFrame(Python.evaluate("{'x': [1, 2]}"))
    )
    var df2 = DataFrame(
        pd.DataFrame(
            Python.evaluate("{'x': ['a', 'b']}"), dtype="object"
        )
    )
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var raised = False
    try:
        _ = concat(dfs)
    except e:
        raised = "dtype mismatch" in String(e)
    assert_true(raised)


def test_concat_dtype_promotion_outer_join() raises:
    """When frames have different column sets, null pads use the promoted dtype."""
    var pd = Python.import_module("pandas")
    # df1 has x as int64, df2 has x as float64 and y as int64
    var df1 = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1]}")))
    var df2 = DataFrame(
        pd.DataFrame(Python.evaluate("{'x': [2.5], 'y': [10]}"))
    )
    var dfs = List[DataFrame]()
    dfs.append(df1^)
    dfs.append(df2^)
    var result = concat(dfs, join="outer")
    # x must be float64 across both rows
    var s_x = result["x"]
    assert_equal(s_x.iloc(0)[Float64], Float64(1.0))
    assert_equal(s_x.iloc(1)[Float64], Float64(2.5))
    # y is only present in df2, so it must be int64 (no promotion needed)
    var s_y = result["y"]
    assert_equal(s_y.iloc(1)[Int64], Int64(10))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
