"""Tests for .str string accessor methods and .dt accessor stubs."""
from std.python import Python
from testing import assert_true, assert_false, assert_equal, TestSuite
from bison import Series


fn test_str_upper() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo', 'Bar', 'BAZ']"), dtype="string"))
    var result = Series(s.str().upper())
    assert_equal(result.iloc(0)[String], "FOO")
    assert_equal(result.iloc(1)[String], "BAR")
    assert_equal(result.iloc(2)[String], "BAZ")


fn test_str_lower() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['FOO', 'Bar', 'baz']"), dtype="string"))
    var result = Series(s.str().lower())
    assert_equal(result.iloc(0)[String], "foo")
    assert_equal(result.iloc(1)[String], "bar")
    assert_equal(result.iloc(2)[String], "baz")


fn test_str_strip() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['  hello  ', ' world ']"), dtype="string"))
    var result = Series(s.str().strip())
    assert_equal(result.iloc(0)[String], "hello")
    assert_equal(result.iloc(1)[String], "world")


fn test_str_lstrip() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['  hello  ', '  world']"), dtype="string"))
    var result = Series(s.str().lstrip())
    assert_equal(result.iloc(0)[String], "hello  ")
    assert_equal(result.iloc(1)[String], "world")


fn test_str_rstrip() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['  hello  ', 'world  ']"), dtype="string"))
    var result = Series(s.str().rstrip())
    assert_equal(result.iloc(0)[String], "  hello")
    assert_equal(result.iloc(1)[String], "world")


fn test_str_strip_chars() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['***hello***', '---world---']"), dtype="string"))
    var result = Series(s.str().strip("*"))
    assert_equal(result.iloc(0)[String], "hello")


fn test_str_contains() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world', 'hi']"), dtype="string"))
    var result = Series(s.str().contains("ello"))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_false(result.iloc(2)[Bool])


fn test_str_contains_case_insensitive() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['Hello', 'WORLD', 'hi']"), dtype="string"))
    var result = Series(s.str().contains("hello", `case`=False))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_false(result.iloc(2)[Bool])


fn test_str_startswith() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world', 'hey']"), dtype="string"))
    var result = Series(s.str().startswith("he"))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_true(result.iloc(2)[Bool])


fn test_str_endswith() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world', 'jello']"), dtype="string"))
    var result = Series(s.str().endswith("llo"))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_true(result.iloc(2)[Bool])


fn test_str_replace_literal() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo bar', 'foo baz']"), dtype="string"))
    var result = Series(s.str().replace("foo", "qux", regex=False))
    assert_equal(result.iloc(0)[String], "qux bar")
    assert_equal(result.iloc(1)[String], "qux baz")


fn test_str_replace_regex() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo123', 'bar456']"), dtype="string"))
    var result = Series(s.str().replace("[0-9]+", "NUM"))
    assert_equal(result.iloc(0)[String], "fooNUM")
    assert_equal(result.iloc(1)[String], "barNUM")


fn test_str_len() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'hi', 'hey there']"), dtype="string"))
    var result = Series(s.str().len())
    assert_equal(result.iloc(0)[Int64], Int64(5))
    assert_equal(result.iloc(1)[Int64], Int64(2))
    assert_equal(result.iloc(2)[Int64], Int64(9))


fn test_str_get() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world']"), dtype="string"))
    var result = Series(s.str().get(1))
    assert_equal(result.iloc(0)[String], "e")
    assert_equal(result.iloc(1)[String], "o")


fn test_str_slice() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world']"), dtype="string"))
    var result = Series(s.str().slice(1, 4))
    assert_equal(result.iloc(0)[String], "ell")
    assert_equal(result.iloc(1)[String], "orl")


fn test_str_slice_step() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['abcdef', 'ghijkl']"), dtype="string"))
    var result = Series(s.str().slice(0, 6, 2))
    assert_equal(result.iloc(0)[String], "ace")
    assert_equal(result.iloc(1)[String], "gik")


fn test_str_cat() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo', 'bar', 'baz']"), dtype="string"))
    var result = s.str().cat(sep=",")
    assert_equal(result, "foo,bar,baz")


fn test_str_find() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello world', 'foobar']"), dtype="string"))
    var result = Series(s.str().find("o"))
    assert_equal(result.iloc(0)[Int64], Int64(4))
    assert_equal(result.iloc(1)[Int64], Int64(1))


fn test_str_count() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['aabbcc', 'aaaa', 'b']"), dtype="string"))
    var result = Series(s.str().count("a"))
    assert_equal(result.iloc(0)[Int64], Int64(2))
    assert_equal(result.iloc(1)[Int64], Int64(4))
    assert_equal(result.iloc(2)[Int64], Int64(0))


fn test_str_match() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello world', 'world hello', 'foo']"), dtype="string"))
    var result = Series(s.str().match("^hello"))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_false(result.iloc(2)[Bool])


fn test_str_split() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['a,b,c', 'x,y']"), dtype="string"))
    var result = s.str().split(",")
    # result is a List[List[String]]
    assert_equal(len(result[0]), 3)
    assert_equal(len(result[1]), 2)


fn test_dt_year_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-01', '2021-06-15']"))))
    var raised = False
    try:
        var acc = s.dt()
        _ = acc.year()
    except:
        raised = True
    assert_true(raised)


fn test_dt_month_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-01']"))))
    var raised = False
    try:
        var acc = s.dt()
        _ = acc.month()
    except:
        raised = True
    assert_true(raised)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
