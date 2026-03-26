"""Tests for .str string accessor methods and .dt accessor stubs."""
from std.python import Python, PythonObject
from std.testing import assert_true, assert_false, assert_equal, TestSuite
from bison import Series


def test_str_upper() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo', 'Bar', 'BAZ']"), dtype="string"))
    var result = Series(s.str().upper())
    assert_equal(result.iloc(0)[String], "FOO")
    assert_equal(result.iloc(1)[String], "BAR")
    assert_equal(result.iloc(2)[String], "BAZ")


def test_str_lower() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['FOO', 'Bar', 'baz']"), dtype="string"))
    var result = Series(s.str().lower())
    assert_equal(result.iloc(0)[String], "foo")
    assert_equal(result.iloc(1)[String], "bar")
    assert_equal(result.iloc(2)[String], "baz")


def test_str_strip() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['  hello  ', ' world ']"), dtype="string"))
    var result = Series(s.str().strip())
    assert_equal(result.iloc(0)[String], "hello")
    assert_equal(result.iloc(1)[String], "world")


def test_str_lstrip() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['  hello  ', '  world']"), dtype="string"))
    var result = Series(s.str().lstrip())
    assert_equal(result.iloc(0)[String], "hello  ")
    assert_equal(result.iloc(1)[String], "world")


def test_str_rstrip() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['  hello  ', 'world  ']"), dtype="string"))
    var result = Series(s.str().rstrip())
    assert_equal(result.iloc(0)[String], "  hello")
    assert_equal(result.iloc(1)[String], "world")


def test_str_strip_chars() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['***hello***', '---world---']"), dtype="string"))
    var result = Series(s.str().strip("*"))
    assert_equal(result.iloc(0)[String], "hello")


def test_str_contains() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world', 'hi']"), dtype="string"))
    var result = Series(s.str().contains("ello"))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_false(result.iloc(2)[Bool])


def test_str_contains_case_insensitive() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['Hello', 'WORLD', 'hi']"), dtype="string"))
    var result = Series(s.str().contains("hello", `case`=False))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_false(result.iloc(2)[Bool])


def test_str_startswith() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world', 'hey']"), dtype="string"))
    var result = Series(s.str().startswith("he"))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_true(result.iloc(2)[Bool])


def test_str_endswith() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world', 'jello']"), dtype="string"))
    var result = Series(s.str().endswith("llo"))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_true(result.iloc(2)[Bool])


def test_str_replace_literal() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo bar', 'foo baz']"), dtype="string"))
    var result = Series(s.str().replace("foo", "qux", regex=False))
    assert_equal(result.iloc(0)[String], "qux bar")
    assert_equal(result.iloc(1)[String], "qux baz")


def test_str_replace_regex() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo123', 'bar456']"), dtype="string"))
    var result = Series(s.str().replace("[0-9]+", "NUM"))
    assert_equal(result.iloc(0)[String], "fooNUM")
    assert_equal(result.iloc(1)[String], "barNUM")


def test_str_len() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'hi', 'hey there']"), dtype="string"))
    var result = Series(s.str().len())
    assert_equal(result.iloc(0)[Int64], Int64(5))
    assert_equal(result.iloc(1)[Int64], Int64(2))
    assert_equal(result.iloc(2)[Int64], Int64(9))


def test_str_get() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world']"), dtype="string"))
    var result = Series(s.str().get(1))
    assert_equal(result.iloc(0)[String], "e")
    assert_equal(result.iloc(1)[String], "o")


def test_str_slice() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello', 'world']"), dtype="string"))
    var result = Series(s.str().slice(1, 4))
    assert_equal(result.iloc(0)[String], "ell")
    assert_equal(result.iloc(1)[String], "orl")


def test_str_slice_step() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['abcdef', 'ghijkl']"), dtype="string"))
    var result = Series(s.str().slice(0, 6, 2))
    assert_equal(result.iloc(0)[String], "ace")
    assert_equal(result.iloc(1)[String], "gik")


def test_str_cat() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['foo', 'bar', 'baz']"), dtype="string"))
    var result = s.str().cat(sep=",")
    assert_equal(result, "foo,bar,baz")


def test_str_find() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello world', 'foobar']"), dtype="string"))
    var result = Series(s.str().find("o"))
    assert_equal(result.iloc(0)[Int64], Int64(4))
    assert_equal(result.iloc(1)[Int64], Int64(1))


def test_str_count() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['aabbcc', 'aaaa', 'b']"), dtype="string"))
    var result = Series(s.str().count("a"))
    assert_equal(result.iloc(0)[Int64], Int64(2))
    assert_equal(result.iloc(1)[Int64], Int64(4))
    assert_equal(result.iloc(2)[Int64], Int64(0))


def test_str_match() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['hello world', 'world hello', 'foo']"), dtype="string"))
    var result = Series(s.str().match("^hello"))
    assert_true(result.iloc(0)[Bool])
    assert_false(result.iloc(1)[Bool])
    assert_false(result.iloc(2)[Bool])


def test_str_split() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['a,b,c', 'x,y']"), dtype="string"))
    var result = s.str().split(",")
    # result is a List[List[String]]
    assert_equal(len(result[0]), 3)
    assert_equal(len(result[1]), 2)


def test_dt_year() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15', '2021-06-30']"))))
    var result = Series(s.dt().year())
    assert_equal(result.iloc(0)[Int64], Int64(2020))
    assert_equal(result.iloc(1)[Int64], Int64(2021))


def test_dt_month() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15', '2021-06-30']"))))
    var result = Series(s.dt().month())
    assert_equal(result.iloc(0)[Int64], Int64(1))
    assert_equal(result.iloc(1)[Int64], Int64(6))


def test_dt_day() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15', '2021-06-30']"))))
    var result = Series(s.dt().day())
    assert_equal(result.iloc(0)[Int64], Int64(15))
    assert_equal(result.iloc(1)[Int64], Int64(30))


def test_dt_hour() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:30:00', '2021-06-30 23:00:00']"))))
    var result = Series(s.dt().hour())
    assert_equal(result.iloc(0)[Int64], Int64(8))
    assert_equal(result.iloc(1)[Int64], Int64(23))


def test_dt_minute() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:30:00', '2021-06-30 23:45:00']"))))
    var result = Series(s.dt().minute())
    assert_equal(result.iloc(0)[Int64], Int64(30))
    assert_equal(result.iloc(1)[Int64], Int64(45))


def test_dt_second() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:30:15', '2021-06-30 23:45:59']"))))
    var result = Series(s.dt().second())
    assert_equal(result.iloc(0)[Int64], Int64(15))
    assert_equal(result.iloc(1)[Int64], Int64(59))


def test_dt_dayofweek() raises:
    var pd = Python.import_module("pandas")
    # 2020-01-06 is Monday (0), 2020-01-12 is Sunday (6)
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-06', '2020-01-12']"))))
    var result = Series(s.dt().dayofweek())
    assert_equal(result.iloc(0)[Int64], Int64(0))
    assert_equal(result.iloc(1)[Int64], Int64(6))


def test_dt_dayofyear() raises:
    var pd = Python.import_module("pandas")
    # 2020-01-01 is day 1, 2020-12-31 is day 366 (2020 is a leap year)
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-01', '2020-12-31']"))))
    var result = Series(s.dt().dayofyear())
    assert_equal(result.iloc(0)[Int64], Int64(1))
    assert_equal(result.iloc(1)[Int64], Int64(366))


def test_dt_quarter() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15', '2020-07-01']"))))
    var result = Series(s.dt().quarter())
    assert_equal(result.iloc(0)[Int64], Int64(1))
    assert_equal(result.iloc(1)[Int64], Int64(3))


def test_dt_date() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15', '2021-06-30']"))))
    var acc = s.dt()
    var col = acc.date()
    # date() returns a PythonObject column; verify against pandas
    var pd_result = pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15', '2021-06-30']"))).dt.date
    assert_equal(String(col._data[List[PythonObject]][0].__str__()), String(pd_result[0].__str__()))
    assert_equal(String(col._data[List[PythonObject]][1].__str__()), String(pd_result[1].__str__()))


def test_dt_time() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:30:15', '2021-06-30 23:45:59']"))))
    var acc = s.dt()
    var col = acc.time()
    var pd_result = pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:30:15', '2021-06-30 23:45:59']"))).dt.time
    assert_equal(String(col._data[List[PythonObject]][0].__str__()), String(pd_result[0].__str__()))
    assert_equal(String(col._data[List[PythonObject]][1].__str__()), String(pd_result[1].__str__()))


def test_dt_floor() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:37:00', '2021-06-30 23:45:00']"))))
    var col = s.dt().floor("h")
    var pd_result = pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:37:00', '2021-06-30 23:45:00']"))).dt.floor("h")
    assert_equal(String(col._data[List[PythonObject]][0].__str__()), String(pd_result[0].__str__()))
    assert_equal(String(col._data[List[PythonObject]][1].__str__()), String(pd_result[1].__str__()))


def test_dt_ceil() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:37:00', '2021-06-30 23:01:00']"))))
    var col = s.dt().ceil("h")
    var pd_result = pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:37:00', '2021-06-30 23:01:00']"))).dt.ceil("h")
    assert_equal(String(col._data[List[PythonObject]][0].__str__()), String(pd_result[0].__str__()))
    assert_equal(String(col._data[List[PythonObject]][1].__str__()), String(pd_result[1].__str__()))


def test_dt_round() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:20:00', '2021-06-30 23:50:00']"))))
    var col = s.dt().round("h")
    var pd_result = pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:20:00', '2021-06-30 23:50:00']"))).dt.round("h")
    assert_equal(String(col._data[List[PythonObject]][0].__str__()), String(pd_result[0].__str__()))
    assert_equal(String(col._data[List[PythonObject]][1].__str__()), String(pd_result[1].__str__()))


def test_dt_tz_localize() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:00:00', '2021-06-30 12:00:00']"))))
    var col = s.dt().tz_localize("UTC")
    var pd_result = pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:00:00', '2021-06-30 12:00:00']"))).dt.tz_localize("UTC")
    assert_equal(String(col._data[List[PythonObject]][0].__str__()), String(pd_result[0].__str__()))
    assert_equal(String(col._data[List[PythonObject]][1].__str__()), String(pd_result[1].__str__()))


def test_dt_tz_convert() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:00:00', '2021-06-30 12:00:00']")).tz_localize("UTC")))
    var col = s.dt().tz_convert("US/Eastern")
    var pd_result = pd.Series(pd.to_datetime(Python.evaluate("['2020-01-15 08:00:00', '2021-06-30 12:00:00']")).tz_localize("UTC")).dt.tz_convert("US/Eastern")
    assert_equal(String(col._data[List[PythonObject]][0].__str__()), String(pd_result[0].__str__()))
    assert_equal(String(col._data[List[PythonObject]][1].__str__()), String(pd_result[1].__str__()))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
