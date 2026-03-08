"""Tests for Series construction and basic attributes."""
from python import Python, PythonObject
from testing import assert_equal, assert_true, assert_false
from bison import Series


def test_from_pandas():
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1, 2, 3]"), name="vals")
    var s = Series.from_pandas(pd_s)
    assert_equal(s.name, "vals")
    assert_equal(s.__len__(), 3)


def test_size():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]")))
    assert_equal(s.size(), 3)


def test_empty_false():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1]")))
    assert_false(s.empty())


def test_empty_true():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[]"), dtype="float64"))
    assert_true(s.empty())


def test_shape():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3, 4]")))
    var sh = s.shape()
    assert_equal(sh[0], 4)


def test_to_pandas_roundtrip():
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[7, 8, 9]"))
    var s = Series(pd_s)
    var back = s.to_pandas()
    assert_equal(back.__len__(), 3)


def test_stub_raises_sum():
    """Stub methods must raise with 'not implemented'."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var raised = False
    try:
        _ = s.sum()
    except e:
        raised = True
        assert_true(String(e).__contains__("not implemented"))
    assert_true(raised)


def test_stub_raises_head():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]")))
    var raised = False
    try:
        _ = s.head()
    except:
        raised = True
    assert_true(raised)


def main():
    test_from_pandas()
    test_size()
    test_empty_false()
    test_empty_true()
    test_shape()
    test_to_pandas_roundtrip()
    test_stub_raises_sum()
    test_stub_raises_head()
    print("test_series: all tests passed")
