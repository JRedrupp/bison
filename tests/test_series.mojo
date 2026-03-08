"""Tests for Series construction and basic attributes."""
from python import Python, PythonObject
from testing import assert_equal, assert_true, assert_false
from bison import Series


def test_from_pandas() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series([1, 2, 3], name="vals")
    var s = Series.from_pandas(pd_s)
    assert_equal(s.name, "vals")
    assert_equal(s.__len__(), 3)


def test_size() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series([10, 20, 30]))
    assert_equal(s.size(), 3)


def test_empty_false() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series([1]))
    assert_false(s.empty())


def test_empty_true() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series([], dtype="float64"))
    assert_true(s.empty())


def test_shape() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series([1, 2, 3, 4]))
    var sh = s.shape()
    assert_equal(sh[0], 4)


def test_to_pandas_roundtrip() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series([7, 8, 9])
    var s = Series(pd_s)
    var back = s.to_pandas()
    assert_equal(int(back.__len__()), 3)


def test_stub_raises_sum() raises:
    """Stub methods must raise with 'not implemented'."""
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series([1, 2, 3]))
    var raised = False
    try:
        _ = s.sum()
    except e:
        raised = True
        assert_true(str(e).__contains__("not implemented"))
    assert_true(raised)


def test_stub_raises_head() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series([1, 2, 3]))
    var raised = False
    try:
        _ = s.head()
    except:
        raised = True
    assert_true(raised)


def main() raises:
    test_from_pandas()
    test_size()
    test_empty_false()
    test_empty_true()
    test_shape()
    test_to_pandas_roundtrip()
    test_stub_raises_sum()
    test_stub_raises_head()
    print("test_series: all tests passed")
