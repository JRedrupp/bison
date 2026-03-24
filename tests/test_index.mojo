"""Tests for Index methods: get_loc, unique, sort_values."""
from std.testing import assert_equal, assert_true, TestSuite
from bison import Index


def _make_index(a: String, b: String, c: String) -> Index:
    var data = List[String]()
    data.append(a)
    data.append(b)
    data.append(c)
    return Index(data^)


def _make_index_named(a: String, b: String, c: String, name: String) -> Index:
    var data = List[String]()
    data.append(a)
    data.append(b)
    data.append(c)
    return Index(data^, name)


def test_get_loc_found() raises:
    var idx = _make_index("a", "b", "c")
    assert_equal(idx.get_loc("a"), 0)
    assert_equal(idx.get_loc("b"), 1)
    assert_equal(idx.get_loc("c"), 2)


def test_get_loc_first_occurrence() raises:
    var data = List[String]()
    data.append("x")
    data.append("y")
    data.append("x")
    var idx = Index(data^)
    assert_equal(idx.get_loc("x"), 0)


def test_get_loc_not_found() raises:
    var idx = _make_index("a", "b", "c")
    var raised = False
    try:
        _ = idx.get_loc("z")
    except:
        raised = True
    assert_true(raised)


def test_unique_no_duplicates() raises:
    var idx = _make_index("a", "b", "c")
    var u = idx.unique()
    assert_equal(u.__len__(), 3)
    assert_equal(u[0], "a")
    assert_equal(u[1], "b")
    assert_equal(u[2], "c")


def test_unique_with_duplicates() raises:
    var data = List[String]()
    data.append("a")
    data.append("b")
    data.append("a")
    data.append("c")
    data.append("b")
    var idx = Index(data^)
    var u = idx.unique()
    assert_equal(u.__len__(), 3)
    assert_equal(u[0], "a")
    assert_equal(u[1], "b")
    assert_equal(u[2], "c")


def test_unique_preserves_name() raises:
    var idx = _make_index_named("x", "x", "y", "myname")
    var u = idx.unique()
    assert_equal(u.name, "myname")


def test_unique_empty() raises:
    var data = List[String]()
    var idx = Index(data^)
    var u = idx.unique()
    assert_equal(u.__len__(), 0)


def test_sort_values_ascending() raises:
    var idx = _make_index("c", "a", "b")
    var s = idx.sort_values()
    assert_equal(s[0], "a")
    assert_equal(s[1], "b")
    assert_equal(s[2], "c")


def test_sort_values_descending() raises:
    var idx = _make_index("c", "a", "b")
    var s = idx.sort_values(ascending=False)
    assert_equal(s[0], "c")
    assert_equal(s[1], "b")
    assert_equal(s[2], "a")


def test_sort_values_already_sorted() raises:
    var idx = _make_index("a", "b", "c")
    var s = idx.sort_values()
    assert_equal(s[0], "a")
    assert_equal(s[1], "b")
    assert_equal(s[2], "c")


def test_sort_values_preserves_name() raises:
    var idx = _make_index_named("b", "a", "c", "myname")
    var s = idx.sort_values()
    assert_equal(s.name, "myname")


def test_sort_values_single_element() raises:
    var data = List[String]()
    data.append("a")
    var idx = Index(data^)
    var s = idx.sort_values()
    assert_equal(s.__len__(), 1)
    assert_equal(s[0], "a")


def test_sort_values_does_not_mutate_original() raises:
    var idx = _make_index("c", "a", "b")
    _ = idx.sort_values()
    assert_equal(idx[0], "c")
    assert_equal(idx[1], "a")
    assert_equal(idx[2], "b")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
