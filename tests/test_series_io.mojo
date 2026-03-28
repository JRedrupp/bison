"""Tests for Series output and serialization methods."""
from std.python import Python
from std.testing import assert_equal, assert_true, assert_false, TestSuite
from bison import Series


def test_to_list_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]"), dtype="int64"))
    var lst = s.to_list()
    assert_equal(len(lst), 3)
    assert_true(lst[0].isa[Int64]())
    assert_true(lst[0][Int64] == 10)
    assert_true(lst[1][Int64] == 20)
    assert_true(lst[2][Int64] == 30)


def test_to_list_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.5, 2.5, 3.5]")))
    var lst = s.to_list()
    assert_equal(len(lst), 3)
    assert_true(lst[0].isa[Float64]())
    assert_true(lst[0][Float64] == 1.5)
    assert_true(lst[1][Float64] == 2.5)
    assert_true(lst[2][Float64] == 3.5)


def test_to_list_bool() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[True, False, True]")))
    var lst = s.to_list()
    assert_equal(len(lst), 3)
    assert_true(lst[0].isa[Bool]())
    assert_true(lst[0][Bool])
    assert_false(lst[1][Bool])
    assert_true(lst[2][Bool])


def test_to_list_string() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate('["a", "b", "c"]'), dtype="string"))
    var lst = s.to_list()
    assert_equal(len(lst), 3)
    assert_true(lst[0].isa[String]())
    assert_equal(lst[0][String], "a")
    assert_equal(lst[1][String], "b")
    assert_equal(lst[2][String], "c")


def test_to_list_null_float() raises:
    var pd = Python.import_module("pandas")
    # null becomes DFScalar.null() — not a NaN float
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var lst = s.to_list()
    assert_equal(len(lst), 3)
    assert_true(lst[0][Float64] == 1.0)
    assert_true(lst[1].is_null())
    assert_true(lst[2][Float64] == 3.0)


def test_to_numpy_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var arr = s.to_numpy()
    assert_equal(len(arr), 3)
    assert_true(arr[0] == 1.0)
    assert_true(arr[1] == 2.0)
    assert_true(arr[2] == 3.0)


def test_to_numpy_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.5, 2.5, 3.5]")))
    var arr = s.to_numpy()
    assert_equal(len(arr), 3)
    assert_true(arr[0] == 1.5)
    assert_true(arr[2] == 3.5)


def test_to_numpy_bool() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[True, False, True]")))
    var arr = s.to_numpy()
    assert_equal(len(arr), 3)
    assert_true(arr[0] == 1.0)
    assert_true(arr[1] == 0.0)
    assert_true(arr[2] == 1.0)


def test_to_numpy_null() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, None, 3.0]")))
    var arr = s.to_numpy()
    assert_equal(len(arr), 3)
    assert_true(arr[0] == 1.0)
    assert_true(arr[1] != arr[1])  # NaN
    assert_true(arr[2] == 3.0)


def test_to_frame_default_name() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), name="col1", dtype="int64"))
    var df = s.to_frame()
    # Should be a native bison DataFrame with one column named "col1"
    assert_equal(df.shape()[0], 3)
    assert_equal(df.shape()[1], 1)
    assert_equal(df.columns()[0], "col1")


def test_to_frame_custom_name() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20]"), name="old", dtype="int64"))
    var df = s.to_frame(name="new_col")
    assert_equal(df.shape()[1], 1)
    assert_equal(df.columns()[0], "new_col")


def test_to_dict_int() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]"), dtype="int64"))
    var d = s.to_dict()
    assert_true(d["0"].isa[Int64]())
    assert_true(d["0"][Int64] == 10)
    assert_true(d["1"][Int64] == 20)
    assert_true(d["2"][Int64] == 30)


def test_to_dict_float() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var d = s.to_dict()
    assert_true(d["0"].isa[Float64]())
    assert_true(d["0"][Float64] == 1.0)
    assert_true(d["2"][Float64] == 3.0)


def test_to_dict_string() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate('["x", "y"]'), dtype="string"))
    var d = s.to_dict()
    assert_true(d["0"].isa[String]())
    assert_equal(d["0"][String], "x")
    assert_equal(d["1"][String], "y")


def test_to_dict_custom_index() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[100, 200]"), dtype="int64",
                              index=Python.evaluate('["a", "b"]')))
    var d = s.to_dict()
    assert_true(d["a"][Int64] == 100)
    assert_true(d["b"][Int64] == 200)


def test_to_csv_returns_string() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var csv = s.to_csv()
    assert_true(len(csv) > 0)
    # Default: "0,1\n1,2\n2,3\n"
    assert_true(csv.find("1") >= 0)
    assert_true(csv.find("2") >= 0)
    assert_true(csv.find("3") >= 0)


def test_to_csv_row_format() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20]"), dtype="int64"))
    var csv = s.to_csv()
    assert_true(csv.startswith("0,10\n"))
    assert_true(csv.find("1,20\n") >= 0)


def test_to_csv_writes_file() raises:
    var pd = Python.import_module("pandas")
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".csv"))
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var ret = s.to_csv(path)
    assert_equal(ret, "")
    # Read back and verify content
    var builtins = Python.import_module("builtins")
    var content = String(builtins.open(path).read())
    assert_true(len(content) > 0)


def test_to_json_returns_string() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var js = s.to_json()
    assert_true(len(js) > 0)
    assert_true(js.find('"0"') >= 0)
    assert_true(js.find('"1"') >= 0)
    assert_true(js.find('"2"') >= 0)


def test_to_json_values() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[10, 20, 30]"), dtype="int64"))
    var js = s.to_json()
    assert_true(js.find("10") >= 0)
    assert_true(js.find("20") >= 0)
    assert_true(js.find("30") >= 0)


def test_to_json_writes_file() raises:
    var pd = Python.import_module("pandas")
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".json"))
    var s = Series(pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64"))
    var ret = s.to_json(path)
    assert_equal(ret, "")
    # Read back and verify content
    var json_mod = Python.import_module("json")
    var builtins = Python.import_module("builtins")
    var data = json_mod.load(builtins.open(path))
    assert_true(Bool(data["0"] == 1))
    assert_true(Bool(data["2"] == 3))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
