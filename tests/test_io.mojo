"""Tests for read_csv and DataFrame.to_csv."""
from python import Python
from testing import assert_equal, assert_true, TestSuite
from bison import read_csv, read_parquet, read_json, read_excel, DataFrame


def test_read_csv_basic():
    """Read a CSV file with int, float, and string columns."""
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".csv"))
    with open(path, "w") as f:
        f.write("a,b,c\n1,2.5,hello\n3,4.5,world\n")

    var df = read_csv(path)
    var shape = df.shape()
    assert_equal(shape[0], 2)
    assert_equal(shape[1], 3)
    var cols = df.columns()
    assert_equal(cols[0], "a")
    assert_equal(cols[1], "b")
    assert_equal(cols[2], "c")


def test_read_csv_sep():
    """Custom separator (tab-separated values)."""
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".tsv"))
    with open(path, "w") as f:
        f.write("x\ty\n10\t20\n30\t40\n")

    var df = read_csv(path, sep="\t")
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)
    assert_equal(df.columns()[0], "x")
    assert_equal(df.columns()[1], "y")


def test_read_csv_nrows():
    """Nrows parameter limits the number of data rows read."""
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".csv"))
    with open(path, "w") as f:
        f.write("a,b\n1,2\n3,4\n5,6\n7,8\n")

    var df = read_csv(path, nrows=Python.evaluate("2"))
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)


def test_read_csv_no_header():
    """With header=-1 there is no header row; columns are auto-numbered as strings."""
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".csv"))
    with open(path, "w") as f:
        f.write("1,2,3\n4,5,6\n")

    var df = read_csv(path, header=-1)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 3)
    var cols = df.columns()
    assert_equal(cols[0], "0")
    assert_equal(cols[1], "1")
    assert_equal(cols[2], "2")


def test_to_csv_returns_string():
    """Calling to_csv() with no path returns a non-empty CSV string."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var csv_str = df.to_csv()
    assert_true(len(csv_str) > 0)
    assert_true(csv_str.find("a") >= 0)
    assert_true(csv_str.find("b") >= 0)


def test_to_csv_no_index():
    """Passing index=False omits the row-number column (no leading comma in header)."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [10, 20]}")))
    var csv_str = df.to_csv(index=False)
    # Header line should start with "x", not ",x"
    assert_true(csv_str.startswith("x\n"))
    # Data rows should contain the values
    assert_true(csv_str.find("10") >= 0)
    assert_true(csv_str.find("20") >= 0)


def test_csv_roundtrip():
    """Write a DataFrame to CSV with index=False, read it back, check shape."""
    var pd = Python.import_module("pandas")
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".csv"))
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    )
    _ = df.to_csv(path, index=False)

    var df2 = read_csv(path)
    assert_equal(df2.shape()[0], 3)
    assert_equal(df2.shape()[1], 2)
    assert_equal(df2.columns()[0], "a")
    assert_equal(df2.columns()[1], "b")


def test_read_parquet_stub():
    var raised = False
    try:
        _ = read_parquet("/tmp/nonexistent.parquet")
    except:
        raised = True
    assert_true(raised)


def test_read_json_stub():
    var raised = False
    try:
        _ = read_json("/tmp/nonexistent.json")
    except:
        raised = True
    assert_true(raised)


def test_read_excel_stub():
    var raised = False
    try:
        _ = read_excel("/tmp/nonexistent.xlsx")
    except:
        raised = True
    assert_true(raised)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
