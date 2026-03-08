"""Tests that IO stubs raise 'not implemented'."""
from python import Python
from testing import assert_true
from bison import read_csv, read_parquet, read_json, read_excel, DataFrame


def test_read_csv_stub():
    var raised = False
    try:
        _ = read_csv("/tmp/nonexistent.csv")
    except:
        raised = True
    assert_true(raised)


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


def test_to_csv_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2]}")))
    var raised = False
    try:
        _ = df.to_csv()
    except:
        raised = True
    assert_true(raised)


def main():
    test_read_csv_stub()
    test_read_parquet_stub()
    test_read_json_stub()
    test_read_excel_stub()
    test_to_csv_stub()
    print("test_io: all tests passed")
