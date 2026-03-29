"""Tests for DataFrame IO (read and write methods)."""
from std.python import Python
from std.testing import assert_equal, assert_true, TestSuite
from bison import read_csv, read_parquet, read_json, read_excel, DataFrame, DFScalar


def test_read_csv_basic() raises:
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


def test_read_csv_sep() raises:
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


def test_read_csv_nrows() raises:
    """Nrows parameter limits the number of data rows read."""
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".csv"))
    with open(path, "w") as f:
        f.write("a,b\n1,2\n3,4\n5,6\n7,8\n")

    var df = read_csv(path, nrows=Python.evaluate("2"))
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)


def test_read_csv_no_header() raises:
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


def test_to_csv_returns_string() raises:
    """Calling to_csv() with no path returns a non-empty CSV string."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var csv_str = df.to_csv()
    assert_true(len(csv_str) > 0)
    assert_true(csv_str.find("a") >= 0)
    assert_true(csv_str.find("b") >= 0)


def test_to_csv_no_index() raises:
    """Passing index=False omits the row-number column (no leading comma in header)."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [10, 20]}")))
    var csv_str = df.to_csv(index=False)
    # Header line should start with "x", not ",x"
    assert_true(csv_str.startswith("x\n"))
    # Data rows should contain the values
    assert_true(csv_str.find("10") >= 0)
    assert_true(csv_str.find("20") >= 0)


def test_csv_roundtrip() raises:
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


def test_read_parquet_missing_file() raises:
    """Read_parquet raises when the file does not exist."""
    var raised = False
    try:
        _ = read_parquet("/tmp/bison_nonexistent_file.parquet")
    except:
        raised = True
    assert_true(raised)


def test_parquet_roundtrip() raises:
    """Write a DataFrame to Parquet and read it back (skipped when pyarrow is absent)."""
    var pyarrow_available = False
    try:
        _ = Python.import_module("pyarrow")
        pyarrow_available = True
    except:
        pass
    if not pyarrow_available:
        return

    var pd = Python.import_module("pandas")
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".parquet"))
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0]}"))
    )
    df.to_parquet(path)

    var df2 = read_parquet(path)
    var shape = df2.shape()
    assert_equal(shape[0], 3)
    assert_equal(shape[1], 2)
    assert_equal(df2.columns()[0], "a")
    assert_equal(df2.columns()[1], "b")


def test_read_json_records() raises:
    """Read a JSON file in records format (list of dicts)."""
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".json"))
    with open(path, "w") as f:
        f.write('[{"a": 1, "b": 2.5, "c": "hello"}, {"a": 3, "b": 4.5, "c": "world"}]')

    var df = read_json(path)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 3)
    assert_equal(df.columns()[0], "a")
    assert_equal(df.columns()[1], "b")
    assert_equal(df.columns()[2], "c")


def test_read_json_lines() raises:
    """Read a JSON Lines (NDJSON) file with lines=True."""
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".jsonl"))
    with open(path, "w") as f:
        f.write('{"x": 1, "y": "foo"}\n{"x": 2, "y": "bar"}\n')

    var df = read_json(path, lines=True)
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)
    assert_equal(df.columns()[0], "x")
    assert_equal(df.columns()[1], "y")


def test_read_json_split() raises:
    """Read a JSON file in split format."""
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".json"))
    with open(path, "w") as f:
        f.write('{"columns": ["a", "b"], "index": [0, 1], "data": [[1, 2], [3, 4]]}')

    var df = read_json(path, orient="split")
    assert_equal(df.shape()[0], 2)
    assert_equal(df.shape()[1], 2)
    assert_equal(df.columns()[0], "a")
    assert_equal(df.columns()[1], "b")


def test_to_json_records() raises:
    """Serialize a DataFrame to JSON records format."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3, 4]}")))
    var json_str = df.to_json(orient="records")
    assert_true(len(json_str) > 0)
    assert_true(json_str.find('"a"') >= 0)
    assert_true(json_str.find('"b"') >= 0)


def test_to_json_split() raises:
    """Serialize a DataFrame to JSON split format."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [10, 20], 'y': [30, 40]}")))
    var json_str = df.to_json(orient="split")
    assert_true(json_str.find('"columns"') >= 0)
    assert_true(json_str.find('"data"') >= 0)
    assert_true(json_str.find('"index"') >= 0)


def test_json_roundtrip() raises:
    """Write a DataFrame to JSON (records orient), read it back, check shape/columns."""
    var pd = Python.import_module("pandas")
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".json"))
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))
    )
    _ = df.to_json(path, orient="records")

    var df2 = read_json(path, orient="records")
    assert_equal(df2.shape()[0], 3)
    assert_equal(df2.shape()[1], 2)
    assert_equal(df2.columns()[0], "a")
    assert_equal(df2.columns()[1], "b")


def test_read_excel_stub() raises:
    var raised = False
    try:
        _ = read_excel("/tmp/nonexistent.xlsx")
    except:
        raised = True
    assert_true(raised)


def test_to_parquet_writes_file() raises:
    """Write a DataFrame to Parquet and verify the file exists (skipped when pyarrow is absent)."""
    var pyarrow_available = False
    try:
        _ = Python.import_module("pyarrow")
        pyarrow_available = True
    except:
        pass
    if not pyarrow_available:
        return

    var pd = Python.import_module("pandas")
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".parquet"))
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3.0, 4.0]}")))
    df.to_parquet(path)
    var os = Python.import_module("os")
    assert_true(Bool(os.path.exists(path)))


def test_to_excel_writes_file() raises:
    """Write a DataFrame to Excel and verify the file exists (skipped when openpyxl is absent)."""
    var openpyxl_available = False
    try:
        _ = Python.import_module("openpyxl")
        openpyxl_available = True
    except:
        pass
    if not openpyxl_available:
        return

    var pd = Python.import_module("pandas")
    var tempfile = Python.import_module("tempfile")
    var path = String(tempfile.mktemp(suffix=".xlsx"))
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [10, 20], 'y': [30, 40]}")))
    df.to_excel(path, index=False)
    var os = Python.import_module("os")
    assert_true(Bool(os.path.exists(path)))


def test_to_dict_int_columns() raises:
    """DataFrame.to_dict returns a nested dict {col: {index_label: value}}."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}")))
    var d = df.to_dict()
    # Each column maps to an inner dict keyed by stringified row index
    assert_equal(len(d["a"]), 3)
    assert_equal(len(d["b"]), 3)
    assert_true(d["a"]["0"].isa[Int64]())
    assert_equal(Int(d["a"]["0"][Int64]), 1)
    assert_equal(Int(d["a"]["1"][Int64]), 2)
    assert_equal(Int(d["b"]["0"][Int64]), 4)


def test_to_dict_list_orient() raises:
    """DataFrame.to_dict raises for orient='list' (unsupported; use to_records)."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [10, 20]}")))
    var raised = False
    try:
        _ = df.to_dict(orient="list")
    except:
        raised = True
    assert_true(raised)


def test_to_dict_unsupported_orient() raises:
    """DataFrame.to_dict raises for orient values other than 'dict'."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1]}")))
    var raised = False
    try:
        _ = df.to_dict(orient="records")
    except:
        raised = True
    assert_true(raised)


def test_to_records_basic() raises:
    """DataFrame.to_records returns a list of row dicts with index included by default."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [10, 20], 'b': [30, 40]}")))
    var records = df.to_records()
    assert_equal(len(records), 2)
    # Each record contains 'index', 'a', 'b'
    assert_equal(Int(records[0]["index"][Int64]), 0)
    assert_equal(Int(records[0]["a"][Int64]), 10)
    assert_equal(Int(records[0]["b"][Int64]), 30)
    assert_equal(Int(records[1]["index"][Int64]), 1)
    assert_equal(Int(records[1]["a"][Int64]), 20)
    assert_equal(Int(records[1]["b"][Int64]), 40)


def test_to_records_no_index() raises:
    """DataFrame.to_records with index=False omits the 'index' key from each record."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'v': [1, 2]}")))
    var records = df.to_records(index=False)
    assert_equal(len(records), 2)
    # 'index' key must not be present
    var has_index = False
    try:
        _ = records[0]["index"]
        has_index = True
    except:
        pass
    assert_true(not has_index)
    assert_equal(Int(records[0]["v"][Int64]), 1)


def test_to_records_index_collision() raises:
    """DataFrame.to_records raises when a column is named 'index' and index=True."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'index': [99]}")))
    # index=True (default) must raise
    var raised = False
    try:
        _ = df.to_records()
    except:
        raised = True
    assert_true(raised)
    # index=False must succeed and return the column value
    var records = df.to_records(index=False)
    assert_equal(len(records), 1)
    assert_equal(Int(records[0]["index"][Int64]), 99)


def test_to_numpy_int_float() raises:
    """DataFrame.to_numpy returns a row-major list of Float64 lists for numeric columns."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': [3.5, 4.5]}")))
    var arr = df.to_numpy()
    assert_equal(len(arr), 2)
    assert_equal(len(arr[0]), 2)
    assert_equal(arr[0][0], Float64(1.0))
    assert_equal(arr[0][1], Float64(3.5))
    assert_equal(arr[1][0], Float64(2.0))
    assert_equal(arr[1][1], Float64(4.5))


def test_to_numpy_string_raises() raises:
    """DataFrame.to_numpy raises for non-numeric (string) columns."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2], 'b': ['x', 'y']}"))
    )
    var raised = False
    try:
        _ = df.to_numpy()
    except:
        raised = True
    assert_true(raised)


def test_to_string_contains_headers() raises:
    """DataFrame.to_string output contains the column names."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'alpha': [1, 2], 'beta': [3, 4]}")))
    var s = df.to_string()
    assert_true(len(s) > 0)
    assert_true(s.find("alpha") >= 0)
    assert_true(s.find("beta") >= 0)


def test_to_string_contains_values() raises:
    """DataFrame.to_string output contains the cell values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [42, 99], 'b': [7, 8]}")))
    var s = df.to_string()
    assert_true(s.find("42") >= 0)
    assert_true(s.find("99") >= 0)
    assert_true(s.find("7") >= 0)


def test_to_html_structure() raises:
    """DataFrame.to_html output contains expected HTML table tags."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1], 'b': [2]}")))
    var h = df.to_html()
    assert_true(h.find("<table") >= 0)
    assert_true(h.find("</table>") >= 0)
    assert_true(h.find("<thead>") >= 0)
    assert_true(h.find("<tbody>") >= 0)
    assert_true(h.find("<th>a</th>") >= 0)
    assert_true(h.find("<th>b</th>") >= 0)
    assert_true(h.find("<td>1</td>") >= 0)
    assert_true(h.find("<td>2</td>") >= 0)


def test_to_html_escapes_special_chars() raises:
    """DataFrame.to_html escapes '<', '>', '&' in column names and cell values."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'x<y': [1]}"))
    )
    var h = df.to_html()
    assert_true(h.find("x&lt;y") >= 0)


def test_to_markdown_structure() raises:
    """DataFrame.to_markdown output is a valid Markdown pipe table with a separator row."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'col1': [1, 2], 'col2': [3, 4]}")))
    var m = df.to_markdown()
    assert_true(len(m) > 0)
    assert_true(m.find("|") >= 0)
    assert_true(m.find("---") >= 0)
    assert_true(m.find("col1") >= 0)
    assert_true(m.find("col2") >= 0)
    assert_true(m.find("1") >= 0)
    assert_true(m.find("3") >= 0)


def test_null_sentinel_to_records() raises:
    """Null cells in to_records produce DFScalar.null(), not zero-like defaults."""
    var pd = Python.import_module("pandas")
    var py = Python.evaluate("{'a': [1, None, 3], 'b': ['x', 'y', None]}")
    var df = DataFrame(pd.DataFrame(py))
    var records = df.to_records(index=False)
    assert_equal(len(records), 3)
    # Non-null cells have values
    assert_true(not records[0]["a"].is_null())
    assert_true(not records[2]["a"].is_null())
    assert_true(not records[0]["b"].is_null())
    assert_true(not records[1]["b"].is_null())
    # Null cells must report is_null() == True (not zero/empty string)
    assert_true(records[1]["a"].is_null())
    assert_true(records[2]["b"].is_null())


def test_null_sentinel_to_dict() raises:
    """Null cells in DataFrame.to_dict produce DFScalar.null(), not zero-like defaults."""
    var pd = Python.import_module("pandas")
    var py = Python.evaluate("{'v': [10, None, 30]}")
    var df = DataFrame(pd.DataFrame(py))
    var d = df.to_dict()
    # Inner dict is keyed by stringified row index
    assert_true(not d["v"]["0"].is_null())
    assert_true(d["v"]["1"].is_null())
    assert_true(not d["v"]["2"].is_null())


def test_null_roundtrip_records() raises:
    """Nulls survive a to_records -> from_records round-trip."""
    var pd = Python.import_module("pandas")
    var py = Python.evaluate("{'a': [1, None, 3], 'b': ['x', None, 'z']}")
    var df = DataFrame(pd.DataFrame(py))
    var records = df.to_records(index=False)
    var df2 = DataFrame.from_records(records)
    # Shape is preserved
    var shape = df2.shape()
    assert_equal(shape[0], 3)
    assert_equal(shape[1], 2)
    # Null mask is propagated: isna() should match the original
    var isna_a = df2["a"].isna()
    assert_true(not isna_a.iloc(0)[Bool])
    assert_true(isna_a.iloc(1)[Bool])
    assert_true(not isna_a.iloc(2)[Bool])
    var isna_b = df2["b"].isna()
    assert_true(not isna_b.iloc(0)[Bool])
    assert_true(isna_b.iloc(1)[Bool])
    assert_true(not isna_b.iloc(2)[Bool])


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
