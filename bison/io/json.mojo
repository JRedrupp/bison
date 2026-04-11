from std.python import Python, PythonObject
from std.collections import Optional
from ..dataframe import DataFrame
from ..column import Column, NullMask
from ..dtypes import int64, float64, object_, bool_, string_


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------


def _json_py_type(val: PythonObject) raises -> String:
    """Return the Python class name of a JSON value."""
    return String(val.__class__.__name__)


def _json_infer_dtype(py_values: PythonObject, n: Int) raises -> String:
    """Determine the best bison dtype for a Python list of JSON values.

    Returns ``"int64"``, ``"float64"``, ``"bool"``, or ``"string"``.
    The most general type encountered wins: string overrides all others;
    float64 overrides int64 and bool; int64 overrides bool.
    None values are skipped.
    """
    var found_float = False
    var found_int = False
    var found_bool = False
    for ri in range(n):
        var tname = _json_py_type(py_values[ri])
        if tname == "NoneType":
            continue
        elif tname == "str":
            return "string"
        elif tname == "float":
            found_float = True
        elif tname == "int":
            found_int = True
        elif tname == "bool":
            found_bool = True
        else:
            return "string"
    if found_float:
        return "float64"
    if found_int:
        return "int64"
    if found_bool:
        return "bool"
    return "int64"  # default for all-null columns


def _json_build_column(
    name: String,
    py_values: PythonObject,
    n: Int,
) raises -> Column:
    """Build a Column from a Python list of JSON values.

    Infers dtype by inspecting Python types: bool > int64 > float64 > string.
    None values are tracked in the null_mask.
    """
    var dtype_str = _json_infer_dtype(py_values, n)

    if dtype_str == "bool":
        var data = List[Bool]()
        var null_mask = NullMask()
        for ri in range(n):
            var val = py_values[ri]
            if _json_py_type(val) == "NoneType":
                data.append(False)
                null_mask.append_null()
            else:
                data.append(Bool(val.__bool__()))
                null_mask.append_valid()
        var col = Column(name, data^, bool_)
        if null_mask.has_nulls():
            col._null_mask = null_mask^
        col._try_activate_storage()
        return col^

    elif dtype_str == "int64":
        var data = List[Int64]()
        var null_mask = NullMask()
        for ri in range(n):
            var val = py_values[ri]
            if _json_py_type(val) == "NoneType":
                data.append(Int64(0))
                null_mask.append_null()
            else:
                data.append(Int64(Int(py=val)))
                null_mask.append_valid()
        var col = Column(name, data^, int64)
        if null_mask.has_nulls():
            col._null_mask = null_mask^
        col._try_activate_storage()
        return col^

    elif dtype_str == "float64":
        var data = List[Float64]()
        var null_mask = NullMask()
        for ri in range(n):
            var val = py_values[ri]
            if _json_py_type(val) == "NoneType":
                data.append(Float64(0))
                null_mask.append_null()
            else:
                data.append(atof(String(val)))
                null_mask.append_valid()
        var col = Column(name, data^, float64)
        if null_mask.has_nulls():
            col._null_mask = null_mask^
        col._try_activate_storage()
        return col^

    else:  # "string"
        var data = List[String]()
        var null_mask = NullMask()
        for ri in range(n):
            var val = py_values[ri]
            if _json_py_type(val) == "NoneType":
                data.append(String(""))
                null_mask.append_null()
            else:
                data.append(String(val))
                null_mask.append_valid()
        var col = Column(name, data^, string_)
        if null_mask.has_nulls():
            col._null_mask = null_mask^
        col._try_activate_storage()
        return col^


def _json_records_to_df(
    records: PythonObject,
    n: Int,
    py_builtins: PythonObject,
) raises -> DataFrame:
    """Build a DataFrame from a Python list of record dicts."""
    if n == 0:
        return DataFrame()

    # Collect column names from the first record.
    var first = records[0]
    var keys = py_builtins.list(first.keys())
    var ncols = Int(keys.__len__())
    var col_names = List[String]()
    for ci in range(ncols):
        col_names.append(String(keys[ci]))

    # For each column, gather all values into a Python list.
    var cols = List[Column]()
    for ci in range(ncols):
        var key = col_names[ci]
        var py_vals = Python.evaluate("[]")
        for ri in range(n):
            var row = records[ri]
            try:
                py_vals.append(row[key])
            except:
                py_vals.append(Python.evaluate("None"))
        var col = _json_build_column(key, py_vals, n)
        cols.append(col^)

    return DataFrame(cols^)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def read_json(
    path_or_buf: String,
    orient: String = "",
    dtype: Optional[PythonObject] = None,
    lines: Bool = False,
) raises -> DataFrame:
    """Read a JSON file into a DataFrame.

    Parameters
    ----------
    path_or_buf : Path to the JSON file.
    orient      : Expected JSON format. Supported values: ``"records"``
                  (list of dicts), ``"split"`` (split format with
                  ``"columns"`` / ``"data"`` keys), ``"columns"`` (dict of
                  dicts keyed by column name), ``"index"`` (dict of dicts
                  keyed by row index), ``"values"`` (list of lists).
                  The default (``""``) auto-detects based on the shape of
                  the parsed object.
    dtype       : Not yet used (accepted for API compatibility).
    lines       : If ``True``, treat each line of the file as a separate
                  JSON object (JSON Lines / NDJSON format).
    """
    var json_mod = Python.import_module("json")
    var py_builtins = Python.import_module("builtins")

    # Read file content via Mojo built-in file I/O.
    var content: String
    with open(path_or_buf, "r") as f:
        content = f.read()

    if lines:
        # JSON Lines: each non-empty line is a separate JSON object.
        var py_content = PythonObject(content)
        var py_lines = py_content.splitlines()
        var nlines = Int(py_lines.__len__())
        var records = Python.evaluate("[]")
        for i in range(nlines):
            var line_str = String(py_lines[i]).strip()
            if len(line_str) > 0:
                records.append(json_mod.loads(line_str))
        var n = Int(records.__len__())
        return _json_records_to_df(records, n, py_builtins)

    # Parse the entire JSON content.
    var parsed = json_mod.loads(content)

    # Determine effective orient (auto-detect when not specified).
    var eff_orient = orient
    if len(eff_orient) == 0:
        var parsed_type = String(parsed.__class__.__name__)
        if parsed_type == "list":
            eff_orient = "records"
        else:
            # dict — probe for split-format keys.
            var has_split = False
            try:
                _ = parsed["columns"]
                _ = parsed["data"]
                has_split = True
            except:
                pass
            if has_split:
                eff_orient = "split"
            else:
                eff_orient = "columns"

    if eff_orient == "records":
        var n = Int(parsed.__len__())
        return _json_records_to_df(parsed, n, py_builtins)

    elif eff_orient == "split":
        # {"columns": [...], "index": [...], "data": [[...], ...]}
        var py_cols = parsed["columns"]
        var py_data = parsed["data"]
        var ncols = Int(py_cols.__len__())
        var nrows = Int(py_data.__len__())
        var col_names = List[String]()
        for ci in range(ncols):
            col_names.append(String(py_cols[ci]))
        var cols = List[Column]()
        for ci in range(ncols):
            var py_vals = Python.evaluate("[]")
            for ri in range(nrows):
                var row = py_data[ri]
                if ci < Int(row.__len__()):
                    py_vals.append(row[ci])
                else:
                    py_vals.append(Python.evaluate("None"))
            var col = _json_build_column(col_names[ci], py_vals, nrows)
            cols.append(col^)
        return DataFrame(cols^)

    elif eff_orient == "index":
        # {"0": {"col1": val1, ...}, "1": {...}, ...}
        var index_keys = py_builtins.list(parsed.keys())
        var nrows = Int(index_keys.__len__())
        if nrows == 0:
            return DataFrame()
        var first_row = parsed[index_keys[0]]
        var col_keys = py_builtins.list(first_row.keys())
        var ncols = Int(col_keys.__len__())
        var col_names = List[String]()
        for ci in range(ncols):
            col_names.append(String(col_keys[ci]))
        var cols = List[Column]()
        for ci in range(ncols):
            var key = col_names[ci]
            var py_vals = Python.evaluate("[]")
            for ri in range(nrows):
                var idx_key = index_keys[ri]
                try:
                    py_vals.append(parsed[idx_key][key])
                except:
                    py_vals.append(Python.evaluate("None"))
            var col = _json_build_column(key, py_vals, nrows)
            cols.append(col^)
        return DataFrame(cols^)

    elif eff_orient == "values":
        # [[val1, val2], ...]
        # Column names are auto-generated as "0", "1", ... (String representation
        # of integer indices, consistent with bison's String-only column names).
        var nrows = Int(parsed.__len__())
        if nrows == 0:
            return DataFrame()
        var ncols = Int(parsed[0].__len__())
        var col_names = List[String]()
        for ci in range(ncols):
            col_names.append(String(ci))
        var cols = List[Column]()
        for ci in range(ncols):
            var py_vals = Python.evaluate("[]")
            for ri in range(nrows):
                var row = parsed[ri]
                if ci < Int(row.__len__()):
                    py_vals.append(row[ci])
                else:
                    py_vals.append(Python.evaluate("None"))
            var col = _json_build_column(col_names[ci], py_vals, nrows)
            cols.append(col^)
        return DataFrame(cols^)

    else:  # "columns" orient (default pandas behavior)
        # {"col1": {"0": val1, "1": val2, ...}, ...}
        var py_col_keys = py_builtins.list(parsed.keys())
        var ncols = Int(py_col_keys.__len__())
        if ncols == 0:
            return DataFrame()
        var first_col_dict = parsed[py_col_keys[0]]
        var idx_keys = py_builtins.list(first_col_dict.keys())
        var nrows = Int(idx_keys.__len__())
        var col_names = List[String]()
        for ci in range(ncols):
            col_names.append(String(py_col_keys[ci]))
        var cols = List[Column]()
        for ci in range(ncols):
            var col_name = col_names[ci]
            var col_dict = parsed[col_name]
            var py_vals = Python.evaluate("[]")
            for ri in range(nrows):
                try:
                    py_vals.append(col_dict[idx_keys[ri]])
                except:
                    py_vals.append(Python.evaluate("None"))
            var col = _json_build_column(col_name, py_vals, nrows)
            cols.append(col^)
        return DataFrame(cols^)
