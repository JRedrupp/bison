from std.python import Python, PythonObject
from std.collections import Optional
from ..dataframe import DataFrame
from ..column import Column
from ..dtypes import int64, float64, object_, bool_


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------


def _in_na_set(s: String, na_set: List[String]) -> Bool:
    """Return True if *s* matches any string in *na_set*."""
    for i in range(len(na_set)):
        if s == na_set[i]:
            return True
    return False


def _is_bool_value(s: String) -> Bool:
    """Return True if *s* is a case-insensitive match for 'true' or 'false'."""
    var lower = s.lower()
    return lower == "true" or lower == "false"


def _parse_bool_value(s: String) -> Bool:
    """Parse a bool CSV token — 'true' (case-insensitive) returns True."""
    return s.lower() == "true"


def _infer_and_build_column(
    name: String,
    raw: List[String],
    na_set: List[String],
) raises -> Column:
    """Infer the best dtype and build a Column from raw CSV string values.

    Priority: Bool > Int64 > Float64 > String (object_).
    Null-mask is set for any cell whose value is in *na_set*.
    """
    var n = len(raw)

    if n == 0:
        return Column(name, List[String](), object_)

    # ------------------------------------------------------------------
    # Try Bool — "true" / "false" (case-insensitive).
    # ------------------------------------------------------------------
    var all_bool = True
    for i in range(n):
        if _in_na_set(raw[i], na_set):
            continue
        if not _is_bool_value(raw[i]):
            all_bool = False
            break

    if all_bool:
        var data = List[Bool]()
        var null_mask = List[Bool]()
        var has_null = False
        for i in range(n):
            if _in_na_set(raw[i], na_set):
                data.append(False)
                null_mask.append(True)
                has_null = True
            else:
                data.append(_parse_bool_value(raw[i]))
                null_mask.append(False)
        var col = Column(name, data^, bool_)
        if has_null:
            col._null_mask = null_mask^
        return col^

    # ------------------------------------------------------------------
    # Try Int64 — atol() raises for anything that isn't a decimal integer.
    # ------------------------------------------------------------------
    var all_int = True
    for i in range(n):
        if _in_na_set(raw[i], na_set):
            continue
        try:
            _ = atol(raw[i])
        except:
            all_int = False
            break

    if all_int:
        var data = List[Int64]()
        var null_mask = List[Bool]()
        var has_null = False
        for i in range(n):
            if _in_na_set(raw[i], na_set):
                data.append(Int64(0))
                null_mask.append(True)
                has_null = True
            else:
                data.append(Int64(atol(raw[i])))
                null_mask.append(False)
        var col = Column(name, data^, int64)
        if has_null:
            col._null_mask = null_mask^
        return col^

    # ------------------------------------------------------------------
    # Try Float64 — atof() raises for non-numeric strings.
    # ------------------------------------------------------------------
    var all_float = True
    for i in range(n):
        if _in_na_set(raw[i], na_set):
            continue
        try:
            _ = atof(raw[i])
        except:
            all_float = False
            break

    if all_float:
        var data = List[Float64]()
        var null_mask = List[Bool]()
        var has_null = False
        for i in range(n):
            if _in_na_set(raw[i], na_set):
                data.append(Float64(0))
                null_mask.append(True)
                has_null = True
            else:
                data.append(atof(raw[i]))
                null_mask.append(False)
        var col = Column(name, data^, float64)
        if has_null:
            col._null_mask = null_mask^
        return col^

    # ------------------------------------------------------------------
    # Default: String (stored as List[String] with object_ dtype)
    # ------------------------------------------------------------------
    var data = List[String]()
    var null_mask = List[Bool]()
    var has_null = False
    for i in range(n):
        if _in_na_set(raw[i], na_set):
            data.append(String(""))
            null_mask.append(True)
            has_null = True
        else:
            data.append(raw[i])
            null_mask.append(False)
    var col = Column(name, data^, object_)
    if has_null:
        col._null_mask = null_mask^
    return col^


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def read_csv(
    filepath: String,
    sep: String = ",",
    header: Int = 0,
    index_col: Optional[PythonObject] = None,
    usecols: Optional[PythonObject] = None,
    dtype: Optional[PythonObject] = None,
    nrows: Optional[PythonObject] = None,
    skiprows: Optional[PythonObject] = None,
    na_values: Optional[PythonObject] = None,
    encoding: String = "utf-8",
    parse_dates: Bool = False,
) raises -> DataFrame:
    """Read a CSV file into a DataFrame.

    Parameters
    ----------
    filepath   : Path to the CSV file.
    sep        : Field delimiter (default ``","``).
    header     : Row number to use as column names (0-based). Pass ``-1``
                 for no header; auto-generated names ``0, 1, …`` are used.
    index_col  : Not yet used (accepted for API compatibility).
    usecols    : List of column names or indices to read. ``None`` reads all.
    dtype      : Not yet used (accepted for API compatibility).
    nrows      : Maximum number of data rows to read.
    skiprows   : If an integer, skip that many rows immediately after the
                 header. List-of-indices form is not yet supported.
    na_values  : Additional strings to recognise as NA/NaN.
    encoding   : File encoding (passed to the OS ``open`` call).
    parse_dates: Not yet used (accepted for API compatibility).
    """
    var py_csv = Python.import_module("csv")
    var py_io = Python.import_module("io")
    var py_builtins = Python.import_module("builtins")

    # Read file content via Mojo's built-in file I/O, then parse with
    # Python's csv.reader for correct RFC 4180 quoting/escaping.
    var all_rows: PythonObject
    with open(filepath, "r") as f:
        var content = f.read()
        var reader = py_csv.reader(py_io.StringIO(content), delimiter=sep)
        all_rows = py_builtins.list(reader)
    var total = all_rows.__len__()

    if total == 0:
        return DataFrame()

    # ------------------------------------------------------------------
    # Determine column names and the first data row index.
    # ------------------------------------------------------------------
    var col_names = List[String]()
    var data_start: Int

    if header >= 0 and header < total:
        var hrow = all_rows[header]
        var hlen = hrow.__len__()
        for i in range(hlen):
            col_names.append(String(hrow[i]))
        data_start = header + 1
    else:
        # No header row — generate numeric column names.
        var nc = all_rows[0].__len__()
        for i in range(nc):
            col_names.append(String(i))
        data_start = 0

    var ncols = len(col_names)
    if ncols == 0:
        return DataFrame()

    # ------------------------------------------------------------------
    # Handle skiprows (integer form only: skip N rows after header).
    # ------------------------------------------------------------------
    if skiprows:
        try:
            data_start += Int(py=skiprows.value())
        except:
            pass  # unsupported form — ignore

    # ------------------------------------------------------------------
    # Determine the effective row limit.
    # ------------------------------------------------------------------
    var max_rows = total - data_start
    if max_rows < 0:
        max_rows = 0
    if nrows:
        try:
            var nr = Int(py=nrows.value())
            if nr < max_rows:
                max_rows = nr
        except:
            pass

    # ------------------------------------------------------------------
    # Build the NA-value set (defaults + user-supplied extras).
    # ------------------------------------------------------------------
    var na_set = List[String]()
    na_set.append("")
    na_set.append("NA")
    na_set.append("N/A")
    na_set.append("NULL")
    na_set.append("NaN")
    na_set.append("nan")
    na_set.append("None")
    na_set.append("<NA>")
    if na_values:
        try:
            var nv = na_values.value()
            var nvlen = nv.__len__()
            for i in range(nvlen):
                na_set.append(String(nv[i]))
        except:
            # Single value rather than a list.
            na_set.append(String(na_values.value()))

    # ------------------------------------------------------------------
    # Resolve which column indices (and their output names) to keep.
    # ------------------------------------------------------------------
    var col_indices = List[Int]()
    var out_col_names = List[String]()

    if usecols:
        try:
            var uc = usecols.value()
            var uclen = uc.__len__()
            for i in range(uclen):
                # Try string (column name) first, then integer index.
                try:
                    var cname = String(uc[i])
                    for j in range(ncols):
                        if col_names[j] == cname:
                            col_indices.append(j)
                            out_col_names.append(col_names[j])
                            break
                except:
                    var ci = Int(py=uc[i])
                    if ci >= 0 and ci < ncols:
                        col_indices.append(ci)
                        out_col_names.append(col_names[ci])
        except:
            pass

    if len(col_indices) == 0:
        # No usecols filter (or it resolved to nothing) — use all columns.
        for i in range(ncols):
            col_indices.append(i)
            out_col_names.append(col_names[i])

    var n_out = len(col_indices)

    # ------------------------------------------------------------------
    # Collect raw string values per output column.
    # ------------------------------------------------------------------
    var raw_data = List[List[String]]()
    for _ in range(n_out):
        raw_data.append(List[String]())

    var rows_read = 0
    var ri = data_start
    while ri < total and rows_read < max_rows:
        var row = all_rows[ri]
        var nf = row.__len__()
        for oi in range(n_out):
            var ci = col_indices[oi]
            if ci < nf:
                raw_data[oi].append(String(row[ci]))
            else:
                raw_data[oi].append(String(""))
        rows_read += 1
        ri += 1

    # ------------------------------------------------------------------
    # Type-infer and build each Column.
    # ------------------------------------------------------------------
    var cols = List[Column]()
    for oi in range(n_out):
        var col = _infer_and_build_column(
            out_col_names[oi], raw_data[oi], na_set
        )
        cols.append(col^)

    return DataFrame(cols^)
