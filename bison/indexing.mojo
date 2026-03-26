from std.python import PythonObject
from std.memory import UnsafePointer
from .column import (
    Column,
    ColumnData,
    DFScalar,
    SeriesScalar,
    _scalar_from_col,
    _col_cell_pyobj,
    _SetScalarInColMutVisitor,
    visit_col_data_mut_raises,
)
from .index import ColumnIndex
from .dtypes import object_
from .series import Series
from .dataframe import DataFrame


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------


def _df_col_index(df: DataFrame, name: String) raises -> Int:
    """Return the integer position of column *name* in *df*."""
    for i in range(len(df._cols)):
        if df._cols[i].name == name:
            return i
    raise Error("column '" + name + "' not found")


def _parse_int_label(label: String) raises -> Int:
    """Parse a decimal integer string into an ``Int``.

    Supports an optional leading ``'-'`` sign.  Raises when the string
    contains non-digit characters.
    """
    var n = len(label)
    if n == 0:
        raise Error("loc: empty row label")
    var bytes = label.as_bytes()
    var start = 0
    var negative = False
    if bytes[0] == UInt8(ord("-")):
        negative = True
        start = 1
    elif bytes[0] == UInt8(ord("+")):
        start = 1
    if start >= n:
        raise Error("loc: invalid row label: " + label)
    var result = 0
    for i in range(start, n):
        var digit = Int(bytes[i]) - ord("0")
        if digit < 0 or digit > 9:
            raise Error("loc: not an integer label: " + label)
        result = result * 10 + digit
    return -result if negative else result


def _df_row_index(df: DataFrame, label: String) raises -> Int:
    """Return the integer row position for the given row *label*.

    If the first column has a non-empty ``_index`` the label is matched
    via ``Column._index_label()``.  When the index is empty the default
    integer range index (0, 1, …) is assumed and the label must be a
    decimal integer string.
    """
    var nrows = df.shape()[0]
    if nrows == 0:
        raise Error("loc: DataFrame is empty")
    if len(df._cols) == 0:
        raise Error("loc: DataFrame has no columns")
    var n_idx = df._cols[0]._index_len()
    if n_idx > 0:
        for i in range(n_idx):
            if df._cols[0]._index_label(i) == label:
                return i
        raise Error("loc: label '" + label + "' not found in index")
    # Default RangeIndex: parse the label as an integer.
    var row = _parse_int_label(label)
    if row < 0:
        row = nrows + row
    if row < 0 or row >= nrows:
        raise Error("loc: label '" + label + "' out of range")
    return row


def _set_scalar_in_col(mut col: Column, row: Int, value: DFScalar) raises:
    """Write *value* into *col* at integer position *row*.

    Type coercion mirrors pandas ``at`` / ``iat`` behaviour:
    * Int64 value → Int64 or Float64 or Bool column (cast).
    * Float64 value → Float64 column; truncated to Int64 when the
      target is an integer column (fractional part is discarded,
      matching pandas ``iat`` behaviour).
    * Bool value → Bool or Int64 or Float64 column (0/1).
    * String value → String column only.
    Raises when the types are incompatible.
    """
    var visitor = _SetScalarInColMutVisitor(row, value)
    visit_col_data_mut_raises(visitor, col._data)


def _set_series_scalar_in_col(
    mut col: Column, row: Int, value: SeriesScalar
) raises:
    """Write a ``SeriesScalar`` cell into *col* at position *row*.

    Behaves like ``_set_scalar_in_col`` but also handles the
    ``PythonObject`` arm of ``SeriesScalar``.
    """
    if value.isa[PythonObject]():
        # Only object columns accept PythonObject values.
        if col._data.isa[List[PythonObject]]():
            col._data[List[PythonObject]][row] = value[PythonObject]
        else:
            raise Error("iloc: cannot assign PythonObject to typed column")
        return
    # Use the DFScalar path for the four typed arms.
    var ds: DFScalar
    if value.isa[Int64]():
        ds = DFScalar(value[Int64])
    elif value.isa[Float64]():
        ds = DFScalar(value[Float64])
    elif value.isa[Bool]():
        ds = DFScalar(value[Bool])
    else:
        ds = DFScalar(value[String])
    _set_scalar_in_col(col, row, ds)


def _row_as_series(df: DataFrame, row: Int) raises -> Series:
    """Build a ``Series`` representing row *row* of *df*.

    The returned Series has ``object_`` dtype; each element is a
    ``PythonObject`` wrapping the cell value.  The ``_index`` of the
    returned column holds the column names as ``PythonObject`` strings,
    matching the pandas ``df.iloc[i]`` behaviour.
    """
    var ncols = df.shape()[1]
    var data = List[PythonObject]()
    var index = List[PythonObject]()
    for ci in range(ncols):
        index.append(PythonObject(df._cols[ci].name))
        data.append(_col_cell_pyobj(df._cols[ci], row))
    var result_col = Column("", ColumnData(data^), object_, index^)
    return Series(result_col^)


# ------------------------------------------------------------------
# Public indexer structs
# ------------------------------------------------------------------


struct LocIndexer[O: MutOrigin]:
    """Label-based row indexer (.loc).

    Construct via ``LocIndexer(UnsafePointer(to=df))`` where *df* is a
    mutable ``DataFrame``.  The pointer must remain valid for the
    lifetime of the indexer.
    """

    var _df: UnsafePointer[DataFrame, Self.O]

    def __init__(out self, ptr: UnsafePointer[DataFrame, Self.O]):
        self._df = ptr

    def __getitem__(self, key: String) raises -> Series:
        """Return row *key* as a Series (index = column names)."""
        ref df = self._df[]
        var row = _df_row_index(df, key)
        var nrows = df.shape()[0]
        if row < 0 or row >= nrows:
            raise Error("loc: row index out of bounds")
        return _row_as_series(df, row)

    def __setitem__(self, key: String, value: Series) raises:
        """Assign Series *value* to row *key*.

        *value* must have exactly as many elements as there are columns.
        Each element is written into the corresponding column at the
        row position identified by *key*.
        """
        ref df = self._df[]
        var row = _df_row_index(df, key)
        var ncols = df.shape()[1]
        var nrows = df.shape()[0]
        if row < 0 or row >= nrows:
            raise Error("loc: row index out of bounds")
        if value.size() != ncols:
            raise Error(
                "loc: Series length "
                + String(value.size())
                + " != number of columns "
                + String(ncols)
            )
        for ci in range(ncols):
            var cell = value.iloc(ci)
            _set_series_scalar_in_col(df._cols[ci], row, cell)


struct ILocIndexer[O: MutOrigin]:
    """Integer-position-based row indexer (.iloc).

    Construct via ``ILocIndexer(UnsafePointer(to=df))``.
    """

    var _df: UnsafePointer[DataFrame, Self.O]

    def __init__(out self, ptr: UnsafePointer[DataFrame, Self.O]):
        self._df = ptr

    def __getitem__(self, key: Int) raises -> Series:
        """Return row *key* (integer position) as a Series."""
        ref df = self._df[]
        var nrows = df.shape()[0]
        var row = key
        if row < 0:
            row = nrows + row
        if row < 0 or row >= nrows:
            raise Error(
                "iloc: row index "
                + String(key)
                + " out of bounds for DataFrame with "
                + String(nrows)
                + " rows"
            )
        return _row_as_series(df, row)

    def __setitem__(self, key: Int, value: Series) raises:
        """Assign Series *value* to row *key* (integer position).

        *value* must have exactly as many elements as there are columns.
        """
        ref df = self._df[]
        var nrows = df.shape()[0]
        var row = key
        if row < 0:
            row = nrows + row
        if row < 0 or row >= nrows:
            raise Error(
                "iloc: row index "
                + String(key)
                + " out of bounds for DataFrame with "
                + String(nrows)
                + " rows"
            )
        var ncols = df.shape()[1]
        if value.size() != ncols:
            raise Error(
                "iloc: Series length "
                + String(value.size())
                + " != number of columns "
                + String(ncols)
            )
        for ci in range(ncols):
            var cell = value.iloc(ci)
            _set_series_scalar_in_col(df._cols[ci], row, cell)


struct AtIndexer[O: MutOrigin]:
    """Label-based scalar accessor (.at).

    Construct via ``AtIndexer(UnsafePointer(to=df))``.
    """

    var _df: UnsafePointer[DataFrame, Self.O]

    def __init__(out self, ptr: UnsafePointer[DataFrame, Self.O]):
        self._df = ptr

    def __getitem__(self, row: String, col: String) raises -> DFScalar:
        """Return the scalar at row label *row*, column name *col*."""
        ref df = self._df[]
        var row_idx = _df_row_index(df, row)
        var col_idx = _df_col_index(df, col)
        return _scalar_from_col(df._cols[col_idx], row_idx)

    def __setitem__(self, row: String, col: String, value: DFScalar) raises:
        """Set the scalar at row label *row*, column name *col* to *value*."""
        ref df = self._df[]
        var row_idx = _df_row_index(df, row)
        var col_idx = _df_col_index(df, col)
        _set_scalar_in_col(df._cols[col_idx], row_idx, value)


struct IAtIndexer[O: MutOrigin]:
    """Integer-based scalar accessor (.iat).

    Construct via ``IAtIndexer(UnsafePointer(to=df))``.
    """

    var _df: UnsafePointer[DataFrame, Self.O]

    def __init__(out self, ptr: UnsafePointer[DataFrame, Self.O]):
        self._df = ptr

    def __getitem__(self, row: Int, col: Int) raises -> DFScalar:
        """Return the scalar at integer row *row*, column position *col*."""
        ref df = self._df[]
        var nrows = df.shape()[0]
        var ncols = df.shape()[1]
        var r = row if row >= 0 else nrows + row
        if r < 0 or r >= nrows:
            raise Error(
                "iat: row index "
                + String(row)
                + " out of bounds for DataFrame with "
                + String(nrows)
                + " rows"
            )
        if col < 0 or col >= ncols:
            raise Error(
                "iat: column index "
                + String(col)
                + " out of bounds for DataFrame with "
                + String(ncols)
                + " columns"
            )
        return _scalar_from_col(df._cols[col], r)

    def __setitem__(self, row: Int, col: Int, value: DFScalar) raises:
        """Set the scalar at integer row *row*, column position *col* to *value*.
        """
        ref df = self._df[]
        var nrows = df.shape()[0]
        var ncols = df.shape()[1]
        var r = row if row >= 0 else nrows + row
        if r < 0 or r >= nrows:
            raise Error(
                "iat: row index "
                + String(row)
                + " out of bounds for DataFrame with "
                + String(nrows)
                + " rows"
            )
        if col < 0 or col >= ncols:
            raise Error(
                "iat: column index "
                + String(col)
                + " out of bounds for DataFrame with "
                + String(ncols)
                + " columns"
            )
        _set_scalar_in_col(df._cols[col], r, value)
