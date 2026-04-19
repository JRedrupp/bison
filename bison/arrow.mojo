"""Arrow ↔ Column conversion utilities.

Bridges between marrow's Apache Arrow arrays and bison's internal Column type.
Only int64, float64, bool, and string columns are supported. Columns backed
by List[PythonObject] raise an informative error.

Public API
----------
- column_to_marrow_array  — Column → AnyArray
- marrow_array_to_column  — AnyArray → Column
- dataframe_to_record_batch — DataFrame → RecordBatch
- record_batch_to_dataframe — RecordBatch → DataFrame
- dataframe_to_table       — DataFrame → Table
- table_to_dataframe       — Table → DataFrame
"""
from marrow.arrays import AnyArray, StringArray
from marrow.builders import array, StringBuilder
from marrow.dtypes import (
    int64 as _m_int64,
    float64 as _m_float64,
    bool_ as _m_bool_,
    string as _m_string,
    DataType as _MarrowDataType,
    Field as _MarrowField,
    Float64Type,
    Int64Type,
)
from marrow.schema import Schema as _MarrowSchema
from marrow.tabular import RecordBatch, Table
from .column import Column, ColumnStorage, NullMask
from .dataframe import DataFrame
from .dtypes import int64, float64, bool_, object_, string_


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------


def _is_null(col: Column, i: Int) -> Bool:
    """Return True if element i is null."""
    return col.is_null(i)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def column_to_marrow_array(col: Column) raises -> AnyArray:
    """Convert a bison Column to a marrow AnyArray.

    Null elements become Arrow null values.
    List[PythonObject] columns raise an error — Arrow has no object type.
    """
    # Fast path: column already holds a marrow AnyArray — O(1) ref-bump.
    if col._storage.isa[AnyArray]():
        return col._storage[AnyArray].copy()

    var n = len(col)

    if col.is_int():
        var src = col._int64_data()
        var vals = List[Optional[Int]]()
        for i in range(n):
            if _is_null(col, i):
                vals.append(None)
            else:
                vals.append(Int(src[i]))
        return AnyArray(array[Int64Type](vals^))

    elif col.is_float():
        var src = col._float64_data()
        var vals = List[Optional[Float64]]()
        for i in range(n):
            if _is_null(col, i):
                vals.append(None)
            else:
                vals.append(src[i])
        return AnyArray(array[Float64Type](vals^))

    elif col.is_bool():
        var src = col._bool_data()
        var vals = List[Optional[Bool]]()
        for i in range(n):
            if _is_null(col, i):
                vals.append(None)
            else:
                vals.append(src[i])
        return AnyArray(array(vals^))

    elif col.is_string():
        var src = col._str_data()
        var b = StringBuilder(capacity=n)
        for i in range(n):
            if _is_null(col, i):
                b.append_null()
            else:
                b.append(src[i])
        return AnyArray(b.finish())

    else:
        raise Error(
            "column_to_marrow_array: List[PythonObject] columns cannot be"
            " converted to Arrow"
        )


def marrow_array_to_column(arr: AnyArray, name: String) raises -> Column:
    """Convert a marrow AnyArray to a bison Column.

    Arrow null elements (validity bit = 0) are recorded via the column's
    null mask. A fully-valid array leaves the mask empty (bison's
    no-nulls sentinel). Only int64, float64, bool, and string Arrow
    types are supported.

    The input ``arr`` is also stored directly on the returned column's
    ``_storage`` field (zero-copy via ArcPointer ref-bump) so
    dual-backend readers can use the marrow representation immediately.
    """
    var dt = arr.dtype()
    var n = arr.length()

    if dt == _m_int64:
        ref src = arr.as_int64()
        var data = List[Int64]()
        var null_mask = NullMask()
        for i in range(n):
            if not arr.is_valid(i):
                data.append(Int64(0))
                null_mask.append_null()
            else:
                data.append(rebind[Int64](src.unsafe_get(i)))
                null_mask.append_valid()
        var col = Column(name, data^, int64)
        if null_mask.has_nulls():
            col.set_null_mask(null_mask^)
        col._storage = ColumnStorage(arr.copy())
        return col^

    elif dt == _m_float64:
        ref src = arr.as_float64()
        var data = List[Float64]()
        var null_mask = NullMask()
        for i in range(n):
            if not arr.is_valid(i):
                data.append(Float64(0))
                null_mask.append_null()
            else:
                data.append(rebind[Float64](src.unsafe_get(i)))
                null_mask.append_valid()
        var col = Column(name, data^, float64)
        if null_mask.has_nulls():
            col.set_null_mask(null_mask^)
        col._storage = ColumnStorage(arr.copy())
        return col^

    elif dt == _m_bool_:
        ref src = arr.as_bool()
        var data = List[Bool]()
        var null_mask = NullMask()
        for i in range(n):
            if not arr.is_valid(i):
                data.append(False)
                null_mask.append_null()
            else:
                data.append(src[i].value())
                null_mask.append_valid()
        var col = Column(name, data^, bool_)
        if null_mask.has_nulls():
            col.set_null_mask(null_mask^)
        col._storage = ColumnStorage(arr.copy())
        return col^

    elif dt == _m_string:
        ref src = arr.as_string()
        var data = List[String]()
        var null_mask = NullMask()
        for i in range(n):
            if not arr.is_valid(i):
                data.append("")
                null_mask.append_null()
            else:
                data.append(String(src.unsafe_get(UInt(i))))
                null_mask.append_valid()
        # #644: string-backed columns carry string_ dtype.
        var col = Column(name, data^, string_)
        if null_mask.has_nulls():
            col.set_null_mask(null_mask^)
        col._storage = ColumnStorage(arr.copy())
        return col^

    else:
        raise Error(
            "marrow_array_to_column: unsupported Arrow type — only"
            " int64, float64, bool, and string are supported"
        )


def dataframe_to_record_batch(df: DataFrame) raises -> RecordBatch:
    """Convert a bison DataFrame to a marrow RecordBatch.

    Each Column is converted via column_to_marrow_array in column order.
    Schema field names match the column names.
    """
    var arrays = List[AnyArray]()
    var fields = List[_MarrowField]()
    for i in range(len(df._cols)):
        var col_name = df._cols[i].name.value()
        arrays.append(column_to_marrow_array(df._cols[i]))
        fields.append(_MarrowField(col_name, arrays[i].dtype()))
    var schema = _MarrowSchema(fields=fields^)
    return RecordBatch(schema=schema^, columns=arrays^)


def record_batch_to_dataframe(rb: RecordBatch) raises -> DataFrame:
    """Convert a marrow RecordBatch to a bison DataFrame.

    Each field/column pair is converted via marrow_array_to_column and
    assembled into a DataFrame in schema order.
    """
    var cols = List[Column]()
    var n = rb.num_columns()
    for i in range(n):
        var col_name = rb.schema.fields[i].name
        cols.append(marrow_array_to_column(rb.column(i).copy(), col_name))
    return DataFrame(cols^)


def dataframe_to_table(df: DataFrame) raises -> Table:
    """Convert a bison DataFrame to a marrow Table.

    Wraps the result of dataframe_to_record_batch in a single-batch Table.
    """
    var rb = dataframe_to_record_batch(df)
    var schema = rb.schema
    var batches = List[RecordBatch]()
    batches.append(rb^)
    return Table.from_batches(schema, batches^)


def table_to_dataframe(table: Table) raises -> DataFrame:
    """Convert a marrow Table to a bison DataFrame.

    Converts the Table to RecordBatches, then converts the first batch.
    For multi-batch Tables (uncommon with Parquet), only the first batch
    is used.
    """
    var batches = table.to_batches()
    if len(batches) == 0:
        return DataFrame()
    return record_batch_to_dataframe(batches[0])
