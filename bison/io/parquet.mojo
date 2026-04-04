from std.python import Python, PythonObject
from std.collections import Optional
from ..dataframe import DataFrame
from ..arrow import table_to_dataframe
from marrow.parquet import read_table as _marrow_read_table


def _read_parquet_pandas(
    path: String,
    engine: String,
    columns: Optional[PythonObject],
    filters: Optional[PythonObject],
) raises -> DataFrame:
    """Fallback: read Parquet via pandas interop."""
    var pd = Python.import_module("pandas")
    var pd_df: PythonObject
    if columns and filters:
        pd_df = pd.read_parquet(
            path,
            engine=engine,
            columns=columns.value(),
            filters=filters.value(),
        )
    elif columns:
        pd_df = pd.read_parquet(path, engine=engine, columns=columns.value())
    elif filters:
        pd_df = pd.read_parquet(path, engine=engine, filters=filters.value())
    else:
        pd_df = pd.read_parquet(path, engine=engine)
    return DataFrame.from_pandas(pd_df)


def read_parquet(
    path: String,
    engine: String = "auto",
    columns: Optional[PythonObject] = None,
    filters: Optional[PythonObject] = None,
) raises -> DataFrame:
    """Read a Parquet file into a DataFrame.

    Uses marrow's native Parquet reader (via the Arrow C Stream Interface)
    for int64, float64, bool, and string columns. Falls back to pandas
    when ``filters`` are specified or when the file contains unsupported
    column types.

    Parameters
    ----------
    path    : Path to the Parquet file.
    engine  : Parquet library to use (``"auto"``, ``"pyarrow"``,
              ``"fastparquet"``).  Only used on the pandas fallback path.
    columns : List of column names to read.  ``None`` reads all columns.
    filters : Row-group filters in PyArrow DNF form.  Forces the pandas
              fallback path because marrow does not support row-group
              filtering.
    """
    # filters require the pandas path — marrow has no filter support.
    if filters:
        return _read_parquet_pandas(path, engine, columns, filters)

    try:
        var table = _marrow_read_table(path)
        var df = table_to_dataframe(table)

        # Apply column selection if requested.
        if columns:
            var py_cols = columns.value()
            var n = Int(py_cols.__len__())
            var col_names = List[String]()
            for i in range(n):
                col_names.append(String(py_cols[i]))
            df = df.filter(items=col_names^)

        return df^
    except:
        # Unsupported Arrow types (e.g. nested, decimal) → pandas fallback.
        return _read_parquet_pandas(path, engine, columns, filters)
