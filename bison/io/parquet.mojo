from std.python import Python, PythonObject
from std.collections import Optional
from ..dataframe import DataFrame


def read_parquet(
    path: String,
    engine: String = "auto",
    columns: Optional[PythonObject] = None,
    filters: Optional[PythonObject] = None,
) raises -> DataFrame:
    """Read a Parquet file into a DataFrame.

    Uses pandas interop: calls ``pandas.read_parquet`` then wraps the result.
    Requires ``pyarrow`` or ``fastparquet`` to be installed.

    Parameters
    ----------
    path    : Path to the Parquet file.
    engine  : Parquet library to use (``"auto"``, ``"pyarrow"``,
              ``"fastparquet"``).  Passed directly to pandas.
    columns : List of column names to read.  ``None`` reads all columns.
    filters : Row-group filters passed directly to pandas (PyArrow DNF form).
    """
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


def to_parquet(
    df: PythonObject,
    path: String,
    engine: String = "auto",
    compression: String = "snappy",
    index: Bool = True,
) raises:
    """Write a pandas DataFrame to a Parquet file.

    Parameters
    ----------
    df          : A pandas DataFrame (``PythonObject``).
    path        : Destination file path.
    engine      : Parquet library to use (``"auto"``, ``"pyarrow"``,
                  ``"fastparquet"``).  Passed directly to pandas.
    compression : Compression codec (default ``"snappy"``).
    index       : Whether to write the row index (default ``True``).
    """
    df.to_parquet(path, engine=engine, compression=compression, index=index)
