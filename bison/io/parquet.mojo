from python import PythonObject
from .._errors import _not_implemented


fn read_parquet(
    path: String,
    engine: String = "auto",
    columns: PythonObject = PythonObject(None),
    filters: PythonObject = PythonObject(None),
) raises -> PythonObject:
    """Read a Parquet file into a DataFrame. STUB."""
    _not_implemented("read_parquet")
    return PythonObject(None)


fn to_parquet(
    df: PythonObject,
    path: String,
    engine: String = "auto",
    compression: String = "snappy",
    index: Bool = True,
) raises:
    """Write a DataFrame to Parquet. STUB."""
    _not_implemented("DataFrame.to_parquet")
