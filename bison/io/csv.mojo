from python import PythonObject
from .._errors import _not_implemented


fn read_csv(
    filepath: String,
    sep: String = ",",
    header: Int = 0,
    index_col: PythonObject = PythonObject(None),
    usecols: PythonObject = PythonObject(None),
    dtype: PythonObject = PythonObject(None),
    nrows: PythonObject = PythonObject(None),
    skiprows: PythonObject = PythonObject(None),
    na_values: PythonObject = PythonObject(None),
    encoding: String = "utf-8",
    parse_dates: Bool = False,
) raises -> PythonObject:
    """Read a CSV file into a DataFrame. STUB."""
    _not_implemented("read_csv")
    return PythonObject(None)


fn to_csv(
    df: PythonObject,
    path_or_buf: String = "",
    sep: String = ",",
    index: Bool = True,
    header: Bool = True,
    encoding: String = "utf-8",
) raises -> PythonObject:
    """Write a DataFrame to CSV. STUB."""
    _not_implemented("DataFrame.to_csv")
    return PythonObject(None)
