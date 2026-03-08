from python import PythonObject
from collections import Optional
from .._errors import _not_implemented


fn read_csv(
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
