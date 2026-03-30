from std.python import PythonObject
from std.collections import Optional
from .._errors import _not_implemented
from ..dataframe import DataFrame


def read_excel(
    io: String,
    sheet_name: Int = 0,
    header: Int = 0,
    index_col: Optional[PythonObject] = None,
    usecols: Optional[PythonObject] = None,
    dtype: Optional[PythonObject] = None,
    skiprows: Optional[PythonObject] = None,
    nrows: Optional[PythonObject] = None,
) raises -> DataFrame:
    """Read an Excel file into a DataFrame. STUB."""
    _not_implemented("read_excel")
    return DataFrame(PythonObject(None))
