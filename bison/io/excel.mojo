from python import PythonObject
from collections import Optional
from .._errors import _not_implemented


fn read_excel(
    io: String,
    sheet_name: Int = 0,
    header: Int = 0,
    index_col: Optional[PythonObject] = None,
    usecols: Optional[PythonObject] = None,
    dtype: Optional[PythonObject] = None,
    skiprows: Optional[PythonObject] = None,
    nrows: Optional[PythonObject] = None,
) raises -> PythonObject:
    """Read an Excel file into a DataFrame. STUB."""
    _not_implemented("read_excel")
    return PythonObject(None)


fn to_excel(
    df: PythonObject,
    excel_writer: String,
    sheet_name: String = "Sheet1",
    index: Bool = True,
    header: Bool = True,
) raises:
    """Write a DataFrame to an Excel file. STUB."""
    _not_implemented("DataFrame.to_excel")
