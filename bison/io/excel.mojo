from python import PythonObject
from .._errors import _not_implemented


fn read_excel(
    io: String,
    sheet_name: PythonObject = PythonObject(0),
    header: Int = 0,
    index_col: PythonObject = PythonObject(None),
    usecols: PythonObject = PythonObject(None),
    dtype: PythonObject = PythonObject(None),
    skiprows: PythonObject = PythonObject(None),
    nrows: PythonObject = PythonObject(None),
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
