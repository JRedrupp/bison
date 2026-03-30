from std.python import Python, PythonObject
from std.collections import Optional
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
    """Read an Excel file into a DataFrame.

    Uses pandas interop: calls ``pandas.read_excel`` then wraps the result.
    Requires ``openpyxl`` (for ``.xlsx``) or ``xlrd`` (for legacy ``.xls``)
    to be installed.

    Parameters
    ----------
    io         : Path to the Excel file.
    sheet_name : Sheet index (0-based int).  Default ``0`` reads the first
                 sheet.
    header     : Row number to use as column names (0-based).
    index_col  : Column(s) to use as the row index.  ``None`` uses the
                 default integer index.
    usecols    : Columns to read.  ``None`` reads all columns.
    dtype      : Data type(s) to force on one or more columns.
    skiprows   : Number of rows to skip at the start of the sheet.
    nrows      : Maximum number of data rows to read.
    """
    var pd = Python.import_module("pandas")
    var py_none = Python.evaluate("None")
    var _index_col = index_col.value() if index_col else py_none
    var _usecols = usecols.value() if usecols else py_none
    var _dtype = dtype.value() if dtype else py_none
    var _skiprows = skiprows.value() if skiprows else py_none
    var _nrows = nrows.value() if nrows else py_none
    var pd_df = pd.read_excel(
        io,
        sheet_name=sheet_name,
        header=header,
        index_col=_index_col,
        usecols=_usecols,
        dtype=_dtype,
        skiprows=_skiprows,
        nrows=_nrows,
    )
    return DataFrame.from_pandas(pd_df)
