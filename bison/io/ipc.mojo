from std.python import Python, PythonObject
from ..dataframe import DataFrame


def read_ipc(path: String) raises -> DataFrame:
    """Read an Arrow IPC (Feather v2) file into a DataFrame.

    Uses PyArrow's feather module to read the file, then converts to a
    bison DataFrame via pandas interop.

    Parameters
    ----------
    path : Path to the Arrow IPC / Feather file.
    """
    var pf = Python.import_module("pyarrow.feather")
    var table = pf.read_table(path)
    var pd_df = table.to_pandas()
    return DataFrame.from_pandas(pd_df)


def write_ipc(df: DataFrame, path: String) raises:
    """Write a DataFrame to an Arrow IPC (Feather v2) file.

    Converts the DataFrame to a PyArrow Table via pandas, then writes
    using PyArrow's feather module.

    Parameters
    ----------
    df   : The DataFrame to write.
    path : Destination file path.
    """
    var pa = Python.import_module("pyarrow")
    var pf = Python.import_module("pyarrow.feather")
    var pd_df = df.to_pandas()
    var table = pa.Table.from_pandas(pd_df)
    pf.write_feather(table, path)
