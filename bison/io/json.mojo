from std.python import PythonObject
from std.collections import Optional
from .._errors import _not_implemented
from ..dataframe import DataFrame


fn read_json(
    path_or_buf: String,
    orient: String = "",
    dtype: Optional[PythonObject] = None,
    lines: Bool = False,
) raises -> DataFrame:
    """Read a JSON file into a DataFrame. STUB."""
    _not_implemented("read_json")
    return DataFrame(PythonObject(None))


fn to_json(
    df: PythonObject,
    path_or_buf: String = "",
    orient: String = "",
    lines: Bool = False,
    indent: Int = 0,
) raises -> String:
    """Write a DataFrame to JSON. STUB."""
    _not_implemented("DataFrame.to_json")
    return String("")
