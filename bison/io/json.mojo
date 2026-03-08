from python import PythonObject
from .._errors import _not_implemented


fn read_json(
    path_or_buf: String,
    orient: String = "",
    dtype: PythonObject = PythonObject(None),
    lines: Bool = False,
) raises -> PythonObject:
    """Read a JSON file into a DataFrame. STUB."""
    _not_implemented("read_json")
    return PythonObject(None)


fn to_json(
    df: PythonObject,
    path_or_buf: String = "",
    orient: String = "",
    lines: Bool = False,
    indent: Int = 0,
) raises -> PythonObject:
    """Write a DataFrame to JSON. STUB."""
    _not_implemented("DataFrame.to_json")
    return PythonObject(None)
