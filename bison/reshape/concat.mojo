from python import PythonObject
from .._errors import _not_implemented
from ..dataframe import DataFrame


fn concat(
    objs: PythonObject,
    axis: Int = 0,
    join: String = "outer",
    ignore_index: Bool = False,
    keys: PythonObject = PythonObject(None),
    sort: Bool = False,
) raises -> DataFrame:
    """Concatenate bison objects along an axis. STUB."""
    _not_implemented("concat")
    return DataFrame()
