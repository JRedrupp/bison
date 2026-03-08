from python import PythonObject
from .._errors import _not_implemented


fn concat(
    objs: PythonObject,
    axis: Int = 0,
    join: String = "outer",
    ignore_index: Bool = False,
    keys: PythonObject = PythonObject(None),
    sort: Bool = False,
) raises -> PythonObject:
    """Concatenate bison objects along an axis. STUB."""
    _not_implemented("concat")
    return PythonObject(None)
