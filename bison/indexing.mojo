from python import PythonObject
from ._errors import _not_implemented


struct LocIndexer:
    """Label-based indexer (.loc)."""

    fn __getitem__(self, key: PythonObject) raises -> PythonObject:
        _not_implemented("DataFrame.loc.__getitem__")
        return PythonObject(None)

    fn __setitem__(self, key: PythonObject, value: PythonObject) raises:
        _not_implemented("DataFrame.loc.__setitem__")


struct ILocIndexer:
    """Integer-position-based indexer (.iloc)."""

    fn __getitem__(self, key: PythonObject) raises -> PythonObject:
        _not_implemented("DataFrame.iloc.__getitem__")
        return PythonObject(None)

    fn __setitem__(self, key: PythonObject, value: PythonObject) raises:
        _not_implemented("DataFrame.iloc.__setitem__")


struct AtIndexer:
    """Label-based scalar accessor (.at)."""

    fn __getitem__(self, row: PythonObject, col: String) raises -> PythonObject:
        _not_implemented("DataFrame.at.__getitem__")
        return PythonObject(None)

    fn __setitem__(self, row: PythonObject, col: String, value: PythonObject) raises:
        _not_implemented("DataFrame.at.__setitem__")


struct IAtIndexer:
    """Integer-based scalar accessor (.iat)."""

    fn __getitem__(self, row: Int, col: Int) raises -> PythonObject:
        _not_implemented("DataFrame.iat.__getitem__")
        return PythonObject(None)

    fn __setitem__(self, row: Int, col: Int, value: PythonObject) raises:
        _not_implemented("DataFrame.iat.__setitem__")
