from ._errors import _not_implemented
from .column import DFScalar
from .series import Series


struct LocIndexer:
    """Label-based indexer (.loc)."""

    fn __getitem__(self, key: String) raises -> Series:
        _not_implemented("DataFrame.loc.__getitem__")
        return Series()

    fn __setitem__(self, key: String, value: Series) raises:
        _not_implemented("DataFrame.loc.__setitem__")


struct ILocIndexer:
    """Integer-position-based indexer (.iloc)."""

    fn __getitem__(self, key: Int) raises -> Series:
        _not_implemented("DataFrame.iloc.__getitem__")
        return Series()

    fn __setitem__(self, key: Int, value: Series) raises:
        _not_implemented("DataFrame.iloc.__setitem__")


struct AtIndexer:
    """Label-based scalar accessor (.at)."""

    fn __getitem__(self, row: String, col: String) raises -> DFScalar:
        _not_implemented("DataFrame.at.__getitem__")
        return DFScalar(Int64(0))

    fn __setitem__(self, row: String, col: String, value: DFScalar) raises:
        _not_implemented("DataFrame.at.__setitem__")


struct IAtIndexer:
    """Integer-based scalar accessor (.iat)."""

    fn __getitem__(self, row: Int, col: Int) raises -> DFScalar:
        _not_implemented("DataFrame.iat.__getitem__")
        return DFScalar(Int64(0))

    fn __setitem__(self, row: Int, col: Int, value: DFScalar) raises:
        _not_implemented("DataFrame.iat.__setitem__")
