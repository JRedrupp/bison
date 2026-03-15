from ._errors import _not_implemented
from .series import Series


struct DataFrameGroupBy:
    """GroupBy object returned by DataFrame.groupby().

    Stub-only type — all methods raise 'not implemented' until a native
    implementation is added.  No backing state is held at this stage.
    """

    fn __init__(out self):
        pass

    fn agg(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.agg")
        return Series()

    fn aggregate(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.aggregate")
        return Series()

    fn transform(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.transform")
        return Series()

    fn apply(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.apply")
        return Series()

    fn sum(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.sum")
        return Series()

    fn mean(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.mean")
        return Series()

    fn min(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.min")
        return Series()

    fn max(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.max")
        return Series()

    fn count(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.count")
        return Series()

    fn nunique(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.nunique")
        return Series()

    fn first(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.first")
        return Series()

    fn last(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.last")
        return Series()

    fn size(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.size")
        return Series()

    fn std(self, ddof: Int = 1) raises -> Series:
        _not_implemented("DataFrameGroupBy.std")
        return Series()

    fn var(self, ddof: Int = 1) raises -> Series:
        _not_implemented("DataFrameGroupBy.var")
        return Series()

    fn filter(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.filter")
        return Series()


struct SeriesGroupBy:
    """GroupBy object returned by Series.groupby().

    Stub-only type — all methods raise 'not implemented' until a native
    implementation is added.  No backing state is held at this stage.
    """

    fn __init__(out self):
        pass

    fn agg(self, func: String) raises -> Series:
        _not_implemented("SeriesGroupBy.agg")
        return Series()

    fn aggregate(self, func: String) raises -> Series:
        _not_implemented("SeriesGroupBy.aggregate")
        return Series()

    fn transform(self, func: String) raises -> Series:
        _not_implemented("SeriesGroupBy.transform")
        return Series()

    fn apply(self, func: String) raises -> Series:
        _not_implemented("SeriesGroupBy.apply")
        return Series()

    fn sum(self) raises -> Series:
        _not_implemented("SeriesGroupBy.sum")
        return Series()

    fn mean(self) raises -> Series:
        _not_implemented("SeriesGroupBy.mean")
        return Series()

    fn min(self) raises -> Series:
        _not_implemented("SeriesGroupBy.min")
        return Series()

    fn max(self) raises -> Series:
        _not_implemented("SeriesGroupBy.max")
        return Series()

    fn count(self) raises -> Series:
        _not_implemented("SeriesGroupBy.count")
        return Series()

    fn nunique(self) raises -> Series:
        _not_implemented("SeriesGroupBy.nunique")
        return Series()

    fn first(self) raises -> Series:
        _not_implemented("SeriesGroupBy.first")
        return Series()

    fn last(self) raises -> Series:
        _not_implemented("SeriesGroupBy.last")
        return Series()

    fn size(self) raises -> Series:
        _not_implemented("SeriesGroupBy.size")
        return Series()

    fn std(self, ddof: Int = 1) raises -> Series:
        _not_implemented("SeriesGroupBy.std")
        return Series()

    fn var(self, ddof: Int = 1) raises -> Series:
        _not_implemented("SeriesGroupBy.var")
        return Series()
