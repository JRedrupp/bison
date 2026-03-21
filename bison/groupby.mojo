from ._errors import _not_implemented
from .series import Series


struct DataFrameGroupBy:
    """GroupBy object returned by DataFrame.groupby().

    Stub-only type — all methods raise 'not implemented' until a native
    implementation is added.  No backing state is held at this stage.
    """

    def __init__(out self):
        pass

    def agg(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.agg")
        return Series()

    def aggregate(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.aggregate")
        return Series()

    def transform(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.transform")
        return Series()

    def apply(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.apply")
        return Series()

    def sum(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.sum")
        return Series()

    def mean(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.mean")
        return Series()

    def min(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.min")
        return Series()

    def max(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.max")
        return Series()

    def count(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.count")
        return Series()

    def nunique(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.nunique")
        return Series()

    def first(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.first")
        return Series()

    def last(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.last")
        return Series()

    def size(self) raises -> Series:
        _not_implemented("DataFrameGroupBy.size")
        return Series()

    def std(self, ddof: Int = 1) raises -> Series:
        _not_implemented("DataFrameGroupBy.std")
        return Series()

    def var(self, ddof: Int = 1) raises -> Series:
        _not_implemented("DataFrameGroupBy.var")
        return Series()

    def filter(self, func: String) raises -> Series:
        _not_implemented("DataFrameGroupBy.filter")
        return Series()


struct SeriesGroupBy:
    """GroupBy object returned by Series.groupby().

    Stub-only type — all methods raise 'not implemented' until a native
    implementation is added.  No backing state is held at this stage.
    """

    def __init__(out self):
        pass

    def agg(self, func: String) raises -> Series:
        _not_implemented("SeriesGroupBy.agg")
        return Series()

    def aggregate(self, func: String) raises -> Series:
        _not_implemented("SeriesGroupBy.aggregate")
        return Series()

    def transform(self, func: String) raises -> Series:
        _not_implemented("SeriesGroupBy.transform")
        return Series()

    def apply(self, func: String) raises -> Series:
        _not_implemented("SeriesGroupBy.apply")
        return Series()

    def sum(self) raises -> Series:
        _not_implemented("SeriesGroupBy.sum")
        return Series()

    def mean(self) raises -> Series:
        _not_implemented("SeriesGroupBy.mean")
        return Series()

    def min(self) raises -> Series:
        _not_implemented("SeriesGroupBy.min")
        return Series()

    def max(self) raises -> Series:
        _not_implemented("SeriesGroupBy.max")
        return Series()

    def count(self) raises -> Series:
        _not_implemented("SeriesGroupBy.count")
        return Series()

    def nunique(self) raises -> Series:
        _not_implemented("SeriesGroupBy.nunique")
        return Series()

    def first(self) raises -> Series:
        _not_implemented("SeriesGroupBy.first")
        return Series()

    def last(self) raises -> Series:
        _not_implemented("SeriesGroupBy.last")
        return Series()

    def size(self) raises -> Series:
        _not_implemented("SeriesGroupBy.size")
        return Series()

    def std(self, ddof: Int = 1) raises -> Series:
        _not_implemented("SeriesGroupBy.std")
        return Series()

    def var(self, ddof: Int = 1) raises -> Series:
        _not_implemented("SeriesGroupBy.var")
        return Series()
