from std.python import PythonObject
from ._errors import _not_implemented
from .series import Series


struct DataFrameGroupBy:
    """GroupBy object returned by DataFrame.groupby()."""

    var _pd_gb: PythonObject   # backing pandas GroupBy — stub stage only

    fn __init__(out self, pd_gb: PythonObject):
        self._pd_gb = pd_gb

    fn agg(self, func: PythonObject) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.agg")
        return PythonObject(None)

    fn aggregate(self, func: PythonObject) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.aggregate")
        return PythonObject(None)

    fn transform(self, func: PythonObject) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.transform")
        return PythonObject(None)

    fn apply(self, func: PythonObject) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.apply")
        return PythonObject(None)

    fn sum(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.sum")
        return PythonObject(None)

    fn mean(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.mean")
        return PythonObject(None)

    fn min(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.min")
        return PythonObject(None)

    fn max(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.max")
        return PythonObject(None)

    fn count(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.count")
        return PythonObject(None)

    fn nunique(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.nunique")
        return PythonObject(None)

    fn first(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.first")
        return PythonObject(None)

    fn last(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.last")
        return PythonObject(None)

    fn size(self) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.size")
        return PythonObject(None)

    fn std(self, ddof: Int = 1) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.std")
        return PythonObject(None)

    fn var(self, ddof: Int = 1) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.var")
        return PythonObject(None)

    fn filter(self, func: PythonObject) raises -> PythonObject:
        _not_implemented("DataFrameGroupBy.filter")
        return PythonObject(None)


struct SeriesGroupBy:
    """GroupBy object returned by Series.groupby()."""

    var _pd_gb: PythonObject

    fn __init__(out self, pd_gb: PythonObject):
        self._pd_gb = pd_gb

    fn agg(self, func: PythonObject) raises -> Series:
        _not_implemented("SeriesGroupBy.agg")
        return Series(PythonObject(None))

    fn aggregate(self, func: PythonObject) raises -> Series:
        _not_implemented("SeriesGroupBy.aggregate")
        return Series(PythonObject(None))

    fn transform(self, func: PythonObject) raises -> Series:
        _not_implemented("SeriesGroupBy.transform")
        return Series(PythonObject(None))

    fn apply(self, func: PythonObject) raises -> Series:
        _not_implemented("SeriesGroupBy.apply")
        return Series(PythonObject(None))

    fn sum(self) raises -> Series:
        _not_implemented("SeriesGroupBy.sum")
        return Series(PythonObject(None))

    fn mean(self) raises -> Series:
        _not_implemented("SeriesGroupBy.mean")
        return Series(PythonObject(None))

    fn min(self) raises -> Series:
        _not_implemented("SeriesGroupBy.min")
        return Series(PythonObject(None))

    fn max(self) raises -> Series:
        _not_implemented("SeriesGroupBy.max")
        return Series(PythonObject(None))

    fn count(self) raises -> Series:
        _not_implemented("SeriesGroupBy.count")
        return Series(PythonObject(None))

    fn nunique(self) raises -> Series:
        _not_implemented("SeriesGroupBy.nunique")
        return Series(PythonObject(None))

    fn first(self) raises -> Series:
        _not_implemented("SeriesGroupBy.first")
        return Series(PythonObject(None))

    fn last(self) raises -> Series:
        _not_implemented("SeriesGroupBy.last")
        return Series(PythonObject(None))

    fn size(self) raises -> Series:
        _not_implemented("SeriesGroupBy.size")
        return Series(PythonObject(None))

    fn std(self, ddof: Int = 1) raises -> Series:
        _not_implemented("SeriesGroupBy.std")
        return Series(PythonObject(None))

    fn var(self, ddof: Int = 1) raises -> Series:
        _not_implemented("SeriesGroupBy.var")
        return Series(PythonObject(None))
