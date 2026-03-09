from python import Python, PythonObject
from collections import Optional
from ._errors import _not_implemented
from .dtypes import BisonDtype, object_
from .column import Column
from .accessors.str_accessor import StringMethods
from .accessors.dt_accessor import DatetimeMethods


struct Series(Copyable, Movable):
    """A one-dimensional labeled array, mirroring the pandas Series API."""

    var _col: Column
    var name: String

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    fn __init__(out self):
        """Empty Series — used as stub return placeholder."""
        self._col = Column()
        self.name = ""

    fn __init__(out self, var col: Column):
        self.name = col.name
        self._col = col^

    fn __init__(out self, pd_s: PythonObject, name: String = "") raises:
        """Convenience constructor: wraps a pandas Series."""
        var col_name: String
        if name != "":
            col_name = name
        else:
            col_name = String(pd_s.name)
        self._col = Column.from_pandas(pd_s, col_name)
        self.name = self._col.name

    fn __copyinit__(out self, existing: Self):
        self._col = existing._col.copy()
        self.name = existing.name

    fn __moveinit__(out self, deinit existing: Self):
        self._col = existing._col^
        self.name = existing.name^

    @staticmethod
    fn from_pandas(pd_s: PythonObject) raises -> Series:
        var col_name = String(pd_s.name)
        return Series(Column.from_pandas(pd_s, col_name))

    fn to_pandas(self) raises -> PythonObject:
        return self._col.to_pandas()

    # ------------------------------------------------------------------
    # Attributes
    # ------------------------------------------------------------------

    fn shape(self) -> Tuple[Int]:
        return (self._col.__len__(),)

    fn size(self) -> Int:
        return self._col.__len__()

    fn empty(self) -> Bool:
        return self._col.__len__() == 0

    fn dtype(self) raises -> BisonDtype:
        _not_implemented("Series.dtype")
        return object_

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------

    fn head(self, n: Int = 5) raises -> Series:
        _not_implemented("Series.head")
        return Series()

    fn tail(self, n: Int = 5) raises -> Series:
        _not_implemented("Series.tail")
        return Series()

    fn iloc(self, i: Int) raises -> PythonObject:
        _not_implemented("Series.iloc")
        return PythonObject(None)

    fn at(self, label: String) raises -> PythonObject:
        _not_implemented("Series.at")
        return PythonObject(None)

    # ------------------------------------------------------------------
    # Arithmetic
    # ------------------------------------------------------------------

    fn add(self, other: Series) raises -> Series:
        _not_implemented("Series.add")
        return Series()

    fn sub(self, other: Series) raises -> Series:
        _not_implemented("Series.sub")
        return Series()

    fn mul(self, other: Series) raises -> Series:
        _not_implemented("Series.mul")
        return Series()

    fn div(self, other: Series) raises -> Series:
        _not_implemented("Series.div")
        return Series()

    fn floordiv(self, other: Series) raises -> Series:
        _not_implemented("Series.floordiv")
        return Series()

    fn mod(self, other: Series) raises -> Series:
        _not_implemented("Series.mod")
        return Series()

    fn pow(self, other: Series) raises -> Series:
        _not_implemented("Series.pow")
        return Series()

    fn radd(self, other: Series) raises -> Series:
        _not_implemented("Series.radd")
        return Series()

    fn rsub(self, other: Series) raises -> Series:
        _not_implemented("Series.rsub")
        return Series()

    fn rmul(self, other: Series) raises -> Series:
        _not_implemented("Series.rmul")
        return Series()

    fn rdiv(self, other: Series) raises -> Series:
        _not_implemented("Series.rdiv")
        return Series()

    fn rfloordiv(self, other: Series) raises -> Series:
        _not_implemented("Series.rfloordiv")
        return Series()

    fn rmod(self, other: Series) raises -> Series:
        _not_implemented("Series.rmod")
        return Series()

    fn rpow(self, other: Series) raises -> Series:
        _not_implemented("Series.rpow")
        return Series()

    # ------------------------------------------------------------------
    # Comparison
    # ------------------------------------------------------------------

    fn eq(self, other: Series) raises -> Series:
        _not_implemented("Series.eq")
        return Series()

    fn ne(self, other: Series) raises -> Series:
        _not_implemented("Series.ne")
        return Series()

    fn lt(self, other: Series) raises -> Series:
        _not_implemented("Series.lt")
        return Series()

    fn le(self, other: Series) raises -> Series:
        _not_implemented("Series.le")
        return Series()

    fn gt(self, other: Series) raises -> Series:
        _not_implemented("Series.gt")
        return Series()

    fn ge(self, other: Series) raises -> Series:
        _not_implemented("Series.ge")
        return Series()

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    fn sum(self, skipna: Bool = True) raises -> Float64:
        return self._col.sum(skipna)

    fn mean(self) raises -> Float64:
        _not_implemented("Series.mean")
        return Float64(0)

    fn median(self) raises -> Float64:
        _not_implemented("Series.median")
        return Float64(0)

    fn min(self) raises -> Float64:
        _not_implemented("Series.min")
        return Float64(0)

    fn max(self) raises -> Float64:
        _not_implemented("Series.max")
        return Float64(0)

    fn std(self, ddof: Int = 1) raises -> Float64:
        _not_implemented("Series.std")
        return Float64(0)

    fn var(self, ddof: Int = 1) raises -> Float64:
        _not_implemented("Series.var")
        return Float64(0)

    fn count(self) raises -> Int:
        _not_implemented("Series.count")
        return 0

    fn nunique(self) raises -> Int:
        _not_implemented("Series.nunique")
        return 0

    fn describe(self) raises -> Series:
        _not_implemented("Series.describe")
        return Series()

    fn value_counts(self, normalize: Bool = False, sort: Bool = True) raises -> Series:
        _not_implemented("Series.value_counts")
        return Series()

    fn quantile(self, q: Float64 = 0.5) raises -> Float64:
        _not_implemented("Series.quantile")
        return Float64(0)

    fn cumsum(self) raises -> Series:
        _not_implemented("Series.cumsum")
        return Series()

    fn cumprod(self) raises -> Series:
        _not_implemented("Series.cumprod")
        return Series()

    fn cummin(self) raises -> Series:
        _not_implemented("Series.cummin")
        return Series()

    fn cummax(self) raises -> Series:
        _not_implemented("Series.cummax")
        return Series()

    # ------------------------------------------------------------------
    # Missing data
    # ------------------------------------------------------------------

    fn isna(self) raises -> Series:
        _not_implemented("Series.isna")
        return Series()

    fn isnull(self) raises -> Series:
        _not_implemented("Series.isnull")
        return Series()

    fn notna(self) raises -> Series:
        _not_implemented("Series.notna")
        return Series()

    fn notnull(self) raises -> Series:
        _not_implemented("Series.notnull")
        return Series()

    fn fillna(self, value: PythonObject) raises -> Series:
        _not_implemented("Series.fillna")
        return Series()

    fn dropna(self) raises -> Series:
        _not_implemented("Series.dropna")
        return Series()

    fn ffill(self) raises -> Series:
        _not_implemented("Series.ffill")
        return Series()

    fn bfill(self) raises -> Series:
        _not_implemented("Series.bfill")
        return Series()

    # ------------------------------------------------------------------
    # Sorting
    # ------------------------------------------------------------------

    fn sort_values(self, ascending: Bool = True) raises -> Series:
        _not_implemented("Series.sort_values")
        return Series()

    fn sort_index(self, ascending: Bool = True) raises -> Series:
        _not_implemented("Series.sort_index")
        return Series()

    fn argsort(self) raises -> Series:
        _not_implemented("Series.argsort")
        return Series()

    fn rank(self) raises -> Series:
        _not_implemented("Series.rank")
        return Series()

    # ------------------------------------------------------------------
    # Reshaping / transformations
    # ------------------------------------------------------------------

    fn apply(self, func: PythonObject) raises -> Series:
        _not_implemented("Series.apply")
        return Series()

    fn map(self, func: PythonObject) raises -> Series:
        _not_implemented("Series.map")
        return Series()

    fn astype(self, dtype: String) raises -> Series:
        _not_implemented("Series.astype")
        return Series()

    fn copy(self) raises -> Series:
        _not_implemented("Series.copy")
        return Series()

    fn reset_index(self, drop: Bool = False) raises -> Series:
        _not_implemented("Series.reset_index")
        return Series()

    fn rename(self, new_name: String) raises -> Series:
        _not_implemented("Series.rename")
        return Series()

    fn clip(self, lower: PythonObject, upper: PythonObject) raises -> Series:
        _not_implemented("Series.clip")
        return Series()

    fn abs(self) raises -> Series:
        _not_implemented("Series.abs")
        return Series()

    fn round(self, decimals: Int = 0) raises -> Series:
        _not_implemented("Series.round")
        return Series()

    fn unique(self) raises -> Series:
        _not_implemented("Series.unique")
        return Series()

    fn isin(self, values: PythonObject) raises -> Series:
        _not_implemented("Series.isin")
        return Series()

    fn between(self, left: PythonObject, right: PythonObject) raises -> Series:
        _not_implemented("Series.between")
        return Series()

    fn where(self, cond: Series) raises -> Series:
        _not_implemented("Series.where")
        return Series()

    fn mask(self, cond: Series) raises -> Series:
        _not_implemented("Series.mask")
        return Series()

    # ------------------------------------------------------------------
    # Interop
    # ------------------------------------------------------------------

    fn to_list(self) raises -> PythonObject:
        _not_implemented("Series.to_list")
        return PythonObject(None)

    fn to_numpy(self) raises -> PythonObject:
        _not_implemented("Series.to_numpy")
        return PythonObject(None)

    fn to_frame(self, name: String = "") raises -> PythonObject:
        _not_implemented("Series.to_frame")
        return PythonObject(None)

    fn to_dict(self) raises -> PythonObject:
        _not_implemented("Series.to_dict")
        return PythonObject(None)

    fn to_csv(self, path: String = "") raises -> String:
        _not_implemented("Series.to_csv")
        return String("")

    fn to_json(self, path: String = "") raises -> String:
        _not_implemented("Series.to_json")
        return String("")

    # ------------------------------------------------------------------
    # String / Datetime accessors (return accessor structs)
    # ------------------------------------------------------------------

    fn str(self) raises -> StringMethods:
        _not_implemented("Series.str")
        return StringMethods()

    fn dt(self) raises -> DatetimeMethods:
        _not_implemented("Series.dt")
        return DatetimeMethods()

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    fn __repr__(self) raises -> String:
        return String(self.to_pandas())

    fn __len__(self) -> Int:
        return self._col.__len__()
