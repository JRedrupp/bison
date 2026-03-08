from python import Python, PythonObject
from collections import Optional
from ._errors import _not_implemented
from .dtypes import BisonDtype, object_
from .accessors.str_accessor import StringMethods
from .accessors.dt_accessor import DatetimeMethods


struct Series(Copyable, Movable):
    """A one-dimensional labeled array, mirroring the pandas Series API."""

    var _pd_s: PythonObject   # backing pandas Series — stub stage only
    var name: String
    var _len: Int

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    fn __init__(out self, pd_s: PythonObject, name: String = ""):
        self._pd_s = pd_s
        self.name = name
        try:
            self._len = pd_s.__len__()
        except:
            self._len = 0

    fn __copyinit__(out self, existing: Self):
        self._pd_s = existing._pd_s
        self.name = existing.name
        self._len = existing._len

    fn __moveinit__(out self, deinit existing: Self):
        self._pd_s = existing._pd_s^
        self.name = existing.name^
        self._len = existing._len

    @staticmethod
    fn from_pandas(pd_s: PythonObject) raises -> Series:
        var name = String(pd_s.name)
        return Series(pd_s, name)

    fn to_pandas(self) -> PythonObject:
        return self._pd_s

    # ------------------------------------------------------------------
    # Attributes
    # ------------------------------------------------------------------

    fn shape(self) -> Tuple[Int]:
        return (self._len,)

    fn size(self) -> Int:
        return self._len

    fn empty(self) -> Bool:
        return self._len == 0

    fn dtype(self) raises -> BisonDtype:
        _not_implemented("Series.dtype")
        return object_

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------

    fn head(self, n: Int = 5) raises -> Series:
        _not_implemented("Series.head")
        return Series(self._pd_s, self.name)

    fn tail(self, n: Int = 5) raises -> Series:
        _not_implemented("Series.tail")
        return Series(self._pd_s, self.name)

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
        return Series(self._pd_s, self.name)

    fn sub(self, other: Series) raises -> Series:
        _not_implemented("Series.sub")
        return Series(self._pd_s, self.name)

    fn mul(self, other: Series) raises -> Series:
        _not_implemented("Series.mul")
        return Series(self._pd_s, self.name)

    fn div(self, other: Series) raises -> Series:
        _not_implemented("Series.div")
        return Series(self._pd_s, self.name)

    fn floordiv(self, other: Series) raises -> Series:
        _not_implemented("Series.floordiv")
        return Series(self._pd_s, self.name)

    fn mod(self, other: Series) raises -> Series:
        _not_implemented("Series.mod")
        return Series(self._pd_s, self.name)

    fn pow(self, other: Series) raises -> Series:
        _not_implemented("Series.pow")
        return Series(self._pd_s, self.name)

    fn radd(self, other: Series) raises -> Series:
        _not_implemented("Series.radd")
        return Series(self._pd_s, self.name)

    fn rsub(self, other: Series) raises -> Series:
        _not_implemented("Series.rsub")
        return Series(self._pd_s, self.name)

    fn rmul(self, other: Series) raises -> Series:
        _not_implemented("Series.rmul")
        return Series(self._pd_s, self.name)

    fn rdiv(self, other: Series) raises -> Series:
        _not_implemented("Series.rdiv")
        return Series(self._pd_s, self.name)

    fn rfloordiv(self, other: Series) raises -> Series:
        _not_implemented("Series.rfloordiv")
        return Series(self._pd_s, self.name)

    fn rmod(self, other: Series) raises -> Series:
        _not_implemented("Series.rmod")
        return Series(self._pd_s, self.name)

    fn rpow(self, other: Series) raises -> Series:
        _not_implemented("Series.rpow")
        return Series(self._pd_s, self.name)

    # ------------------------------------------------------------------
    # Comparison
    # ------------------------------------------------------------------

    fn eq(self, other: Series) raises -> Series:
        _not_implemented("Series.eq")
        return Series(self._pd_s, self.name)

    fn ne(self, other: Series) raises -> Series:
        _not_implemented("Series.ne")
        return Series(self._pd_s, self.name)

    fn lt(self, other: Series) raises -> Series:
        _not_implemented("Series.lt")
        return Series(self._pd_s, self.name)

    fn le(self, other: Series) raises -> Series:
        _not_implemented("Series.le")
        return Series(self._pd_s, self.name)

    fn gt(self, other: Series) raises -> Series:
        _not_implemented("Series.gt")
        return Series(self._pd_s, self.name)

    fn ge(self, other: Series) raises -> Series:
        _not_implemented("Series.ge")
        return Series(self._pd_s, self.name)

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    fn sum(self) raises -> Float64:
        _not_implemented("Series.sum")
        return Float64(0)

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
        return Series(self._pd_s, self.name)

    fn value_counts(self, normalize: Bool = False, sort: Bool = True) raises -> Series:
        _not_implemented("Series.value_counts")
        return Series(self._pd_s, self.name)

    fn quantile(self, q: Float64 = 0.5) raises -> Float64:
        _not_implemented("Series.quantile")
        return Float64(0)

    fn cumsum(self) raises -> Series:
        _not_implemented("Series.cumsum")
        return Series(self._pd_s, self.name)

    fn cumprod(self) raises -> Series:
        _not_implemented("Series.cumprod")
        return Series(self._pd_s, self.name)

    fn cummin(self) raises -> Series:
        _not_implemented("Series.cummin")
        return Series(self._pd_s, self.name)

    fn cummax(self) raises -> Series:
        _not_implemented("Series.cummax")
        return Series(self._pd_s, self.name)

    # ------------------------------------------------------------------
    # Missing data
    # ------------------------------------------------------------------

    fn isna(self) raises -> Series:
        _not_implemented("Series.isna")
        return Series(self._pd_s, self.name)

    fn isnull(self) raises -> Series:
        _not_implemented("Series.isnull")
        return Series(self._pd_s, self.name)

    fn notna(self) raises -> Series:
        _not_implemented("Series.notna")
        return Series(self._pd_s, self.name)

    fn notnull(self) raises -> Series:
        _not_implemented("Series.notnull")
        return Series(self._pd_s, self.name)

    fn fillna(self, value: PythonObject) raises -> Series:
        _not_implemented("Series.fillna")
        return Series(self._pd_s, self.name)

    fn dropna(self) raises -> Series:
        _not_implemented("Series.dropna")
        return Series(self._pd_s, self.name)

    fn ffill(self) raises -> Series:
        _not_implemented("Series.ffill")
        return Series(self._pd_s, self.name)

    fn bfill(self) raises -> Series:
        _not_implemented("Series.bfill")
        return Series(self._pd_s, self.name)

    # ------------------------------------------------------------------
    # Sorting
    # ------------------------------------------------------------------

    fn sort_values(self, ascending: Bool = True) raises -> Series:
        _not_implemented("Series.sort_values")
        return Series(self._pd_s, self.name)

    fn sort_index(self, ascending: Bool = True) raises -> Series:
        _not_implemented("Series.sort_index")
        return Series(self._pd_s, self.name)

    fn argsort(self) raises -> Series:
        _not_implemented("Series.argsort")
        return Series(self._pd_s, self.name)

    fn rank(self) raises -> Series:
        _not_implemented("Series.rank")
        return Series(self._pd_s, self.name)

    # ------------------------------------------------------------------
    # Reshaping / transformations
    # ------------------------------------------------------------------

    fn apply(self, func: PythonObject) raises -> Series:
        _not_implemented("Series.apply")
        return Series(self._pd_s, self.name)

    fn map(self, func: PythonObject) raises -> Series:
        _not_implemented("Series.map")
        return Series(self._pd_s, self.name)

    fn astype(self, dtype: String) raises -> Series:
        _not_implemented("Series.astype")
        return Series(self._pd_s, self.name)

    fn copy(self) raises -> Series:
        _not_implemented("Series.copy")
        return Series(self._pd_s, self.name)

    fn reset_index(self, drop: Bool = False) raises -> Series:
        _not_implemented("Series.reset_index")
        return Series(self._pd_s, self.name)

    fn rename(self, new_name: String) raises -> Series:
        _not_implemented("Series.rename")
        return Series(self._pd_s, self.name)

    fn clip(self, lower: PythonObject, upper: PythonObject) raises -> Series:
        _not_implemented("Series.clip")
        return Series(self._pd_s, self.name)

    fn abs(self) raises -> Series:
        _not_implemented("Series.abs")
        return Series(self._pd_s, self.name)

    fn round(self, decimals: Int = 0) raises -> Series:
        _not_implemented("Series.round")
        return Series(self._pd_s, self.name)

    fn unique(self) raises -> Series:
        _not_implemented("Series.unique")
        return Series(self._pd_s, self.name)

    fn isin(self, values: PythonObject) raises -> Series:
        _not_implemented("Series.isin")
        return Series(self._pd_s, self.name)

    fn between(self, left: PythonObject, right: PythonObject) raises -> Series:
        _not_implemented("Series.between")
        return Series(self._pd_s, self.name)

    fn where(self, cond: Series) raises -> Series:
        _not_implemented("Series.where")
        return Series(self._pd_s, self.name)

    fn mask(self, cond: Series) raises -> Series:
        _not_implemented("Series.mask")
        return Series(self._pd_s, self.name)

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
        return String(self._pd_s)

    fn __len__(self) -> Int:
        return self._len
