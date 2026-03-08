from python import Python, PythonObject
from ._errors import _not_implemented
from .dtypes import BisonDtype, object_
from .accessors.str_accessor import StringMethods
from .accessors.dt_accessor import DatetimeMethods


struct Series:
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
            self._len = int(pd_s.__len__())
        except:
            self._len = 0

    @staticmethod
    fn from_pandas(pd_s: PythonObject) raises -> Series:
        var name = str(pd_s.name)
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
        return self

    fn tail(self, n: Int = 5) raises -> Series:
        _not_implemented("Series.tail")
        return self

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
        return self

    fn sub(self, other: Series) raises -> Series:
        _not_implemented("Series.sub")
        return self

    fn mul(self, other: Series) raises -> Series:
        _not_implemented("Series.mul")
        return self

    fn div(self, other: Series) raises -> Series:
        _not_implemented("Series.div")
        return self

    fn floordiv(self, other: Series) raises -> Series:
        _not_implemented("Series.floordiv")
        return self

    fn mod(self, other: Series) raises -> Series:
        _not_implemented("Series.mod")
        return self

    fn pow(self, other: Series) raises -> Series:
        _not_implemented("Series.pow")
        return self

    fn radd(self, other: Series) raises -> Series:
        _not_implemented("Series.radd")
        return self

    fn rsub(self, other: Series) raises -> Series:
        _not_implemented("Series.rsub")
        return self

    fn rmul(self, other: Series) raises -> Series:
        _not_implemented("Series.rmul")
        return self

    fn rdiv(self, other: Series) raises -> Series:
        _not_implemented("Series.rdiv")
        return self

    fn rfloordiv(self, other: Series) raises -> Series:
        _not_implemented("Series.rfloordiv")
        return self

    fn rmod(self, other: Series) raises -> Series:
        _not_implemented("Series.rmod")
        return self

    fn rpow(self, other: Series) raises -> Series:
        _not_implemented("Series.rpow")
        return self

    # ------------------------------------------------------------------
    # Comparison
    # ------------------------------------------------------------------

    fn eq(self, other: Series) raises -> Series:
        _not_implemented("Series.eq")
        return self

    fn ne(self, other: Series) raises -> Series:
        _not_implemented("Series.ne")
        return self

    fn lt(self, other: Series) raises -> Series:
        _not_implemented("Series.lt")
        return self

    fn le(self, other: Series) raises -> Series:
        _not_implemented("Series.le")
        return self

    fn gt(self, other: Series) raises -> Series:
        _not_implemented("Series.gt")
        return self

    fn ge(self, other: Series) raises -> Series:
        _not_implemented("Series.ge")
        return self

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    fn sum(self) raises -> PythonObject:
        _not_implemented("Series.sum")
        return PythonObject(None)

    fn mean(self) raises -> PythonObject:
        _not_implemented("Series.mean")
        return PythonObject(None)

    fn median(self) raises -> PythonObject:
        _not_implemented("Series.median")
        return PythonObject(None)

    fn min(self) raises -> PythonObject:
        _not_implemented("Series.min")
        return PythonObject(None)

    fn max(self) raises -> PythonObject:
        _not_implemented("Series.max")
        return PythonObject(None)

    fn std(self, ddof: Int = 1) raises -> PythonObject:
        _not_implemented("Series.std")
        return PythonObject(None)

    fn var(self, ddof: Int = 1) raises -> PythonObject:
        _not_implemented("Series.var")
        return PythonObject(None)

    fn count(self) raises -> Int:
        _not_implemented("Series.count")
        return 0

    fn nunique(self) raises -> Int:
        _not_implemented("Series.nunique")
        return 0

    fn describe(self) raises -> Series:
        _not_implemented("Series.describe")
        return self

    fn value_counts(self, normalize: Bool = False, sort: Bool = True) raises -> Series:
        _not_implemented("Series.value_counts")
        return self

    fn quantile(self, q: Float64 = 0.5) raises -> PythonObject:
        _not_implemented("Series.quantile")
        return PythonObject(None)

    fn cumsum(self) raises -> Series:
        _not_implemented("Series.cumsum")
        return self

    fn cumprod(self) raises -> Series:
        _not_implemented("Series.cumprod")
        return self

    fn cummin(self) raises -> Series:
        _not_implemented("Series.cummin")
        return self

    fn cummax(self) raises -> Series:
        _not_implemented("Series.cummax")
        return self

    # ------------------------------------------------------------------
    # Missing data
    # ------------------------------------------------------------------

    fn isna(self) raises -> Series:
        _not_implemented("Series.isna")
        return self

    fn isnull(self) raises -> Series:
        _not_implemented("Series.isnull")
        return self

    fn notna(self) raises -> Series:
        _not_implemented("Series.notna")
        return self

    fn notnull(self) raises -> Series:
        _not_implemented("Series.notnull")
        return self

    fn fillna(self, value: PythonObject) raises -> Series:
        _not_implemented("Series.fillna")
        return self

    fn dropna(self) raises -> Series:
        _not_implemented("Series.dropna")
        return self

    fn ffill(self) raises -> Series:
        _not_implemented("Series.ffill")
        return self

    fn bfill(self) raises -> Series:
        _not_implemented("Series.bfill")
        return self

    # ------------------------------------------------------------------
    # Sorting
    # ------------------------------------------------------------------

    fn sort_values(self, ascending: Bool = True) raises -> Series:
        _not_implemented("Series.sort_values")
        return self

    fn sort_index(self, ascending: Bool = True) raises -> Series:
        _not_implemented("Series.sort_index")
        return self

    fn argsort(self) raises -> Series:
        _not_implemented("Series.argsort")
        return self

    fn rank(self) raises -> Series:
        _not_implemented("Series.rank")
        return self

    # ------------------------------------------------------------------
    # Reshaping / transformations
    # ------------------------------------------------------------------

    fn apply(self, func: PythonObject) raises -> Series:
        _not_implemented("Series.apply")
        return self

    fn map(self, func: PythonObject) raises -> Series:
        _not_implemented("Series.map")
        return self

    fn astype(self, dtype: String) raises -> Series:
        _not_implemented("Series.astype")
        return self

    fn copy(self) raises -> Series:
        _not_implemented("Series.copy")
        return self

    fn reset_index(self, drop: Bool = False) raises -> Series:
        _not_implemented("Series.reset_index")
        return self

    fn rename(self, new_name: String) raises -> Series:
        _not_implemented("Series.rename")
        return self

    fn clip(self, lower: PythonObject, upper: PythonObject) raises -> Series:
        _not_implemented("Series.clip")
        return self

    fn abs(self) raises -> Series:
        _not_implemented("Series.abs")
        return self

    fn round(self, decimals: Int = 0) raises -> Series:
        _not_implemented("Series.round")
        return self

    fn unique(self) raises -> PythonObject:
        _not_implemented("Series.unique")
        return PythonObject(None)

    fn isin(self, values: PythonObject) raises -> Series:
        _not_implemented("Series.isin")
        return self

    fn between(self, left: PythonObject, right: PythonObject) raises -> Series:
        _not_implemented("Series.between")
        return self

    fn where(self, cond: Series) raises -> Series:
        _not_implemented("Series.where")
        return self

    fn mask(self, cond: Series) raises -> Series:
        _not_implemented("Series.mask")
        return self

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

    fn to_csv(self, path: String = "") raises -> PythonObject:
        _not_implemented("Series.to_csv")
        return PythonObject(None)

    fn to_json(self, path: String = "") raises -> PythonObject:
        _not_implemented("Series.to_json")
        return PythonObject(None)

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
        return str(self._pd_s)

    fn __len__(self) -> Int:
        return self._len
