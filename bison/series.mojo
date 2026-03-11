from python import Python, PythonObject
from collections import Optional
from ._errors import _not_implemented
from .dtypes import BisonDtype, object_, bool_
from .column import Column, ColumnData, SeriesScalar
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
        var size = self._col.__len__()
        var end = n
        if end > size:
            end = size
        if end < 0:
            end = 0
        return Series(self._col.slice(0, end))

    fn tail(self, n: Int = 5) raises -> Series:
        var size = self._col.__len__()
        var start = size - n
        if start < 0:
            start = 0
        return Series(self._col.slice(start, size))

    fn iloc(self, i: Int) raises -> SeriesScalar:
        var size = self._col.__len__()
        var idx = i
        if idx < 0:
            idx = size + idx
        if idx < 0 or idx >= size:
            raise Error(
                "index "
                + String(i)
                + " is out of bounds for Series of length "
                + String(size)
            )
        if self._col._data.isa[List[Int64]]():
            return SeriesScalar(self._col._data[List[Int64]][idx])
        elif self._col._data.isa[List[Float64]]():
            return SeriesScalar(self._col._data[List[Float64]][idx])
        elif self._col._data.isa[List[Bool]]():
            return SeriesScalar(self._col._data[List[Bool]][idx])
        elif self._col._data.isa[List[String]]():
            return SeriesScalar(self._col._data[List[String]][idx])
        else:
            return SeriesScalar(self._col._data[List[PythonObject]][idx])

    fn at(self, label: String) raises -> SeriesScalar:
        for i in range(len(self._col._index)):
            if String(self._col._index[i]) == label:
                return self.iloc(i)
        raise Error("Series.at: label '" + label + "' not found in index")

    # ------------------------------------------------------------------
    # Arithmetic
    # ------------------------------------------------------------------

    fn add(self, other: Series) raises -> Series:
        return Series(self._col._arith_add(other._col))

    fn sub(self, other: Series) raises -> Series:
        return Series(self._col._arith_sub(other._col))

    fn mul(self, other: Series) raises -> Series:
        return Series(self._col._arith_mul(other._col))

    fn div(self, other: Series) raises -> Series:
        return Series(self._col._arith_div(other._col))

    fn floordiv(self, other: Series) raises -> Series:
        return Series(self._col._arith_floordiv(other._col))

    fn mod(self, other: Series) raises -> Series:
        return Series(self._col._arith_mod(other._col))

    fn pow(self, other: Series) raises -> Series:
        return Series(self._col._arith_pow(other._col))

    fn radd(self, other: Series) raises -> Series:
        return other.add(self)

    fn rsub(self, other: Series) raises -> Series:
        return other.sub(self)

    fn rmul(self, other: Series) raises -> Series:
        return other.mul(self)

    fn rdiv(self, other: Series) raises -> Series:
        return other.div(self)

    fn rfloordiv(self, other: Series) raises -> Series:
        return other.floordiv(self)

    fn rmod(self, other: Series) raises -> Series:
        return other.mod(self)

    fn rpow(self, other: Series) raises -> Series:
        return other.pow(self)

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

    fn mean(self, skipna: Bool = True) raises -> Float64:
        return self._col.mean(skipna)

    fn median(self, skipna: Bool = True) raises -> Float64:
        return self._col.median(skipna)

    fn min(self, skipna: Bool = True) raises -> Float64:
        return self._col.min(skipna)

    fn max(self, skipna: Bool = True) raises -> Float64:
        return self._col.max(skipna)

    fn std(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        return self._col.std(ddof, skipna)

    fn var(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        return self._col.var(ddof, skipna)

    fn count(self) -> Int:
        return self._col.count()

    fn nunique(self) raises -> Int:
        return self._col.nunique()

    fn describe(self) raises -> Series:
        """Return summary statistics as a Series (count, mean, std, min, quartiles, max)."""
        return Series(self._col.describe())

    fn value_counts(self, normalize: Bool = False, sort: Bool = True) raises -> Series:
        """Return a Series with the count (or proportion) of each unique value."""
        return Series(self._col.value_counts(normalize, sort))

    fn quantile(self, q: Float64 = 0.5) raises -> Float64:
        return self._col.quantile(q)

    fn cumsum(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cumsum(skipna))

    fn cumprod(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cumprod(skipna))

    fn cummin(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cummin(skipna))

    fn cummax(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cummax(skipna))

    # ------------------------------------------------------------------
    # Missing data
    # ------------------------------------------------------------------

    fn isna(self) raises -> Series:
        """Return a boolean Series that is True where values are null/NaN."""
        var n = len(self._col)
        var has_mask = len(self._col._null_mask) > 0
        var result = List[Bool]()
        for i in range(n):
            result.append(has_mask and self._col._null_mask[i])
        var col_data = ColumnData(result^)
        var col = Column(self._col.name, col_data^, bool_)
        return Series(col^)

    fn isnull(self) raises -> Series:
        """Alias for isna()."""
        return self.isna()

    fn notna(self) raises -> Series:
        """Return a boolean Series that is True where values are not null/NaN."""
        var n = len(self._col)
        var has_mask = len(self._col._null_mask) > 0
        var result = List[Bool]()
        for i in range(n):
            result.append(not (has_mask and self._col._null_mask[i]))
        var col_data = ColumnData(result^)
        var col = Column(self._col.name, col_data^, bool_)
        return Series(col^)

    fn notnull(self) raises -> Series:
        """Alias for notna()."""
        return self.notna()

    fn fillna(self, value: PythonObject) raises -> Series:
        """Return a copy of the Series with null/NaN values replaced by *value*."""
        var has_mask = len(self._col._null_mask) > 0
        if not has_mask:
            return Series(self._col.copy())
        var n = len(self._col)
        var idx = self._col._index.copy()
        if self._col._data.isa[List[Int64]]():
            var fill_val = Int64(Int(py=value))
            var data = List[Int64]()
            ref d = self._col._data[List[Int64]]
            for i in range(n):
                if self._col._null_mask[i]:
                    data.append(fill_val)
                else:
                    data.append(d[i])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            return Series(col^)
        elif self._col._data.isa[List[Float64]]():
            var fill_val = Float64(String(value))
            var data = List[Float64]()
            ref d = self._col._data[List[Float64]]
            for i in range(n):
                if self._col._null_mask[i]:
                    data.append(fill_val)
                else:
                    data.append(d[i])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            return Series(col^)
        elif self._col._data.isa[List[Bool]]():
            var fill_val = Bool(value.__bool__())
            var data = List[Bool]()
            ref d = self._col._data[List[Bool]]
            for i in range(n):
                if self._col._null_mask[i]:
                    data.append(fill_val)
                else:
                    data.append(d[i])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            return Series(col^)
        elif self._col._data.isa[List[String]]():
            var fill_val = String(value)
            var data = List[String]()
            ref d = self._col._data[List[String]]
            for i in range(n):
                if self._col._null_mask[i]:
                    data.append(fill_val)
                else:
                    data.append(d[i])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            return Series(col^)
        else:
            var data = List[PythonObject]()
            ref d = self._col._data[List[PythonObject]]
            for i in range(n):
                if self._col._null_mask[i]:
                    data.append(value)
                else:
                    data.append(d[i])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            return Series(col^)

    fn dropna(self) raises -> Series:
        """Return a new Series with null/NaN elements removed."""
        var has_mask = len(self._col._null_mask) > 0
        if not has_mask:
            return Series(self._col.copy())
        var keep = List[Int]()
        for i in range(len(self._col._null_mask)):
            if not self._col._null_mask[i]:
                keep.append(i)
        var result_col = self._col.take(keep)
        # All kept elements are non-null; clear the mask.
        result_col._null_mask = List[Bool]()
        return Series(result_col^)

    fn ffill(self) raises -> Series:
        """Forward-fill: propagate the last non-null value forward over nulls."""
        var has_mask = len(self._col._null_mask) > 0
        if not has_mask:
            return Series(self._col.copy())
        var n = len(self._col)
        var idx = self._col._index.copy()
        if self._col._data.isa[List[Int64]]():
            ref d = self._col._data[List[Int64]]
            var data = List[Int64]()
            var new_mask = List[Bool]()
            var last_val = Int64(0)
            var found = False
            for i in range(n):
                if self._col._null_mask[i]:
                    if found:
                        data.append(last_val)
                        new_mask.append(False)
                    else:
                        data.append(Int64(0))
                        new_mask.append(True)
                else:
                    last_val = d[i]
                    found = True
                    data.append(d[i])
                    new_mask.append(False)
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)
        elif self._col._data.isa[List[Float64]]():
            ref d = self._col._data[List[Float64]]
            var data = List[Float64]()
            var new_mask = List[Bool]()
            var last_val = Float64(0)
            var found = False
            for i in range(n):
                if self._col._null_mask[i]:
                    if found:
                        data.append(last_val)
                        new_mask.append(False)
                    else:
                        data.append(Float64(0))
                        new_mask.append(True)
                else:
                    last_val = d[i]
                    found = True
                    data.append(d[i])
                    new_mask.append(False)
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)
        elif self._col._data.isa[List[Bool]]():
            ref d = self._col._data[List[Bool]]
            var data = List[Bool]()
            var new_mask = List[Bool]()
            var last_val = False
            var found = False
            for i in range(n):
                if self._col._null_mask[i]:
                    if found:
                        data.append(last_val)
                        new_mask.append(False)
                    else:
                        data.append(False)
                        new_mask.append(True)
                else:
                    last_val = d[i]
                    found = True
                    data.append(d[i])
                    new_mask.append(False)
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)
        elif self._col._data.isa[List[String]]():
            ref d = self._col._data[List[String]]
            var data = List[String]()
            var new_mask = List[Bool]()
            var last_val = String("")
            var found = False
            for i in range(n):
                if self._col._null_mask[i]:
                    if found:
                        data.append(last_val)
                        new_mask.append(False)
                    else:
                        data.append(String(""))
                        new_mask.append(True)
                else:
                    last_val = d[i]
                    found = True
                    data.append(d[i])
                    new_mask.append(False)
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)
        else:
            ref d = self._col._data[List[PythonObject]]
            var data = List[PythonObject]()
            var new_mask = List[Bool]()
            var none_val = Python.evaluate("None")
            var last_val = none_val
            var found = False
            for i in range(n):
                if self._col._null_mask[i]:
                    if found:
                        data.append(last_val)
                        new_mask.append(False)
                    else:
                        data.append(none_val)
                        new_mask.append(True)
                else:
                    last_val = d[i]
                    found = True
                    data.append(d[i])
                    new_mask.append(False)
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)

    fn bfill(self) raises -> Series:
        """Backward-fill: propagate the next non-null value backward over nulls."""
        var has_mask = len(self._col._null_mask) > 0
        if not has_mask:
            return Series(self._col.copy())
        var n = len(self._col)
        var idx = self._col._index.copy()
        if self._col._data.isa[List[Int64]]():
            ref d = self._col._data[List[Int64]]
            var rev_data = List[Int64]()
            var rev_mask = List[Bool]()
            var next_val = Int64(0)
            var found = False
            for ri in range(n):
                var i = n - 1 - ri
                if not self._col._null_mask[i]:
                    next_val = d[i]
                    found = True
                    rev_data.append(d[i])
                    rev_mask.append(False)
                else:
                    if found:
                        rev_data.append(next_val)
                        rev_mask.append(False)
                    else:
                        rev_data.append(Int64(0))
                        rev_mask.append(True)
            var data = List[Int64]()
            var new_mask = List[Bool]()
            for ri in range(n):
                data.append(rev_data[n - 1 - ri])
                new_mask.append(rev_mask[n - 1 - ri])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)
        elif self._col._data.isa[List[Float64]]():
            ref d = self._col._data[List[Float64]]
            var rev_data = List[Float64]()
            var rev_mask = List[Bool]()
            var next_val = Float64(0)
            var found = False
            for ri in range(n):
                var i = n - 1 - ri
                if not self._col._null_mask[i]:
                    next_val = d[i]
                    found = True
                    rev_data.append(d[i])
                    rev_mask.append(False)
                else:
                    if found:
                        rev_data.append(next_val)
                        rev_mask.append(False)
                    else:
                        rev_data.append(Float64(0))
                        rev_mask.append(True)
            var data = List[Float64]()
            var new_mask = List[Bool]()
            for ri in range(n):
                data.append(rev_data[n - 1 - ri])
                new_mask.append(rev_mask[n - 1 - ri])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)
        elif self._col._data.isa[List[Bool]]():
            ref d = self._col._data[List[Bool]]
            var rev_data = List[Bool]()
            var rev_mask = List[Bool]()
            var next_val = False
            var found = False
            for ri in range(n):
                var i = n - 1 - ri
                if not self._col._null_mask[i]:
                    next_val = d[i]
                    found = True
                    rev_data.append(d[i])
                    rev_mask.append(False)
                else:
                    if found:
                        rev_data.append(next_val)
                        rev_mask.append(False)
                    else:
                        rev_data.append(False)
                        rev_mask.append(True)
            var data = List[Bool]()
            var new_mask = List[Bool]()
            for ri in range(n):
                data.append(rev_data[n - 1 - ri])
                new_mask.append(rev_mask[n - 1 - ri])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)
        elif self._col._data.isa[List[String]]():
            ref d = self._col._data[List[String]]
            var rev_data = List[String]()
            var rev_mask = List[Bool]()
            var next_val = String("")
            var found = False
            for ri in range(n):
                var i = n - 1 - ri
                if not self._col._null_mask[i]:
                    next_val = d[i]
                    found = True
                    rev_data.append(d[i])
                    rev_mask.append(False)
                else:
                    if found:
                        rev_data.append(next_val)
                        rev_mask.append(False)
                    else:
                        rev_data.append(String(""))
                        rev_mask.append(True)
            var data = List[String]()
            var new_mask = List[Bool]()
            for ri in range(n):
                data.append(rev_data[n - 1 - ri])
                new_mask.append(rev_mask[n - 1 - ri])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)
        else:
            ref d = self._col._data[List[PythonObject]]
            var none_val = Python.evaluate("None")
            var rev_data = List[PythonObject]()
            var rev_mask = List[Bool]()
            var next_val = none_val
            var found = False
            for ri in range(n):
                var i = n - 1 - ri
                if not self._col._null_mask[i]:
                    next_val = d[i]
                    found = True
                    rev_data.append(d[i])
                    rev_mask.append(False)
                else:
                    if found:
                        rev_data.append(next_val)
                        rev_mask.append(False)
                    else:
                        rev_data.append(none_val)
                        rev_mask.append(True)
            var data = List[PythonObject]()
            var new_mask = List[Bool]()
            for ri in range(n):
                data.append(rev_data[n - 1 - ri])
                new_mask.append(rev_mask[n - 1 - ri])
            var col_data = ColumnData(data^)
            var col = Column(self._col.name, col_data^, self._col.dtype, idx^)
            var has_any_null = False
            for i in range(len(new_mask)):
                if new_mask[i]:
                    has_any_null = True
                    break
            if has_any_null:
                col._null_mask = new_mask^
            return Series(col^)

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
