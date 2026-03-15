from std.python import Python, PythonObject
from std.collections import Optional, Dict
from ._errors import _not_implemented
from .dtypes import BisonDtype, object_, bool_, int64, float64, dtype_from_string
from .column import Column, ColumnData, DFScalar, SeriesScalar, FloatTransformFn
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

    fn __copyinit__(out self, copy: Self):
        self._col = copy._col.copy()
        self.name = copy.name

    fn __moveinit__(out self, deinit take: Self):
        self._col = take._col^
        self.name = take.name^

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
        return Series(self._col._cmp_eq(other._col))

    fn ne(self, other: Series) raises -> Series:
        return Series(self._col._cmp_ne(other._col))

    fn lt(self, other: Series) raises -> Series:
        return Series(self._col._cmp_lt(other._col))

    fn le(self, other: Series) raises -> Series:
        return Series(self._col._cmp_le(other._col))

    fn gt(self, other: Series) raises -> Series:
        return Series(self._col._cmp_gt(other._col))

    fn ge(self, other: Series) raises -> Series:
        return Series(self._col._cmp_ge(other._col))

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

    fn fillna(self, value: DFScalar) raises -> Series:
        """Return a copy of the Series with null/NaN values replaced by *value*."""
        var has_mask = len(self._col._null_mask) > 0
        if not has_mask:
            return Series(self._col.copy())
        var n = len(self._col)
        var idx = self._col._index.copy()
        if self._col._data.isa[List[Int64]]():
            var fill_val: Int64
            if value.isa[Int64]():
                fill_val = value[Int64]
            elif value.isa[Float64]():
                fill_val = Int64(Int(value[Float64]))
            elif value.isa[Bool]():
                fill_val = Int64(1) if value[Bool] else Int64(0)
            else:
                raise Error("fillna: cannot fill Int64 column with String value")
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
            var fill_val: Float64
            if value.isa[Float64]():
                fill_val = value[Float64]
            elif value.isa[Int64]():
                fill_val = Float64(value[Int64])
            elif value.isa[Bool]():
                fill_val = Float64(1.0) if value[Bool] else Float64(0.0)
            else:
                raise Error("fillna: cannot fill Float64 column with String value")
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
            var fill_val: Bool
            if value.isa[Bool]():
                fill_val = value[Bool]
            elif value.isa[Int64]():
                fill_val = value[Int64] != Int64(0)
            elif value.isa[Float64]():
                fill_val = value[Float64] != Float64(0.0)
            else:
                raise Error("fillna: cannot fill Bool column with String value")
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
            var fill_val: String
            if value.isa[String]():
                fill_val = value[String]
            elif value.isa[Int64]():
                fill_val = String(Int(value[Int64]))
            elif value.isa[Float64]():
                fill_val = String(value[Float64])
            else:
                fill_val = String("True") if value[Bool] else String("False")
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
            raise Error("fillna: PythonObject columns are not supported")

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

    fn _sort_perm(self, ascending: Bool) raises -> List[Int]:
        """Return an insertion-sort permutation over the column values.

        perm[i] = original index of the i-th element in sorted order.
        Null elements are placed at the end regardless of direction.
        """
        var n = len(self._col)
        var perm = List[Int]()
        for i in range(n):
            perm.append(i)
        if n <= 1:
            return perm^
        var has_mask = len(self._col._null_mask) > 0
        if self._col._data.isa[List[Int64]]():
            ref d = self._col._data[List[Int64]]
            for i in range(1, n):
                var key = perm[i]
                var j = i - 1
                while j >= 0:
                    var prev = perm[j]
                    var key_null = has_mask and self._col._null_mask[key]
                    var prev_null = has_mask and self._col._null_mask[prev]
                    var do_swap: Bool
                    if key_null:
                        do_swap = False
                    elif prev_null:
                        do_swap = True
                    elif ascending:
                        do_swap = d[key] < d[prev]
                    else:
                        do_swap = d[key] > d[prev]
                    if not do_swap:
                        break
                    perm[j + 1] = prev
                    j -= 1
                perm[j + 1] = key
        elif self._col._data.isa[List[Float64]]():
            ref d = self._col._data[List[Float64]]
            for i in range(1, n):
                var key = perm[i]
                var j = i - 1
                while j >= 0:
                    var prev = perm[j]
                    var key_null = has_mask and self._col._null_mask[key]
                    var prev_null = has_mask and self._col._null_mask[prev]
                    var do_swap: Bool
                    if key_null:
                        do_swap = False
                    elif prev_null:
                        do_swap = True
                    elif ascending:
                        do_swap = d[key] < d[prev]
                    else:
                        do_swap = d[key] > d[prev]
                    if not do_swap:
                        break
                    perm[j + 1] = prev
                    j -= 1
                perm[j + 1] = key
        elif self._col._data.isa[List[Bool]]():
            ref d = self._col._data[List[Bool]]
            for i in range(1, n):
                var key = perm[i]
                var j = i - 1
                while j >= 0:
                    var prev = perm[j]
                    var key_null = has_mask and self._col._null_mask[key]
                    var prev_null = has_mask and self._col._null_mask[prev]
                    var do_swap: Bool
                    if key_null:
                        do_swap = False
                    elif prev_null:
                        do_swap = True
                    elif ascending:
                        do_swap = (not d[key]) and d[prev]  # False < True
                    else:
                        do_swap = d[key] and (not d[prev])  # True > False
                    if not do_swap:
                        break
                    perm[j + 1] = prev
                    j -= 1
                perm[j + 1] = key
        elif self._col._data.isa[List[String]]():
            ref d = self._col._data[List[String]]
            for i in range(1, n):
                var key = perm[i]
                var j = i - 1
                while j >= 0:
                    var prev = perm[j]
                    var key_null = has_mask and self._col._null_mask[key]
                    var prev_null = has_mask and self._col._null_mask[prev]
                    var do_swap: Bool
                    if key_null:
                        do_swap = False
                    elif prev_null:
                        do_swap = True
                    elif ascending:
                        do_swap = d[key] < d[prev]
                    else:
                        do_swap = d[key] > d[prev]
                    if not do_swap:
                        break
                    perm[j + 1] = prev
                    j -= 1
                perm[j + 1] = key
        else:
            ref d = self._col._data[List[PythonObject]]
            for i in range(1, n):
                var key = perm[i]
                var j = i - 1
                while j >= 0:
                    var prev = perm[j]
                    var key_null = has_mask and self._col._null_mask[key]
                    var prev_null = has_mask and self._col._null_mask[prev]
                    var do_swap: Bool
                    if key_null:
                        do_swap = False
                    elif prev_null:
                        do_swap = True
                    elif ascending:
                        do_swap = Bool(d[key] < d[prev])
                    else:
                        do_swap = Bool(d[key] > d[prev])
                    if not do_swap:
                        break
                    perm[j + 1] = prev
                    j -= 1
                perm[j + 1] = key
        return perm^

    fn sort_values(self, ascending: Bool = True) raises -> Series:
        """Return a new Series sorted by value.

        Null elements are placed at the end regardless of direction.
        The original index labels are reordered to follow their data rows.
        """
        var n = len(self._col)
        if n == 0:
            return Series(self._col.copy())
        var perm = self._sort_perm(ascending)
        var sorted_col = self._col.take(perm)
        if len(self._col._index) > 0:
            var new_idx = List[PythonObject]()
            for k in range(n):
                new_idx.append(self._col._index[perm[k]])
            sorted_col._index = new_idx^
        return Series(sorted_col^)

    fn sort_index(self, ascending: Bool = True) raises -> Series:
        """Return a new Series sorted by index label.

        When the Series has a default RangeIndex the data is already ordered
        for ``ascending=True``; ``ascending=False`` reverses it.
        For explicit index labels, Python comparison is used so any
        comparable index type (int, float, str) works.
        """
        var n = len(self._col)
        if n == 0:
            return Series(self._col.copy())
        if len(self._col._index) == 0:
            # Default RangeIndex [0, 1, ..., n-1].
            if not ascending:
                var rev_perm = List[Int]()
                for i in range(n):
                    rev_perm.append(n - 1 - i)
                var sorted_col = self._col.take(rev_perm)
                var builtins = Python.import_module("builtins")
                var new_idx = List[PythonObject]()
                for k in range(n):
                    new_idx.append(builtins.int(n - 1 - k))
                sorted_col._index = new_idx^
                return Series(sorted_col^)
            return Series(self._col.copy())
        # Sort permutation by index labels via Python comparison.
        var perm = List[Int]()
        for i in range(n):
            perm.append(i)
        for i in range(1, n):
            var key = perm[i]
            var j = i - 1
            while j >= 0:
                var prev = perm[j]
                var do_swap: Bool
                if ascending:
                    do_swap = Bool(self._col._index[key] < self._col._index[prev])
                else:
                    do_swap = Bool(self._col._index[key] > self._col._index[prev])
                if not do_swap:
                    break
                perm[j + 1] = prev
                j -= 1
            perm[j + 1] = key
        var sorted_col = self._col.take(perm)
        var new_idx = List[PythonObject]()
        for k in range(n):
            new_idx.append(self._col._index[perm[k]])
        sorted_col._index = new_idx^
        return Series(sorted_col^)

    fn argsort(self) raises -> Series:
        """Return the integer indices that would sort the Series values.

        The result is a Series with the same index as the input.
        For non-null elements, values are the 0-based positions in the
        original Series that would produce a sorted sequence.
        Null positions in the sort permutation produce NaN (Float64 dtype);
        otherwise the result dtype is Int64.
        """
        var n = len(self._col)
        if n == 0:
            return Series(self._col.copy())
        var perm = self._sort_perm(True)
        var has_mask = len(self._col._null_mask) > 0
        # Determine whether any element is actually null; only then use Float64.
        var has_any_null = False
        if has_mask:
            for i in range(n):
                if self._col._null_mask[i]:
                    has_any_null = True
                    break
        var idx = self._col._index.copy()
        if not has_any_null:
            var result_data = List[Int64]()
            for i in range(n):
                result_data.append(Int64(perm[i]))
            var col = Column(self._col.name, ColumnData(result_data^), int64, idx^)
            return Series(col^)
        # Build Float64 result: NaN wherever the sort permutation points to a
        # null element in the original (those positions are at the tail of perm).
        var result_data = List[Float64]()
        var result_mask = List[Bool]()
        for i in range(n):
            var orig_pos = perm[i]
            if self._col._null_mask[orig_pos]:
                result_data.append(Float64(0))
                result_mask.append(True)
            else:
                result_data.append(Float64(perm[i]))
                result_mask.append(False)
        var col = Column(self._col.name, ColumnData(result_data^), float64, idx^)
        col._null_mask = result_mask^
        return Series(col^)

    fn rank(self) raises -> Series:
        """Return 1-based float ranks (average method for ties, NaN for nulls)."""
        var n = len(self._col)
        if n == 0:
            return Series(self._col.copy())
        var perm = self._sort_perm(True)
        var has_mask = len(self._col._null_mask) > 0
        # Count non-null elements (null elements sit at the tail of perm).
        var n_non_null = n
        if has_mask:
            n_non_null = 0
            for i in range(n):
                if not self._col._null_mask[i]:
                    n_non_null += 1
        # Prepare Float64 result; null slots are marked in rank_mask.
        var ranks = List[Float64]()
        var rank_mask = List[Bool]()
        for i in range(n):
            ranks.append(Float64(0))
            rank_mask.append(False)
        if has_mask:
            for i in range(n_non_null, n):
                rank_mask[perm[i]] = True
        # Assign average ranks for each tied group (type-dispatch).
        # avg = ((i+1) + (j+1)) / 2 where i..j is the 0-based sorted range.
        if self._col._data.isa[List[Int64]]():
            ref d = self._col._data[List[Int64]]
            var i = 0
            while i < n_non_null:
                var j = i
                while j < n_non_null - 1 and d[perm[j]] == d[perm[j + 1]]:
                    j += 1
                var avg_rank = Float64(i + j + 2) / Float64(2)
                for k in range(i, j + 1):
                    ranks[perm[k]] = avg_rank
                i = j + 1
        elif self._col._data.isa[List[Float64]]():
            ref d = self._col._data[List[Float64]]
            var i = 0
            while i < n_non_null:
                var j = i
                while j < n_non_null - 1 and d[perm[j]] == d[perm[j + 1]]:
                    j += 1
                var avg_rank = Float64(i + j + 2) / Float64(2)
                for k in range(i, j + 1):
                    ranks[perm[k]] = avg_rank
                i = j + 1
        elif self._col._data.isa[List[Bool]]():
            ref d = self._col._data[List[Bool]]
            var i = 0
            while i < n_non_null:
                var j = i
                while j < n_non_null - 1 and d[perm[j]] == d[perm[j + 1]]:
                    j += 1
                var avg_rank = Float64(i + j + 2) / Float64(2)
                for k in range(i, j + 1):
                    ranks[perm[k]] = avg_rank
                i = j + 1
        elif self._col._data.isa[List[String]]():
            ref d = self._col._data[List[String]]
            var i = 0
            while i < n_non_null:
                var j = i
                while j < n_non_null - 1 and d[perm[j]] == d[perm[j + 1]]:
                    j += 1
                var avg_rank = Float64(i + j + 2) / Float64(2)
                for k in range(i, j + 1):
                    ranks[perm[k]] = avg_rank
                i = j + 1
        else:
            ref d = self._col._data[List[PythonObject]]
            var i = 0
            while i < n_non_null:
                var j = i
                while j < n_non_null - 1 and Bool(d[perm[j]] == d[perm[j + 1]]):
                    j += 1
                var avg_rank = Float64(i + j + 2) / Float64(2)
                for k in range(i, j + 1):
                    ranks[perm[k]] = avg_rank
                i = j + 1
        var idx = self._col._index.copy()
        var col = Column(self._col.name, ColumnData(ranks^), float64, idx^)
        # n_non_null < n iff there are nulls — no need to re-scan the mask.
        if n_non_null < n:
            col._null_mask = rank_mask^
        return Series(col^)

    # ------------------------------------------------------------------
    # Reshaping / transformations
    # ------------------------------------------------------------------

    fn apply[F: FloatTransformFn](self) raises -> Series:
        """Apply a compile-time function element-wise. Call as ``s.apply[my_fn]()``."""
        return Series(self._col._apply[F]())

    fn map[F: FloatTransformFn](self) raises -> Series:
        """Map a compile-time function element-wise. Call as ``s.map[my_fn]()``."""
        return Series(self._col._apply[F]())

    fn astype(self, dtype: String) raises -> Series:
        return Series(self._col._astype(dtype_from_string(dtype)))

    fn copy(self) -> Series:
        return Series(self._col.copy())

    fn reset_index(self, drop: Bool = False) raises -> Series:
        return Series(self._col._reset_index(drop))

    fn rename(self, new_name: String) raises -> Series:
        var c = self._col.copy()
        c.name = new_name
        return Series(c^)

    fn clip(self, lower: Float64, upper: Float64) raises -> Series:
        return Series(self._col._clip(lower, upper))

    fn abs(self) raises -> Series:
        return Series(self._col._abs())

    fn round(self, decimals: Int = 0) raises -> Series:
        return Series(self._col._round(decimals))

    fn unique(self) raises -> Series:
        return Series(self._col._unique())

    fn isin(self, values: List[Int64]) raises -> Series:
        return Series(self._col._isin_int(values))

    fn isin(self, values: List[Float64]) raises -> Series:
        return Series(self._col._isin_float(values))

    fn isin(self, values: List[String]) raises -> Series:
        return Series(self._col._isin_str(values))

    fn isin(self, values: List[Bool]) raises -> Series:
        return Series(self._col._isin_bool(values))

    fn between(self, left: Float64, right: Float64) raises -> Series:
        return Series(self._col._between(left, right))

    fn where(self, cond: Series) raises -> Series:
        return Series(self._col._where(cond._col))

    fn mask(self, cond: Series) raises -> Series:
        return Series(self._col._mask(cond._col))

    # ------------------------------------------------------------------
    # Interop
    # ------------------------------------------------------------------

    fn to_list(self) raises -> List[DFScalar]:
        _not_implemented("Series.to_list")
        return List[DFScalar]()

    fn to_numpy(self) raises -> List[Float64]:
        _not_implemented("Series.to_numpy")
        return List[Float64]()

    fn to_frame(self, name: String = "") raises -> Series:
        _not_implemented("Series.to_frame")
        return Series()

    fn to_dict(self) raises -> Dict[String, DFScalar]:
        _not_implemented("Series.to_dict")
        return Dict[String, DFScalar]()

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
        if not self._col._data.isa[List[String]]():
            raise Error("Series.str: accessor requires a string Series")
        ref d = self._col._data[List[String]]
        var data = d.copy()
        var null_mask = self._col._null_mask.copy()
        return StringMethods(data^, null_mask^, self._col.name)

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
