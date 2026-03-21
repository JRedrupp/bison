from std.python import Python, PythonObject
from std.collections import Optional, Dict
from ._errors import _not_implemented
from .dtypes import BisonDtype, object_, bool_, int64, float64, dtype_from_string
from .column import Column, ColumnData, DFScalar, SeriesScalar, FloatTransformFn, _csv_quote_field, _col_cell_str, _col_cell_pyobj
from .accessors.str_accessor import StringMethods
from .accessors.dt_accessor import DatetimeMethods

struct Series(Copyable, Movable):
    """A one-dimensional labeled array, mirroring the pandas Series API."""

    var _col: Column
    var name: String

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(out self):
        """Empty Series — used as stub return placeholder."""
        self._col = Column()
        self.name = ""

    def __init__(out self, var col: Column):
        self.name = col.name
        self._col = col^

    def __init__(out self, pd_s: PythonObject, name: String = "") raises:
        """Convenience constructor: wraps a pandas Series."""
        var col_name: String
        if name != "":
            col_name = name
        else:
            col_name = String(pd_s.name)
        self._col = Column.from_pandas(pd_s, col_name)
        self.name = self._col.name

    def __init__(out self, *, copy: Self):
        self._col = copy._col.copy()
        self.name = copy.name

    def __init__(out self, *, deinit take: Self):
        self._col = take._col^
        self.name = take.name^

    @staticmethod
    def from_pandas(pd_s: PythonObject) raises -> Series:
        var col_name = String(pd_s.name)
        return Series(Column.from_pandas(pd_s, col_name))

    def to_pandas(self) raises -> PythonObject:
        return self._col.to_pandas()

    # ------------------------------------------------------------------
    # Attributes
    # ------------------------------------------------------------------

    def shape(self) -> Tuple[Int]:
        return (self._col.__len__(),)

    def size(self) -> Int:
        return self._col.__len__()

    def empty(self) -> Bool:
        return self._col.__len__() == 0

    def dtype(self) raises -> BisonDtype:
        _not_implemented("Series.dtype")
        return object_

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------

    def head(self, n: Int = 5) raises -> Series:
        var size = self._col.__len__()
        var end = n
        if end > size:
            end = size
        if end < 0:
            end = 0
        return Series(self._col.slice(0, end))

    def tail(self, n: Int = 5) raises -> Series:
        var size = self._col.__len__()
        var start = size - n
        if start < 0:
            start = 0
        return Series(self._col.slice(start, size))

    def iloc(self, i: Int) raises -> SeriesScalar:
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

    def at(self, label: String) raises -> SeriesScalar:
        for i in range(len(self._col._index)):
            if String(self._col._index[i]) == label:
                return self.iloc(i)
        raise Error("Series.at: label '" + label + "' not found in index")

    # ------------------------------------------------------------------
    # Arithmetic
    # ------------------------------------------------------------------

    def add(self, other: Series) raises -> Series:
        return Series(self._col._arith_add(other._col))

    def sub(self, other: Series) raises -> Series:
        return Series(self._col._arith_sub(other._col))

    def mul(self, other: Series) raises -> Series:
        return Series(self._col._arith_mul(other._col))

    def div(self, other: Series) raises -> Series:
        return Series(self._col._arith_div(other._col))

    def floordiv(self, other: Series) raises -> Series:
        return Series(self._col._arith_floordiv(other._col))

    def mod(self, other: Series) raises -> Series:
        return Series(self._col._arith_mod(other._col))

    def pow(self, other: Series) raises -> Series:
        return Series(self._col._arith_pow(other._col))

    def radd(self, other: Series) raises -> Series:
        return other.add(self)

    def rsub(self, other: Series) raises -> Series:
        return other.sub(self)

    def rmul(self, other: Series) raises -> Series:
        return other.mul(self)

    def rdiv(self, other: Series) raises -> Series:
        return other.div(self)

    def rfloordiv(self, other: Series) raises -> Series:
        return other.floordiv(self)

    def rmod(self, other: Series) raises -> Series:
        return other.mod(self)

    def rpow(self, other: Series) raises -> Series:
        return other.pow(self)

    # ------------------------------------------------------------------
    # Comparison
    # ------------------------------------------------------------------

    def eq(self, other: Series) raises -> Series:
        return Series(self._col._cmp_eq(other._col))

    def ne(self, other: Series) raises -> Series:
        return Series(self._col._cmp_ne(other._col))

    def lt(self, other: Series) raises -> Series:
        return Series(self._col._cmp_lt(other._col))

    def le(self, other: Series) raises -> Series:
        return Series(self._col._cmp_le(other._col))

    def gt(self, other: Series) raises -> Series:
        return Series(self._col._cmp_gt(other._col))

    def ge(self, other: Series) raises -> Series:
        return Series(self._col._cmp_ge(other._col))

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def sum(self, skipna: Bool = True) raises -> Float64:
        return self._col.sum(skipna)

    def mean(self, skipna: Bool = True) raises -> Float64:
        return self._col.mean(skipna)

    def median(self, skipna: Bool = True) raises -> Float64:
        return self._col.median(skipna)

    def min(self, skipna: Bool = True) raises -> Float64:
        return self._col.min(skipna)

    def max(self, skipna: Bool = True) raises -> Float64:
        return self._col.max(skipna)

    def std(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        return self._col.std(ddof, skipna)

    def var(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        return self._col.var(ddof, skipna)

    def count(self) -> Int:
        return self._col.count()

    def nunique(self) raises -> Int:
        return self._col.nunique()

    def describe(self) raises -> Series:
        """Return summary statistics as a Series (count, mean, std, min, quartiles, max)."""
        return Series(self._col.describe())

    def value_counts(self, normalize: Bool = False, sort: Bool = True) raises -> Series:
        """Return a Series with the count (or proportion) of each unique value."""
        return Series(self._col.value_counts(normalize, sort))

    def quantile(self, q: Float64 = 0.5) raises -> Float64:
        return self._col.quantile(q)

    def cumsum(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cumsum(skipna))

    def cumprod(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cumprod(skipna))

    def cummin(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cummin(skipna))

    def cummax(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cummax(skipna))

    # ------------------------------------------------------------------
    # Missing data
    # ------------------------------------------------------------------

    def isna(self) raises -> Series:
        """Return a boolean Series that is True where values are null/NaN."""
        var n = len(self._col)
        var has_mask = len(self._col._null_mask) > 0
        var result = List[Bool]()
        for i in range(n):
            result.append(has_mask and self._col._null_mask[i])
        var col_data = ColumnData(result^)
        var col = Column(self._col.name, col_data^, bool_)
        return Series(col^)

    def isnull(self) raises -> Series:
        """Alias for isna()."""
        return self.isna()

    def notna(self) raises -> Series:
        """Return a boolean Series that is True where values are not null/NaN."""
        var n = len(self._col)
        var has_mask = len(self._col._null_mask) > 0
        var result = List[Bool]()
        for i in range(n):
            result.append(not (has_mask and self._col._null_mask[i]))
        var col_data = ColumnData(result^)
        var col = Column(self._col.name, col_data^, bool_)
        return Series(col^)

    def notnull(self) raises -> Series:
        """Alias for notna()."""
        return self.notna()

    def fillna(self, value: DFScalar) raises -> Series:
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

    def dropna(self) raises -> Series:
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

    def ffill(self) raises -> Series:
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

    def bfill(self) raises -> Series:
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

    def _sort_perm(self, ascending: Bool, na_last: Bool = True) raises -> List[Int]:
        """Return an insertion-sort permutation over the column values.

        perm[i] = original index of the i-th element in sorted order.
        When na_last is True (default), null elements are placed at the end.
        When na_last is False, null elements are placed at the beginning.
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
                    if key_null and prev_null:
                        do_swap = False
                    elif key_null:
                        do_swap = not na_last
                    elif prev_null:
                        do_swap = na_last
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
                    if key_null and prev_null:
                        do_swap = False
                    elif key_null:
                        do_swap = not na_last
                    elif prev_null:
                        do_swap = na_last
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
                    if key_null and prev_null:
                        do_swap = False
                    elif key_null:
                        do_swap = not na_last
                    elif prev_null:
                        do_swap = na_last
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
                    if key_null and prev_null:
                        do_swap = False
                    elif key_null:
                        do_swap = not na_last
                    elif prev_null:
                        do_swap = na_last
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
                    if key_null and prev_null:
                        do_swap = False
                    elif key_null:
                        do_swap = not na_last
                    elif prev_null:
                        do_swap = na_last
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

    def sort_values(self, ascending: Bool = True, na_position: String = "last") raises -> Series:
        """Return a new Series sorted by value.

        Null elements are placed at the end when na_position is ``"last"``
        (default) or at the beginning when na_position is ``"first"``.
        The original index labels are reordered to follow their data rows.
        """
        var n = len(self._col)
        if n == 0:
            return Series(self._col.copy())
        var perm = self._sort_perm(ascending, na_position == "last")
        var sorted_col = self._col.take(perm)
        if len(self._col._index) > 0:
            var new_idx = List[PythonObject]()
            for k in range(n):
                new_idx.append(self._col._index[perm[k]])
            sorted_col._index = new_idx^
        return Series(sorted_col^)

    def sort_index(self, ascending: Bool = True) raises -> Series:
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

    def argsort(self) raises -> Series:
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

    def rank(self) raises -> Series:
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

    def apply[F: FloatTransformFn](self) raises -> Series:
        """Apply a compile-time function element-wise. Call as ``s.apply[my_fn]()``."""
        return Series(self._col._apply[F]())

    def map[F: FloatTransformFn](self) raises -> Series:
        """Map a compile-time function element-wise. Call as ``s.map[my_fn]()``."""
        return Series(self._col._apply[F]())

    def astype(self, dtype: String) raises -> Series:
        return Series(self._col._astype(dtype_from_string(dtype)))

    def copy(self) -> Series:
        return Series(self._col.copy())

    def reset_index(self, drop: Bool = False) raises -> Series:
        return Series(self._col._reset_index(drop))

    def rename(self, new_name: String) raises -> Series:
        var c = self._col.copy()
        c.name = new_name
        return Series(c^)

    def clip(self, lower: Optional[Float64] = None, upper: Optional[Float64] = None) raises -> Series:
        return Series(self._col._clip(lower, upper))

    def abs(self) raises -> Series:
        return Series(self._col._abs())

    def round(self, decimals: Int = 0) raises -> Series:
        return Series(self._col._round(decimals))

    def unique(self) raises -> Series:
        return Series(self._col._unique())

    def isin(self, values: List[Int64]) raises -> Series:
        return Series(self._col._isin_int(values))

    def isin(self, values: List[Float64]) raises -> Series:
        return Series(self._col._isin_float(values))

    def isin(self, values: List[String]) raises -> Series:
        return Series(self._col._isin_str(values))

    def isin(self, values: List[Bool]) raises -> Series:
        return Series(self._col._isin_bool(values))

    def between(self, left: Float64, right: Float64) raises -> Series:
        return Series(self._col._between(left, right))

    def where(self, cond: Series, other: Optional[DFScalar] = None) raises -> Series:
        return Series(self._col._where(cond._col, other))

    def mask(self, cond: Series, other: Optional[DFScalar] = None) raises -> Series:
        return Series(self._col._mask(cond._col, other))

    # ------------------------------------------------------------------
    # Interop
    # ------------------------------------------------------------------

    def to_list(self) raises -> List[DFScalar]:
        """Return the Series values as a ``List[DFScalar]``.

        Null values are represented as the zero-like default for the dtype
        (``0`` for integers, ``NaN`` for floats, ``False`` for bools,
        ``""`` for strings).  Object-dtype columns raise ``Error``.
        """
        var result = List[DFScalar]()
        ref col = self._col
        var has_mask = len(col._null_mask) > 0
        var n = col.__len__()
        if col._data.isa[List[Int64]]():
            ref data = col._data[List[Int64]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(DFScalar(Int64(0)))
                else:
                    result.append(DFScalar(data[i]))
        elif col._data.isa[List[Float64]]():
            var nan = Float64(0) / Float64(0)
            ref data = col._data[List[Float64]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(DFScalar(nan))
                else:
                    result.append(DFScalar(data[i]))
        elif col._data.isa[List[Bool]]():
            ref data = col._data[List[Bool]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(DFScalar(False))
                else:
                    result.append(DFScalar(data[i]))
        elif col._data.isa[List[String]]():
            ref data = col._data[List[String]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(DFScalar(String("")))
                else:
                    result.append(DFScalar(data[i]))
        else:
            raise Error("Series.to_list: object dtype is not supported")
        return result^

    def to_numpy(self) raises -> List[Float64]:
        """Return the Series values as a ``List[Float64]``.

        Integer and bool values are cast to ``Float64``.  Null values become
        ``NaN``.  String and object-dtype columns raise ``Error``.
        """
        var result = List[Float64]()
        ref col = self._col
        var has_mask = len(col._null_mask) > 0
        var nan = Float64(0) / Float64(0)
        var n = col.__len__()
        if col._data.isa[List[Int64]]():
            ref data = col._data[List[Int64]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(nan)
                else:
                    result.append(Float64(data[i]))
        elif col._data.isa[List[Float64]]():
            ref data = col._data[List[Float64]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(nan)
                else:
                    result.append(data[i])
        elif col._data.isa[List[Bool]]():
            ref data = col._data[List[Bool]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(nan)
                else:
                    result.append(Float64(1.0) if data[i] else Float64(0.0))
        else:
            raise Error("Series.to_numpy: non-numeric dtype is not supported")
        return result^

    def to_frame(self, name: String = "") raises -> DataFrame:
        """Convert the Series to a single-column DataFrame.

        Parameters
        ----------
        name : Column name in the resulting DataFrame.  When empty the
               Series' own name is used.

        Returns
        -------
        DataFrame
            A native bison ``DataFrame`` with one column.
        """
        var col_name = name if name != "" else self.name
        var col = Column(copy=self._col)
        col.name = col_name
        var cols = List[Column]()
        cols.append(col^)
        return DataFrame(cols^)

    def to_dict(self) raises -> Dict[String, DFScalar]:
        """Return the Series as a ``Dict`` mapping index label → value.

        Index labels are stringified.  Null values follow the same zero-like
        defaults as ``to_list``.  Object-dtype columns raise ``Error``.
        """
        var result = Dict[String, DFScalar]()
        ref col = self._col
        var has_mask = len(col._null_mask) > 0
        var has_index = len(col._index) > 0
        var n = col.__len__()
        if col._data.isa[List[Int64]]():
            ref data = col._data[List[Int64]]
            for i in range(n):
                var key = String(col._index[i]) if has_index else String(i)
                if has_mask and col._null_mask[i]:
                    result[key] = DFScalar(Int64(0))
                else:
                    result[key] = DFScalar(data[i])
        elif col._data.isa[List[Float64]]():
            var nan = Float64(0) / Float64(0)
            ref data = col._data[List[Float64]]
            for i in range(n):
                var key = String(col._index[i]) if has_index else String(i)
                if has_mask and col._null_mask[i]:
                    result[key] = DFScalar(nan)
                else:
                    result[key] = DFScalar(data[i])
        elif col._data.isa[List[Bool]]():
            ref data = col._data[List[Bool]]
            for i in range(n):
                var key = String(col._index[i]) if has_index else String(i)
                if has_mask and col._null_mask[i]:
                    result[key] = DFScalar(False)
                else:
                    result[key] = DFScalar(data[i])
        elif col._data.isa[List[String]]():
            ref data = col._data[List[String]]
            for i in range(n):
                var key = String(col._index[i]) if has_index else String(i)
                if has_mask and col._null_mask[i]:
                    result[key] = DFScalar(String(""))
                else:
                    result[key] = DFScalar(data[i])
        else:
            raise Error("Series.to_dict: object dtype is not supported")
        return result^

    def to_csv(self, path: String = "") raises -> String:
        """Serialize the Series to a CSV-formatted string or file.

        Each row is written as ``<index>,<value>`` with no header line,
        matching pandas' default ``Series.to_csv`` output.

        Parameters
        ----------
        path : File path to write.  When empty (default) the CSV text is
               returned as a ``String``.
        """
        var result = String()
        ref col = self._col
        var has_index = len(col._index) > 0
        var n = col.__len__()
        for i in range(n):
            var key = String(col._index[i]) if has_index else String(i)
            var val = _col_cell_str(col, i)
            result += _csv_quote_field(key, ",") + "," + _csv_quote_field(val, ",") + "\n"
        if len(path) > 0:
            with open(path, "w") as f:
                f.write(result)
            return String("")
        return result^

    def to_json(self, path: String = "") raises -> String:
        """Serialize the Series to a JSON-formatted string or file.

        The output is a JSON object mapping each index label (as a string)
        to its value, e.g. ``{"0":1,"1":2,"2":3}``.

        Parameters
        ----------
        path : File path to write.  When empty (default) the JSON text is
               returned as a ``String``.
        """
        var json_mod = Python.import_module("json")
        ref col = self._col
        var has_index = len(col._index) > 0
        var n = col.__len__()
        var py_dict = Python.evaluate("{}")
        for i in range(n):
            var key = String(col._index[i]) if has_index else String(i)
            py_dict[key] = _col_cell_pyobj(col, i)
        var result = String(json_mod.dumps(py_dict))
        if len(path) > 0:
            with open(path, "w") as f:
                f.write(result)
            return String("")
        return result^

    # ------------------------------------------------------------------
    # String / Datetime accessors (return accessor structs)
    # ------------------------------------------------------------------

    def str(self) raises -> StringMethods:
        if not self._col._data.isa[List[String]]():
            raise Error("Series.str: accessor requires a string Series")
        ref d = self._col._data[List[String]]
        var data = d.copy()
        var null_mask = self._col._null_mask.copy()
        return StringMethods(data^, null_mask^, self._col.name)

    def dt(self) raises -> DatetimeMethods:
        _not_implemented("Series.dt")
        return DatetimeMethods()

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) raises -> String:
        return String(self.to_pandas())

    def __len__(self) -> Int:
        return self._col.__len__()



# ------------------------------------------------------------------
# Shared string-list utilities
# ------------------------------------------------------------------

def _sort_col_names(names: List[String]) -> List[String]:
    """Return a copy of *names* sorted in ascending order (selection sort)."""
    var n = len(names)
    var order = List[Int]()
    for i in range(n):
        order.append(i)
    for i in range(n):
        var min_idx = i
        for j in range(i + 1, n):
            if names[order[j]] < names[order[min_idx]]:
                min_idx = j
        if min_idx != i:
            var tmp = order[i]
            order[i] = order[min_idx]
            order[min_idx] = tmp
    var result = List[String]()
    for i in range(n):
        result.append(names[order[i]])
    return result^


struct DataFrame(Copyable, Movable):
    """A two-dimensional labeled data structure, mirroring the pandas DataFrame API."""

    var _cols: List[Column]

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(out self):
        """Empty DataFrame — used as stub return placeholder."""
        self._cols = List[Column]()

    def __init__(out self, var cols: List[Column]):
        self._cols = cols^

    def __init__(out self, pd_df: PythonObject) raises:
        """Convenience constructor: wraps a pandas DataFrame via Column.from_pandas."""
        var pd_cols = pd_df.columns.tolist()
        var n = Int(pd_df.columns.__len__())
        self._cols = List[Column]()
        for i in range(n):
            var col_name = String(pd_cols[i])
            self._cols.append(Column.from_pandas(pd_df[pd_cols[i]], col_name))

    def __init__(out self, *, copy: Self):
        self._cols = copy._cols.copy()

    def __init__(out self, *, deinit take: Self):
        self._cols = take._cols^

    @staticmethod
    def from_pandas(pd_df: PythonObject) raises -> DataFrame:
        return DataFrame(pd_df)

    def to_pandas(self) raises -> PythonObject:
        var pd = Python.import_module("pandas")
        var dict_ = Python.evaluate("{}")
        for i in range(self._cols.__len__()):
            var pd_series = self._cols[i].to_pandas()
            dict_[self._cols[i].name] = pd_series
        return pd.DataFrame(dict_)

    @staticmethod
    def from_dict(data: Dict[String, ColumnData]) raises -> DataFrame:
        """Create DataFrame from a native dict mapping column names to column data."""
        var cols = List[Column]()
        for entry in data.items():
            var col_data = entry.value
            var dtype = Column._sniff_dtype(col_data)
            cols.append(Column(entry.key, col_data^, dtype))
        return DataFrame(cols^)

    @staticmethod
    def from_records(
        records: List[Dict[String, DFScalar]],
        columns: Optional[List[String]] = None,
    ) raises -> DataFrame:
        """Create DataFrame from a list of row dicts (row-to-column transposition)."""
        if len(records) == 0:
            return DataFrame()

        # Determine column names
        var col_names = List[String]()
        if columns:
            col_names = columns.value().copy()
        else:
            # Collect keys from the first record, then sort them alphabetically.
            # Mojo's Dict does not guarantee insertion-order iteration, so sorting
            # ensures the resulting column order is deterministic regardless of how
            # the caller constructed each row dict.
            var unsorted = List[String]()
            for entry in records[0].items():
                unsorted.append(entry.key)
            col_names = _sort_col_names(unsorted)

        var cols = List[Column]()

        for ci in range(len(col_names)):
            var col_name = col_names[ci]
            var null_mask = List[Bool]()

            # Scan ALL rows to determine dominant column dtype.
            # Promotion order matches pandas/NumPy type coercion rules:
            # String > Float64 > Int64 > Bool.
            # This prevents a panic when the same column has mixed DFScalar arms
            # across rows (e.g. first row Int64, second row Float64).
            var has_int = False
            var has_float = False
            var has_bool = False
            var has_string = False
            var found = False
            for ri in range(len(records)):
                try:
                    var v = records[ri][col_name]
                    if v.isa[Int64]():
                        has_int = True
                    elif v.isa[Float64]():
                        has_float = True
                    elif v.isa[Bool]():
                        has_bool = True
                    else:  # String
                        has_string = True
                    found = True
                except:
                    pass

            if not found:
                # All rows missing this key — object column, all null
                var data = List[PythonObject]()
                var py_none = Python.evaluate("None")
                for _ in range(len(records)):
                    data.append(py_none)
                    null_mask.append(True)
                var col = Column(col_name, ColumnData(data^), object_)
                col._null_mask = null_mask^
                cols.append(col^)
                continue

            if has_string:
                # String dominates: convert all non-null values to String
                var data = List[String]()
                for ri in range(len(records)):
                    try:
                        var v = records[ri][col_name]
                        var val: String
                        if v.isa[String]():
                            val = v[String]
                        elif v.isa[Int64]():
                            val = String(Int(v[Int64]))
                        elif v.isa[Float64]():
                            val = String(v[Float64])
                        else:  # Bool
                            val = String("True") if v[Bool] else String("False")
                        data.append(val)
                        null_mask.append(False)
                    except:
                        data.append(String(""))
                        null_mask.append(True)
                var col = Column(col_name, ColumnData(data^), object_)
                col._null_mask = null_mask^
                cols.append(col^)
            elif has_float:
                # Float64 dominates Int64 and Bool
                var data = List[Float64]()
                for ri in range(len(records)):
                    try:
                        var v = records[ri][col_name]
                        var val: Float64
                        if v.isa[Float64]():
                            val = v[Float64]
                        elif v.isa[Int64]():
                            val = Float64(v[Int64])
                        else:  # Bool
                            val = Float64(1) if v[Bool] else Float64(0)
                        data.append(val)
                        null_mask.append(False)
                    except:
                        data.append(Float64(0.0))
                        null_mask.append(True)
                var col = Column(col_name, ColumnData(data^), float64)
                col._null_mask = null_mask^
                cols.append(col^)
            elif has_int:
                # Int64 (Bool values are promoted to Int64)
                var data = List[Int64]()
                var has_nulls = False
                for ri in range(len(records)):
                    try:
                        var v = records[ri][col_name]
                        var val: Int64
                        if v.isa[Int64]():
                            val = v[Int64]
                        else:  # Bool
                            val = Int64(1) if v[Bool] else Int64(0)
                        data.append(val)
                        null_mask.append(False)
                    except:
                        data.append(Int64(0))
                        null_mask.append(True)
                        has_nulls = True
                if has_nulls:
                    # Promote to float64 so NaN can be represented (mirrors pandas behavior)
                    var fdata = List[Float64]()
                    for i in range(len(data)):
                        fdata.append(Float64(data[i]))
                    var col = Column(col_name, ColumnData(fdata^), float64)
                    col._null_mask = null_mask^
                    cols.append(col^)
                    continue
                var col = Column(col_name, ColumnData(data^), int64)
                col._null_mask = null_mask^
                cols.append(col^)
            else:  # Bool only
                var data = List[Bool]()
                var has_nulls = False
                for ri in range(len(records)):
                    try:
                        var v = records[ri][col_name]
                        data.append(v[Bool])
                        null_mask.append(False)
                    except:
                        data.append(False)
                        null_mask.append(True)
                        has_nulls = True
                if has_nulls:
                    # Promote to object so None can be represented (mirrors pandas behavior)
                    var py_none = Python.evaluate("None")
                    var odata = List[PythonObject]()
                    for i in range(len(data)):
                        if null_mask[i]:
                            odata.append(py_none)
                        else:
                            odata.append(PythonObject(data[i]))
                    var col = Column(col_name, ColumnData(odata^), object_)
                    col._null_mask = null_mask^
                    cols.append(col^)
                    continue
                var col = Column(col_name, ColumnData(data^), bool_)
                col._null_mask = null_mask^
                cols.append(col^)

        return DataFrame(cols^)

    # ------------------------------------------------------------------
    # Attributes
    # ------------------------------------------------------------------

    def shape(self) -> Tuple[Int, Int]:
        var ncols = self._cols.__len__()
        if ncols == 0:
            return (0, 0)
        return (self._cols[0].__len__(), ncols)

    def size(self) -> Int:
        var s = self.shape()
        return s[0] * s[1]

    def empty(self) -> Bool:
        if self._cols.__len__() == 0:
            return True
        return self._cols[0].__len__() == 0

    def columns(self) -> List[String]:
        var result = List[String]()
        for i in range(self._cols.__len__()):
            result.append(self._cols[i].name)
        return result^

    def ndim(self) -> Int:
        return 2

    def dtypes(self) raises -> Series:
        """Return a Series with the dtype of each column, indexed by column name."""
        var n = len(self._cols)
        var dtype_names = List[String]()
        var idx = List[PythonObject]()
        for i in range(n):
            dtype_names.append(self._cols[i].dtype.name)
            idx.append(PythonObject(self._cols[i].name))
        var col_data = ColumnData(dtype_names^)
        var result_col = Column("", col_data^, object_, idx^)
        return Series(result_col^)

    def info(self) raises:
        """Print a concise summary of the DataFrame to stdout."""
        var s = self.shape()
        var nrows = s[0]
        var ncols = s[1]
        print("<class 'bison.DataFrame'>")
        if nrows > 0:
            print(
                "RangeIndex: "
                + String(nrows)
                + " entries, 0 to "
                + String(nrows - 1)
            )
        else:
            print("RangeIndex: 0 entries")
        print("Data columns (total " + String(ncols) + " columns):")
        for i in range(ncols):
            var non_null = self._cols[i].count()
            print(
                " "
                + String(i)
                + "   "
                + self._cols[i].name
                + "   "
                + String(non_null)
                + " non-null   "
                + self._cols[i].dtype.name
            )
        var dtype_counts = Dict[String, Int]()
        for i in range(ncols):
            var dn = self._cols[i].dtype.name
            if dn in dtype_counts:
                dtype_counts[dn] = dtype_counts[dn] + 1
            else:
                dtype_counts[dn] = 1
        var dtype_summary = String("")
        var first = True
        for entry in dtype_counts.items():
            if not first:
                dtype_summary += ", "
            dtype_summary += entry.key + "(" + String(entry.value) + ")"
            first = False
        print("dtypes: " + dtype_summary)
        var total_bytes = Int64(0)
        for i in range(ncols):
            total_bytes += Int64(len(self._cols[i]) * self._cols[i].dtype.itemsize)
        print("memory usage: " + String(total_bytes) + " bytes")

    def memory_usage(self, deep: Bool = False) raises -> Series:
        """Return a Series with memory usage in bytes for each column.

        *deep* is accepted for API compatibility but ignored — object columns
        are always estimated at ``itemsize`` bytes per element.
        """
        var n = len(self._cols)
        var values = List[Int64]()
        var idx = List[PythonObject]()
        for i in range(n):
            values.append(Int64(len(self._cols[i]) * self._cols[i].dtype.itemsize))
            idx.append(PythonObject(self._cols[i].name))
        var col_data = ColumnData(values^)
        var result_col = Column("", col_data^, int64, idx^)
        return Series(result_col^)

    # ------------------------------------------------------------------
    # Selection / indexing
    # ------------------------------------------------------------------

    def __getitem__(self, key: String) raises -> Series:
        for i in range(len(self._cols)):
            if self._cols[i].name == key:
                return Series(self._cols[i].copy())
        raise Error("DataFrame.__getitem__: column not found: " + key)

    def __setitem__(mut self, key: String, value: Series) raises:
        var new_col = value._col.copy()
        new_col.name = key
        for i in range(len(self._cols)):
            if self._cols[i].name == key:
                self._cols[i] = new_col^
                return
        self._cols.append(new_col^)

    def get(self, key: String, default: Optional[Series] = None) -> Optional[Series]:
        for i in range(len(self._cols)):
            if self._cols[i].name == key:
                return Series(self._cols[i].copy())
        if default:
            return Optional[Series](default.value().copy())
        return None

    def head(self, n: Int = 5) -> DataFrame:
        var nrows = self.shape()[0]
        var take = n
        if take > nrows:
            take = nrows
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].slice(0, take))
        return DataFrame(result_cols^)

    def tail(self, n: Int = 5) -> DataFrame:
        var nrows = self.shape()[0]
        var take = n
        if take > nrows:
            take = nrows
        var start = nrows - take
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].slice(start, nrows))
        return DataFrame(result_cols^)

    def sample(
        self,
        n: Int = 1,
        frac: Optional[Float64] = None,
        random_state: Optional[Int] = None,
    ) raises -> DataFrame:
        """Return a random sample of rows.

        Uses a Fisher-Yates shuffle driven by a 32-bit xorshift PRNG.
        Pass *random_state* (any non-zero integer) for reproducibility.
        """
        var nrows = self.shape()[0]
        if nrows == 0:
            return DataFrame()
        var take: Int
        if frac:
            var frac_val = frac.value()
            take = Int(Float64(nrows) * frac_val)
        else:
            take = n
        if take > nrows:
            take = nrows
        # Build index list [0, 1, ..., nrows-1]
        var indices = List[Int]()
        for i in range(nrows):
            indices.append(i)
        # Fisher-Yates shuffle — xorshift32 PRNG
        var state: Int = 1
        if random_state:
            state = random_state.value()
        if state == 0:
            state = 1  # xorshift must not start at 0
        for i in range(nrows):
            # xorshift32 step (kept positive via masking)
            state = state ^ (state << 13)
            state = state ^ (state >> 17)
            state = state ^ (state << 5)
            state = state & 0x7FFFFFFF
            var j = i + (state % (nrows - i))
            var tmp = indices[i]
            indices[i] = indices[j]
            indices[j] = tmp
        # Collect the first `take` shuffled indices
        var selected = List[Int]()
        for i in range(take):
            selected.append(indices[i])
        var result_cols = List[Column]()
        for ci in range(len(self._cols)):
            result_cols.append(self._cols[ci].take(selected))
        return DataFrame(result_cols^)

    def filter(
        self,
        items: Optional[List[String]] = None,
        like: String = "",
        regex: String = "",
        axis: Int = 1,
    ) raises -> DataFrame:
        """Select columns by label.

        *items*: keep only columns whose name is in the list.
        *like*:  keep columns whose name contains the substring.
        *regex*: keep columns whose name matches the pattern (uses Python re).
        """
        if axis != 1:
            raise Error("DataFrame.filter: axis=0 not yet implemented")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var col_name = self._cols[i].name
            var keep = False
            if items:
                ref items_list = items.value()
                for j in range(len(items_list)):
                    if col_name == items_list[j]:
                        keep = True
                        break
            elif like != "":
                keep = col_name.find(like) != -1
            elif regex != "":
                # Regex requires Python's re module — there is no native
                # regex engine in Mojo's standard library yet.
                var re_mod = Python.import_module("re")
                keep = Bool(re_mod.search(regex, col_name).__bool__())
            else:
                keep = True
            if keep:
                result_cols.append(self._cols[i].copy())
        return DataFrame(result_cols^)

    def select_dtypes(
        self,
        include: Optional[List[String]] = None,
        exclude: Optional[List[String]] = None,
    ) -> DataFrame:
        """Return a subset of columns matching *include* and not in *exclude*.

        Both parameters accept a list of dtype name strings (e.g. ``"int64"``,
        ``"float64"``, ``"object"``).
        """
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var dtype_name = self._cols[i].dtype.name
            var included = True
            if include:
                ref inc_list = include.value()
                included = False
                for j in range(len(inc_list)):
                    if dtype_name == inc_list[j]:
                        included = True
                        break
            if included and exclude:
                ref exc_list = exclude.value()
                for j in range(len(exc_list)):
                    if dtype_name == exc_list[j]:
                        included = False
                        break
            if included:
                result_cols.append(self._cols[i].copy())
        return DataFrame(result_cols^)

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def sum(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.sum: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].sum(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def mean(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.mean: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].mean(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def median(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.median: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].median(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def min(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.min: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].min(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def max(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.max: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].max(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def std(self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.std: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].std(ddof, skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def var(self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.var: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].var(ddof, skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def count(self, axis: Int = 0) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.count: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(Float64(self._cols[i].count()))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def nunique(self, axis: Int = 0) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.nunique: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(Float64(self._cols[i].nunique()))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def describe(self, include: Optional[List[String]] = None, exclude: Optional[List[String]] = None) raises -> DataFrame:
        _not_implemented("DataFrame.describe")
        return DataFrame()

    def quantile(self, q: Float64 = 0.5, axis: Int = 0) raises -> Series:
        if axis != 0:
            raise Error("DataFrame.quantile: axis=1 not yet implemented")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].quantile(q))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def abs(self) raises -> DataFrame:
        _not_implemented("DataFrame.abs")
        return DataFrame()

    def cumsum(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis != 0:
            raise Error("DataFrame.cumsum: axis=1 not yet implemented")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cumsum(skipna))
        return DataFrame(result_cols^)

    def cumprod(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis != 0:
            raise Error("DataFrame.cumprod: axis=1 not yet implemented")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cumprod(skipna))
        return DataFrame(result_cols^)

    def cummin(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis != 0:
            raise Error("DataFrame.cummin: axis=1 not yet implemented")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cummin(skipna))
        return DataFrame(result_cols^)

    def cummax(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis != 0:
            raise Error("DataFrame.cummax: axis=1 not yet implemented")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cummax(skipna))
        return DataFrame(result_cols^)

    def agg(self, func: String, axis: Int = 0) raises -> Series:
        _not_implemented("DataFrame.agg")
        return Series()

    def aggregate(self, func: String, axis: Int = 0) raises -> Series:
        _not_implemented("DataFrame.aggregate")
        return Series()

    def apply(self, func: String, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.apply")
        return DataFrame()

    def applymap(self, func: String) raises -> DataFrame:
        _not_implemented("DataFrame.applymap")
        return DataFrame()

    def transform(self, func: String, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.transform")
        return DataFrame()

    def eval(self, expr: String) raises -> Series:
        _not_implemented("DataFrame.eval")
        return Series()

    def query(self, expr: String) raises -> DataFrame:
        _not_implemented("DataFrame.query")
        return DataFrame()

    def pipe(self, func: String) raises -> DataFrame:
        _not_implemented("DataFrame.pipe")
        return DataFrame()

    # ------------------------------------------------------------------
    # Missing data
    # ------------------------------------------------------------------

    def isna(self) raises -> DataFrame:
        """Return a boolean DataFrame that is True where values are null/NaN."""
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var s = Series(self._cols[i].copy())
            var bool_s = s.isna()
            result_cols.append(bool_s._col.copy())
        return DataFrame(result_cols^)

    def isnull(self) raises -> DataFrame:
        """Alias for isna()."""
        return self.isna()

    def notna(self) raises -> DataFrame:
        """Return a boolean DataFrame that is True where values are not null/NaN."""
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var s = Series(self._cols[i].copy())
            var bool_s = s.notna()
            result_cols.append(bool_s._col.copy())
        return DataFrame(result_cols^)

    def notnull(self) raises -> DataFrame:
        """Alias for notna()."""
        return self.notna()

    def fillna(self, value: DFScalar, axis: Int = 0) raises -> DataFrame:
        """Return a copy of the DataFrame with null/NaN values replaced by *value*.

        *axis* is accepted for API compatibility (only axis=0 is supported).
        """
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var s = Series(self._cols[i].copy())
            var filled = s.fillna(value)
            result_cols.append(filled._col.copy())
        return DataFrame(result_cols^)

    def dropna(
        self,
        axis: Int = 0,
        how: String = "any",
        thresh: Optional[Int] = None,
        subset: Optional[List[String]] = None,
    ) raises -> DataFrame:
        """Remove rows (axis=0) or columns (axis=1) with null/NaN values.

        *how*:    "any" drops a row/column if ANY value is null;
                  "all" drops a row/column only if ALL values are null.
        *thresh*: keep rows that have at least *thresh* non-null values
                  (overrides *how*).
        *subset*: column names to consider when checking for nulls (axis=0 only).
        """
        if axis == 1:
            # Drop columns that contain nulls.
            var result_cols = List[Column]()
            for i in range(len(self._cols)):
                var col = self._cols[i].copy()
                var mask_len = len(col._null_mask)
                var has_null = False
                if mask_len > 0:
                    if how == "all":
                        # Drop only when every value is null.
                        var all_null = True
                        for j in range(mask_len):
                            if not col._null_mask[j]:
                                all_null = False
                                break
                        has_null = all_null
                    else:
                        for j in range(mask_len):
                            if col._null_mask[j]:
                                has_null = True
                                break
                if not has_null:
                    result_cols.append(col^)
            return DataFrame(result_cols^)

        # axis == 0: drop rows.
        var ncols = len(self._cols)
        if ncols == 0:
            return DataFrame()
        var nrows = len(self._cols[0])

        # Determine which column indices to check (subset parameter).
        var check_indices = List[Int]()
        if subset:
            var sub = subset.value().copy()
            for si in range(len(sub)):
                for ci in range(ncols):
                    if self._cols[ci].name == sub[si]:
                        check_indices.append(ci)
                        break
        else:
            for ci in range(ncols):
                check_indices.append(ci)

        # Determine which rows to keep.
        var keep_rows = List[Int]()
        for r in range(nrows):
            var null_count = 0
            var check_count = len(check_indices)
            for ki in range(check_count):
                var ci = check_indices[ki]
                var mask_len = len(self._cols[ci]._null_mask)
                if mask_len > r and self._cols[ci]._null_mask[r]:
                    null_count += 1

            var keep: Bool
            if thresh:
                # Keep if non-null count >= thresh.
                keep = (check_count - null_count) >= thresh.value()
            elif how == "all":
                # Drop only if every checked value is null.
                keep = null_count < check_count
            else:
                # "any": drop if any checked value is null.
                keep = null_count == 0
            if keep:
                keep_rows.append(r)

        var result_cols = List[Column]()
        for i in range(ncols):
            result_cols.append(self._cols[i].take(keep_rows))
        return DataFrame(result_cols^)

    def ffill(self, axis: Int = 0) raises -> DataFrame:
        """Forward-fill null values column-wise (axis=0)."""
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var s = Series(self._cols[i].copy())
            var filled = s.ffill()
            result_cols.append(filled._col.copy())
        return DataFrame(result_cols^)

    def bfill(self, axis: Int = 0) raises -> DataFrame:
        """Backward-fill null values column-wise (axis=0)."""
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var s = Series(self._cols[i].copy())
            var filled = s.bfill()
            result_cols.append(filled._col.copy())
        return DataFrame(result_cols^)

    def interpolate(self, method: String = "linear", axis: Int = 0) raises -> DataFrame:
        """Fill null values using linear interpolation (Float64 columns only).

        Non-numeric columns are returned unchanged.
        Only method="linear" is supported natively.
        """
        if method != "linear":
            raise Error(
                "DataFrame.interpolate: method='" + method + "' is not supported; only 'linear' is supported"
            )
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var col = self._cols[i].copy()
            var has_mask = len(col._null_mask) > 0
            if not has_mask or not col.dtype.is_float():
                result_cols.append(col^)
                continue
            # Linear interpolation for Float64 columns.
            var n = len(col)
            ref d = col._data[List[Float64]]
            var data = List[Float64]()
            var new_mask = List[Bool]()
            for j in range(n):
                data.append(d[j])
                new_mask.append(col._null_mask[j])
            # Fill leading nulls with the first non-null value.
            var first_valid = -1
            for j in range(n):
                if not new_mask[j]:
                    first_valid = j
                    break
            if first_valid == -1:
                # All null — leave unchanged.
                result_cols.append(col^)
                continue
            for j in range(first_valid):
                data[j] = data[first_valid]
                new_mask[j] = False
            # Fill trailing nulls with the last non-null value.
            var last_valid = -1
            for j in range(n):
                if not new_mask[n - 1 - j]:
                    last_valid = n - 1 - j
                    break
            for j in range(last_valid + 1, n):
                data[j] = data[last_valid]
                new_mask[j] = False
            # Interpolate interior nulls between two known values.
            var seg_start = -1
            for j in range(n):
                if not new_mask[j]:
                    if seg_start >= 0:
                        var v0 = data[seg_start]
                        var v1 = data[j]
                        var gap = j - seg_start
                        for k in range(seg_start + 1, j):
                            var t = Float64(k - seg_start) / Float64(gap)
                            data[k] = v0 + t * (v1 - v0)
                            new_mask[k] = False
                    seg_start = j
            var col_data = ColumnData(data^)
            var new_col = Column(col.name, col_data^, col.dtype, col._index.copy())
            result_cols.append(new_col^)
        return DataFrame(result_cols^)

    # ------------------------------------------------------------------
    # Reshaping / sorting
    # ------------------------------------------------------------------

    def sort_values(self, by: List[String], ascending: Bool = True, na_position: String = "last") raises -> DataFrame:
        """Return a new DataFrame sorted by one or more columns.

        Rows are reordered so that the values in the first ``by`` column are
        sorted (ascending or descending).  Ties in the primary key are broken
        by subsequent keys using a stable insertion sort applied in reverse
        key order.  Null elements are placed at the end when na_position is
        ``"last"`` (default) or at the beginning when na_position is
        ``"first"``.
        """
        var n_rows = self.shape()[0]
        if n_rows == 0 or len(by) == 0:
            return DataFrame(self._cols.copy())

        # Build the sort permutation using a stable multi-key approach:
        # process keys in reverse order (last key first) so that when a
        # higher-priority key is applied next it dominates via the stable
        # insertion sort.  Each step reorders the key column by the current
        # permutation, sorts the reordered values to produce a sub-perm, then
        # composes: new_perm[j] = perm[sub_perm[j]].
        var perm = List[Int]()
        for i in range(n_rows):
            perm.append(i)
        var k = len(by) - 1
        while k >= 0:
            var key_col = self[by[k]]
            var sub_perm = Series(key_col._col.take(perm))._sort_perm(ascending, na_position == "last")
            var new_perm = List[Int]()
            for j in range(n_rows):
                new_perm.append(perm[sub_perm[j]])
            perm = new_perm^
            k -= 1

        # Reorder index labels (parallel to the data).
        var has_index = len(self._cols[0]._index) > 0
        var new_idx = List[PythonObject]()
        if has_index:
            for k in range(n_rows):
                new_idx.append(self._cols[0]._index[perm[k]])

        # Apply permutation to every column.
        var new_cols = List[Column]()
        for i in range(len(self._cols)):
            var taken = self._cols[i].take(perm)
            if has_index:
                taken._index = new_idx.copy()
            new_cols.append(taken^)
        return DataFrame(new_cols^)

    def sort_index(self, axis: Int = 0, ascending: Bool = True) raises -> DataFrame:
        """Return a new DataFrame sorted by its index labels.

        ``axis=0`` (default) sorts by row index labels.  When the DataFrame has
        a default RangeIndex (no explicit index stored), ascending order is
        already the natural order and is returned as-is; descending reverses the
        rows.  For an explicit index, an insertion sort using Python comparison
        orders the rows.

        ``axis=1`` sorts the column labels lexicographically and reorders the
        columns accordingly.
        """
        if axis == 1:
            # Sort column labels lexicographically.
            var n_cols = len(self._cols)
            if n_cols == 0:
                return DataFrame(self._cols.copy())
            # Build a permutation over column positions using insertion sort.
            var perm = List[Int]()
            for i in range(n_cols):
                perm.append(i)
            for i in range(1, n_cols):
                var key = perm[i]
                var key_name = self._cols[key].name
                var j = i - 1
                while j >= 0:
                    var prev = perm[j]
                    var do_swap: Bool
                    if ascending:
                        do_swap = self._cols[prev].name > key_name
                    else:
                        do_swap = self._cols[prev].name < key_name
                    if not do_swap:
                        break
                    perm[j + 1] = prev
                    j -= 1
                perm[j + 1] = key
            var new_cols = List[Column]()
            for i in range(n_cols):
                new_cols.append(self._cols[perm[i]].copy())
            return DataFrame(new_cols^)

        var n_rows = self.shape()[0]
        if n_rows == 0 or len(self._cols) == 0:
            return DataFrame(self._cols.copy())

        var perm = List[Int]()
        if len(self._cols[0]._index) == 0:
            # Default RangeIndex — ascending is already sorted.
            if ascending:
                return DataFrame(self._cols.copy())
            for i in range(n_rows):
                perm.append(n_rows - 1 - i)
        else:
            # Explicit index: insertion sort by Python comparison.
            for i in range(n_rows):
                perm.append(i)
            for i in range(1, n_rows):
                var key = perm[i]
                var j = i - 1
                while j >= 0:
                    var prev = perm[j]
                    var do_swap: Bool
                    if ascending:
                        do_swap = Bool(self._cols[0]._index[key] < self._cols[0]._index[prev])
                    else:
                        do_swap = Bool(self._cols[0]._index[key] > self._cols[0]._index[prev])
                    if not do_swap:
                        break
                    perm[j + 1] = prev
                    j -= 1
                perm[j + 1] = key

        # Reorder index labels and apply permutation to every column.
        var has_index = len(self._cols[0]._index) > 0
        var new_idx = List[PythonObject]()
        if has_index:
            for k in range(n_rows):
                new_idx.append(self._cols[0]._index[perm[k]])
        var new_cols = List[Column]()
        for i in range(len(self._cols)):
            var taken = self._cols[i].take(perm)
            if has_index:
                taken._index = new_idx.copy()
            new_cols.append(taken^)
        return DataFrame(new_cols^)

    def reset_index(self, drop: Bool = False) raises -> DataFrame:
        """Replace the row index with a default RangeIndex.

        When ``drop=True`` the existing index labels are discarded.
        When ``drop=False`` (default) the existing index is promoted to a new
        column named ``"index"`` prepended to the result, and the row index is
        then cleared to a default RangeIndex.  On a DataFrame that already has
        a default RangeIndex, both modes simply return an identical copy.
        """
        var ncols = len(self._cols)
        if ncols == 0:
            return DataFrame()
        var has_index = len(self._cols[0]._index) > 0
        var new_cols = List[Column]()
        if not drop and has_index:
            # Promote the index to a PythonObject column called "index".
            var idx_data = List[PythonObject]()
            for i in range(len(self._cols[0]._index)):
                idx_data.append(self._cols[0]._index[i])
            var empty_idx = List[PythonObject]()
            new_cols.append(Column("index", ColumnData(idx_data^), object_, empty_idx^))
        for i in range(ncols):
            var c = self._cols[i].copy()
            c._index = List[PythonObject]()
            new_cols.append(c^)
        return DataFrame(new_cols^)

    def set_index(self, keys: List[String], drop: Bool = True) raises -> DataFrame:
        """Promote one column to the row index.

        ``keys`` must contain exactly one column name; multi-key (MultiIndex)
        is not yet supported and raises.  When ``drop=True`` (default) the key
        column is removed from the result columns.
        """
        if len(keys) == 0:
            raise Error("DataFrame.set_index: keys must not be empty")
        if len(keys) > 1:
            raise Error(
                "DataFrame.set_index: MultiIndex not yet supported; "
                + "pass a single key"
            )
        var key = keys[0]
        # Find the key column.
        var key_col_idx: Int = -1
        for i in range(len(self._cols)):
            if self._cols[i].name == key:
                key_col_idx = i
                break
        if key_col_idx == -1:
            raise Error("DataFrame.set_index: column not found: " + key)
        # Extract the key column's values as the new index.
        var new_idx = self._cols[key_col_idx]._to_pyobj_index()
        # Build result columns (skip key column when drop=True).
        var new_cols = List[Column]()
        for i in range(len(self._cols)):
            if drop and i == key_col_idx:
                continue
            var c = self._cols[i].copy()
            c._index = new_idx.copy()
            new_cols.append(c^)
        return DataFrame(new_cols^)

    def rename(self, columns: Optional[Dict[String, String]] = None, index: Optional[Dict[String, String]] = None) raises -> DataFrame:
        """Rename column labels and/or row index labels.

        ``columns`` maps old column names to new ones; missing keys are left
        unchanged.  ``index`` maps old index label strings to new ones; missing
        keys are left unchanged.  Both can be applied in the same call.
        """
        var new_cols = List[Column]()
        var builtins = Python.import_module("builtins")
        for i in range(len(self._cols)):
            var c = self._cols[i].copy()
            if columns:
                ref col_map = columns.value()
                if c.name in col_map:
                    c.name = col_map[c.name]
            if index and len(c._index) > 0:
                ref idx_map = index.value()
                for k in range(len(c._index)):
                    var lbl = String(c._index[k])
                    if lbl in idx_map:
                        c._index[k] = builtins.str(idx_map[lbl])
            new_cols.append(c^)
        return DataFrame(new_cols^)

    def rename_axis(self, mapper: Optional[String] = None, axis: Int = 0) raises -> DataFrame:
        """Return a copy with the axis name set to *mapper*.

        Note: bison does not currently store axis names (the ``Index.name``
        field is not wired into ``Column._index`` storage).  This method
        returns a deep copy and silently ignores *mapper*.  The limitation is
        tracked in SESSION.md as a tech-debt item.
        """
        return self._deep_copy()

    def reindex(self, labels: Optional[List[String]] = None, axis: Int = 0, fill_value: Optional[DFScalar] = None) raises -> DataFrame:
        """Conform the DataFrame to a new set of labels along an axis.

        ``axis=1`` reorders/selects columns; missing columns are filled with
        *fill_value* (null when *fill_value* is not provided).
        ``axis=0`` reorders/selects rows by index label; missing labels produce
        null rows (or rows filled with *fill_value*).

        Returns an identical copy when ``labels`` is not provided.
        """
        if not labels:
            return self._deep_copy()
        ref new_labels = labels.value()
        var ncols = len(self._cols)
        if axis == 1:
            # Column reindex: reorder and/or fill new columns.
            var nrows = self.shape()[0]
            # Build a name→col_index lookup.
            var col_map = Dict[String, Int]()
            for i in range(ncols):
                col_map[self._cols[i].name] = i
            # Determine the shared index for the result.
            var shared_idx = List[PythonObject]()
            if ncols > 0:
                shared_idx = self._cols[0]._index.copy()
            # Infer a common dtype from existing columns for null-fill.
            # If all columns share the same dtype, use it; otherwise fall back
            # to float64 (the widest numeric type).
            var inferred_dtype = float64
            if ncols > 0:
                inferred_dtype = self._cols[0].dtype
                for j in range(1, ncols):
                    if self._cols[j].dtype != inferred_dtype:
                        inferred_dtype = float64
                        break
            var new_cols = List[Column]()
            for k in range(len(new_labels)):
                var lbl = new_labels[k]
                if lbl in col_map:
                    var c = self._cols[col_map[lbl]].copy()
                    new_cols.append(c^)
                else:
                    # Insert a fill/null column.
                    if fill_value:
                        var c = Column._fill_scalar(lbl, fill_value.value(), nrows, shared_idx.copy())
                        new_cols.append(c^)
                    else:
                        # Null column: infer dtype from existing columns so
                        # that a frame of all-int64 columns produces an int64
                        # null column rather than float64.
                        var c = Column._null_column(lbl, inferred_dtype, nrows, shared_idx.copy())
                        new_cols.append(c^)
            return DataFrame(new_cols^)
        else:
            # Row reindex: reorder and/or fill new rows.
            if ncols == 0:
                return DataFrame()
            var nrows = self.shape()[0]
            var has_index = len(self._cols[0]._index) > 0
            # Build label → row-position map.
            var label_to_row = Dict[String, Int]()
            for i in range(nrows):
                var key: String
                if has_index:
                    key = String(self._cols[0]._index[i])
                else:
                    key = String(i)
                label_to_row[key] = i
            # Build the per-output-row index list (row_indices) and new _index.
            var row_indices = List[Int]()
            var new_pyidx = List[PythonObject]()
            var builtins = Python.import_module("builtins")
            for k in range(len(new_labels)):
                var lbl = new_labels[k]
                if lbl in label_to_row:
                    var src_row = label_to_row[lbl]
                    row_indices.append(src_row)
                    if has_index:
                        new_pyidx.append(self._cols[0]._index[src_row])
                    else:
                        new_pyidx.append(builtins.int(src_row))
                else:
                    row_indices.append(-1)
                    new_pyidx.append(builtins.str(lbl))
            # Reindex each column.
            var new_cols = List[Column]()
            for i in range(ncols):
                var c = self._cols[i]._reindex_rows(row_indices, fill_value)
                c._index = new_pyidx.copy()
                new_cols.append(c^)
            return DataFrame(new_cols^)

    def drop(self, labels: Optional[List[String]] = None, axis: Int = 0, columns: Optional[List[String]] = None) raises -> DataFrame:
        """Drop columns (axis=1 / columns kwarg) or rows (axis=0) by label.

        Column drop: pass ``columns=[...]`` or ``axis=1`` with ``labels=[...]``.
        Row drop: pass ``axis=0`` with ``labels=[...]`` matching index labels or
        (for default RangeIndex) integer row positions as strings.
        Raises if a requested label is not found.
        """
        var ncols = len(self._cols)
        if ncols == 0:
            return DataFrame()

        # Determine drop mode: column axis if ``columns`` kwarg is set, or axis==1.
        var drop_cols: Bool
        var drop_labels: List[String]
        if columns:
            drop_cols = True
            drop_labels = columns.value().copy()
        elif axis == 1:
            drop_cols = True
            if labels:
                drop_labels = labels.value().copy()
            else:
                drop_labels = List[String]()
        else:
            drop_cols = False
            if labels:
                drop_labels = labels.value().copy()
            else:
                drop_labels = List[String]()

        if drop_cols:
            # Build a set of column names to remove.
            var drop_set = Dict[String, Bool]()
            for i in range(len(drop_labels)):
                drop_set[drop_labels[i]] = True
            # Verify all requested labels exist.
            for i in range(len(drop_labels)):
                var found = False
                for j in range(ncols):
                    if self._cols[j].name == drop_labels[i]:
                        found = True
                        break
                if not found:
                    raise Error("DataFrame.drop: column not found: " + drop_labels[i])
            # Collect surviving columns.
            var result_cols = List[Column]()
            for i in range(ncols):
                if self._cols[i].name not in drop_set:
                    result_cols.append(self._cols[i].copy())
            return DataFrame(result_cols^)
        else:
            # Row drop: match drop_labels against the index.
            var nrows = self.shape()[0]
            var has_index = ncols > 0 and len(self._cols[0]._index) > 0
            var drop_set = Dict[String, Bool]()
            for i in range(len(drop_labels)):
                drop_set[drop_labels[i]] = True
            var keep_indices = List[Int]()
            for i in range(nrows):
                var key: String
                if has_index:
                    key = String(self._cols[0]._index[i])
                else:
                    key = String(i)
                if key not in drop_set:
                    keep_indices.append(i)
            # Verify all requested labels were found.
            var found_set = Dict[String, Bool]()
            for i in range(nrows):
                var key: String
                if has_index:
                    key = String(self._cols[0]._index[i])
                else:
                    key = String(i)
                if key in drop_set:
                    found_set[key] = True
            for i in range(len(drop_labels)):
                if drop_labels[i] not in found_set:
                    raise Error("DataFrame.drop: index label not found: " + drop_labels[i])
            var result_cols = List[Column]()
            for i in range(ncols):
                result_cols.append(self._cols[i].take(keep_indices))
            return DataFrame(result_cols^)

    def drop_duplicates(self, subset: Optional[List[String]] = None, keep: String = "first") raises -> DataFrame:
        """Return a DataFrame with duplicate rows removed.

        Delegates to ``duplicated`` to identify duplicates, then retains rows
        where the duplicate flag is False.  See ``duplicated`` for ``subset``
        and ``keep`` semantics.
        """
        var dup = self.duplicated(subset, keep)
        ref dup_data = dup._col._data[List[Bool]]
        var keep_indices = List[Int]()
        for i in range(len(dup_data)):
            if not dup_data[i]:
                keep_indices.append(i)
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].take(keep_indices))
        return DataFrame(result_cols^)

    def duplicated(self, subset: Optional[List[String]] = None, keep: String = "first") raises -> Series:
        """Return a boolean Series indicating duplicate rows.

        Each row is fingerprinted by concatenating per-column cell values
        using a length-prefixed encoding: each cell ``v`` is encoded as
        ``"<len(v)>:<v>"`` and the per-cell tokens are concatenated without
        any separator.  For example, a cell with value ``"abc"`` contributes
        ``"3:abc"`` to the key.  This scheme is safe for cells containing any
        byte value, including NUL.

        - ``keep="first"``  — False for the first occurrence, True for subsequent.
        - ``keep="last"``   — False for the last occurrence, True for earlier ones.
        - ``keep=False``    — True for every occurrence of a repeated row.

        ``subset`` restricts the columns used to build the fingerprint.
        Raises if any name in ``subset`` is not a column in this DataFrame.
        """
        var nrows = self.shape()[0]
        var ncols = len(self._cols)

        # Build list of working column indices (subset or all).
        var work_indices = List[Int]()
        if subset:
            var sub = subset.value().copy()
            for s in range(len(sub)):
                var name = sub[s]
                var found = False
                for j in range(ncols):
                    if self._cols[j].name == name:
                        work_indices.append(j)
                        found = True
                        break
                if not found:
                    raise Error("DataFrame.duplicated: column not found in subset: " + name)
        else:
            for j in range(ncols):
                work_indices.append(j)

        # Build a row-key string for each row using a length-prefixed encoding.
        # Each cell is encoded as "<decimal_length>:<value>" and concatenated
        # without any separator.  This is unambiguous regardless of the cell
        # content (e.g. cells that contain NUL bytes or digit/colon sequences
        # will never produce the same key as a row with different cell values).
        var keys = List[String]()
        for i in range(nrows):
            var key = String("")
            for k in range(len(work_indices)):
                var cell = _col_cell_str(self._cols[work_indices[k]], i)
                key = key + String(len(cell)) + ":" + cell
            keys.append(key)

        var result = List[Bool]()

        if keep == "first":
            var seen = Dict[String, Bool]()
            for i in range(nrows):
                if keys[i] in seen:
                    result.append(True)
                else:
                    seen[keys[i]] = True
                    result.append(False)
        elif keep == "last":
            # Pass 1: find last occurrence index of each key.
            var last_idx = Dict[String, Int]()
            for i in range(nrows):
                last_idx[keys[i]] = i
            # Pass 2: mark True for all rows that are NOT the last occurrence.
            for i in range(nrows):
                result.append(last_idx[keys[i]] != i)
        elif keep == "False":
            # Count all occurrences.
            var counts = Dict[String, Int]()
            for i in range(nrows):
                counts[keys[i]] = counts.get(keys[i], 0) + 1
            for i in range(nrows):
                result.append(counts[keys[i]] > 1)
        else:
            raise Error("DataFrame.duplicated: keep must be 'first', 'last', or 'False'")

        var col = Column("", ColumnData(result^), bool_)
        return Series(col^)

    def pivot(self, index: String = "", columns: String = "", values: String = "") raises -> DataFrame:
        _not_implemented("DataFrame.pivot")
        return DataFrame()

    def pivot_table(self, values: Optional[List[String]] = None, index: Optional[List[String]] = None, columns: Optional[List[String]] = None, aggfunc: String = "mean") raises -> DataFrame:
        _not_implemented("DataFrame.pivot_table")
        return DataFrame()

    def melt(self, id_vars: Optional[List[String]] = None, value_vars: Optional[List[String]] = None, var_name: String = "variable", value_name: String = "value") raises -> DataFrame:
        _not_implemented("DataFrame.melt")
        return DataFrame()

    def stack(self, level: Int = -1) raises -> Series:
        _not_implemented("DataFrame.stack")
        return Series()

    def unstack(self, level: Int = -1) raises -> DataFrame:
        _not_implemented("DataFrame.unstack")
        return DataFrame()

    def transpose(self) raises -> DataFrame:
        _not_implemented("DataFrame.transpose")
        return DataFrame()

    def T(self) raises -> DataFrame:
        _not_implemented("DataFrame.T")
        return DataFrame()

    def swaplevel(self, i: Int = -2, j: Int = -1, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.swaplevel")
        return DataFrame()

    def explode(self, column: String) raises -> DataFrame:
        _not_implemented("DataFrame.explode")
        return DataFrame()

    def clip(self, lower: Optional[Float64] = None, upper: Optional[Float64] = None) raises -> DataFrame:
        """Clamp numeric column values to [lower, upper].

        Either bound may be ``None`` (no clipping on that side).  Non-numeric
        columns (Bool, String, Object) raise from the underlying ``Column._clip``.
        """
        if not lower and not upper:
            return self._deep_copy()
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i]._clip(lower, upper))
        return DataFrame(result_cols^)

    def round(self, decimals: Int = 0) raises -> DataFrame:
        """Round numeric column values to *decimals* decimal places."""
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i]._round(decimals))
        return DataFrame(result_cols^)

    def astype(self, dtype: String) raises -> DataFrame:
        """Cast every column to *dtype*.

        Supported target dtypes: any value accepted by ``dtype_from_string``
        (e.g. ``"float64"``, ``"int64"``, ``"bool"``).
        """
        var target = dtype_from_string(dtype)
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i]._astype(target))
        return DataFrame(result_cols^)

    def _deep_copy(self) -> DataFrame:
        """Return an independent column-wise copy of this DataFrame (internal helper)."""
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].copy())
        return DataFrame(result_cols^)

    def copy(self, deep: Bool) raises -> DataFrame:
        """Return an independent copy of this DataFrame.

        When ``deep=True`` (the default in pandas), a full independent copy of
        all columns is returned.

        ``deep=False`` raises ``Error`` because shallow-copy semantics require
        reference-counted storage, which Mojo's ownership model does not yet
        support.  Pass ``deep=True`` (or call ``df.copy(True)``) to obtain a
        deep copy.
        """
        if not deep:
            raise Error(
                "DataFrame.copy: deep=False is not yet supported; Mojo's"
                " ownership model requires reference-counted storage for"
                " shallow copies"
            )
        return self._deep_copy()

    def assign(self, cols: Dict[String, Series]) raises -> DataFrame:
        """Return a new DataFrame with additional or replaced columns.

        Each key in *cols* is a column name; the value is the new Series.
        Existing columns are replaced in-place; new columns are appended.
        """
        var df = self._deep_copy()
        for entry in cols.items():
            df[entry.key] = entry.value
        return df^

    def insert(mut self, loc: Int, column: String, value: DFScalar) raises:
        """Insert a constant-valued column at position *loc*.

        *loc* is clamped to ``[0, ncols]``.  Raises if *column* already exists.
        """
        for i in range(len(self._cols)):
            if self._cols[i].name == column:
                raise Error("DataFrame.insert: column already exists: " + column)
        var nrows = self.shape()[0]
        var idx = List[PythonObject]()
        if len(self._cols) > 0:
            idx = self._cols[0]._index.copy()
        var new_col = Column._fill_scalar(column, value, nrows, idx)
        var insert_at = loc
        if insert_at < 0:
            insert_at = 0
        if insert_at > len(self._cols):
            insert_at = len(self._cols)
        var new_cols = List[Column]()
        for i in range(insert_at):
            new_cols.append(self._cols[i].copy())
        new_cols.append(new_col^)
        for i in range(insert_at, len(self._cols)):
            new_cols.append(self._cols[i].copy())
        self._cols = new_cols^

    def pop(mut self, item: String) raises -> Series:
        """Remove column *item* and return it as a Series.

        Raises if *item* is not a column in this DataFrame.
        """
        var idx = -1
        for i in range(len(self._cols)):
            if self._cols[i].name == item:
                idx = i
                break
        if idx == -1:
            raise Error("DataFrame.pop: column not found: " + item)
        var result = Series(self._cols[idx].copy())
        var new_cols = List[Column]()
        for i in range(len(self._cols)):
            if i != idx:
                new_cols.append(self._cols[i].copy())
        self._cols = new_cols^
        return result^

    def where(self, cond: Series, other: Optional[DFScalar] = None) raises -> DataFrame:
        """Keep each element where *cond* is True; replace with *other* otherwise.

        When *other* is ``None`` (the default), non-matching cells become null.
        """
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i]._where(cond._col, other))
        return DataFrame(result_cols^)

    def mask(self, cond: Series, other: Optional[DFScalar] = None) raises -> DataFrame:
        """Replace each element with *other* where *cond* is True; keep otherwise.

        When *other* is ``None`` (the default), matching cells become null.
        """
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i]._mask(cond._col, other))
        return DataFrame(result_cols^)

    def isin(self, values: Dict[String, List[DFScalar]]) raises -> DataFrame:
        """Return a boolean DataFrame: True where each element is in the corresponding list.

        *values* maps column names to lists of scalars.  Columns not present in
        *values* produce an all-False Bool column.  Scalar types are coerced to
        match the column arm (e.g. Int64 scalars against a Float64 column).
        """
        var nrows = self.shape()[0]
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var col_name = self._cols[i].name
            if col_name not in values:
                # All-False Bool column for columns not in the dict.
                var false_data = List[Bool]()
                for j in range(nrows):
                    false_data.append(False)
                result_cols.append(Column(col_name, ColumnData(false_data^), bool_))
            else:
                var result_col = self._cols[i]._isin_scalars(values[col_name])
                result_col.name = col_name
                result_cols.append(result_col^)
        return DataFrame(result_cols^)

    def combine_first(self, other: DataFrame) raises -> DataFrame:
        """Fill null positions in self with corresponding values from other.

        For each column present in both DataFrames: keep self's value where
        non-null; take other's value otherwise.  Columns present only in
        ``other`` are appended as-is.  Same-named columns must have the same
        dtype; raises on dtype mismatch.
        """
        var result_cols = List[Column]()
        # Phase 1: iterate self columns, fill from other where available.
        for i in range(len(self._cols)):
            var found = False
            for j in range(len(other._cols)):
                if other._cols[j].name == self._cols[i].name:
                    result_cols.append(self._cols[i]._combine_first_col(other._cols[j]))
                    found = True
                    break
            if not found:
                result_cols.append(self._cols[i].copy())
        # Phase 2: append columns that exist only in other.
        for j in range(len(other._cols)):
            var in_self = False
            for i in range(len(self._cols)):
                if self._cols[i].name == other._cols[j].name:
                    in_self = True
                    break
            if not in_self:
                result_cols.append(other._cols[j].copy())
        return DataFrame(result_cols^)

    def update(mut self, other: DataFrame) raises:
        """Update in-place with non-null values from other.

        For each column present in both DataFrames: overwrite self's null
        positions with other's non-null values (other wins where non-null).
        Columns in other that are absent from self are ignored.
        """
        for j in range(len(other._cols)):
            for i in range(len(self._cols)):
                if self._cols[i].name == other._cols[j].name:
                    self._cols[i] = other._cols[j]._combine_first_col(self._cols[i])
                    break

    # ------------------------------------------------------------------
    # Combining
    # ------------------------------------------------------------------

    def merge(
        self,
        right: DataFrame,
        how: String = "inner",
        on: Optional[List[String]] = None,
        left_on: Optional[List[String]] = None,
        right_on: Optional[List[String]] = None,
        left_index: Bool = False,
        right_index: Bool = False,
        suffixes: Optional[List[String]] = None,
    ) raises -> DataFrame:
        _not_implemented("DataFrame.merge")
        return DataFrame()

    def join(
        self,
        other: DataFrame,
        on: Optional[List[String]] = None,
        how: String = "left",
        lsuffix: String = "",
        rsuffix: String = "",
        sort: Bool = False,
    ) raises -> DataFrame:
        _not_implemented("DataFrame.join")
        return DataFrame()

    def append(self, other: DataFrame, ignore_index: Bool = False) raises -> DataFrame:
        _not_implemented("DataFrame.append")
        return DataFrame()

    # ------------------------------------------------------------------
    # GroupBy
    # ------------------------------------------------------------------

    def groupby(
        self,
        by: List[String],
        axis: Int = 0,
        as_index: Bool = True,
        sort: Bool = True,
        dropna: Bool = True,
    ) raises -> DataFrameGroupBy:
        _not_implemented("DataFrame.groupby")
        return DataFrameGroupBy()

    def resample(self, rule: String, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.resample")
        return DataFrame()

    def rolling(self, window: Int, min_periods: Optional[Int] = None) raises -> DataFrame:
        _not_implemented("DataFrame.rolling")
        return DataFrame()

    def expanding(self, min_periods: Int = 1) raises -> DataFrame:
        _not_implemented("DataFrame.expanding")
        return DataFrame()

    def ewm(self, com: Optional[Float64] = None, span: Optional[Float64] = None) raises -> DataFrame:
        _not_implemented("DataFrame.ewm")
        return DataFrame()

    # ------------------------------------------------------------------
    # IO
    # ------------------------------------------------------------------

    def to_csv(self, path_or_buf: String = "", sep: String = ",", index: Bool = True) raises -> String:
        """Serialize the DataFrame to a CSV-formatted string or file.

        Parameters
        ----------
        path_or_buf : File path to write. If empty (default) the CSV text is
                      returned as a ``String`` instead of being written.
        sep         : Field delimiter (default ``","``).
        index       : Whether to include the row index (default ``True``).
        """
        var nrows = self.__len__()
        var ncols = self._cols.__len__()
        var result = String()

        # Header row
        var header_parts = List[String]()
        if index:
            header_parts.append(String(""))  # pandas convention: index column has no header
        for ci in range(ncols):
            header_parts.append(self._cols[ci].name)
        var hline = String()
        for i in range(len(header_parts)):
            if i > 0:
                hline += sep
            hline += _csv_quote_field(header_parts[i], sep)
        result += hline + "\n"

        # Data rows
        for ri in range(nrows):
            var line = String()
            if index:
                line += String(ri) + sep
            for ci in range(ncols):
                if ci > 0:
                    line += sep
                line += _csv_quote_field(_col_cell_str(self._cols[ci], ri), sep)
            result += line + "\n"

        # Write to file or return string.
        if len(path_or_buf) > 0:
            with open(path_or_buf, "w") as f:
                f.write(result)
            return String("")
        return result^

    def to_parquet(self, path: String, engine: String = "auto", compression: String = "snappy") raises:
        _not_implemented("DataFrame.to_parquet")

    def to_json(self, path_or_buf: String = "", orient: String = "") raises -> String:
        """Serialize the DataFrame to a JSON-formatted string or file.

        Parameters
        ----------
        path_or_buf : File path to write. If empty (default) the JSON text is
                      returned as a ``String`` instead of being written.
        orient      : JSON format. Supported values: ``"records"``
                      (list of dicts), ``"split"`` (split format),
                      ``"columns"`` (dict of dicts keyed by column name,
                      default when orient is ``""``), ``"index"`` (dict of
                      dicts keyed by row index), ``"values"`` (list of lists).
        """
        var json_mod = Python.import_module("json")
        var nrows = self.__len__()
        var ncols = self._cols.__len__()
        var eff_orient = orient if len(orient) > 0 else "columns"

        var py_obj: PythonObject

        if eff_orient == "records":
            # [{"col1": val1, "col2": val2}, ...]
            py_obj = Python.evaluate("[]")
            for ri in range(nrows):
                var row = Python.evaluate("{}")
                for ci in range(ncols):
                    row[self._cols[ci].name] = _col_cell_pyobj(self._cols[ci], ri)
                py_obj.append(row)

        elif eff_orient == "split":
            # {"columns": [...], "index": [...], "data": [[...], ...]}
            py_obj = Python.evaluate("{}")
            var cols_list = Python.evaluate("[]")
            for ci in range(ncols):
                cols_list.append(self._cols[ci].name)
            py_obj["columns"] = cols_list
            var idx_list = Python.evaluate("[]")
            for ri in range(nrows):
                idx_list.append(ri)
            py_obj["index"] = idx_list
            var data_list = Python.evaluate("[]")
            for ri in range(nrows):
                var row = Python.evaluate("[]")
                for ci in range(ncols):
                    row.append(_col_cell_pyobj(self._cols[ci], ri))
                data_list.append(row)
            py_obj["data"] = data_list

        elif eff_orient == "index":
            # {"0": {"col1": val1, ...}, ...}
            py_obj = Python.evaluate("{}")
            for ri in range(nrows):
                var row = Python.evaluate("{}")
                for ci in range(ncols):
                    row[self._cols[ci].name] = _col_cell_pyobj(self._cols[ci], ri)
                py_obj[String(ri)] = row

        elif eff_orient == "values":
            # [[val1, val2], ...]
            py_obj = Python.evaluate("[]")
            for ri in range(nrows):
                var row = Python.evaluate("[]")
                for ci in range(ncols):
                    row.append(_col_cell_pyobj(self._cols[ci], ri))
                py_obj.append(row)

        else:  # "columns" (default pandas behavior)
            # {"col1": {"0": val1, "1": val2, ...}, ...}
            py_obj = Python.evaluate("{}")
            for ci in range(ncols):
                var col_dict = Python.evaluate("{}")
                for ri in range(nrows):
                    col_dict[String(ri)] = _col_cell_pyobj(self._cols[ci], ri)
                py_obj[self._cols[ci].name] = col_dict

        var result = String(json_mod.dumps(py_obj))

        if len(path_or_buf) > 0:
            with open(path_or_buf, "w") as f:
                f.write(result)
            return String("")
        return result^

    def to_excel(self, excel_writer: String, sheet_name: String = "Sheet1", index: Bool = True) raises:
        _not_implemented("DataFrame.to_excel")

    def to_dict(self, orient: String = "dict") raises -> Dict[String, List[DFScalar]]:
        _not_implemented("DataFrame.to_dict")
        return Dict[String, List[DFScalar]]()

    def to_records(self, index: Bool = True) raises -> List[Dict[String, DFScalar]]:
        _not_implemented("DataFrame.to_records")
        return List[Dict[String, DFScalar]]()

    def to_numpy(self) raises -> List[List[Float64]]:
        _not_implemented("DataFrame.to_numpy")
        return List[List[Float64]]()

    def to_string(self) raises -> String:
        _not_implemented("DataFrame.to_string")
        return ""

    def to_html(self) raises -> String:
        _not_implemented("DataFrame.to_html")
        return ""

    def to_markdown(self) raises -> String:
        _not_implemented("DataFrame.to_markdown")
        return ""

    # ------------------------------------------------------------------
    # Repr / iteration
    # ------------------------------------------------------------------

    def __repr__(self) raises -> String:
        return String(self.to_pandas())

    def __len__(self) -> Int:
        if self._cols.__len__() == 0:
            return 0
        return self._cols[0].__len__()

    def __contains__(self, key: String) -> Bool:
        for i in range(self._cols.__len__()):
            if self._cols[i].name == key:
                return True
        return False

    def items(self) raises -> List[Series]:
        """Return a list of (column_name, Series) pairs as Series objects.

        Each returned Series corresponds to one column; the column name is
        available via ``series.name``.
        """
        var result = List[Series]()
        for i in range(len(self._cols)):
            result.append(Series(self._cols[i].copy()))
        return result^

    def iterrows(self) raises -> List[Series]:
        """Return a list of row Series, one per row.

        Each Series holds all column values for that row as object dtype
        (``List[PythonObject]``), with column names as the Series index.
        The Series name is the string representation of the row position.
        """
        var nrows = self.shape()[0]
        var ncols = len(self._cols)
        var result = List[Series]()
        var col_idx = List[PythonObject]()
        for j in range(ncols):
            col_idx.append(PythonObject(self._cols[j].name))
        for i in range(nrows):
            var row_data = List[PythonObject]()
            for j in range(ncols):
                row_data.append(_col_cell_pyobj(self._cols[j], i))
            var idx_copy = col_idx.copy()
            var row_col = Column(
                String(i), ColumnData(row_data^), object_, idx_copy^
            )
            result.append(Series(row_col^))
        return result^

    def itertuples(self, index: Bool = True, name: String = "Pandas") raises -> List[Series]:
        """Return a list of row Series, one per row, optionally with the row index prepended.

        When *index* is ``True`` (default) the first element of each Series is
        the integer row position and the corresponding index label is
        ``"Index"``.  *name* is stored as the Series name for each row.
        """
        var nrows = self.shape()[0]
        var ncols = len(self._cols)
        var result = List[Series]()
        var col_idx = List[PythonObject]()
        if index:
            col_idx.append(PythonObject("Index"))
        for j in range(ncols):
            col_idx.append(PythonObject(self._cols[j].name))
        for i in range(nrows):
            var row_data = List[PythonObject]()
            if index:
                row_data.append(PythonObject(i))
            for j in range(ncols):
                row_data.append(_col_cell_pyobj(self._cols[j], i))
            var idx_copy = col_idx.copy()
            var row_col = Column(
                name, ColumnData(row_data^), object_, idx_copy^
            )
            result.append(Series(row_col^))
        return result^



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
