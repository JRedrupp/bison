from std.python import Python, PythonObject
from std.collections import Optional, Dict
from std.builtin.sort import sort as _sort_list
from ._errors import _not_implemented
from .dtypes import (
    BisonDtype,
    object_,
    bool_,
    int64,
    float64,
    dtype_from_string,
    datetime64_ns,
)
from .index import Index, ColumnIndex
from .column import (
    Column,
    ColumnData,
    DFScalar,
    SeriesScalar,
    _Null,
    FloatTransformFn,
    _csv_quote_field,
    _col_cell_str,
    _col_cell_pyobj,
    _scalar_from_col,
)
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
            var raw_name = pd_s.name
            var _is_none = Python.evaluate("lambda x: x is None")
            if Bool(_is_none(raw_name)):
                col_name = ""
            else:
                col_name = String(raw_name)
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
        var raw_name = pd_s.name
        var col_name: String
        var _is_none = Python.evaluate("lambda x: x is None")
        if Bool(_is_none(raw_name)):
            col_name = ""
        else:
            col_name = String(raw_name)
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

    def dtype(self) -> BisonDtype:
        return self._col.dtype

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
        var n = self._col._index_len()
        for i in range(n):
            if self._col._index_label(i) == label:
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
        """Return summary statistics as a Series (count, mean, std, min, quartiles, max).
        """
        return Series(self._col.describe())

    def value_counts(
        self, normalize: Bool = False, sort: Bool = True
    ) raises -> Series:
        """Return a Series with the count (or proportion) of each unique value.
        """
        return Series(self._col.value_counts(normalize, sort))

    def quantile(self, q: Float64 = 0.5, skipna: Bool = True) raises -> Float64:
        return self._col.quantile(q, skipna)

    def cumsum(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cumsum(skipna))

    def cumprod(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cumprod(skipna))

    def cummin(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cummin(skipna))

    def cummax(self, skipna: Bool = True) raises -> Series:
        return Series(self._col.cummax(skipna))

    def sem(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        _not_implemented("Series.sem")
        return 0.0

    def skew(self, skipna: Bool = True) raises -> Float64:
        _not_implemented("Series.skew")
        return 0.0

    def kurt(self, skipna: Bool = True) raises -> Float64:
        _not_implemented("Series.kurt")
        return 0.0

    def idxmin(self, skipna: Bool = True) raises -> Int:
        _not_implemented("Series.idxmin")
        return 0

    def idxmax(self, skipna: Bool = True) raises -> Int:
        _not_implemented("Series.idxmax")
        return 0

    def corr(self, other: Series) raises -> Float64:
        _not_implemented("Series.corr")
        return 0.0

    def cov(self, other: Series, ddof: Int = 1) raises -> Float64:
        _not_implemented("Series.cov")
        return 0.0

    def shift(self, periods: Int = 1) raises -> Series:
        _not_implemented("Series.shift")
        return Series()

    def diff(self, periods: Int = 1) raises -> Series:
        _not_implemented("Series.diff")
        return Series()

    def pct_change(self, periods: Int = 1) raises -> Series:
        _not_implemented("Series.pct_change")
        return Series()

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
        """Return a boolean Series that is True where values are not null/NaN.
        """
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
        """Return a copy of the Series with null/NaN values replaced by *value*.
        """
        if value.is_null():
            raise Error("fillna: fill value cannot be null")
        var has_mask = len(self._col._null_mask) > 0
        if not has_mask:
            return Series(self._col.copy())
        var n = len(self._col)
        var idx = self._col._index
        if self._col._data.isa[List[Int64]]():
            var fill_val: Int64
            if value.isa[Int64]():
                fill_val = value[Int64]
            elif value.isa[Float64]():
                fill_val = Int64(Int(value[Float64]))
            elif value.isa[Bool]():
                fill_val = Int64(1) if value[Bool] else Int64(0)
            else:
                raise Error(
                    "fillna: cannot fill Int64 column with String value"
                )
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
                raise Error(
                    "fillna: cannot fill Float64 column with String value"
                )
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
        """Forward-fill: propagate the last non-null value forward over nulls.
        """
        var has_mask = len(self._col._null_mask) > 0
        if not has_mask:
            return Series(self._col.copy())
        var n = len(self._col)
        var idx = self._col._index
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
        """Backward-fill: propagate the next non-null value backward over nulls.
        """
        var has_mask = len(self._col._null_mask) > 0
        if not has_mask:
            return Series(self._col.copy())
        var n = len(self._col)
        var idx = self._col._index
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

    def _sort_perm(
        self, ascending: Bool, na_last: Bool = True
    ) raises -> List[Int]:
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

    def sort_values(
        self, ascending: Bool = True, na_position: String = "last"
    ) raises -> Series:
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
        if self._col._index_len() > 0:
            sorted_col._index = self._col._index_reorder(perm)
        return Series(sorted_col^)

    def sort_index(self, ascending: Bool = True) raises -> Series:
        """Return a new Series sorted by index label.

        When the Series has a default RangeIndex the data is already ordered
        for ``ascending=True``; ``ascending=False`` reverses it.
        For explicit index labels, native comparison is used for string and
        int64 index arms; Python comparison is used for the PythonObject
        fallback arm.
        """
        var n = len(self._col)
        if n == 0:
            return Series(self._col.copy())
        if self._col._index_len() == 0:
            # Default RangeIndex [0, 1, ..., n-1].
            if not ascending:
                var rev_perm = List[Int]()
                for i in range(n):
                    rev_perm.append(n - 1 - i)
                var sorted_col = self._col.take(rev_perm)
                # Materialise the reversed RangeIndex as Int64.
                var int_idx = List[Int64]()
                for k in range(n):
                    int_idx.append(Int64(n - 1 - k))
                sorted_col._index = ColumnIndex(int_idx^)
                return Series(sorted_col^)
            return Series(self._col.copy())
        # Build sort permutation via the Column helper (handles all index arms).
        var perm = self._col._sort_perm_by_index(ascending)
        var sorted_col = self._col.take(perm)
        sorted_col._index = self._col._index_reorder(perm)
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
        var idx = self._col._index
        if not has_any_null:
            var result_data = List[Int64]()
            for i in range(n):
                result_data.append(Int64(perm[i]))
            var col = Column(
                self._col.name, ColumnData(result_data^), int64, idx^
            )
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
        var col = Column(
            self._col.name, ColumnData(result_data^), float64, idx^
        )
        col._null_mask = result_mask^
        return Series(col^)

    def rank(self) raises -> Series:
        """Return 1-based float ranks (average method for ties, NaN for nulls).
        """
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
        for _ in range(n):
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
        var idx = self._col._index
        var col = Column(self._col.name, ColumnData(ranks^), float64, idx^)
        # n_non_null < n iff there are nulls — no need to re-scan the mask.
        if n_non_null < n:
            col._null_mask = rank_mask^
        return Series(col^)

    # ------------------------------------------------------------------
    # Reshaping / transformations
    # ------------------------------------------------------------------

    def apply[F: FloatTransformFn](self) raises -> Series:
        """Apply a compile-time function element-wise. Call as ``s.apply[my_fn]()``.
        """
        return Series(self._col._apply[F]())

    def map[F: FloatTransformFn](self) raises -> Series:
        """Map a compile-time function element-wise. Call as ``s.map[my_fn]()``.
        """
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

    def clip(
        self, lower: Optional[Float64] = None, upper: Optional[Float64] = None
    ) raises -> Series:
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

    def where(
        self, cond: Series, other: Optional[DFScalar] = None
    ) raises -> Series:
        return Series(self._col._where(cond._col, other))

    def mask(
        self, cond: Series, other: Optional[DFScalar] = None
    ) raises -> Series:
        return Series(self._col._mask(cond._col, other))

    # ------------------------------------------------------------------
    # Interop
    # ------------------------------------------------------------------

    def to_list(self) raises -> List[DFScalar]:
        """Return the Series values as a ``List[DFScalar]``.

        Null values are represented as ``DFScalar.null()``.
        Object-dtype columns raise ``Error``.
        """
        var result = List[DFScalar]()
        ref col = self._col
        var has_mask = len(col._null_mask) > 0
        var n = col.__len__()
        if col._data.isa[List[Int64]]():
            ref data = col._data[List[Int64]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(DFScalar.null())
                else:
                    result.append(DFScalar(data[i]))
        elif col._data.isa[List[Float64]]():
            ref data = col._data[List[Float64]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(DFScalar.null())
                else:
                    result.append(DFScalar(data[i]))
        elif col._data.isa[List[Bool]]():
            ref data = col._data[List[Bool]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(DFScalar.null())
                else:
                    result.append(DFScalar(data[i]))
        elif col._data.isa[List[String]]():
            ref data = col._data[List[String]]
            for i in range(n):
                if has_mask and col._null_mask[i]:
                    result.append(DFScalar.null())
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

        Index labels are stringified.  Null values are represented as
        ``DFScalar.null()``.  Object-dtype cells are stringified.
        """
        var result = Dict[String, DFScalar]()
        ref col = self._col
        var has_index = col._index_len() > 0
        var n = col.__len__()
        for i in range(n):
            var key = col._index_label(i) if has_index else String(i)
            result[key] = _scalar_from_col(col, i)
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
        var has_index = col._index_len() > 0
        var n = col.__len__()
        for i in range(n):
            var key = col._index_label(i) if has_index else String(i)
            var val = _col_cell_str(col, i)
            result += (
                _csv_quote_field(key, ",")
                + ","
                + _csv_quote_field(val, ",")
                + "\n"
            )
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
        var has_index = col._index_len() > 0
        var n = col.__len__()
        var py_dict = Python.evaluate("{}")
        for i in range(n):
            var key = col._index_label(i) if has_index else String(i)
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
        if self._col.dtype != datetime64_ns:
            raise Error("Series.dt: accessor requires a datetime Series")
        ref d = self._col._data[List[PythonObject]]
        var data = d.copy()
        var null_mask = self._col._null_mask.copy()
        return DatetimeMethods(data^, null_mask^, self._col.name)

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) raises -> String:
        return String(self.to_pandas())

    def __len__(self) -> Int:
        return self._col.__len__()

    def groupby(
        self,
        by: List[String],
        as_index: Bool = True,
        sort: Bool = True,
        dropna: Bool = True,
    ) raises -> SeriesGroupBy:
        return SeriesGroupBy(self, by, as_index, sort, dropna)


# ------------------------------------------------------------------
# Shared string-list utilities
# ------------------------------------------------------------------


def _frame_cell_as_python(col: Column, row: Int) raises -> PythonObject:
    """Return the value at *row* in *col* as a PythonObject.

    Null cells return Python ``None``.  Used by the native reshaping methods
    (transpose, melt, pivot, stack, explode) to avoid repeated isa chains.
    """
    return _col_cell_pyobj(col, row)


def _frame_cell_as_str(col: Column, row: Int) raises -> String:
    """Return the value at *row* in *col* as a String key.

    Used by ``DataFrame.pivot`` to build row/column key dictionaries.
    """
    return _col_cell_str(col, row)


def _html_escape(s: String) -> String:
    """Return *s* with HTML special characters replaced by entities.

    Handles ``&``, ``<``, ``>``, ``"``, and ``'``.
    """
    var result = s.replace("&", "&amp;")
    result = result.replace("<", "&lt;")
    result = result.replace(">", "&gt;")
    result = result.replace('"', "&quot;")
    result = result.replace("'", "&#39;")
    return result^


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
    """A two-dimensional labeled data structure, mirroring the pandas DataFrame API.
    """

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
        """Convenience constructor: wraps a pandas DataFrame via Column.from_pandas.
        """
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
        """Create DataFrame from a native dict mapping column names to column data.
        """
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
        """Create DataFrame from a list of row dicts (row-to-column transposition).
        """
        if len(records) == 0:
            return DataFrame()

        # Determine column names
        var col_names: List[String]
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
                        pass
                    elif v.isa[String]():
                        has_string = True
                    # _Null arm: contributes no type info, but row exists
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
                        if v.is_null():
                            data.append(String(""))
                            null_mask.append(True)
                            continue
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
                        if v.is_null():
                            data.append(Float64(0.0))
                            null_mask.append(True)
                            continue
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
                        if v.is_null():
                            data.append(Int64(0))
                            null_mask.append(True)
                            has_nulls = True
                            continue
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
                        if v.is_null():
                            data.append(False)
                            null_mask.append(True)
                            has_nulls = True
                            continue
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
        """Return a Series with the dtype of each column, indexed by column name.
        """
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
            total_bytes += Int64(
                len(self._cols[i]) * self._cols[i].dtype.itemsize
            )
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
            values.append(
                Int64(len(self._cols[i]) * self._cols[i].dtype.itemsize)
            )
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

    def get(
        self, key: String, default: Optional[Series] = None
    ) -> Optional[Series]:
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
        replace: Bool = False,
        random_state: Optional[Int] = None,
    ) raises -> DataFrame:
        """Return a random sample of rows.

        When *replace* is ``False`` (default) rows are sampled without
        replacement using a Fisher-Yates shuffle; *n* is capped at the number
        of rows.  When *replace* is ``True`` rows may repeat and *n* may
        exceed the number of rows.

        Uses a 32-bit xorshift PRNG.  Pass *random_state* (any non-zero
        integer) for reproducibility.
        """
        var nrows = self.shape()[0]
        if nrows == 0:
            return DataFrame()
        var take: Int
        if frac:
            take = Int(Float64(nrows) * frac.value())
        else:
            take = n
        # xorshift32 PRNG state
        var state: Int = 1
        if random_state:
            state = random_state.value()
        if state == 0:
            state = 1  # xorshift must not start at 0
        var selected = List[Int]()
        if replace:
            # Independent draws — each row may appear more than once.
            for _ in range(take):
                state = state ^ (state << 13)
                state = state ^ (state >> 17)
                state = state ^ (state << 5)
                state = state & 0x7FFFFFFF
                selected.append(state % nrows)
        else:
            if take > nrows:
                take = nrows
            # Fisher-Yates partial shuffle for sampling without replacement.
            var indices = List[Int]()
            for i in range(nrows):
                indices.append(i)
            for i in range(take):
                state = state ^ (state << 13)
                state = state ^ (state >> 17)
                state = state ^ (state << 5)
                state = state & 0x7FFFFFFF
                var j = i + (state % (nrows - i))
                var tmp = indices[i]
                indices[i] = indices[j]
                indices[j] = tmp
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
        """Select columns (axis=1) or rows (axis=0) by label.

        *items*: keep only labels in the list.
        *like*:  keep labels whose name contains the substring.
        *regex*: keep labels whose name matches the pattern (uses Python re).
        """
        if axis == 0:
            # Row filtering by index label.
            if len(self._cols) == 0:
                return self.copy()
            var n_rows = self._cols[0].__len__()
            var has_index = self._has_index()
            var keep_rows = List[Int]()
            for i in range(n_rows):
                var label = self._cols[0]._index_label(
                    i
                ) if has_index else String(i)
                var keep = False
                if items:
                    ref items_list = items.value()
                    for j in range(len(items_list)):
                        if label == items_list[j]:
                            keep = True
                            break
                elif like != "":
                    keep = label.find(like) != -1
                elif regex != "":
                    var re_mod = Python.import_module("re")
                    keep = Bool(re_mod.search(regex, label).__bool__())
                else:
                    keep = True
                if keep:
                    keep_rows.append(i)
            var result_cols = List[Column]()
            for ci in range(len(self._cols)):
                result_cols.append(self._cols[ci].take(keep_rows))
            return DataFrame(result_cols^)
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
            _not_implemented("DataFrame.sum")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].sum(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def mean(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.mean")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].mean(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def median(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.median")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].median(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def min(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.min")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].min(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def max(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.max")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].max(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def std(
        self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True
    ) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.std")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].std(ddof, skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def var(
        self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True
    ) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.var")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].var(ddof, skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def count(self, axis: Int = 0) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.count")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(Float64(self._cols[i].count()))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def nunique(self, axis: Int = 0) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.nunique")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(Float64(self._cols[i].nunique()))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def describe(
        self,
        include: Optional[List[String]] = None,
        exclude: Optional[List[String]] = None,
    ) raises -> DataFrame:
        _not_implemented("DataFrame.describe")
        return DataFrame()

    def quantile(
        self, q: Float64 = 0.5, axis: Int = 0, skipna: Bool = True
    ) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.quantile")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].quantile(q, skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column("", col_data^, dtype)
        return Series(result_col^)

    def abs(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i]._abs())
        return DataFrame(result_cols^)

    def cumsum(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis != 0:
            _not_implemented("DataFrame.cumsum")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cumsum(skipna))
        return DataFrame(result_cols^)

    def cumprod(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis != 0:
            _not_implemented("DataFrame.cumprod")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cumprod(skipna))
        return DataFrame(result_cols^)

    def cummin(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis != 0:
            _not_implemented("DataFrame.cummin")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cummin(skipna))
        return DataFrame(result_cols^)

    def cummax(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis != 0:
            _not_implemented("DataFrame.cummax")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cummax(skipna))
        return DataFrame(result_cols^)

    def sem(
        self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True
    ) raises -> Series:
        _not_implemented("DataFrame.sem")
        return Series()

    def skew(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.skew")
        return Series()

    def kurt(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.kurt")
        return Series()

    def idxmin(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.idxmin")
        return Series()

    def idxmax(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.idxmax")
        return Series()

    def corr(
        self, method: String = "pearson", min_periods: Int = 1
    ) raises -> DataFrame:
        _not_implemented("DataFrame.corr")
        return DataFrame()

    def cov(self, min_periods: Int = 1, ddof: Int = 1) raises -> DataFrame:
        _not_implemented("DataFrame.cov")
        return DataFrame()

    def shift(self, periods: Int = 1, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.shift")
        return DataFrame()

    def diff(self, periods: Int = 1, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.diff")
        return DataFrame()

    def pct_change(self, periods: Int = 1, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.pct_change")
        return DataFrame()

    def agg(self, func: String, axis: Int = 0) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.agg")
        if func == "sum":
            return self.sum()
        elif func == "mean":
            return self.mean()
        elif func == "median":
            return self.median()
        elif func == "min":
            return self.min()
        elif func == "max":
            return self.max()
        elif func == "std":
            return self.std()
        elif func == "var":
            return self.var()
        elif func == "count":
            return self.count()
        elif func == "nunique":
            return self.nunique()
        else:
            raise Error(
                "DataFrame.agg: unsupported aggregation '"
                + func
                + "'. Supported: sum, mean, median, min, max, std, var, count,"
                " nunique"
            )

    def aggregate(self, func: String, axis: Int = 0) raises -> Series:
        return self.agg(func, axis)

    def apply(self, func: String, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.apply")
        return DataFrame()

    def applymap(self, func: String) raises -> DataFrame:
        _not_implemented("DataFrame.applymap")
        return DataFrame()

    def transform(self, func: String, axis: Int = 0) raises -> DataFrame:
        if axis != 0:
            _not_implemented("DataFrame.transform")
        if func == "abs":
            return self.abs()
        elif func == "cumsum":
            return self.cumsum()
        elif func == "cumprod":
            return self.cumprod()
        elif func == "cummin":
            return self.cummin()
        elif func == "cummax":
            return self.cummax()
        else:
            raise Error(
                "DataFrame.transform: unsupported func '"
                + func
                + "'. Supported: abs, cumsum, cumprod, cummin, cummax"
            )

    def eval(self, expr: String) raises -> Series:
        var pd_df = self.to_pandas()
        # Use module-level pd.eval() with an explicit local_dict so that pandas
        # skips its sys._getframe() caller-scope resolution, which fails when
        # called from Mojo's shallow Python call stack.
        var eval_fn = Python.evaluate(
            "lambda df, e: __import__('pandas').eval("
            "e, local_dict={c: df[c] for c in df.columns}, engine='python')"
        )
        var result = eval_fn(pd_df, expr)
        return Series.from_pandas(result)

    def query(self, expr: String) raises -> DataFrame:
        var pd_df = self.to_pandas()
        # Filter via module-level pd.eval() with explicit local_dict to avoid
        # sys._getframe() failures when called from Mojo's shallow call stack.
        var query_fn = Python.evaluate(
            "lambda df, e: df.loc["
            "__import__('pandas').eval("
            "e, local_dict={c: df[c] for c in df.columns}, engine='python')]"
        )
        var result = query_fn(pd_df, expr)
        return DataFrame.from_pandas(result)

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
        """Return a boolean DataFrame that is True where values are not null/NaN.
        """
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

    def interpolate(
        self, method: String = "linear", axis: Int = 0
    ) raises -> DataFrame:
        """Fill null values using linear interpolation (Float64 columns only).

        Non-numeric columns are returned unchanged.
        Only method="linear" is supported natively.
        """
        if method != "linear":
            raise Error(
                "DataFrame.interpolate: method='"
                + method
                + "' is not supported; only 'linear' is supported"
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
            var new_col = Column(col.name, col_data^, col.dtype, col._index)
            result_cols.append(new_col^)
        return DataFrame(result_cols^)

    # ------------------------------------------------------------------
    # Reshaping / sorting
    # ------------------------------------------------------------------

    def sort_values(
        self,
        by: List[String],
        ascending: Bool = True,
        na_position: String = "last",
    ) raises -> DataFrame:
        """Return a new DataFrame sorted by one or more columns.

        Rows are reordered so that the values in the first ``by`` column are
        sorted (ascending or descending).  Ties in the primary key are broken
        by subsequent keys using a stable insertion sort applied in reverse
        key order.  Null elements are placed at the end when na_position is
        ``"last"`` (default) or at the beginning when na_position is
        ``"first"``.
        """
        var asc = List[Bool]()
        asc.append(ascending)
        return self._sort_values_impl(by, asc, na_position)

    def sort_values(
        self,
        by: List[String],
        ascending: List[Bool],
        na_position: String = "last",
    ) raises -> DataFrame:
        """Return a new DataFrame sorted by one or more columns.

        ``ascending`` may be a list with one entry per key in ``by``,
        allowing each key column to be sorted independently.  If the list is
        shorter than ``by`` the remaining keys default to ascending order.
        """
        return self._sort_values_impl(by, ascending, na_position)

    def _sort_values_impl(
        self, by: List[String], ascending: List[Bool], na_position: String
    ) raises -> DataFrame:
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
            var asc = ascending[k] if k < len(ascending) else True
            var key_col = self[by[k]]
            var sub_perm = Series(key_col._col.take(perm))._sort_perm(
                asc, na_position == "last"
            )
            var new_perm = List[Int]()
            for j in range(n_rows):
                new_perm.append(perm[sub_perm[j]])
            perm = new_perm^
            k -= 1

        # Reorder index labels (parallel to the data).
        var has_index = self._has_index()
        var new_idx: ColumnIndex
        if has_index:
            new_idx = self._cols[0]._index_reorder(perm)
        else:
            new_idx = ColumnIndex(List[PythonObject]())

        # Apply permutation to every column.
        var new_cols = List[Column]()
        for i in range(len(self._cols)):
            var taken = self._cols[i].take(perm)
            if has_index:
                taken._index = new_idx
            new_cols.append(taken^)
        return DataFrame(new_cols^)

    def sort_index(
        self, axis: Int = 0, ascending: Bool = True
    ) raises -> DataFrame:
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
        if self._cols[0]._index_len() == 0:
            # Default RangeIndex — ascending is already sorted.
            if ascending:
                return DataFrame(self._cols.copy())
            for i in range(n_rows):
                perm.append(n_rows - 1 - i)
        else:
            # Explicit index: use the Column helper (dispatches on index arm).
            perm = self._cols[0]._sort_perm_by_index(ascending)

        # Reorder index labels and apply permutation to every column.
        var has_index = self._has_index()
        var new_idx: ColumnIndex
        if has_index:
            new_idx = self._cols[0]._index_reorder(perm)
        else:
            new_idx = ColumnIndex(List[PythonObject]())
        var new_cols = List[Column]()
        for i in range(len(self._cols)):
            var taken = self._cols[i].take(perm)
            if has_index:
                taken._index = new_idx
            new_cols.append(taken^)
        return DataFrame(new_cols^)

    def reset_index(self, drop: Bool = False) raises -> DataFrame:
        """Replace the row index with a default RangeIndex.

        When ``drop=True`` the existing index labels are discarded.
        When ``drop=False`` (default) the existing index is promoted to new
        column(s) prepended to the result, and the row index is then cleared to
        a default RangeIndex.  On a DataFrame that already has a default
        RangeIndex, both modes simply return an identical copy.

        For a scalar index (single-key ``set_index``), a single column named
        ``"index"`` is prepended.  For a MultiIndex created by a multi-key
        ``set_index``, one column per level is prepended using the original key
        column names stored in ``Column._index_names``.
        """
        var ncols = len(self._cols)
        if ncols == 0:
            return DataFrame()
        var n_idx = self._cols[0]._index_len()
        var has_index = n_idx > 0
        var new_cols = List[Column]()
        if not drop and has_index:
            var empty_col_idx = ColumnIndex(List[PythonObject]())
            if self._cols[0]._index.isa[Index]():
                ref str_idx = self._cols[0]._index[Index]
                var str_data = List[String]()
                for i in range(n_idx):
                    str_data.append(str_idx[i])
                new_cols.append(
                    Column(
                        "index", ColumnData(str_data^), object_, empty_col_idx^
                    )
                )
            elif self._cols[0]._index.isa[List[Int64]]():
                ref int_idx = self._cols[0]._index[List[Int64]]
                var int_data = List[Int64]()
                for i in range(n_idx):
                    int_data.append(int_idx[i])
                new_cols.append(
                    Column(
                        "index", ColumnData(int_data^), int64, empty_col_idx^
                    )
                )
            else:
                ref obj_idx = self._cols[0]._index[List[PythonObject]]
                ref idx_names = self._cols[0]._index_names
                var n_levels = len(idx_names)
                if n_levels > 1:
                    # MultiIndex: expand each tuple level to its own column.
                    for k in range(n_levels):
                        var level_data = List[PythonObject]()
                        for i in range(n_idx):
                            level_data.append(obj_idx[i].__getitem__(k))
                        var empty2 = ColumnIndex(List[PythonObject]())
                        new_cols.append(
                            Column(
                                idx_names[k],
                                ColumnData(level_data^),
                                object_,
                                empty2^,
                            )
                        )
                else:
                    # Single PythonObject index (e.g. float, datetime).
                    var obj_data = List[PythonObject]()
                    for i in range(n_idx):
                        obj_data.append(obj_idx[i])
                    var empty2 = ColumnIndex(List[PythonObject]())
                    new_cols.append(
                        Column("index", ColumnData(obj_data^), object_, empty2^)
                    )
        for i in range(ncols):
            var c = self._cols[i].copy()
            c._index = ColumnIndex(List[PythonObject]())
            c._index_names = List[String]()
            new_cols.append(c^)
        return DataFrame(new_cols^)

    def set_index(
        self, keys: List[String], drop: Bool = True
    ) raises -> DataFrame:
        """Promote one or more columns to the row index.

        When ``keys`` contains a single column name the index is stored as a
        typed ``ColumnIndex`` (``Index`` for strings, ``List[Int64]`` for
        integers, ``List[PythonObject]`` for other types).

        When ``keys`` contains more than one column name a MultiIndex is created:
        each row's index label is a Python tuple ``(key0_val, key1_val, ...)``,
        stored as a ``List[PythonObject]`` ``ColumnIndex``.  The level names are
        stored in ``Column._index_names`` so that ``reset_index`` can expand
        them back to individual columns.

        When ``drop=True`` (default) the key column(s) are removed from the
        result columns.
        """
        if len(keys) == 0:
            raise Error("DataFrame.set_index: keys must not be empty")
        if len(keys) == 1:
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
            var new_idx = self._cols[key_col_idx]._to_column_index()
            # Build result columns (skip key column when drop=True).
            var new_cols = List[Column]()
            for i in range(len(self._cols)):
                if drop and i == key_col_idx:
                    continue
                var c = self._cols[i].copy()
                c._index = new_idx
                c._index_names = List[String]()
                new_cols.append(c^)
            return DataFrame(new_cols^)
        # --- MultiIndex: len(keys) > 1 ---
        # Validate all keys exist.
        var key_col_indices = List[Int]()
        for k in range(len(keys)):
            var found: Int = -1
            for i in range(len(self._cols)):
                if self._cols[i].name == keys[k]:
                    found = i
                    break
            if found == -1:
                raise Error("DataFrame.set_index: column not found: " + keys[k])
            key_col_indices.append(found)
        # For each key column, extract values as a List[PythonObject].
        var n_rows = self.shape()[0]
        var key_pyobj_lists = List[List[PythonObject]]()
        for k in range(len(keys)):
            var pyobj_list = self._cols[key_col_indices[k]]._to_pyobj_index()
            key_pyobj_lists.append(pyobj_list^)
        # Build a List[PythonObject] index where each entry is a Python tuple.
        var builtins = Python.import_module("builtins")
        var multi_idx = List[PythonObject]()
        for i in range(n_rows):
            var items = builtins.list()
            for k in range(len(keys)):
                _ = items.append(key_pyobj_lists[k][i])
            multi_idx.append(builtins.tuple(items))
        var new_idx = ColumnIndex(multi_idx^)
        # Build result columns, storing level names in _index_names.
        # key_col_set acts as a membership set (value is unused).
        var key_col_set = Dict[Int, Bool]()
        for k in range(len(key_col_indices)):
            key_col_set[key_col_indices[k]] = True
        var new_cols = List[Column]()
        for i in range(len(self._cols)):
            if drop and i in key_col_set:
                continue
            var c = self._cols[i].copy()
            c._index = new_idx
            c._index_names = keys.copy()
            new_cols.append(c^)
        return DataFrame(new_cols^)

    def rename(
        self,
        columns: Optional[Dict[String, String]] = None,
        index: Optional[Dict[String, String]] = None,
    ) raises -> DataFrame:
        """Rename column labels and/or row index labels.

        ``columns`` maps old column names to new ones; missing keys are left
        unchanged.  ``index`` maps old index label strings to new ones; missing
        keys are left unchanged.  Both can be applied in the same call.
        """
        var new_cols = List[Column]()
        for i in range(len(self._cols)):
            var c = self._cols[i].copy()
            if columns:
                ref col_map = columns.value()
                if c.name in col_map:
                    c.name = col_map[c.name]
            if index and c._index_len() > 0:
                ref idx_map = index.value()
                # Rename only supports string index arms natively.  For
                # other arms fall back to string conversion.
                var n_idx = c._index_len()
                if c._index.isa[Index]():
                    ref old = c._index[Index]
                    var new_labels = List[String]()
                    for k in range(n_idx):
                        var lbl = old[k]
                        if lbl in idx_map:
                            new_labels.append(idx_map[lbl])
                        else:
                            new_labels.append(lbl)
                    c._index = ColumnIndex(Index(new_labels^))
                elif c._index.isa[List[Int64]]():
                    ref old = c._index[List[Int64]]
                    var new_ints = List[Int64]()
                    for k in range(n_idx):
                        var lbl = String(Int(old[k]))
                        if lbl in idx_map:
                            new_ints.append(Int64(atol(idx_map[lbl])))
                        else:
                            new_ints.append(old[k])
                    c._index = ColumnIndex(new_ints^)
                else:
                    # PythonObject fallback: stringify labels.
                    var builtins = Python.import_module("builtins")
                    ref old = c._index[List[PythonObject]]
                    var new_objs = List[PythonObject]()
                    for k in range(n_idx):
                        var lbl = String(old[k])
                        if lbl in idx_map:
                            new_objs.append(builtins.str(idx_map[lbl]))
                        else:
                            new_objs.append(old[k])
                    c._index = ColumnIndex(new_objs^)
            new_cols.append(c^)
        return DataFrame(new_cols^)

    def rename_axis(
        self, mapper: Optional[String] = None, axis: Int = 0
    ) raises -> DataFrame:
        """Return a copy with the row-index name set to *mapper*.

        ``axis=0`` (default) sets the row-index name on every column.
        ``axis=1`` sets the column-axis name (stored as a DataFrame-level
        attribute; no-op for now since bison does not yet expose column-axis
        names).  Passing ``mapper=None`` clears the current name.
        """
        var result = self._deep_copy()
        if axis == 0:
            var new_name = String("")
            if mapper:
                new_name = mapper.value()
            for i in range(len(result._cols)):
                result._cols[i]._index_name = new_name
        return result^

    def reindex(
        self,
        labels: Optional[List[String]] = None,
        axis: Int = 0,
        fill_value: Optional[DFScalar] = None,
    ) raises -> DataFrame:
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
            var shared_idx = ColumnIndex(List[PythonObject]())
            if ncols > 0:
                shared_idx = self._cols[0]._index
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
                        var c = Column._fill_scalar(
                            lbl, fill_value.value(), nrows, shared_idx
                        )
                        new_cols.append(c^)
                    else:
                        # Null column: infer dtype from existing columns so
                        # that a frame of all-int64 columns produces an int64
                        # null column rather than float64.
                        var c = Column._null_column(
                            lbl, inferred_dtype, nrows, shared_idx
                        )
                        new_cols.append(c^)
            return DataFrame(new_cols^)
        else:
            # Row reindex: reorder and/or fill new rows.
            if ncols == 0:
                return DataFrame()
            var nrows = self.shape()[0]
            var has_index = self._has_index()
            # Build label → row-position map.
            var label_to_row = Dict[String, Int]()
            for i in range(nrows):
                var key = self._cols[0]._index_label(
                    i
                ) if has_index else String(i)
                label_to_row[key] = i
            # Build the per-output-row index list (row_indices) and new _index.
            var row_indices = List[Int]()
            var new_str_idx = List[String]()
            for k in range(len(new_labels)):
                var lbl = new_labels[k]
                if lbl in label_to_row:
                    var src_row = label_to_row[lbl]
                    row_indices.append(src_row)
                    if has_index:
                        new_str_idx.append(self._cols[0]._index_label(src_row))
                    else:
                        new_str_idx.append(String(src_row))
                else:
                    row_indices.append(-1)
                    new_str_idx.append(lbl)
            var new_col_idx = ColumnIndex(Index(new_str_idx^))
            # Reindex each column.
            var new_cols = List[Column]()
            for i in range(ncols):
                var c = self._cols[i]._reindex_rows(row_indices, fill_value)
                c._index = new_col_idx
                new_cols.append(c^)
            return DataFrame(new_cols^)

    def drop(
        self,
        labels: Optional[List[String]] = None,
        axis: Int = 0,
        columns: Optional[List[String]] = None,
    ) raises -> DataFrame:
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
                    raise Error(
                        "DataFrame.drop: column not found: " + drop_labels[i]
                    )
            # Collect surviving columns.
            var result_cols = List[Column]()
            for i in range(ncols):
                if self._cols[i].name not in drop_set:
                    result_cols.append(self._cols[i].copy())
            return DataFrame(result_cols^)
        else:
            # Row drop: match drop_labels against the index.
            var nrows = self.shape()[0]
            var has_index = self._has_index()
            var drop_set = Dict[String, Bool]()
            for i in range(len(drop_labels)):
                drop_set[drop_labels[i]] = True
            var keep_indices = List[Int]()
            for i in range(nrows):
                var key = self._cols[0]._index_label(
                    i
                ) if has_index else String(i)
                if key not in drop_set:
                    keep_indices.append(i)
            # Verify all requested labels were found.
            var found_set = Dict[String, Bool]()
            for i in range(nrows):
                var key = self._cols[0]._index_label(
                    i
                ) if has_index else String(i)
                if key in drop_set:
                    found_set[key] = True
            for i in range(len(drop_labels)):
                if drop_labels[i] not in found_set:
                    raise Error(
                        "DataFrame.drop: index label not found: "
                        + drop_labels[i]
                    )
            var result_cols = List[Column]()
            for i in range(ncols):
                result_cols.append(self._cols[i].take(keep_indices))
            return DataFrame(result_cols^)

    def drop_duplicates(
        self, subset: Optional[List[String]] = None, keep: String = "first"
    ) raises -> DataFrame:
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

    def duplicated(
        self, subset: Optional[List[String]] = None, keep: String = "first"
    ) raises -> Series:
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
                    raise Error(
                        "DataFrame.duplicated: column not found in subset: "
                        + name
                    )
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
            raise Error(
                "DataFrame.duplicated: keep must be 'first', 'last', or 'False'"
            )

        var col = Column("", ColumnData(result^), bool_)
        return Series(col^)

    def pivot(
        self, index: String = "", columns: String = "", values: String = ""
    ) raises -> DataFrame:
        """Reshape from long to wide format.

        Each unique value in *index* becomes a row, each unique value in
        *columns* becomes a column, and the corresponding *values* cell fills
        each intersection.  Missing intersections are ``None`` / null.
        Raises if *index*, *columns*, or *values* is not a column name, or if
        any (index, columns) pair appears more than once.
        """
        # Locate the three columns.
        var idx_ci = -1
        var col_ci = -1
        var val_ci = -1
        for j in range(len(self._cols)):
            if self._cols[j].name == index:
                idx_ci = j
            if self._cols[j].name == columns:
                col_ci = j
            if self._cols[j].name == values:
                val_ci = j
        if idx_ci == -1:
            raise Error("DataFrame.pivot: index column not found: " + index)
        if col_ci == -1:
            raise Error("DataFrame.pivot: columns column not found: " + columns)
        if val_ci == -1:
            raise Error("DataFrame.pivot: values column not found: " + values)

        var nrows = self.shape()[0]
        var py_none = Python.evaluate("None")

        # Collect unique row-keys (preserve insertion order).
        var row_keys = List[String]()
        var seen_rows = Dict[String, Int]()
        for r in range(nrows):
            var k = _frame_cell_as_str(self._cols[idx_ci], r)
            if k not in seen_rows:
                seen_rows[k] = len(row_keys)
                row_keys.append(k)

        # Collect unique column-keys (preserve insertion order).
        var col_keys = List[String]()
        var seen_cols = Dict[String, Int]()
        for r in range(nrows):
            var k = _frame_cell_as_str(self._cols[col_ci], r)
            if k not in seen_cols:
                seen_cols[k] = len(col_keys)
                col_keys.append(k)

        var n_rk = len(row_keys)
        var n_ck = len(col_keys)

        # Build a dense values table (row_key × col_key).
        # _table[rk][ck] holds a PythonObject value or Python None.
        # We also track which cells were filled to detect duplicates.
        var table = List[List[PythonObject]]()
        var filled = List[List[Bool]]()
        for _ in range(n_rk):
            var row_data = List[PythonObject]()
            var row_filled = List[Bool]()
            for _ in range(n_ck):
                row_data.append(py_none)
                row_filled.append(False)
            table.append(row_data^)
            filled.append(row_filled^)

        for r in range(nrows):
            var rk = seen_rows[_frame_cell_as_str(self._cols[idx_ci], r)]
            var ck = seen_cols[_frame_cell_as_str(self._cols[col_ci], r)]
            if filled[rk][ck]:
                raise Error(
                    "DataFrame.pivot: duplicate entry for ("
                    + row_keys[rk]
                    + ", "
                    + col_keys[ck]
                    + ")"
                )
            table[rk][ck] = _frame_cell_as_python(self._cols[val_ci], r)
            filled[rk][ck] = True

        # Construct index labels (string Index) shared by all result columns.
        var result_idx = ColumnIndex(Index(row_keys^))

        # Build one output Column per col_key.
        var result_cols = List[Column]()
        for ck in range(n_ck):
            var data = List[PythonObject]()
            var null_mask = List[Bool]()
            var any_null = False
            for rk in range(n_rk):
                if not filled[rk][ck]:
                    data.append(py_none)
                    null_mask.append(True)
                    any_null = True
                else:
                    data.append(table[rk][ck])
                    null_mask.append(False)
            var col = Column(col_keys[ck], ColumnData(data^), object_)
            col._index = result_idx
            if any_null:
                col._null_mask = null_mask^
            result_cols.append(col^)

        return DataFrame(result_cols^)

    def pivot_table(
        self,
        values: Optional[List[String]] = None,
        index: Optional[List[String]] = None,
        columns: Optional[List[String]] = None,
        aggfunc: String = "mean",
    ) raises -> DataFrame:
        _not_implemented("DataFrame.pivot_table")
        return DataFrame()

    def melt(
        self,
        id_vars: Optional[List[String]] = None,
        value_vars: Optional[List[String]] = None,
        var_name: String = "variable",
        value_name: String = "value",
    ) raises -> DataFrame:
        """Unpivot a DataFrame from wide to long format.

        *id_vars*: columns to keep as identifier variables (repeated).
        *value_vars*: columns to unpivot into rows (default: all non-id cols).
        *var_name*: name for the new column holding original column names.
        *value_name*: name for the new column holding the cell values.
        """
        var nrows = self.shape()[0]
        var py_none = Python.evaluate("None")

        # Resolve id_vars.
        var id_names = List[String]()
        if id_vars:
            id_names = id_vars.value().copy()

        # Resolve value_vars (all non-id columns by default).
        var val_names = List[String]()
        if value_vars:
            val_names = value_vars.value().copy()
        else:
            for j in range(len(self._cols)):
                var in_id = False
                for k in range(len(id_names)):
                    if self._cols[j].name == id_names[k]:
                        in_id = True
                        break
                if not in_id:
                    val_names.append(self._cols[j].name)

        var n_val = len(val_names)
        var result_cols = List[Column]()

        # ID columns: repeat each id column n_val times (interleaved by row).
        for k in range(len(id_names)):
            var id_ci = -1
            for j in range(len(self._cols)):
                if self._cols[j].name == id_names[k]:
                    id_ci = j
                    break
            if id_ci == -1:
                raise Error(
                    "DataFrame.melt: id column not found: " + id_names[k]
                )
            var indices = List[Int]()
            for _ in range(n_val):
                for r in range(nrows):
                    indices.append(r)
            var new_col = self._cols[id_ci].take(indices)
            new_col.name = id_names[k]
            new_col._index = ColumnIndex(List[PythonObject]())
            result_cols.append(new_col^)

        # Variable column: for each value column, repeat its name nrows times.
        var var_data = List[String]()
        for v in range(n_val):
            for _ in range(nrows):
                var_data.append(val_names[v])
        result_cols.append(Column(var_name, ColumnData(var_data^), object_))

        # Value column: concat all value columns row-by-row.
        var val_data = List[PythonObject]()
        var val_null_mask = List[Bool]()
        var any_null = False
        for v in range(n_val):
            var val_ci = -1
            for j in range(len(self._cols)):
                if self._cols[j].name == val_names[v]:
                    val_ci = j
                    break
            if val_ci == -1:
                raise Error(
                    "DataFrame.melt: value column not found: " + val_names[v]
                )
            ref vcol = self._cols[val_ci]
            for r in range(nrows):
                var is_null = len(vcol._null_mask) > 0 and vcol._null_mask[r]
                if is_null:
                    val_data.append(py_none)
                    val_null_mask.append(True)
                    any_null = True
                else:
                    val_null_mask.append(False)
                    val_data.append(_frame_cell_as_python(vcol, r))
        var val_col = Column(value_name, ColumnData(val_data^), object_)
        if any_null:
            val_col._null_mask = val_null_mask^
        result_cols.append(val_col^)

        return DataFrame(result_cols^)

    def stack(self, level: Int = -1) raises -> Series:
        """Pivot column labels to the innermost row index level.

        Returns a Series whose index is a MultiIndex of
        (original_row_label, column_name) tuples.  All cell values are
        converted to Python objects, so the result uses ``object`` dtype.
        Only level=-1 (the default, single column level) is supported.
        """
        var ncols = len(self._cols)
        if ncols == 0:
            return Series()
        var nrows = self.shape()[0]
        var py = Python.import_module("builtins")
        var py_none = Python.evaluate("None")

        var val_data = List[PythonObject]()
        var null_mask = List[Bool]()
        var idx_objs = List[PythonObject]()
        var any_null = False

        for r in range(nrows):
            # Determine row label.
            var row_label: PythonObject
            if self._has_index():
                row_label = PythonObject(self._cols[0]._index_label(r))
            else:
                row_label = PythonObject(r)
            for j in range(ncols):
                ref col = self._cols[j]
                var is_null = len(col._null_mask) > 0 and col._null_mask[r]
                var tup_items = py.list()
                _ = tup_items.append(row_label)
                _ = tup_items.append(PythonObject(col.name))
                var tup = py.tuple(tup_items)
                idx_objs.append(tup)
                if is_null:
                    val_data.append(py_none)
                    null_mask.append(True)
                    any_null = True
                else:
                    null_mask.append(False)
                    val_data.append(_frame_cell_as_python(col, r))

        var result_col = Column(
            "", ColumnData(val_data^), object_, ColumnIndex(idx_objs^)
        )
        if any_null:
            result_col._null_mask = null_mask^
        return Series(result_col^)

    def unstack(self, level: Int = -1) raises -> DataFrame:
        _not_implemented("DataFrame.unstack")
        return DataFrame()

    def transpose(self) raises -> DataFrame:
        """Transpose rows and columns.

        Returns a new DataFrame where each original column becomes a row and
        each original row becomes a column.  Because rows may contain mixed
        types the result always uses ``object`` dtype.  Column names of the
        result are the original row-index labels; the row index of the result
        holds the original column names.
        """
        var ncols = len(self._cols)
        if ncols == 0:
            return DataFrame()
        var nrows = self.shape()[0]
        var py_none = Python.evaluate("None")

        # Index shared by all result columns: the original column names.
        var orig_col_names = List[String]()
        for j in range(ncols):
            orig_col_names.append(self._cols[j].name)
        var shared_idx = ColumnIndex(Index(orig_col_names^))

        var result_cols = List[Column]()
        for r in range(nrows):
            # Result column name = original row-index label.
            var col_name: String
            if self._has_index():
                col_name = self._cols[0]._index_label(r)
            else:
                col_name = String(r)

            var data = List[PythonObject]()
            var null_mask = List[Bool]()
            var any_null = False
            for j in range(ncols):
                ref col = self._cols[j]
                var is_null = len(col._null_mask) > 0 and col._null_mask[r]
                if is_null:
                    data.append(py_none)
                    null_mask.append(True)
                    any_null = True
                else:
                    null_mask.append(False)
                    data.append(_frame_cell_as_python(col, r))

            var new_col = Column(col_name, ColumnData(data^), object_)
            new_col._index = shared_idx
            if any_null:
                new_col._null_mask = null_mask^
            result_cols.append(new_col^)

        return DataFrame(result_cols^)

    def T(self) raises -> DataFrame:
        """Transpose rows and columns (alias for ``transpose()``)."""
        return self.transpose()

    def swaplevel(
        self, i: Int = -2, j: Int = -1, axis: Int = 0
    ) raises -> DataFrame:
        _not_implemented("DataFrame.swaplevel")
        return DataFrame()

    def explode(self, column: String) raises -> DataFrame:
        """Expand list-like values in *column* into separate rows.

        Each element of a list-like cell in *column* becomes its own row.
        All other columns have their values repeated once per element.
        Scalar (non-list) cells in *column* are kept as single rows.
        """
        var col_ci = -1
        for j in range(len(self._cols)):
            if self._cols[j].name == column:
                col_ci = j
                break
        if col_ci == -1:
            raise Error("DataFrame.explode: column not found: " + column)

        var nrows = self.shape()[0]
        var py = Python.import_module("builtins")
        var py_none = Python.evaluate("None")

        # First pass: build the expanded row indices and sub-indices.
        # For each original row r: if cell is list-like, expand; else keep once.
        var src_indices = List[Int]()  # source row for each output row
        var sub_indices = List[
            Int
        ]()  # position within expanded cell (-1 = scalar)
        ref exp_col = self._cols[col_ci]
        for r in range(nrows):
            var is_null = len(exp_col._null_mask) > 0 and exp_col._null_mask[r]
            if is_null:
                src_indices.append(r)
                sub_indices.append(-1)
                continue
            # Try to treat the cell as an iterable list.
            var expanded = False
            if exp_col._data.isa[List[PythonObject]]():
                var cell = exp_col._data[List[PythonObject]][r]
                try:
                    var cell_len = Int(cell.__len__())
                    for sub in range(cell_len):
                        src_indices.append(r)
                        sub_indices.append(sub)
                    expanded = True
                except:
                    pass
            if not expanded:
                src_indices.append(r)
                sub_indices.append(-1)

        var n_out = len(src_indices)

        # Build each output column.
        var result_cols = List[Column]()
        for j in range(len(self._cols)):
            if j == col_ci:
                # The explode column: pull individual elements for list rows.
                var data = List[PythonObject]()
                var null_mask = List[Bool]()
                var any_null = False
                for k in range(n_out):
                    var r = src_indices[k]
                    var sub = sub_indices[k]
                    var is_null = (
                        len(exp_col._null_mask) > 0 and exp_col._null_mask[r]
                    )
                    if is_null:
                        data.append(py_none)
                        null_mask.append(True)
                        any_null = True
                    elif sub == -1:
                        # Scalar: keep value as-is.
                        data.append(_frame_cell_as_python(exp_col, r))
                        null_mask.append(False)
                    else:
                        # List element.
                        var cell = exp_col._data[List[PythonObject]][r]
                        data.append(cell[sub])
                        null_mask.append(False)
                var new_col = Column(column, ColumnData(data^), object_)
                if any_null:
                    new_col._null_mask = null_mask^
                result_cols.append(new_col^)
            else:
                # Other columns: repeat values by source row index.
                var new_col = self._cols[j].take(src_indices)
                new_col._index = ColumnIndex(List[PythonObject]())
                result_cols.append(new_col^)

        return DataFrame(result_cols^)

    def clip(
        self, lower: Optional[Float64] = None, upper: Optional[Float64] = None
    ) raises -> DataFrame:
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
        """Return an independent column-wise copy of this DataFrame (internal helper).
        """
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
            _not_implemented("DataFrame.copy")
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
                raise Error(
                    "DataFrame.insert: column already exists: " + column
                )
        var nrows = self.shape()[0]
        var idx = ColumnIndex(List[PythonObject]())
        if len(self._cols) > 0:
            idx = self._cols[0]._index
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

    def where(
        self, cond: Series, other: Optional[DFScalar] = None
    ) raises -> DataFrame:
        """Keep each element where *cond* is True; replace with *other* otherwise.

        When *other* is ``None`` (the default), non-matching cells become null.
        """
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i]._where(cond._col, other))
        return DataFrame(result_cols^)

    def mask(
        self, cond: Series, other: Optional[DFScalar] = None
    ) raises -> DataFrame:
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
                for _ in range(nrows):
                    false_data.append(False)
                result_cols.append(
                    Column(col_name, ColumnData(false_data^), bool_)
                )
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
                    result_cols.append(
                        self._cols[i]._combine_first_col(other._cols[j])
                    )
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
                    self._cols[i] = other._cols[j]._combine_first_col(
                        self._cols[i]
                    )
                    break

    # ------------------------------------------------------------------
    # Combining
    # ------------------------------------------------------------------

    @staticmethod
    def _row_key_str(
        df: DataFrame,
        key_cols: List[String],
        row: Int,
        col_idx: Dict[String, Int],
    ) raises -> String:
        """Serialise the key column values at *row* to a single String for hashing.

        *col_idx* must be a pre-built name→column-index map for *df* so that
        each key lookup is O(1) rather than O(n_cols).
        """
        var key = String()
        for k in range(len(key_cols)):
            if k > 0:
                key += "|"
            var i = col_idx[key_cols[k]]
            ref col_data = df._cols[i]._data
            if col_data.isa[List[Int64]]():
                key += String(Int(col_data[List[Int64]][row]))
            elif col_data.isa[List[Float64]]():
                key += String(col_data[List[Float64]][row])
            elif col_data.isa[List[Bool]]():
                key += "1" if col_data[List[Bool]][row] else "0"
            elif col_data.isa[List[String]]():
                key += col_data[List[String]][row]
            else:
                key += String(col_data[List[PythonObject]][row])
        return key

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
        # Determine key columns.
        var lkeys: List[String]
        var rkeys: List[String]
        if on:
            lkeys = on.value().copy()
            rkeys = on.value().copy()
        elif left_on:
            if right_on:
                lkeys = left_on.value().copy()
                rkeys = right_on.value().copy()
            else:
                raise Error("merge requires both 'left_on' and 'right_on'")
        else:
            raise Error("merge requires 'on' or both 'left_on' and 'right_on'")

        var lsuf = "_x"
        var rsuf = "_y"
        if suffixes:
            lsuf = suffixes.value()[0]
            rsuf = suffixes.value()[1]

        # Build name→index maps once so key serialisation is O(1) per lookup.
        var right_col_idx = Dict[String, Int]()
        for i in range(len(right._cols)):
            right_col_idx[right._cols[i].name] = i
        var left_col_idx = Dict[String, Int]()
        for i in range(len(self._cols)):
            left_col_idx[self._cols[i].name] = i

        # Build right hash map: key_str → list of right row indices.
        var right_map = Dict[String, List[Int]]()
        var n_right = right.shape()[0]
        for i in range(n_right):
            var k = DataFrame._row_key_str(right, rkeys, i, right_col_idx)
            if k not in right_map:
                right_map[k] = List[Int]()
            right_map[k].append(i)

        # Match rows — build parallel index lists (-1 means null/unmatched side).
        var out_left = List[Int]()
        var out_right = List[Int]()
        var n_left = self.shape()[0]
        var right_matched = List[Bool]()
        for _ in range(n_right):
            right_matched.append(False)

        for i in range(n_left):
            var k = DataFrame._row_key_str(self, lkeys, i, left_col_idx)
            if k in right_map:
                ref matches = right_map[k]
                for m in range(len(matches)):
                    out_left.append(i)
                    out_right.append(matches[m])
                    right_matched[matches[m]] = True
            elif how == "left" or how == "outer":
                out_left.append(i)
                out_right.append(-1)

        if how == "right" or how == "outer":
            for j in range(n_right):
                if not right_matched[j]:
                    out_left.append(-1)
                    out_right.append(j)

        # Determine output column schema.
        var key_set = Dict[String, Bool]()
        for k in range(len(lkeys)):
            key_set[lkeys[k]] = True

        # Right non-key column names (for overlap detection with left).
        var right_nonkey_names = Dict[String, Bool]()
        for j in range(len(right._cols)):
            if right._cols[j].name not in key_set:
                right_nonkey_names[right._cols[j].name] = True

        # Build output columns.
        var result_cols = List[Column]()

        # Key columns: left values for matched/left-only rows; right values for
        # right-only rows (out_left[r] == -1).  Inner/left joins have no
        # right-only rows so the fill loop is a no-op for those cases.
        for k in range(len(lkeys)):
            for i in range(len(self._cols)):
                if self._cols[i].name == lkeys[k]:
                    var key_col = self._cols[i].take_with_nulls(out_left)
                    # For right-only rows, substitute the right frame's key value.
                    for j in range(len(right._cols)):
                        if right._cols[j].name == lkeys[k]:
                            ref rk = right._cols[j]
                            if (
                                key_col._data.isa[List[Int64]]()
                                and rk._data.isa[List[Int64]]()
                            ):
                                for r in range(len(out_left)):
                                    if out_left[r] < 0:
                                        key_col._int64_data()[
                                            r
                                        ] = rk._int64_data()[out_right[r]]
                                        key_col._null_mask[r] = False
                            elif (
                                key_col._data.isa[List[Float64]]()
                                and rk._data.isa[List[Float64]]()
                            ):
                                for r in range(len(out_left)):
                                    if out_left[r] < 0:
                                        key_col._float64_data()[
                                            r
                                        ] = rk._float64_data()[out_right[r]]
                                        key_col._null_mask[r] = False
                            elif (
                                key_col._data.isa[List[Bool]]()
                                and rk._data.isa[List[Bool]]()
                            ):
                                for r in range(len(out_left)):
                                    if out_left[r] < 0:
                                        key_col._bool_data()[
                                            r
                                        ] = rk._bool_data()[out_right[r]]
                                        key_col._null_mask[r] = False
                            elif (
                                key_col._data.isa[List[String]]()
                                and rk._data.isa[List[String]]()
                            ):
                                for r in range(len(out_left)):
                                    if out_left[r] < 0:
                                        key_col._str_data()[r] = rk._str_data()[
                                            out_right[r]
                                        ]
                                        key_col._null_mask[r] = False
                            elif (
                                key_col._data.isa[List[PythonObject]]()
                                and rk._data.isa[List[PythonObject]]()
                            ):
                                for r in range(len(out_left)):
                                    if out_left[r] < 0:
                                        key_col._obj_data()[r] = rk._obj_data()[
                                            out_right[r]
                                        ]
                                        key_col._null_mask[r] = False
                            break
                    result_cols.append(key_col^)
                    break

        # Left non-key columns.
        for i in range(len(self._cols)):
            if self._cols[i].name in key_set:
                continue
            var col = self._cols[i].take_with_nulls(out_left)
            if col.name in right_nonkey_names:
                col.name = col.name + lsuf
            result_cols.append(col^)

        # Right non-key columns.
        for j in range(len(right._cols)):
            if right._cols[j].name in key_set:
                continue
            var col = right._cols[j].take_with_nulls(out_right)
            var in_left = False
            for i in range(len(self._cols)):
                if (
                    self._cols[i].name not in key_set
                    and self._cols[i].name == right._cols[j].name
                ):
                    in_left = True
                    break
            if in_left:
                col.name = col.name + rsuf
            result_cols.append(col^)

        return DataFrame(result_cols^)

    def join(
        self,
        other: DataFrame,
        on: Optional[List[String]] = None,
        how: String = "left",
        lsuffix: String = "",
        rsuffix: String = "",
        sort: Bool = False,
    ) raises -> DataFrame:
        # Guard unsupported parameters so callers get a clear failure instead
        # of silently wrong data.
        if how != "left":
            _not_implemented("DataFrame.join", "how='" + how + "'")
        if on:
            _not_implemented("DataFrame.join", "'on' parameter")
        if sort:
            _not_implemented("DataFrame.join", "'sort' parameter")

        # Build right column name set for overlap detection.
        var right_names = Dict[String, Bool]()
        for j in range(len(other._cols)):
            right_names[other._cols[j].name] = True

        var left_names = Dict[String, Bool]()
        for i in range(len(self._cols)):
            left_names[self._cols[i].name] = True

        # Detect overlap.
        var overlap = False
        for i in range(len(self._cols)):
            if self._cols[i].name in right_names:
                overlap = True
                break
        if overlap and lsuffix == "" and rsuffix == "":
            raise Error(
                "columns overlap but no suffix specified: use lsuffix/rsuffix"
            )

        var n_left = self.shape()[0]
        var result_cols = List[Column]()

        # Left columns — rename if overlap.
        for i in range(len(self._cols)):
            var col = self._cols[i].copy()
            if col.name in right_names:
                col.name = col.name + lsuffix
            result_cols.append(col^)

        # Right columns — positional alignment, rename if overlap.
        for j in range(len(other._cols)):
            var col = other._cols[j].slice(0, n_left)
            if col.name in left_names:
                col.name = col.name + rsuffix
            result_cols.append(col^)

        return DataFrame(result_cols^)

    def append(
        self, other: DataFrame, ignore_index: Bool = False
    ) raises -> DataFrame:
        if len(self._cols) != len(other._cols):
            raise Error("DataFrames have different number of columns")
        # Build name→index map for other.
        var other_idx = Dict[String, Int]()
        for j in range(len(other._cols)):
            other_idx[other._cols[j].name] = j
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var name = self._cols[i].name
            if name not in other_idx:
                raise Error(
                    "Column '" + name + "' not found in other DataFrame"
                )
            var new_col = self._cols[i].concat(other._cols[other_idx[name]])
            result_cols.append(new_col^)
        # Both frames use default RangeIndex (empty ColumnIndex), so ignore_index
        # makes no observable difference for this common case.
        return DataFrame(result_cols^)

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
        # axis is accepted for API compatibility but ignored (deprecated in
        # pandas 2.0 for groupby).
        return DataFrameGroupBy(self, by, as_index, sort, dropna)

    def resample(self, rule: String, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.resample")
        return DataFrame()

    def rolling(
        self, window: Int, min_periods: Optional[Int] = None
    ) raises -> DataFrame:
        _not_implemented("DataFrame.rolling")
        return DataFrame()

    def expanding(self, min_periods: Int = 1) raises -> DataFrame:
        _not_implemented("DataFrame.expanding")
        return DataFrame()

    def ewm(
        self, com: Optional[Float64] = None, span: Optional[Float64] = None
    ) raises -> DataFrame:
        _not_implemented("DataFrame.ewm")
        return DataFrame()

    # ------------------------------------------------------------------
    # IO
    # ------------------------------------------------------------------

    def to_csv(
        self, path_or_buf: String = "", sep: String = ",", index: Bool = True
    ) raises -> String:
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
            header_parts.append(
                String("")
            )  # pandas convention: index column has no header
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

    def to_parquet(
        self,
        path: String,
        engine: String = "auto",
        compression: String = "snappy",
    ) raises:
        """Write the DataFrame to a Parquet file.

        Uses pandas interop: converts to a pandas DataFrame then calls
        ``pandas.DataFrame.to_parquet``.

        Parameters
        ----------
        path        : Destination file path.
        engine      : Parquet library to use (``"auto"``, ``"pyarrow"``,
                      ``"fastparquet"``).  Passed directly to pandas.
        compression : Compression codec (default ``"snappy"``).
        """
        var pd_df = self.to_pandas()
        pd_df.to_parquet(path, engine=engine, compression=compression)

    def to_json(
        self, path_or_buf: String = "", orient: String = ""
    ) raises -> String:
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
                    row[self._cols[ci].name] = _col_cell_pyobj(
                        self._cols[ci], ri
                    )
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
                    row[self._cols[ci].name] = _col_cell_pyobj(
                        self._cols[ci], ri
                    )
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

    def to_excel(
        self,
        excel_writer: String,
        sheet_name: String = "Sheet1",
        index: Bool = True,
    ) raises:
        """Write the DataFrame to an Excel file.

        Uses pandas interop: converts to a pandas DataFrame then calls
        ``pandas.DataFrame.to_excel``.

        Parameters
        ----------
        excel_writer : Destination file path (``*.xlsx``).
        sheet_name   : Name of the target worksheet (default ``"Sheet1"``).
        index        : Whether to write the row index (default ``True``).
        """
        var pd_df = self.to_pandas()
        pd_df.to_excel(excel_writer, sheet_name=sheet_name, index=index)

    def to_dict(
        self, orient: String = "dict"
    ) raises -> Dict[String, Dict[String, DFScalar]]:
        """Return the DataFrame as a nested ``Dict`` mapping column names to
        index-label → value dicts.

        Matches pandas ``orient="dict"`` semantics: the outer key is the
        column name and the inner key is the stringified row-index label
        (e.g. ``"0"``, ``"1"`` for the default integer index).

        All other orient values raise ``Error``.  For row-oriented output use
        ``to_records()`` (equivalent to ``orient="records"``).

        Parameters
        ----------
        orient : Serialisation orientation.  Only ``"dict"`` is supported.
        """
        if orient != "dict":
            _not_implemented("DataFrame.to_dict with orient='" + orient + "'")
        var result = Dict[String, Dict[String, DFScalar]]()
        var nrows = self.__len__()
        var ncols = self._cols.__len__()
        var has_index = self._has_index()
        for ci in range(ncols):
            ref col = self._cols[ci]
            var inner = Dict[String, DFScalar]()
            for i in range(nrows):
                var key = col._index_label(i) if has_index else String(i)
                inner[key] = _scalar_from_col(col, i)
            result[col.name] = inner^
        return result^

    def to_records(
        self, index: Bool = True
    ) raises -> List[Dict[String, DFScalar]]:
        """Return the DataFrame as a list of row dicts.

        Each dict maps column name to the cell value for that row.  When
        *index* is ``True`` (the default) the row index is included under
        the key ``"index"``.

        Parameters
        ----------
        index : Whether to include the row index (default ``True``).
        """
        var result = List[Dict[String, DFScalar]]()
        var nrows = self.__len__()
        var ncols = self._cols.__len__()
        if index:
            for ci in range(ncols):
                if self._cols[ci].name == "index":
                    raise Error(
                        "DataFrame.to_records: column named 'index' conflicts"
                        " with the index key; pass index=False or rename the"
                        " column"
                    )
        for ri in range(nrows):
            var row = Dict[String, DFScalar]()
            if index:
                row["index"] = DFScalar(Int64(ri))
            for ci in range(ncols):
                ref col = self._cols[ci]
                row[col.name] = _scalar_from_col(col, ri)
            result.append(row^)
        return result^

    def to_numpy(self) raises -> List[List[Float64]]:
        """Return the DataFrame values as a row-major ``List[List[Float64]]``.

        Integer and bool values are cast to ``Float64``.  Null values become
        ``NaN``.  Columns with string or object dtypes raise ``Error``.
        """
        var nrows = self.__len__()
        var ncols = self._cols.__len__()
        var nan = Float64(0) / Float64(0)
        for ci in range(ncols):
            ref col = self._cols[ci]
            if not (
                col._data.isa[List[Int64]]()
                or col._data.isa[List[Float64]]()
                or col._data.isa[List[Bool]]()
            ):
                raise Error(
                    "DataFrame.to_numpy: column '"
                    + col.name
                    + "' has non-numeric dtype"
                )
        var result = List[List[Float64]]()
        for ri in range(nrows):
            var row = List[Float64]()
            for ci in range(ncols):
                ref col = self._cols[ci]
                var has_mask = len(col._null_mask) > 0
                if has_mask and ri < len(col._null_mask) and col._null_mask[ri]:
                    row.append(nan)
                elif col._data.isa[List[Int64]]():
                    row.append(Float64(col._data[List[Int64]][ri]))
                elif col._data.isa[List[Float64]]():
                    row.append(col._data[List[Float64]][ri])
                else:
                    row.append(
                        Float64(1.0) if col._data[List[Bool]][ri] else Float64(
                            0.0
                        )
                    )
            result.append(row^)
        return result^

    def to_string(self) raises -> String:
        """Return a human-readable tabular string representation.

        Mirrors the output format of ``pandas.DataFrame.to_string()``:
        an index column followed by value columns, all right-padded to
        the maximum width needed for each column.  Columns are separated
        by two spaces.
        """
        var nrows = self.__len__()
        var ncols = self._cols.__len__()

        # Index column width: width of the largest row-index label.
        var idx_width = 1
        if nrows > 1:
            var tmp = nrows - 1
            var w = 0
            while tmp > 0:
                tmp //= 10
                w += 1
            idx_width = w

        # Compute per-column display widths.
        var widths = List[Int]()
        for ci in range(ncols):
            var w = len(self._cols[ci].name)
            for ri in range(nrows):
                var s = _col_cell_str(self._cols[ci], ri)
                if len(s) > w:
                    w = len(s)
            widths.append(w)

        # Header row.
        var result = String()
        var line = String()
        for _ in range(idx_width):
            line += " "
        for ci in range(ncols):
            line += "  "
            var name = self._cols[ci].name
            var pad = widths[ci] - len(name)
            for _ in range(pad):
                line += " "
            line += name
        result += line + "\n"

        # Data rows.
        for ri in range(nrows):
            var idx_str = String(ri)
            var row_line = idx_str
            var pad = idx_width - len(idx_str)
            for _ in range(pad):
                row_line += " "
            for ci in range(ncols):
                row_line += "  "
                var cell = _col_cell_str(self._cols[ci], ri)
                var col_pad = widths[ci] - len(cell)
                for _ in range(col_pad):
                    row_line += " "
                row_line += cell
            result += row_line + "\n"

        return result^

    def to_html(self) raises -> String:
        """Return an HTML table representation of the DataFrame.

        Produces the same ``<table>`` skeleton as
        ``pandas.DataFrame.to_html()``, with a ``<thead>`` containing
        the column names and a ``<tbody>`` containing the data rows.
        Cell values are HTML-escaped.
        """
        var nrows = self.__len__()
        var ncols = self._cols.__len__()

        var result = String(
            '<table border="1" class="dataframe">\n'
            "  <thead>\n"
            '    <tr style="text-align: right;">\n'
            "      <th></th>\n"
        )
        for ci in range(ncols):
            result += (
                "      <th>" + _html_escape(self._cols[ci].name) + "</th>\n"
            )
        result += "    </tr>\n  </thead>\n  <tbody>\n"

        for ri in range(nrows):
            result += "    <tr>\n"
            result += "      <th>" + String(ri) + "</th>\n"
            for ci in range(ncols):
                result += (
                    "      <td>"
                    + _html_escape(_col_cell_str(self._cols[ci], ri))
                    + "</td>\n"
                )
            result += "    </tr>\n"

        result += "  </tbody>\n</table>"
        return result^

    def to_markdown(self) raises -> String:
        """Return a GitHub-Flavored Markdown table representation.

        Produces a pipe-delimited Markdown table with an index column,
        a separator row of dashes, and one data row per DataFrame row.
        """
        var nrows = self.__len__()
        var ncols = self._cols.__len__()

        # Index column width.
        var idx_width = 5  # len("index")
        if nrows > 0:
            var tmp_w = len(String(nrows - 1))
            if tmp_w > idx_width:
                idx_width = tmp_w

        # Per-column widths (at least as wide as the header).
        var widths = List[Int]()
        for ci in range(ncols):
            var w = len(self._cols[ci].name)
            for ri in range(nrows):
                var s = _col_cell_str(self._cols[ci], ri)
                if len(s) > w:
                    w = len(s)
            widths.append(w)

        # Header row.
        var result = String("| ")
        var idx_pad = idx_width - 5  # 5 = len("index")
        result += "index"
        for _ in range(idx_pad):
            result += " "
        result += " |"
        for ci in range(ncols):
            result += " "
            var name = self._cols[ci].name
            result += name
            var col_pad = widths[ci] - len(name)
            for _ in range(col_pad):
                result += " "
            result += " |"
        result += "\n"

        # Separator row.
        result += "|"
        result += ":"
        for _ in range(idx_width):
            result += "-"
        result += "|"
        for ci in range(ncols):
            result += ":"
            for _ in range(widths[ci]):
                result += "-"
            result += "|"
        result += "\n"

        # Data rows.
        for ri in range(nrows):
            var idx_str = String(ri)
            result += "| " + idx_str
            var ri_pad = idx_width - len(idx_str)
            for _ in range(ri_pad):
                result += " "
            result += " |"
            for ci in range(ncols):
                result += " "
                var cell = _col_cell_str(self._cols[ci], ri)
                result += cell
                var col_pad = widths[ci] - len(cell)
                for _ in range(col_pad):
                    result += " "
                result += " |"
            result += "\n"

        return result^

    # ------------------------------------------------------------------
    # Repr / iteration
    # ------------------------------------------------------------------

    def __repr__(self) raises -> String:
        return String(self.to_pandas())

    def __len__(self) -> Int:
        if self._cols.__len__() == 0:
            return 0
        return self._cols[0].__len__()

    def _has_index(self) -> Bool:
        """Return ``True`` when the DataFrame carries an explicit row index."""
        return self._cols.__len__() > 0 and self._cols[0]._index_len() > 0

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

    def itertuples(
        self, index: Bool = True, name: String = "Pandas"
    ) raises -> List[Series]:
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


def _groupby_indices(
    df: DataFrame,
    by: List[String],
    sort_keys: Bool,
    dropna: Bool,
    mut group_map: Dict[String, List[Int]],
    mut group_keys: List[String],
) raises:
    """Build key→row-index mapping for a DataFrame groupby.

    Populates *group_map* (key→row-index list) and *group_keys* (ordered key
    list) in place.  Uses DataFrame._row_key_str for key serialisation, the
    same approach as merge.  When dropna=True, rows where any key column is
    null are excluded.  When sort_keys=True, group_keys is sorted
    lexicographically on return.
    """
    var col_idx = Dict[String, Int]()
    for i in range(len(df._cols)):
        col_idx[df._cols[i].name] = i

    var n_rows = df.shape()[0]

    for i in range(n_rows):
        if dropna:
            var skip = False
            for j in range(len(by)):
                var ci = col_idx[by[j]]
                ref col = df._cols[ci]
                if len(col._null_mask) > 0 and col._null_mask[i]:
                    skip = True
                    break
            if skip:
                continue

        var k = DataFrame._row_key_str(df, by, i, col_idx)
        if k not in group_map:
            group_keys.append(k)
            group_map[k] = List[Int]()
        group_map[k].append(i)

    if sort_keys:
        _sort_list(group_keys)


struct DataFrameGroupBy:
    """GroupBy object returned by DataFrame.groupby().

    Delegates all aggregation to the underlying pandas GroupBy object via
    to_pandas() / from_pandas() round-trips, consistent with eval/query.
    """

    var _df: DataFrame
    var _by: List[String]
    var _as_index: Bool
    var _sort: Bool
    var _dropna: Bool
    var _group_map: Dict[String, List[Int]]
    var _group_keys: List[String]

    def __init__(
        out self,
        df: DataFrame,
        by: List[String],
        as_index: Bool,
        sort: Bool,
        dropna: Bool,
    ) raises:
        self._df = df.copy()
        self._by = by.copy()
        self._as_index = as_index
        self._sort = sort
        self._dropna = dropna
        self._group_map = Dict[String, List[Int]]()
        self._group_keys = List[String]()
        _groupby_indices(
            df, by, sort, dropna, self._group_map, self._group_keys
        )

    def _pd_groupby(self) raises -> PythonObject:
        """Return the pandas GroupBy object for this group configuration."""
        var pd_df = self._df.to_pandas()
        var py_by = Python.evaluate("[]")
        for i in range(len(self._by)):
            _ = py_by.append(self._by[i])
        return pd_df.groupby(
            py_by,
            as_index=self._as_index,
            sort=self._sort,
            dropna=self._dropna,
        )

    def _make_result_col(
        self, name: String, var vals: List[Float64]
    ) raises -> Column:
        """Build a float64 result Column with group keys as index."""
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        var col = Column(name, ColumnData(vals^), float64, idx^)
        col._index_name = self._by[0]
        return col^

    def _make_result_col_int64(
        self, name: String, var vals: List[Int64]
    ) raises -> Column:
        """Build an int64 result Column with group keys as index."""
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        var col = Column(name, ColumnData(vals^), int64, idx^)
        col._index_name = self._by[0]
        return col^

    def sum(self) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().sum())
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).sum()
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return DataFrame(result_cols^)

    def mean(self) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().mean())
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).mean()
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return DataFrame(result_cols^)

    def min(self) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().min())
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).min()
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return DataFrame(result_cols^)

    def max(self) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().max())
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).max()
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return DataFrame(result_cols^)

    def std(self, ddof: Int = 1) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().std(ddof))
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).std(ddof)
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return DataFrame(result_cols^)

    def var(self, ddof: Int = 1) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().var(ddof))
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).var(ddof)
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return DataFrame(result_cols^)

    def count(self) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().count())
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            var vals = List[Int64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    Int64(
                        col.take(self._group_map[self._group_keys[j]]).count()
                    )
                )
            result_cols.append(self._make_result_col_int64(col.name, vals^))
        return DataFrame(result_cols^)

    def nunique(self) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().nunique())
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            var vals = List[Int64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    Int64(
                        col.take(self._group_map[self._group_keys[j]]).nunique()
                    )
                )
            result_cols.append(self._make_result_col_int64(col.name, vals^))
        return DataFrame(result_cols^)

    def first(self) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().first())
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            var selected = List[Int]()
            var has_mask = len(col._null_mask) > 0
            for j in range(len(self._group_keys)):
                ref indices = self._group_map[self._group_keys[j]]
                var found = -1
                for k in range(len(indices)):
                    if not has_mask or not col._null_mask[indices[k]]:
                        found = indices[k]
                        break
                selected.append(found)
            var result_col = col.take_with_nulls(selected)
            result_col.name = col.name
            result_col._index = ColumnIndex(Index(self._group_keys.copy()))
            result_col._index_name = self._by[0]
            result_cols.append(result_col^)
        return DataFrame(result_cols^)

    def last(self) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().last())
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            var selected = List[Int]()
            var has_mask = len(col._null_mask) > 0
            for j in range(len(self._group_keys)):
                ref indices = self._group_map[self._group_keys[j]]
                var found = -1
                for k in range(len(indices) - 1, -1, -1):
                    if not has_mask or not col._null_mask[indices[k]]:
                        found = indices[k]
                        break
                selected.append(found)
            var result_col = col.take_with_nulls(selected)
            result_col.name = col.name
            result_col._index = ColumnIndex(Index(self._group_keys.copy()))
            result_col._index_name = self._by[0]
            result_cols.append(result_col^)
        return DataFrame(result_cols^)

    def size(self) raises -> Series:
        if len(self._by) != 1 or not self._as_index:
            return Series.from_pandas(self._pd_groupby().size())
        var vals = List[Int64]()
        for i in range(len(self._group_keys)):
            vals.append(Int64(len(self._group_map[self._group_keys[i]])))
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        # pandas groupby().size() returns a Series with name=None; "" maps to None
        var col = Column("", ColumnData(vals^), int64, idx^)
        col._index_name = self._by[0]
        return Series(col^)

    def agg(self, func: String) raises -> DataFrame:
        if func == "sum":
            return self.sum()
        if func == "mean":
            return self.mean()
        if func == "min":
            return self.min()
        if func == "max":
            return self.max()
        if func == "count":
            return self.count()
        if func == "nunique":
            return self.nunique()
        if func == "first":
            return self.first()
        if func == "last":
            return self.last()
        if func == "std":
            return self.std()
        if func == "var":
            return self.var()
        return DataFrame.from_pandas(self._pd_groupby().agg(func))

    def aggregate(self, func: String) raises -> DataFrame:
        return self.agg(func)

    def transform(self, func: String) raises -> DataFrame:
        if len(self._by) != 1 or not self._as_index:
            return DataFrame.from_pandas(self._pd_groupby().transform(func))
        if func != "sum" and func != "mean" and func != "min" and func != "max":
            return DataFrame.from_pandas(self._pd_groupby().transform(func))
        if len(self._df._cols) == 0:
            return DataFrame()
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        # Build row → group_key mapping by inverting _group_map.
        var n_rows = len(self._df._cols[0])
        var row_key = List[String]()
        for _ in range(n_rows):
            row_key.append(String(""))
        for j in range(len(self._group_keys)):
            var key = self._group_keys[j]
            ref indices = self._group_map[key]
            for k in range(len(indices)):
                row_key[indices[k]] = key
        # For each numeric column, broadcast the group aggregate to every row.
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var key_to_val = Dict[String, Float64]()
            for j in range(len(self._group_keys)):
                var key = self._group_keys[j]
                var sub = col.take(self._group_map[key])
                if func == "sum":
                    key_to_val[key] = sub.sum()
                elif func == "mean":
                    key_to_val[key] = sub.mean()
                elif func == "min":
                    key_to_val[key] = sub.min()
                else:
                    key_to_val[key] = sub.max()
            var nan = Float64(0) / Float64(0)
            var vals = List[Float64]()
            var null_mask = List[Bool]()
            var any_null = False
            for r in range(n_rows):
                if row_key[r] != "":
                    vals.append(key_to_val[row_key[r]])
                    null_mask.append(False)
                else:
                    # Row was excluded by dropna — emit NaN.
                    vals.append(nan)
                    null_mask.append(True)
                    any_null = True
            var result_col = Column(col.name, ColumnData(vals^), float64)
            if any_null:
                result_col._null_mask = null_mask^
            result_cols.append(result_col^)
        return DataFrame(result_cols^)

    def apply(self, func: String) raises -> DataFrame:
        return DataFrame.from_pandas(
            self._pd_groupby().apply(Python.evaluate(func))
        )

    def filter(self, func: String) raises -> DataFrame:
        return DataFrame.from_pandas(
            self._pd_groupby().filter(Python.evaluate(func))
        )


struct SeriesGroupBy:
    """GroupBy object returned by Series.groupby().

    Delegates all aggregation to the underlying pandas GroupBy object via
    to_pandas() / from_pandas() round-trips, consistent with eval/query.
    """

    var _series: Series
    var _by: List[String]
    var _as_index: Bool
    var _sort: Bool
    var _dropna: Bool
    var _group_map: Dict[String, List[Int]]
    var _group_keys: List[String]

    def __init__(
        out self,
        series: Series,
        by: List[String],
        as_index: Bool,
        sort: Bool,
        dropna: Bool,
    ) raises:
        self._series = series.copy()
        self._by = by.copy()
        self._as_index = as_index
        self._sort = sort
        self._dropna = dropna
        self._group_map = Dict[String, List[Int]]()
        self._group_keys = List[String]()
        for i in range(len(by)):
            var k = by[i]
            if k not in self._group_map:
                self._group_keys.append(k)
                self._group_map[k] = List[Int]()
            self._group_map[k].append(i)
        if sort:
            _sort_list(self._group_keys)

    def _pd_groupby(self) raises -> PythonObject:
        """Return the pandas GroupBy object for this group configuration."""
        var pd_s = self._series.to_pandas()
        var py_by = Python.evaluate("[]")
        for i in range(len(self._by)):
            _ = py_by.append(self._by[i])
        return pd_s.groupby(
            py_by,
            as_index=self._as_index,
            sort=self._sort,
            dropna=self._dropna,
        )

    def sum(self) raises -> Series:
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).sum()
            )
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def mean(self) raises -> Series:
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).mean()
            )
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def min(self) raises -> Series:
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).min()
            )
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def max(self) raises -> Series:
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).max()
            )
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def count(self) raises -> Series:
        var result_vals = List[Int64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                Int64(self._series._col.take(self._group_map[key]).count())
            )
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), int64, idx^)
        )

    def nunique(self) raises -> Series:
        var result_vals = List[Int64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                Int64(self._series._col.take(self._group_map[key]).nunique())
            )
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), int64, idx^)
        )

    def first(self) raises -> Series:
        var selected = List[Int]()
        ref col = self._series._col
        var has_mask = len(col._null_mask) > 0
        for i in range(len(self._group_keys)):
            ref indices = self._group_map[self._group_keys[i]]
            var found = -1
            for j in range(len(indices)):
                if not has_mask or not col._null_mask[indices[j]]:
                    found = indices[j]
                    break
            selected.append(found)
        var result_col = col.take_with_nulls(selected)
        result_col.name = self._series.name
        result_col._index = ColumnIndex(Index(self._group_keys.copy()))
        return Series(result_col^)

    def last(self) raises -> Series:
        var selected = List[Int]()
        ref col = self._series._col
        var has_mask = len(col._null_mask) > 0
        for i in range(len(self._group_keys)):
            ref indices = self._group_map[self._group_keys[i]]
            var found = -1
            for j in range(len(indices) - 1, -1, -1):
                if not has_mask or not col._null_mask[indices[j]]:
                    found = indices[j]
                    break
            selected.append(found)
        var result_col = col.take_with_nulls(selected)
        result_col.name = self._series.name
        result_col._index = ColumnIndex(Index(self._group_keys.copy()))
        return Series(result_col^)

    def size(self) raises -> Series:
        var result_vals = List[Int64]()
        for i in range(len(self._group_keys)):
            result_vals.append(Int64(len(self._group_map[self._group_keys[i]])))
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), int64, idx^)
        )

    def std(self, ddof: Int = 1) raises -> Series:
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).std(ddof)
            )
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def var(self, ddof: Int = 1) raises -> Series:
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).var(ddof)
            )
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def agg(self, func: String) raises -> Series:
        if func == "sum":
            return self.sum()
        if func == "mean":
            return self.mean()
        if func == "min":
            return self.min()
        if func == "max":
            return self.max()
        if func == "count":
            return self.count()
        if func == "nunique":
            return self.nunique()
        if func == "first":
            return self.first()
        if func == "last":
            return self.last()
        if func == "size":
            return self.size()
        if func == "std":
            return self.std()
        if func == "var":
            return self.var()
        return Series.from_pandas(self._pd_groupby().agg(func))

    def aggregate(self, func: String) raises -> Series:
        return self.agg(func)

    def transform(self, func: String) raises -> Series:
        var key_to_agg = Dict[String, Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            var sub = self._series._col.take(self._group_map[key])
            if func == "sum":
                key_to_agg[key] = sub.sum()
            elif func == "mean":
                key_to_agg[key] = sub.mean()
            elif func == "min":
                key_to_agg[key] = sub.min()
            elif func == "max":
                key_to_agg[key] = sub.max()
            else:
                return Series.from_pandas(self._pd_groupby().transform(func))
        var n = len(self._series._col)
        var result_vals = List[Float64]()
        for i in range(n):
            result_vals.append(key_to_agg[self._by[i]])
        var result_col = Column(
            self._series.name, ColumnData(result_vals^), float64
        )
        result_col._index = self._series._col._index
        result_col._index_name = self._series._col._index_name
        return Series(result_col^)

    def apply(self, func: String) raises -> Series:
        return Series.from_pandas(
            self._pd_groupby().apply(Python.evaluate(func))
        )
