from std.python import Python, PythonObject
from std.collections import Optional, Dict
from std.utils import Variant
from std.builtin.sort import sort as _sort_list
from std.math import sqrt
from std.memory import UnsafePointer
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
    DictSplitResult,
    _Null,
    FloatTransformFn,
    _csv_quote_field,
    _col_cell_str,
    _col_cell_pyobj,
    _scalar_from_col,
    _SetScalarInColMutVisitor,
    ColumnDataVisitorRaises,
    visit_col_data_raises,
    visit_col_data_mut_raises,
)
from .accessors.str_accessor import StringMethods
from .accessors.dt_accessor import DatetimeMethods
from .expr import parse as _parse_expr, eval_expr as _eval_expr
from .arrow import column_to_marrow_array, marrow_array_to_column
from marrow.arrays import AnyArray
from marrow.builders import StringBuilder as _MarrowStringBuilder
from marrow.dtypes import (
    int64 as _m_int64,
    float64 as _m_float64,
)
from marrow.kernels.groupby import groupby as _marrow_groupby


struct Series(Copyable, ImplicitlyCopyable, Movable):
    """A one-dimensional labeled array, mirroring the pandas Series API."""

    var _col: Column
    var name: Optional[String]

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(out self):
        """Empty Series — used as stub return placeholder."""
        self._col = Column()
        self.name = None

    def __init__(out self, var col: Column):
        self.name = col.name
        self._col = col^

    def __init__(
        out self, pd_s: PythonObject, name: Optional[String] = None
    ) raises:
        """Convenience constructor: wraps a pandas Series."""
        var col_name: Optional[String]
        if name:
            col_name = name
        else:
            var raw_name = pd_s.name
            var _is_none = Python.evaluate("lambda x: x is None")
            if Bool(_is_none(raw_name)):
                col_name = None
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
        var col_name: Optional[String]
        var _is_none = Python.evaluate("lambda x: x is None")
        if Bool(_is_none(raw_name)):
            col_name = None
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

    def __eq__(self, other: Series) raises -> Series:
        """Element-wise ``==`` against another Series, returning a boolean Series.
        """
        return self.eq(other)

    def __ne__(self, other: Series) raises -> Series:
        """Element-wise ``!=`` against another Series, returning a boolean Series.
        """
        return self.ne(other)

    def __lt__(self, other: Series) raises -> Series:
        """Element-wise ``<`` against another Series, returning a boolean Series.
        """
        return self.lt(other)

    def __le__(self, other: Series) raises -> Series:
        """Element-wise ``<=`` against another Series, returning a boolean Series.
        """
        return self.le(other)

    def __gt__(self, other: Series) raises -> Series:
        """Element-wise ``>`` against another Series, returning a boolean Series.
        """
        return self.gt(other)

    def __ge__(self, other: Series) raises -> Series:
        """Element-wise ``>=`` against another Series, returning a boolean Series.
        """
        return self.ge(other)

    # ------------------------------------------------------------------
    # Boolean logical
    # ------------------------------------------------------------------

    def and_(self, other: Series) raises -> Series:
        """Element-wise logical AND with Kleene null semantics."""
        return Series(self._col._bool_and(other._col))

    def or_(self, other: Series) raises -> Series:
        """Element-wise logical OR with Kleene null semantics."""
        return Series(self._col._bool_or(other._col))

    def xor(self, other: Series) raises -> Series:
        """Element-wise logical XOR (null propagates if either operand is null).
        """
        return Series(self._col._bool_xor(other._col))

    def invert(self) raises -> Series:
        """Element-wise logical NOT.  Null elements remain null."""
        return Series(self._col._bool_invert())

    def __and__(self, other: Series) raises -> Series:
        return self.and_(other)

    def __or__(self, other: Series) raises -> Series:
        return self.or_(other)

    def __xor__(self, other: Series) raises -> Series:
        return self.xor(other)

    def __invert__(self) raises -> Series:
        return self.invert()

    def __rand__(self, other: Series) raises -> Series:
        return other.and_(self)

    def __ror__(self, other: Series) raises -> Series:
        return other.or_(self)

    def __rxor__(self, other: Series) raises -> Series:
        return other.xor(self)

    def __gt__(self, other: Float64) raises -> Series:
        """Element-wise ``>`` against a scalar, returning a boolean Series."""
        return Series(self._col._cmp_scalar_gt(other))

    def __lt__(self, other: Float64) raises -> Series:
        """Element-wise ``<`` against a scalar, returning a boolean Series."""
        return Series(self._col._cmp_scalar_lt(other))

    def __ge__(self, other: Float64) raises -> Series:
        """Element-wise ``>=`` against a scalar, returning a boolean Series."""
        return Series(self._col._cmp_scalar_ge(other))

    def __le__(self, other: Float64) raises -> Series:
        """Element-wise ``<=`` against a scalar, returning a boolean Series."""
        return Series(self._col._cmp_scalar_le(other))

    def __eq__(self, other: Float64) raises -> Series:
        """Element-wise ``==`` against a numeric scalar, returning a boolean Series.
        """
        return Series(self._col._cmp_scalar_eq(other))

    def __ne__(self, other: Float64) raises -> Series:
        """Element-wise ``!=`` against a numeric scalar, returning a boolean Series.
        """
        return Series(self._col._cmp_scalar_ne(other))

    def __gt__(self, other: Int64) raises -> Series:
        """Element-wise ``>`` against an integer scalar, returning a boolean Series.
        """
        return Series(self._col._cmp_scalar_gt(other))

    def __lt__(self, other: Int64) raises -> Series:
        """Element-wise ``<`` against an integer scalar, returning a boolean Series.
        """
        return Series(self._col._cmp_scalar_lt(other))

    def __ge__(self, other: Int64) raises -> Series:
        """Element-wise ``>=`` against an integer scalar, returning a boolean Series.
        """
        return Series(self._col._cmp_scalar_ge(other))

    def __le__(self, other: Int64) raises -> Series:
        """Element-wise ``<=`` against an integer scalar, returning a boolean Series.
        """
        return Series(self._col._cmp_scalar_le(other))

    def __eq__(self, other: Int64) raises -> Series:
        """Element-wise ``==`` against an integer scalar, returning a boolean Series.
        """
        return Series(self._col._cmp_scalar_eq(other))

    def __ne__(self, other: Int64) raises -> Series:
        """Element-wise ``!=`` against an integer scalar, returning a boolean Series.
        """
        return Series(self._col._cmp_scalar_ne(other))

    def __eq__(self, other: String) raises -> Series:
        """Element-wise ``==`` against a string scalar, returning a boolean Series.
        """
        var n = len(self._col)
        if self._col._data.isa[List[String]]():
            var rhs = List[String]()
            for _ in range(n):
                rhs.append(other)
            var rhs_col = Column(self._col.name, ColumnData(rhs^), object_)
            return Series(self._col._cmp_eq(rhs_col))
        var result = List[Bool]()
        var has_mask = len(self._col._null_mask) > 0
        if self._col._data.isa[List[PythonObject]]():
            ref d = self._col._data[List[PythonObject]]
            for i in range(n):
                if has_mask and self._col._null_mask[i]:
                    result.append(False)
                else:
                    result.append(String(d[i]) == other)
        else:
            for _ in range(n):
                result.append(False)
        var col = Column(self._col.name, ColumnData(result^), bool_)
        return Series(col^)

    def __ne__(self, other: String) raises -> Series:
        """Element-wise ``!=`` against a string scalar, returning a boolean Series.
        """
        var n = len(self._col)
        if self._col._data.isa[List[String]]():
            var rhs = List[String]()
            for _ in range(n):
                rhs.append(other)
            var rhs_col = Column(self._col.name, ColumnData(rhs^), object_)
            return Series(self._col._cmp_ne(rhs_col))
        var result = List[Bool]()
        var has_mask = len(self._col._null_mask) > 0
        if self._col._data.isa[List[PythonObject]]():
            ref d = self._col._data[List[PythonObject]]
            for i in range(n):
                if has_mask and self._col._null_mask[i]:
                    result.append(True)
                else:
                    result.append(String(d[i]) != other)
        else:
            for _ in range(n):
                result.append(True)
        var col = Column(self._col.name, ColumnData(result^), bool_)
        return Series(col^)

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
        var n = self._col.count() if skipna else len(self._col)
        if n == 0:
            var zero = Float64(0)
            return zero / zero
        return self._col.std(ddof, skipna) / sqrt(Float64(n))

    def skew(self, skipna: Bool = True) raises -> Float64:
        return self._col.skew(skipna)

    def kurt(self, skipna: Bool = True) raises -> Float64:
        return self._col.kurt(skipna)

    def idxmin(self, skipna: Bool = True) raises -> Int:
        var pos = self._col.argmin(skipna)
        if pos == -1:
            raise Error("idxmin: empty or all-null Series")
        var idx_len = self._col._index_len()
        if idx_len == 0:
            return pos
        if self._col._index.isa[List[Int64]]():
            return Int(self._col._index[List[Int64]][pos])
        raise Error("idxmin: index labels are not integer-typed")

    def idxmax(self, skipna: Bool = True) raises -> Int:
        var pos = self._col.argmax(skipna)
        if pos == -1:
            raise Error("idxmax: empty or all-null Series")
        var idx_len = self._col._index_len()
        if idx_len == 0:
            return pos
        if self._col._index.isa[List[Int64]]():
            return Int(self._col._index[List[Int64]][pos])
        raise Error("idxmax: index labels are not integer-typed")

    def corr(self, other: Series) raises -> Float64:
        return self._col.corr(other._col)

    def cov(self, other: Series, ddof: Int = 1) raises -> Float64:
        return self._col.cov(other._col, ddof)

    def shift(self, periods: Int = 1) raises -> Series:
        """Return a Series with values shifted by *periods* positions.

        Positive *periods* lags the series (first *periods* rows become null);
        negative *periods* leads (last *|periods|* rows become null).
        """
        return Series(self._col.shift(periods))

    def diff(self, periods: Int = 1) raises -> Series:
        """Return the first discrete difference of the Series.

        ``result[i] = self[i] - self[i - periods]``.
        Raises for non-numeric Series.
        """
        return Series(self._col.diff(periods))

    def pct_change(self, periods: Int = 1) raises -> Series:
        """Return the percentage change between elements *periods* apart.

        ``result[i] = (self[i] - self[i - periods]) / self[i - periods]``.
        Raises for non-numeric Series.
        """
        return Series(self._col.pct_change(periods))

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
        """Backward-fill: propagate the next non-null value backward over nulls.
        """
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

    def _sort_perm(
        self, ascending: Bool, na_last: Bool = True
    ) raises -> List[Int]:
        """Return a stable merge-sort permutation over the column values.

        ``perm[i]`` is the original index of the *i*-th element in sorted
        order.  When *na_last* is ``True`` (default), null elements are placed
        at the end; when ``False``, at the beginning.

        Delegates to ``Column.sort_perm`` so the logic lives in one place.
        """
        return self._col.sort_perm(ascending, na_last)

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
        var idx = self._col._index.copy()
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

    def sqrt(self) raises -> Series:
        return Series(self._col._sqrt())

    def exp(self) raises -> Series:
        return Series(self._col._exp())

    def log(self) raises -> Series:
        return Series(self._col._log())

    def log10(self) raises -> Series:
        return Series(self._col._log10())

    def ceil(self) raises -> Series:
        return Series(self._col._ceil())

    def floor(self) raises -> Series:
        return Series(self._col._floor())

    def neg(self) raises -> Series:
        return Series(self._col._neg())

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

    def to_frame(self, name: Optional[String] = None) raises -> DataFrame:
        """Convert the Series to a single-column DataFrame.

        Parameters
        ----------
        name : Column name in the resulting DataFrame.  When None (default) the
               Series' own name is used.

        Returns
        -------
        DataFrame
            A native bison ``DataFrame`` with one column.
        """
        var col_name = name if name else self.name
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
        by_null_mask: List[Bool] = List[Bool](),
    ) raises -> SeriesGroupBy:
        return SeriesGroupBy(self, by, as_index, sort, dropna, by_null_mask)


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


# Variant returned by the compile-time-dispatch to_dict[orient] overload.
# Arms correspond to each orient value:
#   Dict[String, Dict[String, DFScalar]]  — "dict" / "index"
#   Dict[String, List[DFScalar]]          — "list"
#   List[Dict[String, DFScalar]]          — "records"
#   DictSplitResult                       — "split" / "tight"
#   Dict[String, Series]                  — "series"
comptime ToDictResult = Variant[
    Dict[String, Dict[String, DFScalar]],
    Dict[String, List[DFScalar]],
    List[Dict[String, DFScalar]],
    DictSplitResult,
    Dict[String, Series],
]

# Compile-time operation selectors for DataFrame._cum_axis1
comptime _CUM_SUM = 0
comptime _CUM_PROD = 1
comptime _CUM_MIN = 2
comptime _CUM_MAX = 3


struct _RowKeyVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that serialises a single row element from a ColumnData arm to a
    String.  Used by ``DataFrame._row_key_str`` to build per-row hash keys
    without raw ``isa`` chains.
    """

    var row: Int
    var result: String

    def __init__(out self, row: Int):
        self.row = row
        self.result = String()

    def on_int64(mut self, data: List[Int64]) raises:
        self.result = String(Int(data[self.row]))

    def on_float64(mut self, data: List[Float64]) raises:
        self.result = String(data[self.row])

    def on_bool(mut self, data: List[Bool]) raises:
        self.result = "1" if data[self.row] else "0"

    def on_str(mut self, data: List[String]) raises:
        self.result = data[self.row]

    def on_obj(mut self, data: List[PythonObject]) raises:
        self.result = String(data[self.row])


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
            dict_[self._cols[i].name.value()] = pd_series
        return pd.DataFrame(dict_)

    @staticmethod
    def from_dict(data: Dict[String, ColumnData]) raises -> DataFrame:
        """Create DataFrame from a native dict mapping column names to column data.
        """
        var cols = List[Column]()
        for entry in data.items():
            var col_data = entry.value.copy()
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
            result.append(self._cols[i].name.value())
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
            idx.append(PythonObject(self._cols[i].name.value()))
        var col_data = ColumnData(dtype_names^)
        var result_col = Column(None, col_data^, object_, idx^)
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
                + self._cols[i].name.value()
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
            idx.append(PythonObject(self._cols[i].name.value()))
        var col_data = ColumnData(values^)
        var result_col = Column(None, col_data^, int64, idx^)
        return Series(result_col^)

    # ------------------------------------------------------------------
    # Selection / indexing
    # ------------------------------------------------------------------

    def iloc(mut self) -> ILocIndexer[origin_of(self)]:
        """Integer-position-based row indexer.

        Returns an ``ILocIndexer`` that supports ``df.iloc()[i]`` (single
        row as Series) and ``df.iloc()[i:j]`` (slice of rows as DataFrame).

        Note: Mojo does not yet support ``@property``, so this must be called
        as ``df.iloc()`` rather than the pandas-compatible ``df.iloc``.
        """
        return ILocIndexer(UnsafePointer(to=self))

    def loc(mut self) -> LocIndexer[origin_of(self)]:
        """Label-based row indexer.

        Returns a ``LocIndexer`` that supports ``df.loc()["label"]`` (single
        row as Series) and ``df.loc()[i:j]`` (integer slice of rows as
        DataFrame).

        Note: Mojo does not yet support ``@property``, so this must be called
        as ``df.loc()`` rather than the pandas-compatible ``df.loc``.
        """
        return LocIndexer(UnsafePointer(to=self))

    def __getitem__(self, key: String) raises -> Series:
        for i in range(len(self._cols)):
            if self._cols[i].name == key:
                return Series(self._cols[i].copy())
        raise Error("DataFrame.__getitem__: column not found: " + key)

    def __getitem__(self, mask: Series) raises -> DataFrame:
        """Filter rows using a boolean mask Series.

        Returns a new DataFrame containing only the rows where *mask* is True.
        Null mask values are treated as False (row excluded).

        Example::

            df[df["a"] > 0.5]
        """
        var n = self.shape()[0]
        var mask_len = len(mask._col)
        if mask_len != n:
            raise Error(
                "DataFrame.__getitem__: boolean mask length "
                + String(mask_len)
                + " does not match DataFrame length "
                + String(n)
            )
        var indices = List[Int]()
        var has_mask = len(mask._col._null_mask) > 0
        if mask._col._data.isa[List[Bool]]():
            ref d = mask._col._data[List[Bool]]
            for i in range(mask_len):
                var is_null = has_mask and mask._col._null_mask[i]
                if not is_null and d[i]:
                    indices.append(i)
        else:
            raise Error(
                "DataFrame.__getitem__: mask Series must have bool dtype"
            )
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].take(indices))
        return DataFrame(result_cols^)

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
            return default
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
                return self.copy(True)
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
            var col_name = self._cols[i].name.value()
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
    # Row-wise helpers (used by axis=1 aggregation paths)
    # ------------------------------------------------------------------

    def _row_numeric_vals(self, row: Int, skipna: Bool) raises -> List[Float64]:
        """Collect float values from every numeric column at *row*.

        If *skipna* is True, null cells are omitted.
        If *skipna* is False, null cells contribute NaN so the result
        propagates NaN (matching pandas skipna=False behaviour).
        """
        var vals = List[Float64]()
        var nan = Float64(0) / Float64(0)
        for ci in range(len(self._cols)):
            ref col = self._cols[ci]
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            if col._null_mask[row]:
                if not skipna:
                    vals.append(nan)
                continue
            if col.dtype.is_integer():
                vals.append(Float64(col._int64_data()[row]))
            else:
                vals.append(col._float64_data()[row])
        return vals^

    def _row_non_null_count(self, row: Int) -> Int:
        """Count non-null cells in row *row* across all dtypes (used by count axis=1).
        """
        var cnt = 0
        for ci in range(len(self._cols)):
            if not self._cols[ci]._null_mask[row]:
                cnt += 1
        return cnt

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def sum(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var s = Float64(0)
                for vi in range(len(vals)):
                    s += vals[vi]
                results.append(s)
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.sum")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].sum(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def mean(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n == 0:
                    results.append(nan)
                    continue
                var s = Float64(0)
                for vi in range(n):
                    s += vals[vi]
                results.append(s / Float64(n))
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.mean")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].mean(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def median(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis == 1:
            return self.quantile(0.5, axis=1, skipna=skipna)
        elif axis != 0:
            _not_implemented("DataFrame.median")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].median(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def min(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n == 0:
                    results.append(nan)
                    continue
                var m = vals[0]
                for j in range(1, n):
                    if vals[j] < m:
                        m = vals[j]
                results.append(m)
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.min")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].min(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def max(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n == 0:
                    results.append(nan)
                    continue
                var m = vals[0]
                for j in range(1, n):
                    if vals[j] > m:
                        m = vals[j]
                results.append(m)
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.max")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].max(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def std(
        self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True
    ) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n <= ddof:
                    results.append(nan)
                    continue
                var s = Float64(0)
                for vi in range(n):
                    s += vals[vi]
                var mean_val = s / Float64(n)
                var sq_sum = Float64(0)
                for vi in range(n):
                    var diff = vals[vi] - mean_val
                    sq_sum += diff * diff
                results.append(sqrt(sq_sum / Float64(n - ddof)))
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.std")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].std(ddof, skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def var(
        self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True
    ) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n <= ddof:
                    results.append(nan)
                    continue
                var s = Float64(0)
                for vi in range(n):
                    s += vals[vi]
                var mean_val = s / Float64(n)
                var sq_sum = Float64(0)
                for vi in range(n):
                    var diff = vals[vi] - mean_val
                    sq_sum += diff * diff
                results.append(sq_sum / Float64(n - ddof))
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.var")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].var(ddof, skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def count(self, axis: Int = 0) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var results = List[Float64]()
            for i in range(nrows):
                results.append(Float64(self._row_non_null_count(i)))
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.count")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(Float64(self._cols[i].count()))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def nunique(self, axis: Int = 0) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var results = List[Float64]()
            for i in range(nrows):
                var seen = Dict[String, Bool]()
                for ci in range(len(self._cols)):
                    ref col = self._cols[ci]
                    if col._null_mask[i]:
                        continue
                    var key: String
                    ref cd = col._data
                    if cd.isa[List[Int64]]():
                        # Represent as Float64 so int 1 == float 1.0 (matches pandas)
                        key = "n:" + String(Float64(Int(cd[List[Int64]][i])))
                    elif cd.isa[List[Float64]]():
                        key = "n:" + String(cd[List[Float64]][i])
                    elif cd.isa[List[Bool]]():
                        key = "b:" + ("1" if cd[List[Bool]][i] else "0")
                    elif cd.isa[List[String]]():
                        key = "s:" + cd[List[String]][i]
                    else:
                        key = "o:" + String(cd[List[PythonObject]][i])
                    seen[key] = True
                results.append(Float64(len(seen)))
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.nunique")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(Float64(self._cols[i].nunique()))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def describe(
        self,
        include: Optional[List[String]] = None,
        exclude: Optional[List[String]] = None,
    ) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var dt = self._cols[i].dtype
            if not (dt.is_integer() or dt.is_float()):
                continue
            var values = List[Float64]()
            values.append(Float64(self._cols[i].count()))
            values.append(self._cols[i].mean(True))
            values.append(self._cols[i].std(1, True))
            values.append(self._cols[i].min(True))
            values.append(self._cols[i].quantile(0.25, True))
            values.append(self._cols[i].quantile(0.5, True))
            values.append(self._cols[i].quantile(0.75, True))
            values.append(self._cols[i].max(True))
            var col_data = ColumnData(values^)
            var col_dtype = Column._sniff_dtype(col_data)
            result_cols.append(Column(self._cols[i].name, col_data^, col_dtype))
        return DataFrame(result_cols^)

    def quantile(
        self, q: Float64 = 0.5, axis: Int = 0, skipna: Bool = True
    ) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n == 0:
                    results.append(nan)
                    continue
                _sort_list(vals)
                var pos = q * Float64(n - 1)
                var lo = Int(pos)
                var hi = lo + 1
                if hi >= n:
                    results.append(vals[n - 1])
                else:
                    var frac = pos - Float64(lo)
                    results.append(vals[lo] + frac * (vals[hi] - vals[lo]))
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            _not_implemented("DataFrame.quantile")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].quantile(q, skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def abs(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i]._abs())
        return DataFrame(result_cols^)

    def sqrt(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            ref col = self._cols[i]
            if col.dtype.is_integer() or col.dtype.is_float():
                result_cols.append(col._sqrt())
            else:
                result_cols.append(col.copy())
        return DataFrame(result_cols^)

    def exp(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            ref col = self._cols[i]
            if col.dtype.is_integer() or col.dtype.is_float():
                result_cols.append(col._exp())
            else:
                result_cols.append(col.copy())
        return DataFrame(result_cols^)

    def log(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            ref col = self._cols[i]
            if col.dtype.is_integer() or col.dtype.is_float():
                result_cols.append(col._log())
            else:
                result_cols.append(col.copy())
        return DataFrame(result_cols^)

    def log10(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            ref col = self._cols[i]
            if col.dtype.is_integer() or col.dtype.is_float():
                result_cols.append(col._log10())
            else:
                result_cols.append(col.copy())
        return DataFrame(result_cols^)

    def ceil(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            ref col = self._cols[i]
            if col.dtype.is_integer() or col.dtype.is_float():
                result_cols.append(col._ceil())
            else:
                result_cols.append(col.copy())
        return DataFrame(result_cols^)

    def floor(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            ref col = self._cols[i]
            if col.dtype.is_integer() or col.dtype.is_float():
                result_cols.append(col._floor())
            else:
                result_cols.append(col.copy())
        return DataFrame(result_cols^)

    def neg(self) raises -> DataFrame:
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            ref col = self._cols[i]
            if col.dtype.is_integer() or col.dtype.is_float():
                result_cols.append(col._neg())
            else:
                result_cols.append(col.copy())
        return DataFrame(result_cols^)

    def _cum_axis1[op: Int](self, skipna: Bool) raises -> DataFrame:
        """Shared axis=1 kernel for cumsum, cumprod, cummin, and cummax.

        ``op`` is one of the ``_CUM_*`` compile-time constants; ``comptime if``
        folds the accumulation branch at compile time so each specialisation
        compiles to a tight loop with no runtime dispatch.
        """
        var nrows = self.shape()[0]
        var ncols = len(self._cols)
        var nan = Float64(0) / Float64(0)
        var result_lists = List[List[Float64]]()
        for _ in range(ncols):
            result_lists.append(List[Float64]())
        for i in range(nrows):
            var running = Float64(0)
            comptime if op == _CUM_PROD:
                running = Float64(1)
            var has_value = False
            var propagate_nan = False
            for ci in range(ncols):
                ref col = self._cols[ci]
                if col._null_mask[i] or not (
                    col.dtype.is_integer() or col.dtype.is_float()
                ):
                    if not skipna:
                        propagate_nan = True
                    result_lists[ci].append(nan if propagate_nan else running)
                else:
                    var v = Float64(
                        col._int64_data()[i]
                    ) if col.dtype.is_integer() else col._float64_data()[i]
                    comptime if op == _CUM_SUM:
                        running += v
                    elif op == _CUM_PROD:
                        running *= v
                    elif op == _CUM_MIN:
                        if not has_value:
                            running = v
                            has_value = True
                        elif v < running:
                            running = v
                    else:
                        if not has_value:
                            running = v
                            has_value = True
                        elif v > running:
                            running = v
                    result_lists[ci].append(nan if propagate_nan else running)
        var result_cols = List[Column]()
        for ci in range(ncols):
            var cd = ColumnData(result_lists[ci].copy())
            result_cols.append(Column(self._cols[ci].name, cd^, float64))
        return DataFrame(result_cols^)

    def cumsum(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis == 1:
            return self._cum_axis1[_CUM_SUM](skipna)
        elif axis != 0:
            raise Error(
                "No axis named " + String(axis) + " for object type DataFrame"
            )
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cumsum(skipna))
        return DataFrame(result_cols^)

    def cumprod(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis == 1:
            return self._cum_axis1[_CUM_PROD](skipna)
        elif axis != 0:
            raise Error(
                "No axis named " + String(axis) + " for object type DataFrame"
            )
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cumprod(skipna))
        return DataFrame(result_cols^)

    def cummin(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis == 1:
            return self._cum_axis1[_CUM_MIN](skipna)
        elif axis != 0:
            raise Error(
                "No axis named " + String(axis) + " for object type DataFrame"
            )
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cummin(skipna))
        return DataFrame(result_cols^)

    def cummax(self, axis: Int = 0, skipna: Bool = True) raises -> DataFrame:
        if axis == 1:
            return self._cum_axis1[_CUM_MAX](skipna)
        elif axis != 0:
            raise Error(
                "No axis named " + String(axis) + " for object type DataFrame"
            )
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].cummax(skipna))
        return DataFrame(result_cols^)

    def sem(
        self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True
    ) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n <= ddof:
                    results.append(nan)
                    continue
                var s = Float64(0)
                for vi in range(n):
                    s += vals[vi]
                var mean_val = s / Float64(n)
                var sq_sum = Float64(0)
                for vi in range(n):
                    var diff = vals[vi] - mean_val
                    sq_sum += diff * diff
                var std_val = sqrt(sq_sum / Float64(n - ddof))
                results.append(std_val / sqrt(Float64(n)))
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            raise Error(
                "No axis named " + String(axis) + " for object type DataFrame"
            )
        var values = List[Float64]()
        for i in range(len(self._cols)):
            var n = self._cols[i].count() if skipna else len(self._cols[i])
            if n == 0:
                var zero = Float64(0)
                values.append(zero / zero)
            else:
                values.append(
                    self._cols[i].std(ddof, skipna) / sqrt(Float64(n))
                )
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def skew(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n < 3:
                    results.append(nan)
                    continue
                var s = Float64(0)
                for vi in range(n):
                    s += vals[vi]
                var mean_val = s / Float64(n)
                var sq_sum = Float64(0)
                for vi in range(n):
                    var diff = vals[vi] - mean_val
                    sq_sum += diff * diff
                var std_val = sqrt(sq_sum / Float64(n - 1))
                if std_val == 0.0:
                    results.append(nan)
                    continue
                var m3 = Float64(0)
                for vi in range(n):
                    var diff = vals[vi] - mean_val
                    m3 += diff * diff * diff
                var fn_ = Float64(n)
                results.append(
                    fn_
                    / ((fn_ - 1.0) * (fn_ - 2.0))
                    * m3
                    / (std_val * std_val * std_val)
                )
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            raise Error(
                "No axis named " + String(axis) + " for object type DataFrame"
            )
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].skew(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def kurt(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis == 1:
            var nrows = self.shape()[0]
            var nan = Float64(0) / Float64(0)
            var results = List[Float64]()
            for i in range(nrows):
                var vals = self._row_numeric_vals(i, skipna)
                var n = len(vals)
                if n < 4:
                    results.append(nan)
                    continue
                var s = Float64(0)
                for vi in range(n):
                    s += vals[vi]
                var mean_val = s / Float64(n)
                var sq_sum = Float64(0)
                for vi in range(n):
                    var diff = vals[vi] - mean_val
                    sq_sum += diff * diff
                var std_val = sqrt(sq_sum / Float64(n - 1))
                if std_val == 0.0:
                    results.append(nan)
                    continue
                var m4 = Float64(0)
                for vi in range(n):
                    var diff = vals[vi] - mean_val
                    m4 += diff * diff * diff * diff
                var fn_ = Float64(n)
                var term1 = (
                    fn_
                    * (fn_ + 1.0)
                    / ((fn_ - 1.0) * (fn_ - 2.0) * (fn_ - 3.0))
                    * m4
                    / (std_val * std_val * std_val * std_val)
                )
                var term2 = (
                    3.0
                    * (fn_ - 1.0)
                    * (fn_ - 1.0)
                    / ((fn_ - 2.0) * (fn_ - 3.0))
                )
                results.append(term1 - term2)
            var col_data = ColumnData(results^)
            var dtype = Column._sniff_dtype(col_data)
            return Series(Column(None, col_data^, dtype))
        elif axis != 0:
            raise Error(
                "No axis named " + String(axis) + " for object type DataFrame"
            )
        var values = List[Float64]()
        for i in range(len(self._cols)):
            values.append(self._cols[i].kurt(skipna))
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def idxmin(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.idxmin")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            var pos = self._cols[i].argmin(skipna)
            if pos == -1:
                var zero = Float64(0)
                values.append(zero / zero)
            else:
                var idx_len = self._cols[i]._index_len()
                if idx_len == 0:
                    values.append(Float64(pos))
                elif self._cols[i]._index.isa[List[Int64]]():
                    values.append(
                        Float64(Int(self._cols[i]._index[List[Int64]][pos]))
                    )
                else:
                    _not_implemented("DataFrame.idxmin with non-integer index")
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def idxmax(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        if axis != 0:
            _not_implemented("DataFrame.idxmax")
        var values = List[Float64]()
        for i in range(len(self._cols)):
            var pos = self._cols[i].argmax(skipna)
            if pos == -1:
                var zero = Float64(0)
                values.append(zero / zero)
            else:
                var idx_len = self._cols[i]._index_len()
                if idx_len == 0:
                    values.append(Float64(pos))
                elif self._cols[i]._index.isa[List[Int64]]():
                    values.append(
                        Float64(Int(self._cols[i]._index[List[Int64]][pos]))
                    )
                else:
                    _not_implemented("DataFrame.idxmax with non-integer index")
        var col_data = ColumnData(values^)
        var dtype = Column._sniff_dtype(col_data)
        var result_col = Column(None, col_data^, dtype)
        return Series(result_col^)

    def corr(
        self, method: String = "pearson", min_periods: Int = 1
    ) raises -> DataFrame:
        if method != "pearson":
            _not_implemented("DataFrame.corr with non-pearson method")
        var n = len(self._cols)
        var result_cols = List[Column]()
        for j in range(n):
            var values = List[Float64]()
            for i in range(n):
                values.append(self._cols[i].corr(self._cols[j]))
            var col_data = ColumnData(values^)
            var dtype = Column._sniff_dtype(col_data)
            result_cols.append(Column(self._cols[j].name, col_data^, dtype))
        return DataFrame(result_cols^)

    def cov(self, min_periods: Int = 1, ddof: Int = 1) raises -> DataFrame:
        var n = len(self._cols)
        var result_cols = List[Column]()
        for j in range(n):
            var values = List[Float64]()
            for i in range(n):
                values.append(self._cols[i].cov(self._cols[j], ddof))
            var col_data = ColumnData(values^)
            var dtype = Column._sniff_dtype(col_data)
            result_cols.append(Column(self._cols[j].name, col_data^, dtype))
        return DataFrame(result_cols^)

    def shift(self, periods: Int = 1, axis: Int = 0) raises -> DataFrame:
        """Return a DataFrame with values shifted by *periods* positions.

        For ``axis=0`` (default), each column is shifted independently:
        positive *periods* lags rows (first *periods* rows become null);
        negative *periods* leads rows (last *|periods|* rows become null).
        String and object columns are supported in addition to numeric.

        For ``axis=1``, values are shifted across columns within each row:
        positive *periods* shifts values to the right (first *periods* columns
        become null); negative *periods* shifts values to the left (last
        *|periods|* columns become null).  All columns must be numeric.
        """
        if axis == 1:
            var nrows = self.shape()[0]
            var ncols = len(self._cols)
            var nan = Float64(0) / Float64(0)
            var result_cols = List[Column]()
            for j in range(ncols):
                var src_j = j - periods
                var values = List[Float64]()
                var null_mask = List[Bool]()
                var has_null = False
                if src_j < 0 or src_j >= ncols:
                    for _ in range(nrows):
                        values.append(nan)
                        null_mask.append(True)
                    has_null = True
                else:
                    ref src_col = self._cols[src_j]
                    if not (
                        src_col.dtype.is_integer() or src_col.dtype.is_float()
                    ):
                        raise Error(
                            "DataFrame.shift(axis=1) requires numeric columns"
                        )
                    var has_src_mask = len(src_col._null_mask) > 0
                    for i in range(nrows):
                        var is_null = has_src_mask and src_col._null_mask[i]
                        if is_null:
                            values.append(nan)
                            null_mask.append(True)
                            has_null = True
                        elif src_col.dtype.is_integer():
                            values.append(Float64(src_col._int64_data()[i]))
                            null_mask.append(False)
                        else:
                            values.append(src_col._float64_data()[i])
                            null_mask.append(False)
                var col_data = ColumnData(values^)
                var dtype = Column._sniff_dtype(col_data)
                var col = Column(self._cols[j].name, col_data^, dtype)
                if has_null:
                    col._null_mask = null_mask^
                result_cols.append(col^)
            return DataFrame(result_cols^)
        elif axis != 0:
            _not_implemented("DataFrame.shift")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].shift(periods))
        return DataFrame(result_cols^)

    def diff(self, periods: Int = 1, axis: Int = 0) raises -> DataFrame:
        """Return element-wise first discrete difference along the rows.

        For ``axis=0`` (default), ``result[i] = self[i] - self[i - periods]``
        for each numeric column.  Exposed positions are null.
        Non-numeric columns raise.

        For ``axis=1``, ``result[row][j] = self[row][j] - self[row][j - periods]``
        shifting the differencing across columns within each row.  Exposed
        column positions are null.  All columns must be numeric.
        """
        if axis == 1:
            var nrows = self.shape()[0]
            var ncols = len(self._cols)
            var nan = Float64(0) / Float64(0)
            var result_cols = List[Column]()
            for j in range(ncols):
                var src_j = j - periods
                var values = List[Float64]()
                var null_mask = List[Bool]()
                var has_null = False
                if src_j < 0 or src_j >= ncols:
                    for _ in range(nrows):
                        values.append(nan)
                        null_mask.append(True)
                    has_null = True
                else:
                    ref cur_col = self._cols[j]
                    ref src_col = self._cols[src_j]
                    if not (
                        cur_col.dtype.is_integer() or cur_col.dtype.is_float()
                    ):
                        raise Error(
                            "DataFrame.diff(axis=1) requires numeric columns"
                        )
                    if not (
                        src_col.dtype.is_integer() or src_col.dtype.is_float()
                    ):
                        raise Error(
                            "DataFrame.diff(axis=1) requires numeric columns"
                        )
                    var has_cur_mask = len(cur_col._null_mask) > 0
                    var has_src_mask = len(src_col._null_mask) > 0
                    for i in range(nrows):
                        var cur_null = has_cur_mask and cur_col._null_mask[i]
                        var src_null = has_src_mask and src_col._null_mask[i]
                        if cur_null or src_null:
                            values.append(nan)
                            null_mask.append(True)
                            has_null = True
                        else:
                            var cur_val: Float64
                            if cur_col.dtype.is_integer():
                                cur_val = Float64(cur_col._int64_data()[i])
                            else:
                                cur_val = cur_col._float64_data()[i]
                            var src_val: Float64
                            if src_col.dtype.is_integer():
                                src_val = Float64(src_col._int64_data()[i])
                            else:
                                src_val = src_col._float64_data()[i]
                            values.append(cur_val - src_val)
                            null_mask.append(False)
                var col_data = ColumnData(values^)
                var dtype = Column._sniff_dtype(col_data)
                var col = Column(self._cols[j].name, col_data^, dtype)
                if has_null:
                    col._null_mask = null_mask^
                result_cols.append(col^)
            return DataFrame(result_cols^)
        elif axis != 0:
            _not_implemented("DataFrame.diff")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].diff(periods))
        return DataFrame(result_cols^)

    def pct_change(self, periods: Int = 1, axis: Int = 0) raises -> DataFrame:
        """Return element-wise percentage change along the rows.

        For ``axis=0`` (default),
        ``result[i] = (self[i] - self[i - periods]) / self[i - periods]``
        for each numeric column.  Exposed positions are null.

        For ``axis=1``,
        ``result[row][j] = (self[row][j] - self[row][j - periods]) / self[row][j - periods]``
        shifting the computation across columns within each row.  Exposed
        column positions are null.  All columns must be numeric.
        """
        if axis == 1:
            var nrows = self.shape()[0]
            var ncols = len(self._cols)
            var nan = Float64(0) / Float64(0)
            var result_cols = List[Column]()
            for j in range(ncols):
                var src_j = j - periods
                var values = List[Float64]()
                var null_mask = List[Bool]()
                var has_null = False
                if src_j < 0 or src_j >= ncols:
                    for _ in range(nrows):
                        values.append(nan)
                        null_mask.append(True)
                    has_null = True
                else:
                    ref cur_col = self._cols[j]
                    ref src_col = self._cols[src_j]
                    if not (
                        cur_col.dtype.is_integer() or cur_col.dtype.is_float()
                    ):
                        raise Error(
                            "DataFrame.pct_change(axis=1) requires numeric"
                            " columns"
                        )
                    if not (
                        src_col.dtype.is_integer() or src_col.dtype.is_float()
                    ):
                        raise Error(
                            "DataFrame.pct_change(axis=1) requires numeric"
                            " columns"
                        )
                    var has_cur_mask = len(cur_col._null_mask) > 0
                    var has_src_mask = len(src_col._null_mask) > 0
                    for i in range(nrows):
                        var cur_null = has_cur_mask and cur_col._null_mask[i]
                        var src_null = has_src_mask and src_col._null_mask[i]
                        if cur_null or src_null:
                            values.append(nan)
                            null_mask.append(True)
                            has_null = True
                        else:
                            var cur_val: Float64
                            if cur_col.dtype.is_integer():
                                cur_val = Float64(cur_col._int64_data()[i])
                            else:
                                cur_val = cur_col._float64_data()[i]
                            var src_val: Float64
                            if src_col.dtype.is_integer():
                                src_val = Float64(src_col._int64_data()[i])
                            else:
                                src_val = src_col._float64_data()[i]
                            values.append((cur_val - src_val) / src_val)
                            null_mask.append(False)
                var col_data = ColumnData(values^)
                var dtype = Column._sniff_dtype(col_data)
                var col = Column(self._cols[j].name, col_data^, dtype)
                if has_null:
                    col._null_mask = null_mask^
                result_cols.append(col^)
            return DataFrame(result_cols^)
        elif axis != 0:
            _not_implemented("DataFrame.pct_change")
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            result_cols.append(self._cols[i].pct_change(periods))
        return DataFrame(result_cols^)

    def agg(self, func: String, axis: Int = 0) raises -> Series:
        if axis == 1:
            if func == "sum":
                return self.sum(axis=1)
            elif func == "mean":
                return self.mean(axis=1)
            elif func == "median":
                return self.median(axis=1)
            elif func == "min":
                return self.min(axis=1)
            elif func == "max":
                return self.max(axis=1)
            elif func == "std":
                return self.std(axis=1)
            elif func == "var":
                return self.var(axis=1)
            elif func == "count":
                return self.count(axis=1)
            elif func == "nunique":
                return self.nunique(axis=1)
            else:
                raise Error(
                    "DataFrame.agg: unsupported aggregation '"
                    + func
                    + "'. Supported: sum, mean, median, min, max, std, var,"
                    " count, nunique"
                )
        elif axis != 0:
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

    def apply(self, func: String, axis: Int = 0) raises -> Series:
        if axis == 0:
            return self.agg(func)
        elif axis == 1:
            if func == "sum":
                return self.sum(axis=1)
            elif func == "mean":
                return self.mean(axis=1)
            elif func == "median":
                return self.median(axis=1)
            elif func == "min":
                return self.min(axis=1)
            elif func == "max":
                return self.max(axis=1)
            elif func == "std":
                return self.std(axis=1)
            elif func == "var":
                return self.var(axis=1)
            elif func == "count":
                return self.count(axis=1)
            elif func == "nunique":
                return self.nunique(axis=1)
            else:
                raise Error(
                    "DataFrame.apply: unsupported func '"
                    + func
                    + "' for axis=1. Supported: sum, mean, median, min, max,"
                    " std, var, count, nunique"
                )
        else:
            raise Error(
                "No axis named " + String(axis) + " for object type DataFrame"
            )

    def apply[F: FloatTransformFn](self, axis: Int = 0) raises -> DataFrame:
        if axis != 0:
            raise Error(
                "DataFrame.apply[F]: compile-time functions only supported"
                " for axis=0 (column-wise element-wise transform)"
            )
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            ref col = self._cols[i]
            if col.dtype.is_integer() or col.dtype.is_float():
                result_cols.append(col._apply[F]())
            else:
                result_cols.append(col.copy())
        return DataFrame(result_cols^)

    def applymap(self, func: String) raises -> DataFrame:
        if func == "abs":
            return self.abs()
        elif func == "round":
            return self.round()
        elif func == "sqrt":
            return self.sqrt()
        elif func == "exp":
            return self.exp()
        elif func == "log":
            return self.log()
        elif func == "log10":
            return self.log10()
        elif func == "ceil":
            return self.ceil()
        elif func == "floor":
            return self.floor()
        elif func == "neg" or func == "negate":
            return self.neg()
        else:
            raise Error(
                "DataFrame.applymap: unsupported func '"
                + func
                + "'. Supported: abs, round, sqrt, exp, log, log10, ceil,"
                " floor, neg"
            )

    def applymap[F: FloatTransformFn](self) raises -> DataFrame:
        return self.apply[F](axis=0)

    def transform(self, func: String, axis: Int = 0) raises -> DataFrame:
        if axis == 1:
            if func == "cumsum":
                return self.cumsum(axis=1)
            elif func == "cumprod":
                return self.cumprod(axis=1)
            elif func == "cummin":
                return self.cummin(axis=1)
            elif func == "cummax":
                return self.cummax(axis=1)
            else:
                raise Error(
                    "DataFrame.transform: unsupported func '"
                    + func
                    + "' for axis=1. Supported: cumsum, cumprod, cummin, cummax"
                )
        elif axis != 0:
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
        elif func == "sqrt":
            return self.sqrt()
        elif func == "exp":
            return self.exp()
        elif func == "log":
            return self.log()
        elif func == "log10":
            return self.log10()
        elif func == "ceil":
            return self.ceil()
        elif func == "floor":
            return self.floor()
        elif func == "neg" or func == "negate":
            return self.neg()
        else:
            raise Error(
                "DataFrame.transform: unsupported func '"
                + func
                + "'. Supported: abs, cumsum, cumprod, cummin, cummax, sqrt,"
                " exp, log, log10, ceil, floor, neg"
            )

    def eval(self, expr: String) raises -> Series:
        """Evaluate *expr* against this DataFrame and return a boolean Series.

        In-scope expressions are comparison predicates, optionally combined
        with ``and``, ``or``, and ``not``, and parenthetical groupings.
        Unsupported syntax (arithmetic, assignment, function calls, etc.)
        raises an Error whose message contains ``"unsupported syntax"``.
        """
        var parsed = _parse_expr(expr)
        return _eval_expr(parsed, self)

    def query(self, expr: String) raises -> DataFrame:
        var parsed = _parse_expr(expr)
        var mask = _eval_expr(parsed, self)
        return self[mask]

    def pipe(self, func: String) raises -> DataFrame:
        if func == "abs":
            return self.abs()
        elif func == "sqrt":
            return self.sqrt()
        elif func == "exp":
            return self.exp()
        elif func == "log":
            return self.log()
        elif func == "log10":
            return self.log10()
        elif func == "ceil":
            return self.ceil()
        elif func == "floor":
            return self.floor()
        elif func == "neg" or func == "negate":
            return self.neg()
        else:
            raise Error(
                "DataFrame.pipe: unsupported func '"
                + func
                + "'. Supported: abs, sqrt, exp, log, log10, ceil, floor, neg"
            )

    def pipe[F: fn(DataFrame) raises -> DataFrame](self) raises -> DataFrame:
        return F(self)

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
            var new_col = Column(
                col.name, col_data^, col.dtype, col._index.copy()
            )
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
            var col_idx = _df_col_index(self, by[k])
            var sub_perm = (
                self._cols[col_idx]
                .take(perm)
                .sort_perm(asc, na_position == "last")
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
                taken._index = new_idx.copy()
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
                var key_name = self._cols[key].name.value()
                var j = i - 1
                while j >= 0:
                    var prev = perm[j]
                    var do_swap: Bool
                    if ascending:
                        do_swap = self._cols[prev].name.value() > key_name
                    else:
                        do_swap = self._cols[prev].name.value() < key_name
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
                taken._index = new_idx.copy()
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
                c._index = new_idx.copy()
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
            c._index = new_idx.copy()
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
                if c.name.value() in col_map:
                    c.name = col_map[c.name.value()]
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
                col_map[self._cols[i].name.value()] = i
            # Determine the shared index for the result.
            var shared_idx = ColumnIndex(List[PythonObject]())
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
                        var c = Column._fill_scalar(
                            lbl, fill_value.value(), nrows, shared_idx.copy()
                        )
                        new_cols.append(c^)
                    else:
                        # Null column: infer dtype from existing columns so
                        # that a frame of all-int64 columns produces an int64
                        # null column rather than float64.
                        var c = Column._null_column(
                            lbl, inferred_dtype, nrows, shared_idx.copy()
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
                c._index = new_col_idx.copy()
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
                if self._cols[i].name.value() not in drop_set:
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

        var col = Column(None, ColumnData(result^), bool_)
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

        # Build a dense values table (row_key × col_key) using flat lists with
        # stride arithmetic: cell (rk, ck) lives at index rk * n_ck + ck.
        # This avoids the double index operation per cell that a
        # List[List[...]] layout would require.
        var table = List[PythonObject]()
        var filled = List[Bool]()
        for _ in range(n_rk * n_ck):
            table.append(py_none)
            filled.append(False)

        for r in range(nrows):
            var rk = seen_rows[_frame_cell_as_str(self._cols[idx_ci], r)]
            var ck = seen_cols[_frame_cell_as_str(self._cols[col_ci], r)]
            var cell = rk * n_ck + ck
            if filled[cell]:
                raise Error(
                    "DataFrame.pivot: duplicate entry for ("
                    + row_keys[rk]
                    + ", "
                    + col_keys[ck]
                    + ")"
                )
            table[cell] = _frame_cell_as_python(self._cols[val_ci], r)
            filled[cell] = True

        # Construct index labels (string Index) shared by all result columns.
        var result_idx = ColumnIndex(Index(row_keys^))

        # Build one output Column per col_key.
        var result_cols = List[Column]()
        for ck in range(n_ck):
            var data = List[PythonObject]()
            var null_mask = List[Bool]()
            var any_null = False
            for rk in range(n_rk):
                var cell = rk * n_ck + ck
                if not filled[cell]:
                    data.append(py_none)
                    null_mask.append(True)
                    any_null = True
                else:
                    data.append(table[cell])
                    null_mask.append(False)
            var col = Column(col_keys[ck], ColumnData(data^), object_)
            col._index = result_idx.copy()
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
        if aggfunc != "mean" and aggfunc != "sum" and aggfunc != "count":
            raise Error(
                "DataFrame.pivot_table: aggfunc must be one of 'mean', 'sum',"
                " or 'count'"
            )

        var idx_names = List[String]()
        if index:
            idx_names = index.value().copy()

        var col_names = List[String]()
        if columns:
            col_names = columns.value().copy()

        var val_names = List[String]()
        if values:
            val_names = values.value().copy()

        var name_to_ci = Dict[String, Int]()
        for j in range(len(self._cols)):
            name_to_ci[self._cols[j].name.value()] = j

        for k in range(len(idx_names)):
            if idx_names[k] not in name_to_ci:
                raise Error(
                    "DataFrame.pivot_table: index column not found: "
                    + idx_names[k]
                )
        for k in range(len(col_names)):
            if col_names[k] not in name_to_ci:
                raise Error(
                    "DataFrame.pivot_table: columns column not found: "
                    + col_names[k]
                )

        if len(val_names) == 0:
            for j in range(len(self._cols)):
                var name = self._cols[j].name.value()
                var used = False
                for k in range(len(idx_names)):
                    if name == idx_names[k]:
                        used = True
                        break
                if not used:
                    for k in range(len(col_names)):
                        if name == col_names[k]:
                            used = True
                            break
                if not used:
                    val_names.append(name)

        if len(val_names) == 0:
            raise Error(
                "DataFrame.pivot_table: values resolved to an empty column list"
            )
        for k in range(len(val_names)):
            if val_names[k] not in name_to_ci:
                raise Error(
                    "DataFrame.pivot_table: values column not found: "
                    + val_names[k]
                )

        var py = Python.import_module("builtins")
        var py_none = Python.evaluate("None")
        var nrows = self.shape()[0]

        var row_labels = List[String]()
        var row_keys = Dict[String, Int]()
        var col_labels = List[String]()
        var col_keys = Dict[String, Int]()

        for r in range(nrows):
            var row_label = String("0")
            if len(idx_names) == 1:
                row_label = _frame_cell_as_str(
                    self._cols[name_to_ci[idx_names[0]]], r
                )
            elif len(idx_names) > 1:
                var items = py.list()
                for k in range(len(idx_names)):
                    _ = items.append(
                        _frame_cell_as_python(
                            self._cols[name_to_ci[idx_names[k]]], r
                        )
                    )
                row_label = String(py.tuple(items))
            if row_label not in row_keys:
                row_keys[row_label] = len(row_labels)
                row_labels.append(row_label)

            var col_label = String("__all__")
            if len(col_names) == 1:
                col_label = _frame_cell_as_str(
                    self._cols[name_to_ci[col_names[0]]], r
                )
            elif len(col_names) > 1:
                var citems = py.list()
                for k in range(len(col_names)):
                    _ = citems.append(
                        _frame_cell_as_python(
                            self._cols[name_to_ci[col_names[k]]], r
                        )
                    )
                col_label = String(py.tuple(citems))
            if col_label not in col_keys:
                col_keys[col_label] = len(col_labels)
                col_labels.append(col_label)

        var n_rk = len(row_labels)
        var n_ck = len(col_labels)
        var n_val = len(val_names)

        var n_out = n_val
        if len(col_names) > 0:
            if n_val == 1:
                n_out = n_ck
            else:
                n_out = n_val * n_ck

        var out_names = List[String]()
        if len(col_names) == 0:
            for vi in range(n_val):
                out_names.append(val_names[vi])
        elif n_val == 1:
            for ck in range(n_ck):
                out_names.append(col_labels[ck])
        else:
            for vi in range(n_val):
                for ck in range(n_ck):
                    out_names.append(val_names[vi] + "|" + col_labels[ck])

        var sums = List[List[Float64]]()
        var counts = List[List[Int]]()
        for _ in range(n_rk):
            var sum_row = List[Float64]()
            var count_row = List[Int]()
            for _ in range(n_out):
                sum_row.append(0.0)
                count_row.append(0)
            sums.append(sum_row^)
            counts.append(count_row^)

        for r in range(nrows):
            var row_label = String("0")
            if len(idx_names) == 1:
                row_label = _frame_cell_as_str(
                    self._cols[name_to_ci[idx_names[0]]], r
                )
            elif len(idx_names) > 1:
                var items = py.list()
                for k in range(len(idx_names)):
                    _ = items.append(
                        _frame_cell_as_python(
                            self._cols[name_to_ci[idx_names[k]]], r
                        )
                    )
                row_label = String(py.tuple(items))
            var rk = row_keys[row_label]

            var col_label = String("__all__")
            if len(col_names) == 1:
                col_label = _frame_cell_as_str(
                    self._cols[name_to_ci[col_names[0]]], r
                )
            elif len(col_names) > 1:
                var citems = py.list()
                for k in range(len(col_names)):
                    _ = citems.append(
                        _frame_cell_as_python(
                            self._cols[name_to_ci[col_names[k]]], r
                        )
                    )
                col_label = String(py.tuple(citems))
            var ck = col_keys[col_label]

            for vi in range(n_val):
                var out_pos = vi
                if len(col_names) > 0:
                    if n_val == 1:
                        out_pos = ck
                    else:
                        out_pos = vi * n_ck + ck

                ref vcol = self._cols[name_to_ci[val_names[vi]]]
                var is_null = len(vcol._null_mask) > 0 and vcol._null_mask[r]
                if not is_null and vcol._data.isa[List[PythonObject]]():
                    is_null = (
                        String(
                            vcol._data[List[PythonObject]][r].__class__.__name__
                        )
                        == "NoneType"
                    )
                if is_null:
                    continue

                if aggfunc == "count":
                    counts[rk][out_pos] += 1
                    continue

                if vcol._data.isa[List[Int64]]():
                    sums[rk][out_pos] += Float64(vcol._data[List[Int64]][r])
                elif vcol._data.isa[List[Float64]]():
                    sums[rk][out_pos] += vcol._data[List[Float64]][r]
                elif vcol._data.isa[List[Bool]]():
                    if vcol._data[List[Bool]][r]:
                        sums[rk][out_pos] += Float64(1.0)
                else:
                    raise Error(
                        "DataFrame.pivot_table: aggfunc "
                        + aggfunc
                        + " requires numeric values; got non-numeric column: "
                        + val_names[vi]
                    )
                counts[rk][out_pos] += 1

        var result_idx = ColumnIndex(Index(row_labels^))
        var result_cols = List[Column]()

        for out_i in range(n_out):
            var data = List[PythonObject]()
            var null_mask = List[Bool]()
            var any_null = False
            for rk in range(n_rk):
                if aggfunc == "count":
                    data.append(PythonObject(Int(counts[rk][out_i])))
                    null_mask.append(False)
                    continue
                if counts[rk][out_i] == 0:
                    data.append(py_none)
                    null_mask.append(True)
                    any_null = True
                    continue
                null_mask.append(False)
                if aggfunc == "sum":
                    data.append(PythonObject(sums[rk][out_i]))
                else:
                    data.append(
                        PythonObject(
                            sums[rk][out_i] / Float64(counts[rk][out_i])
                        )
                    )
            var col = Column(out_names[out_i], ColumnData(data^), object_)
            col._index = result_idx.copy()
            if any_null:
                col._null_mask = null_mask^
            result_cols.append(col^)

        return DataFrame(result_cols^)

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
                    val_names.append(self._cols[j].name.value())

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
                _ = tup_items.append(PythonObject(col.name.value()))
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
        if len(self._cols) == 0:
            return DataFrame()
        if not self._has_index():
            raise Error("DataFrame.unstack: requires an explicit MultiIndex")
        if not self._cols[0]._index.isa[List[PythonObject]]():
            raise Error("DataFrame.unstack: requires a tuple-backed MultiIndex")

        ref idx_objs = self._cols[0]._index[List[PythonObject]]
        if len(idx_objs) == 0:
            raise Error("DataFrame.unstack: requires a non-empty MultiIndex")
        if String(idx_objs[0].__class__.__name__) != "tuple":
            raise Error("DataFrame.unstack: requires a tuple-backed MultiIndex")

        var n_levels = Int(idx_objs[0].__len__())
        var lvl = level
        if lvl < 0:
            lvl += n_levels
        if lvl < 0 or lvl >= n_levels:
            raise Error("DataFrame.unstack: level out of range")
        if n_levels < 2:
            raise Error("DataFrame.unstack: requires at least 2 index levels")

        var py = Python.import_module("builtins")
        var py_none = Python.evaluate("None")

        var row_keys = List[PythonObject]()
        var row_seen = Dict[String, Int]()
        var col_keys = List[PythonObject]()
        var col_seen = Dict[String, Int]()

        for r in range(self.shape()[0]):
            var idx_t = idx_objs[r]
            var col_key = idx_t.__getitem__(lvl)
            var col_s = String(col_key)
            if col_s not in col_seen:
                col_seen[col_s] = len(col_keys)
                col_keys.append(col_key)

            var rem_items = py.list()
            for k in range(n_levels):
                if k == lvl:
                    continue
                _ = rem_items.append(idx_t.__getitem__(k))
            var row_key: PythonObject
            if n_levels - 1 == 1:
                row_key = rem_items.__getitem__(0)
            else:
                row_key = py.tuple(rem_items)
            var row_s = String(row_key)
            if row_s not in row_seen:
                row_seen[row_s] = len(row_keys)
                row_keys.append(row_key)

        var n_rk = len(row_keys)
        var n_ck = len(col_keys)
        var n_src = len(self._cols)

        var table = List[List[PythonObject]]()
        var filled = List[List[Bool]]()
        var n_out = n_src * n_ck
        for _ in range(n_rk):
            var row_data = List[PythonObject]()
            var row_fill = List[Bool]()
            for _ in range(n_out):
                row_data.append(py_none)
                row_fill.append(False)
            table.append(row_data^)
            filled.append(row_fill^)

        for r in range(self.shape()[0]):
            var idx_t = idx_objs[r]
            var col_s = String(idx_t.__getitem__(lvl))
            var ck = col_seen[col_s]

            var rem_items = py.list()
            for k in range(n_levels):
                if k == lvl:
                    continue
                _ = rem_items.append(idx_t.__getitem__(k))
            var row_key: PythonObject
            if n_levels - 1 == 1:
                row_key = rem_items.__getitem__(0)
            else:
                row_key = py.tuple(rem_items)
            var rk = row_seen[String(row_key)]

            for j in range(n_src):
                var out = j * n_ck + ck
                if filled[rk][out]:
                    raise Error(
                        "DataFrame.unstack: duplicate entry for the same index"
                        " combination"
                    )
                table[rk][out] = _frame_cell_as_python(self._cols[j], r)
                filled[rk][out] = True

        var result_idx = ColumnIndex(row_keys^)
        var src_idx_names = self._cols[0]._index_names.copy()
        var rem_idx_names = List[String]()
        if len(src_idx_names) == n_levels:
            for k in range(n_levels):
                if k != lvl:
                    rem_idx_names.append(src_idx_names[k])

        var result_cols = List[Column]()
        for j in range(n_src):
            for ck in range(n_ck):
                var out = j * n_ck + ck
                var out_name: String
                if n_src == 1:
                    out_name = String(col_keys[ck])
                else:
                    var pair_items = py.list()
                    _ = pair_items.append(
                        PythonObject(self._cols[j].name.value())
                    )
                    _ = pair_items.append(col_keys[ck])
                    out_name = String(py.tuple(pair_items))

                var data = List[PythonObject]()
                var null_mask = List[Bool]()
                var any_null = False
                for rk in range(n_rk):
                    if not filled[rk][out]:
                        data.append(py_none)
                        null_mask.append(True)
                        any_null = True
                    else:
                        data.append(table[rk][out])
                        null_mask.append(False)

                var col = Column(out_name, ColumnData(data^), object_)
                col._index = result_idx.copy()
                if len(rem_idx_names) > 1:
                    col._index_names = rem_idx_names.copy()
                    col._index_name = ""
                elif len(rem_idx_names) == 1:
                    col._index_names = List[String]()
                    col._index_name = rem_idx_names[0]
                if any_null:
                    col._null_mask = null_mask^
                result_cols.append(col^)

        return DataFrame(result_cols^)

    def unstack_to_series(self) raises -> Series:
        """Pivot row labels to a new column dimension for a simple-index DataFrame.

        Returns a ``Series`` whose index is a 2-level MultiIndex of
        ``(column_name, row_label)`` tuples and whose values are the cell
        values of this DataFrame in column-major order.  All values are
        converted to Python objects so the result always uses ``object`` dtype.

        This method provides the same result as ``pandas.DataFrame.unstack``
        when the source DataFrame has a regular (non-MultiIndex) row index.
        For DataFrames that already carry a MultiIndex, use
        :meth:`unstack` instead, which returns a :class:`DataFrame`.
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

        for j in range(ncols):
            ref col = self._cols[j]
            var col_label = PythonObject(col.name.value())
            for r in range(nrows):
                var row_label: PythonObject
                if self._has_index():
                    row_label = PythonObject(self._cols[0]._index_label(r))
                else:
                    row_label = PythonObject(r)
                var is_null = len(col._null_mask) > 0 and col._null_mask[r]
                var tup_items = py.list()
                _ = tup_items.append(col_label)
                _ = tup_items.append(row_label)
                idx_objs.append(py.tuple(tup_items))
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
            orig_col_names.append(self._cols[j].name.value())
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
            new_col._index = shared_idx.copy()
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
        if axis != 0:
            raise Error(
                "DataFrame.swaplevel: only axis=0 is currently supported"
            )
        if len(self._cols) == 0:
            return DataFrame()
        if not self._has_index():
            raise Error("DataFrame.swaplevel: requires a MultiIndex on axis 0")
        if not self._cols[0]._index.isa[List[PythonObject]]():
            raise Error(
                "DataFrame.swaplevel: requires a tuple-backed MultiIndex"
            )

        ref idx_objs = self._cols[0]._index[List[PythonObject]]
        if len(idx_objs) == 0:
            raise Error("DataFrame.swaplevel: requires a non-empty MultiIndex")
        if String(idx_objs[0].__class__.__name__) != "tuple":
            raise Error(
                "DataFrame.swaplevel: requires a tuple-backed MultiIndex"
            )

        var n_levels = Int(idx_objs[0].__len__())
        if n_levels < 2:
            raise Error("DataFrame.swaplevel: requires at least 2 index levels")

        var ii = i
        var jj = j
        if ii < 0:
            ii += n_levels
        if jj < 0:
            jj += n_levels
        if ii < 0 or ii >= n_levels or jj < 0 or jj >= n_levels:
            raise Error("DataFrame.swaplevel: level out of range")
        if ii == jj:
            return self.copy(deep=True)

        var py = Python.import_module("builtins")
        var swapped_idx = List[PythonObject]()
        for r in range(len(idx_objs)):
            var tup = idx_objs[r]
            var items = py.list()
            for k in range(n_levels):
                if k == ii:
                    _ = items.append(tup.__getitem__(jj))
                elif k == jj:
                    _ = items.append(tup.__getitem__(ii))
                else:
                    _ = items.append(tup.__getitem__(k))
            swapped_idx.append(py.tuple(items))

        var idx_names = self._cols[0]._index_names.copy()
        if len(idx_names) == n_levels:
            var tmp_name = idx_names[ii]
            idx_names[ii] = idx_names[jj]
            idx_names[jj] = tmp_name

        var new_cols = List[Column]()
        for c in range(len(self._cols)):
            var col = self._cols[c].copy()
            col._index = ColumnIndex(swapped_idx.copy())
            if len(idx_names) == n_levels:
                col._index_names = idx_names.copy()
                col._index_name = ""
            new_cols.append(col^)
        return DataFrame(new_cols^)

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

    def copy(self, deep: Bool = True) -> DataFrame:
        """Return an independent copy of this DataFrame.

        When ``deep=True`` (the default), a full independent copy of all
        columns is returned.

        When ``deep=False``, a deep copy is also returned.  Mojo's value
        semantics mean that all ``DataFrame`` copies are independent by
        construction, so there is no observable difference between a shallow
        and a deep copy.  This matches the documented behaviour of pandas for
        copy-on-write-backed DataFrames.
        """
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
            var col_name = self._cols[i].name.value()
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

        For multi-column keys, uses length-prefixed encoding (``len:value``)
        for each component so that values containing arbitrary characters
        never cause collisions.  For example, (``"a|b"``, ``"c"``) encodes
        as ``"3:a|b1:c"`` while (``"a"``, ``"b|c"``) encodes as
        ``"1:a3:b|c"`` — always distinct.

        Single-column keys return the raw stringified value (no encoding
        needed since there is no inter-column delimiter to collide with).
        """
        var n_keys = len(key_cols)
        var key = String()
        var visitor = _RowKeyVisitor(row)
        for k in range(n_keys):
            var i = col_idx[key_cols[k]]
            visitor.result = String()
            visit_col_data_raises(visitor, df._cols[i]._data)
            var part = visitor.result
            if n_keys == 1:
                return part
            key += String(len(part)) + ":" + part
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
            right_col_idx[right._cols[i].name.value()] = i
        var left_col_idx = Dict[String, Int]()
        for i in range(len(self._cols)):
            left_col_idx[self._cols[i].name.value()] = i

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
            if right._cols[j].name.value() not in key_set:
                right_nonkey_names[right._cols[j].name.value()] = True

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
            if self._cols[i].name.value() in key_set:
                continue
            var col = self._cols[i].take_with_nulls(out_left)
            if col.name.value() in right_nonkey_names:
                col.name = col.name.value() + lsuf
            result_cols.append(col^)

        # Right non-key columns.
        for j in range(len(right._cols)):
            if right._cols[j].name.value() in key_set:
                continue
            var col = right._cols[j].take_with_nulls(out_right)
            var in_left = False
            for i in range(len(self._cols)):
                if (
                    self._cols[i].name.value() not in key_set
                    and self._cols[i].name == right._cols[j].name
                ):
                    in_left = True
                    break
            if in_left:
                col.name = col.name.value() + rsuf
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
        if (
            how != "left"
            and how != "inner"
            and how != "outer"
            and how != "right"
        ):
            raise Error(
                "join: 'how' must be one of 'left', 'right', 'inner', 'outer'"
            )

        # If 'on' is specified, delegate to merge (key-based join).
        if on:
            var suf = List[String]()
            suf.append(lsuffix)
            suf.append(rsuffix)
            var result = self.merge(
                other,
                how=how,
                on=on,
                suffixes=Optional[List[String]](suf^),
            )
            if sort:
                result = result.sort_values(on.value())
            return result^

        # Positional join: align row i of self with row i of other.
        var n_left = self.shape()[0]
        var n_right = other.shape()[0]

        # Build parallel index lists for take_with_nulls (-1 inserts a null).
        var out_left = List[Int]()
        var out_right = List[Int]()

        if how == "left":
            for i in range(n_left):
                out_left.append(i)
                if i < n_right:
                    out_right.append(i)
                else:
                    out_right.append(-1)
        elif how == "inner":
            var min_n = n_left if n_left < n_right else n_right
            for i in range(min_n):
                out_left.append(i)
                out_right.append(i)
        elif how == "outer":
            var max_n = n_left if n_left > n_right else n_right
            for i in range(max_n):
                if i < n_left:
                    out_left.append(i)
                else:
                    out_left.append(-1)
                if i < n_right:
                    out_right.append(i)
                else:
                    out_right.append(-1)
        else:  # how == "right"
            for i in range(n_right):
                out_right.append(i)
                if i < n_left:
                    out_left.append(i)
                else:
                    out_left.append(-1)

        # Build right column name set for overlap detection.
        var right_names = Dict[String, Bool]()
        for j in range(len(other._cols)):
            right_names[other._cols[j].name.value()] = True

        var left_names = Dict[String, Bool]()
        for i in range(len(self._cols)):
            left_names[self._cols[i].name.value()] = True

        # Detect overlap.
        var overlap = False
        for i in range(len(self._cols)):
            if self._cols[i].name.value() in right_names:
                overlap = True
                break
        if overlap and lsuffix == "" and rsuffix == "":
            raise Error(
                "columns overlap but no suffix specified: use lsuffix/rsuffix"
            )

        var result_cols = List[Column]()

        # Left columns — take with nulls, rename if overlap.
        for i in range(len(self._cols)):
            var col = self._cols[i].take_with_nulls(out_left)
            if col.name.value() in right_names:
                col.name = col.name.value() + lsuffix
            result_cols.append(col^)

        # Right columns — take with nulls, rename if overlap.
        for j in range(len(other._cols)):
            var col = other._cols[j].take_with_nulls(out_right)
            if col.name.value() in left_names:
                col.name = col.name.value() + rsuffix
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
            other_idx[other._cols[j].name.value()] = j
        var result_cols = List[Column]()
        for i in range(len(self._cols)):
            var name = self._cols[i].name.value()
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
            header_parts.append(self._cols[ci].name.value())
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

        Uses marrow's native Parquet writer (via the Arrow C Stream
        Interface) for int64, float64, bool, and string columns.  Falls
        back to pandas when the DataFrame contains unsupported column
        types (e.g. object / datetime stored as PythonObject).

        Parameters
        ----------
        path        : Destination file path.
        engine      : Parquet library to use (``"auto"``, ``"pyarrow"``,
                      ``"fastparquet"``).  Only used on the pandas
                      fallback path.
        compression : Compression codec (default ``"snappy"``).  Only
                      used on the pandas fallback path.
        """
        from .arrow import dataframe_to_table
        from marrow.parquet import write_table as _marrow_write_table

        try:
            var table = dataframe_to_table(self)
            _marrow_write_table(table, path)
        except:
            # Fallback for object columns → pandas path.
            var pd_df = self.to_pandas()
            pd_df.to_parquet(path, engine=engine, compression=compression)

    def to_ipc(self, path: String) raises:
        """Write the DataFrame to an Arrow IPC (Feather v2) file.

        Uses PyArrow's feather module via pandas interop.

        Parameters
        ----------
        path : Destination file path.
        """
        from .io.ipc import write_ipc

        write_ipc(self, path)

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
                    row[self._cols[ci].name.value()] = _col_cell_pyobj(
                        self._cols[ci], ri
                    )
                py_obj.append(row)

        elif eff_orient == "split":
            # {"columns": [...], "index": [...], "data": [[...], ...]}
            py_obj = Python.evaluate("{}")
            var cols_list = Python.evaluate("[]")
            for ci in range(ncols):
                cols_list.append(self._cols[ci].name.value())
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
                    row[self._cols[ci].name.value()] = _col_cell_pyobj(
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
                py_obj[self._cols[ci].name.value()] = col_dict

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
        """Return the DataFrame as a nested ``Dict``.

        Supports two orient values directly:

        - ``"dict"`` *(default)* — ``{column: {index_label: value}}``
        - ``"index"`` — ``{index_label: {column: value}}``

        All other orient values raise ``Error`` and direct callers to the
        compile-time generic overload ``to_dict[orient]()``.

        Parameters
        ----------
        orient : ``"dict"`` or ``"index"``.
        """
        var nrows = self.__len__()
        var ncols = self._cols.__len__()
        var has_index = self._has_index()
        if orient == "dict":
            var result = Dict[String, Dict[String, DFScalar]]()
            for ci in range(ncols):
                ref col = self._cols[ci]
                var inner = Dict[String, DFScalar]()
                for i in range(nrows):
                    var key = col._index_label(i) if has_index else String(i)
                    inner[key] = _scalar_from_col(col, i)
                result[col.name.value()] = inner^
            return result^
        elif orient == "index":
            var result = Dict[String, Dict[String, DFScalar]]()
            for ri in range(nrows):
                var row_key = self._cols[0]._index_label(
                    ri
                ) if has_index else String(ri)
                var inner = Dict[String, DFScalar]()
                for ci in range(ncols):
                    inner[self._cols[ci].name.value()] = _scalar_from_col(
                        self._cols[ci], ri
                    )
                result[row_key] = inner^
            return result^
        else:
            raise Error(
                "DataFrame.to_dict: orient='"
                + orient
                + "' is not supported via the runtime-string overload."
                + ' Use the compile-time generic: df.to_dict["'
                + orient
                + '"]()'
            )

    def to_dict[orient: StringLiteral = "dict"](self) raises -> ToDictResult:
        """Compile-time generic orient dispatch — returns a ``ToDictResult`` Variant.

        Supported orient values and their result arms:

        - ``"dict"``    → ``Dict[String, Dict[String, DFScalar]]``
        - ``"index"``   → ``Dict[String, Dict[String, DFScalar]]``
        - ``"list"``    → ``Dict[String, List[DFScalar]]``
        - ``"records"`` → ``List[Dict[String, DFScalar]]``
        - ``"split"``   → ``DictSplitResult`` (columns, index, data)
        - ``"tight"``   → ``DictSplitResult`` (adds index_names, column_names)
        - ``"series"``  → ``Dict[String, Series]``

        Call as ``df.to_dict["list"]()`` and unwrap with
        ``result[Dict[String, List[DFScalar]]]``.

        Unknown orient values produce a compile-time error via ``comptime assert``.
        """
        var nrows = self.__len__()
        var ncols = self._cols.__len__()
        var has_index = self._has_index()

        comptime if orient == "dict":
            var result = Dict[String, Dict[String, DFScalar]]()
            for ci in range(ncols):
                ref col = self._cols[ci]
                var inner = Dict[String, DFScalar]()
                for i in range(nrows):
                    var key = col._index_label(i) if has_index else String(i)
                    inner[key] = _scalar_from_col(col, i)
                result[col.name.value()] = inner^
            return ToDictResult(result^)
        elif orient == "index":
            var result = Dict[String, Dict[String, DFScalar]]()
            for ri in range(nrows):
                var row_key = self._cols[0]._index_label(
                    ri
                ) if has_index else String(ri)
                var inner = Dict[String, DFScalar]()
                for ci in range(ncols):
                    inner[self._cols[ci].name.value()] = _scalar_from_col(
                        self._cols[ci], ri
                    )
                result[row_key] = inner^
            return ToDictResult(result^)
        elif orient == "list":
            var result = Dict[String, List[DFScalar]]()
            for ci in range(ncols):
                ref col = self._cols[ci]
                var vals = List[DFScalar]()
                for i in range(nrows):
                    vals.append(_scalar_from_col(col, i))
                result[col.name.value()] = vals^
            return ToDictResult(result^)
        elif orient == "records":
            return ToDictResult(self.to_records(index=False))
        elif orient == "split":
            var columns = List[String]()
            for ci in range(ncols):
                columns.append(self._cols[ci].name.value())
            var index_labels = List[String]()
            for ri in range(nrows):
                index_labels.append(
                    self._cols[0]._index_label(ri) if has_index else String(ri)
                )
            var data = List[List[DFScalar]]()
            for ri in range(nrows):
                var row = List[DFScalar]()
                for ci in range(ncols):
                    row.append(_scalar_from_col(self._cols[ci], ri))
                data.append(row^)
            var index_names = List[String]()
            var column_names = List[String]()
            return ToDictResult(
                DictSplitResult(
                    columns^, index_labels^, data^, index_names^, column_names^
                )
            )
        elif orient == "tight":
            var columns = List[String]()
            for ci in range(ncols):
                columns.append(self._cols[ci].name.value())
            var index_labels = List[String]()
            for ri in range(nrows):
                index_labels.append(
                    self._cols[0]._index_label(ri) if has_index else String(ri)
                )
            var data = List[List[DFScalar]]()
            for ri in range(nrows):
                var row = List[DFScalar]()
                for ci in range(ncols):
                    row.append(_scalar_from_col(self._cols[ci], ri))
                data.append(row^)
            var index_names = List[String]()
            if ncols > 0:
                index_names.append(self._cols[0]._index_name)
            var column_names = List[String]()
            return ToDictResult(
                DictSplitResult(
                    columns^, index_labels^, data^, index_names^, column_names^
                )
            )
        elif orient == "series":
            var result = Dict[String, Series]()
            for ci in range(ncols):
                result[self._cols[ci].name.value()] = Series(
                    self._cols[ci].copy()
                )
            return ToDictResult(result^)
        else:
            comptime assert False, (
                "DataFrame.to_dict: unknown orient (must be dict, index,"
                " list, records, split, tight, or series)"
            )

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
                row[col.name.value()] = _scalar_from_col(col, ri)
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
                    + col.name.value()
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
            var w = len(self._cols[ci].name.value())
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
            var name = self._cols[ci].name.value()
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
                "      <th>"
                + _html_escape(self._cols[ci].name.value())
                + "</th>\n"
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
            var w = len(self._cols[ci].name.value())
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
            var name = self._cols[ci].name.value()
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
            col_idx.append(PythonObject(self._cols[j].name.value()))
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
            col_idx.append(PythonObject(self._cols[j].name.value()))
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


def _insertion_sort_keys_by[
    T: Comparable & Copyable & Movable & ImplicitlyCopyable
](
    mut group_keys: List[String],
    group_map: Dict[String, List[Int]],
    column_data: List[T],
) raises -> None:
    """Insertion-sort *group_keys* in ascending order of typed column values.

    Uses the first row index stored in *group_map[key]* to look up the
    representative value for each group in column data *column_data*.
    """
    var n = len(group_keys)
    for i in range(1, n):
        var key_i = group_keys[i]
        var val_i = column_data[group_map[key_i][0]]
        var j = i - 1
        while j >= 0:
            var gk_j = group_keys[j]
            if column_data[group_map[gk_j][0]] <= val_i:
                break
            group_keys[j + 1] = gk_j
            j -= 1
        group_keys[j + 1] = key_i


def _groupby_row_less(
    df: DataFrame,
    by: List[String],
    left_row: Int,
    right_row: Int,
    col_idx: Dict[String, Int],
) raises -> Bool:
    """Return True when *left_row* should sort before *right_row* for groupby keys.
    """
    for key_i in range(len(by)):
        var ci = col_idx[by[key_i]]
        ref col = df._cols[ci]
        var left_is_null = len(col._null_mask) > 0 and col._null_mask[left_row]
        var right_is_null = (
            len(col._null_mask) > 0 and col._null_mask[right_row]
        )
        if left_is_null or right_is_null:
            if left_is_null and right_is_null:
                continue
            return not left_is_null and right_is_null

        ref col_data = col._data
        if col_data.isa[List[Int64]]():
            var left_val = col_data[List[Int64]][left_row]
            var right_val = col_data[List[Int64]][right_row]
            if left_val < right_val:
                return True
            if left_val > right_val:
                return False
        elif col_data.isa[List[Float64]]():
            var left_val = col_data[List[Float64]][left_row]
            var right_val = col_data[List[Float64]][right_row]
            if left_val < right_val:
                return True
            if left_val > right_val:
                return False
        elif col_data.isa[List[Bool]]():
            var left_val = col_data[List[Bool]][left_row]
            var right_val = col_data[List[Bool]][right_row]
            if left_val != right_val:
                return not left_val and right_val
        elif col_data.isa[List[String]]():
            var left_val = col_data[List[String]][left_row]
            var right_val = col_data[List[String]][right_row]
            if left_val < right_val:
                return True
            if left_val > right_val:
                return False
        else:
            var left_val = String(col_data[List[PythonObject]][left_row])
            var right_val = String(col_data[List[PythonObject]][right_row])
            if left_val < right_val:
                return True
            if left_val > right_val:
                return False

    return False


def _insertion_sort_multikey_group_keys(
    df: DataFrame,
    by: List[String],
    mut group_keys: List[String],
    group_map: Dict[String, List[Int]],
    col_idx: Dict[String, Int],
) raises -> None:
    """Insertion-sort *group_keys* using typed comparisons across multiple key columns.
    """
    var n = len(group_keys)
    for i in range(1, n):
        var key_i = group_keys[i]
        var row_i = group_map[key_i][0]
        var j = i - 1
        while j >= 0:
            var key_j = group_keys[j]
            var row_j = group_map[key_j][0]
            if not _groupby_row_less(df, by, row_i, row_j, col_idx):
                break
            group_keys[j + 1] = key_j
            j -= 1
        group_keys[j + 1] = key_i


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
        col_idx[df._cols[i].name.value()] = i

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
        var n_groups = len(group_keys)
        if n_groups > 1:
            if len(by) == 1:
                # Single-column groupby: sort by the typed column value so that
                # numeric keys (e.g. 1, 2, 10) are ordered naturally rather than
                # lexicographically ("1", "10", "2").
                var ci = col_idx[by[0]]
                ref col_data = df._cols[ci]._data
                if col_data.isa[List[Int64]]():
                    _insertion_sort_keys_by[Int64](
                        group_keys, group_map, col_data[List[Int64]]
                    )
                elif col_data.isa[List[Float64]]():
                    _insertion_sort_keys_by[Float64](
                        group_keys, group_map, col_data[List[Float64]]
                    )
                else:
                    _sort_list(group_keys)
            else:
                _insertion_sort_multikey_group_keys(
                    df, by, group_keys, group_map, col_idx
                )


def _label_groupby_indices(
    by: List[String],
    by_null_mask: List[Bool],
    sort_keys: Bool,
    dropna: Bool,
    mut group_map: Dict[String, List[Int]],
    mut group_keys: List[String],
) raises:
    """Build key→row-index mapping for label-based Series.groupby.

    Mirrors DataFrame groupby index construction semantics for dropna/sort,
    including explicit handling for externally supplied null-label masks.
    """
    var has_null_mask = len(by_null_mask) > 0
    for i in range(len(by)):
        if has_null_mask and by_null_mask[i]:
            if dropna:
                continue
            var null_key = String("")
            if null_key not in group_map:
                group_keys.append(null_key)
                group_map[null_key] = List[Int]()
            group_map[null_key].append(i)
            continue

        var k = by[i]
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
        self._df = df.copy(True)
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

    def _col_name_index(self) -> Dict[String, Int]:
        """Return a name → column-position map for self._df (O(n_cols))."""
        var m = Dict[String, Int]()
        for i in range(len(self._df._cols)):
            m[self._df._cols[i].name.value()] = i
        return m^

    def _build_group_index(self) raises -> ColumnIndex:
        """Return a properly typed ColumnIndex for the group keys.

        Single-key: Int64 → List[Int64], Float64 → List[Float64], else Index.
        Multi-key:  List[PythonObject] of Python tuples (one per group), which
                    to_pandas() converts to pd.MultiIndex.from_tuples().
        """
        if len(self._by) == 1:
            var col_idx = self._col_name_index()
            var ci = col_idx.get(self._by[0], -1)
            if ci >= 0 and self._df._cols[ci]._data.isa[List[Int64]]():
                ref d = self._df._cols[ci]._data[List[Int64]]
                var int_keys = List[Int64]()
                for i in range(len(self._group_keys)):
                    int_keys.append(d[self._group_map[self._group_keys[i]][0]])
                return ColumnIndex(int_keys^)
            elif ci >= 0 and self._df._cols[ci]._data.isa[List[Float64]]():
                ref d = self._df._cols[ci]._data[List[Float64]]
                var flt_keys = List[Float64]()
                for i in range(len(self._group_keys)):
                    flt_keys.append(d[self._group_map[self._group_keys[i]][0]])
                return ColumnIndex(flt_keys^)
            return ColumnIndex(Index(self._group_keys.copy()))
        # Multi-key: build a List[PythonObject] of Python tuples, one per group.
        var builtins = Python.import_module("builtins")
        # Build name→index map once (O(n_cols)), then resolve key positions.
        var col_idx = self._col_name_index()
        var key_col_indices = List[Int]()
        for k in range(len(self._by)):
            key_col_indices.append(col_idx.get(self._by[k], -1))
        var multi_idx = List[PythonObject]()
        for j in range(len(self._group_keys)):
            var first_row = self._group_map[self._group_keys[j]][0]
            var items = builtins.list()
            for k in range(len(key_col_indices)):
                var ci = key_col_indices[k]
                ref col_data = self._df._cols[ci]._data
                if col_data.isa[List[Int64]]():
                    _ = items.append(
                        PythonObject(Int(col_data[List[Int64]][first_row]))
                    )
                elif col_data.isa[List[Float64]]():
                    _ = items.append(
                        PythonObject(col_data[List[Float64]][first_row])
                    )
                elif col_data.isa[List[Bool]]():
                    _ = items.append(
                        PythonObject(col_data[List[Bool]][first_row])
                    )
                elif col_data.isa[List[String]]():
                    _ = items.append(
                        PythonObject(col_data[List[String]][first_row])
                    )
                else:
                    _ = items.append(col_data[List[PythonObject]][first_row])
            multi_idx.append(builtins.tuple(items))
        return ColumnIndex(multi_idx^)

    def _make_result_col(
        self, name: Optional[String], var vals: List[Float64]
    ) raises -> Column:
        """Build a float64 result Column with group keys as index."""
        var idx = self._build_group_index()
        var col = Column(name, ColumnData(vals^), float64, idx^)
        if len(self._by) == 1:
            col._index_name = self._by[0]
        else:
            col._index_names = self._by.copy()
        return col^

    def _make_result_col_int64(
        self, name: Optional[String], var vals: List[Int64]
    ) raises -> Column:
        """Build an int64 result Column with group keys as index."""
        var idx = self._build_group_index()
        var col = Column(name, ColumnData(vals^), int64, idx^)
        if len(self._by) == 1:
            col._index_name = self._by[0]
        else:
            col._index_names = self._by.copy()
        return col^

    def _make_key_col(self, key_idx: Int) raises -> Column:
        """Build a data Column with the per-group representative key values.

        Used when as_index=False to include the groupby key as a regular
        column in the aggregation result instead of the row index.
        """
        var key_name = self._by[key_idx]
        var col_idx = self._col_name_index()
        var ci = col_idx.get(key_name, -1)
        if ci < 0:
            raise Error(
                "DataFrameGroupBy._make_key_col: column not found: " + key_name
            )
        var indices = List[Int]()
        for j in range(len(self._group_keys)):
            indices.append(self._group_map[self._group_keys[j]][0])
        var key_col = self._df._cols[ci].take(indices)
        key_col._index = ColumnIndex(List[PythonObject]())
        key_col._index_name = String("")
        key_col._index_names = List[String]()
        return key_col^

    def _wrap_agg_result(
        self, var result_cols: List[Column]
    ) raises -> DataFrame:
        """Return a DataFrame from aggregation result columns.

        When as_index=True (default), the group keys form the row index.
        When as_index=False, the group keys are prepended as regular data
        columns and all result columns carry the default RangeIndex.
        """
        if self._as_index:
            return DataFrame(result_cols^)
        var final_cols = List[Column]()
        for k in range(len(self._by)):
            final_cols.append(self._make_key_col(k))
        for i in range(len(result_cols)):
            var c = result_cols[i].copy()
            c._index = ColumnIndex(List[PythonObject]())
            c._index_name = String("")
            c._index_names = List[String]()
            final_cols.append(c^)
        return DataFrame(final_cols^)

    def _can_use_marrow_agg(self, agg: String) -> Bool:
        """Check if marrow hash-aggregate fast path can be used."""
        if len(self._by) != 1 or not self._as_index:
            return False
        if not (
            agg == "sum"
            or agg == "mean"
            or agg == "min"
            or agg == "max"
            or agg == "count"
        ):
            return False
        # Key column must be Arrow-convertible (not List[PythonObject]).
        for i in range(len(self._df._cols)):
            if self._df._cols[i].name.value() == self._by[0]:
                if self._df._cols[i]._data.isa[List[PythonObject]]():
                    return False
                break
        return True

    def _marrow_agg(self, agg: String) raises -> DataFrame:
        """Run aggregation via marrow's fused hash-aggregate kernel.

        Converts key + value columns to marrow arrays, calls the kernel,
        then post-processes: dropna filter, sort, dtype cast-back, and
        index construction.
        """
        var key_col_name = self._by[0]

        # Find key column.
        var key_col_idx = -1
        for i in range(len(self._df._cols)):
            if self._df._cols[i].name.value() == key_col_name:
                key_col_idx = i
                break
        if key_col_idx < 0:
            raise Error(
                "DataFrameGroupBy._marrow_agg: key column not found: "
                + key_col_name
            )

        # Convert key column to marrow array.
        var key_arr = column_to_marrow_array(self._df._cols[key_col_idx])

        # Collect value columns and their original dtypes.
        var value_names = List[String]()
        var value_dtypes = List[BisonDtype]()
        var marrow_values = List[AnyArray]()
        var marrow_aggs = List[String]()

        for i in range(len(self._df._cols)):
            if i == key_col_idx:
                continue
            ref col = self._df._cols[i]
            # For count, include all convertible columns.
            # For other aggs, only numeric columns.
            if agg == "count":
                if col._data.isa[List[PythonObject]]():
                    continue
            else:
                if not (col.dtype.is_integer() or col.dtype.is_float()):
                    continue
            marrow_values.append(column_to_marrow_array(col))
            marrow_aggs.append(agg)
            value_names.append(col.name.value())
            value_dtypes.append(col.dtype)

        if len(marrow_values) == 0:
            return DataFrame(List[Column]())

        # Call marrow groupby kernel.
        var rb = _marrow_groupby(key_arr, marrow_values, marrow_aggs)
        var num_groups = rb.num_rows()

        if num_groups == 0:
            return DataFrame(List[Column]())

        # Extract key column from result (column 0).
        var result_key = rb.column(0).copy()

        # Determine valid (non-null key) row mask for dropna.
        var keep = List[Int]()
        for i in range(num_groups):
            if self._dropna and not rb.column(0).is_valid(i):
                continue
            keep.append(i)

        # Sort by key if requested.
        if self._sort and len(keep) > 1:
            # Build sortable key values from the marrow result key column.
            var sort_vals = List[Float64]()
            var sort_strs = List[String]()
            var use_numeric = result_key.dtype().is_numeric()
            if use_numeric:
                for i in range(num_groups):
                    if result_key.dtype() == _m_float64:
                        sort_vals.append(
                            rebind[Float64](
                                result_key.as_primitive[
                                    _m_float64
                                ]().unsafe_get(i)
                            )
                        )
                    else:
                        sort_vals.append(
                            Float64(
                                Int(
                                    result_key.as_primitive[
                                        _m_int64
                                    ]().unsafe_get(i)
                                )
                            )
                        )
            else:
                for i in range(num_groups):
                    sort_strs.append(
                        String(result_key.as_string().unsafe_get(UInt(i)))
                    )

            # Insertion sort on keep indices — G is typically small.
            for i in range(1, len(keep)):
                var j = i
                while j > 0:
                    var a = keep[j - 1]
                    var b = keep[j]
                    var should_swap = (
                        sort_vals[a]
                        > sort_vals[b] if use_numeric else sort_strs[a]
                        > sort_strs[b]
                    )
                    if should_swap:
                        keep[j - 1] = b
                        keep[j] = a
                        j -= 1
                    else:
                        break

        var n_keep = len(keep)

        # Build ColumnIndex from the key column.
        var idx: ColumnIndex
        if result_key.dtype() == _m_int64:
            var int_keys = List[Int64]()
            for i in range(n_keep):
                int_keys.append(
                    rebind[Int64](
                        result_key.as_primitive[_m_int64]().unsafe_get(keep[i])
                    )
                )
            idx = ColumnIndex(int_keys^)
        elif result_key.dtype() == _m_float64:
            var flt_keys = List[Float64]()
            for i in range(n_keep):
                flt_keys.append(
                    rebind[Float64](
                        result_key.as_primitive[_m_float64]().unsafe_get(
                            keep[i]
                        )
                    )
                )
            idx = ColumnIndex(flt_keys^)
        else:
            var str_keys = List[String]()
            for i in range(n_keep):
                str_keys.append(
                    String(result_key.as_string().unsafe_get(UInt(keep[i])))
                )
            idx = ColumnIndex(Index(str_keys^))

        # Build result columns.
        var result_cols = List[Column]()
        for v in range(len(value_names)):
            var rb_col = rb.column(v + 1).copy()  # +1 to skip key column.
            var is_count = agg == "count"
            var orig_is_int = value_dtypes[v].is_integer()

            # Determine whether to produce int64 or float64 result.
            if is_count or (orig_is_int and agg != "mean"):
                # int64 result: count always int64; sum/min/max of int -> int64.
                var vals = List[Int64]()
                var null_mask = List[Bool]()
                var has_null = False
                for i in range(n_keep):
                    var ri = keep[i]
                    if not rb_col.is_valid(ri):
                        vals.append(Int64(0))
                        null_mask.append(True)
                        has_null = True
                    elif is_count:
                        vals.append(
                            rebind[Int64](
                                rb_col.as_primitive[_m_int64]().unsafe_get(ri)
                            )
                        )
                        null_mask.append(False)
                    else:
                        # sum/min/max of integer col: marrow stores as float64.
                        vals.append(
                            Int64(
                                Float64(
                                    rb_col.as_primitive[
                                        _m_float64
                                    ]().unsafe_get(ri)
                                )
                            )
                        )
                        null_mask.append(False)
                var col = Column(
                    value_names[v], ColumnData(vals^), int64, idx.copy()
                )
                col._index_name = key_col_name
                if has_null:
                    col._null_mask = null_mask^
                result_cols.append(col^)
            else:
                # float64 result: mean, or float col sum/min/max.
                var vals = List[Float64]()
                var null_mask = List[Bool]()
                var has_null = False
                for i in range(n_keep):
                    var ri = keep[i]
                    if not rb_col.is_valid(ri):
                        vals.append(Float64(0))
                        null_mask.append(True)
                        has_null = True
                    else:
                        vals.append(
                            rebind[Float64](
                                rb_col.as_primitive[_m_float64]().unsafe_get(ri)
                            )
                        )
                        null_mask.append(False)
                var col = Column(
                    value_names[v], ColumnData(vals^), float64, idx.copy()
                )
                col._index_name = key_col_name
                if has_null:
                    col._null_mask = null_mask^
                result_cols.append(col^)

        return DataFrame(result_cols^)

    def sum(self) raises -> DataFrame:
        if self._can_use_marrow_agg("sum"):
            return self._marrow_agg("sum")
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            if col.dtype.is_integer():
                var vals = List[Int64]()
                for j in range(len(self._group_keys)):
                    vals.append(
                        col.take(
                            self._group_map[self._group_keys[j]]
                        ).sum_int64()
                    )
                result_cols.append(self._make_result_col_int64(col.name, vals^))
            else:
                var vals = List[Float64]()
                for j in range(len(self._group_keys)):
                    vals.append(
                        col.take(self._group_map[self._group_keys[j]]).sum()
                    )
                result_cols.append(self._make_result_col(col.name, vals^))
        return self._wrap_agg_result(result_cols^)

    def mean(self) raises -> DataFrame:
        if self._can_use_marrow_agg("mean"):
            return self._marrow_agg("mean")
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).mean()
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return self._wrap_agg_result(result_cols^)

    def min(self) raises -> DataFrame:
        if self._can_use_marrow_agg("min"):
            return self._marrow_agg("min")
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            if col.dtype.is_integer():
                var vals = List[Int64]()
                for j in range(len(self._group_keys)):
                    vals.append(
                        col.take(
                            self._group_map[self._group_keys[j]]
                        ).min_int64()
                    )
                result_cols.append(self._make_result_col_int64(col.name, vals^))
            else:
                var vals = List[Float64]()
                for j in range(len(self._group_keys)):
                    vals.append(
                        col.take(self._group_map[self._group_keys[j]]).min()
                    )
                result_cols.append(self._make_result_col(col.name, vals^))
        return self._wrap_agg_result(result_cols^)

    def max(self) raises -> DataFrame:
        if self._can_use_marrow_agg("max"):
            return self._marrow_agg("max")
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            if col.dtype.is_integer():
                var vals = List[Int64]()
                for j in range(len(self._group_keys)):
                    vals.append(
                        col.take(
                            self._group_map[self._group_keys[j]]
                        ).max_int64()
                    )
                result_cols.append(self._make_result_col_int64(col.name, vals^))
            else:
                var vals = List[Float64]()
                for j in range(len(self._group_keys)):
                    vals.append(
                        col.take(self._group_map[self._group_keys[j]]).max()
                    )
                result_cols.append(self._make_result_col(col.name, vals^))
        return self._wrap_agg_result(result_cols^)

    def std(self, ddof: Int = 1) raises -> DataFrame:
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).std(ddof)
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return self._wrap_agg_result(result_cols^)

    def var(self, ddof: Int = 1) raises -> DataFrame:
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            if not (col.dtype.is_integer() or col.dtype.is_float()):
                continue
            var vals = List[Float64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    col.take(self._group_map[self._group_keys[j]]).var(ddof)
                )
            result_cols.append(self._make_result_col(col.name, vals^))
        return self._wrap_agg_result(result_cols^)

    def count(self) raises -> DataFrame:
        if self._can_use_marrow_agg("count"):
            return self._marrow_agg("count")
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            var vals = List[Int64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    Int64(
                        col.take(self._group_map[self._group_keys[j]]).count()
                    )
                )
            result_cols.append(self._make_result_col_int64(col.name, vals^))
        return self._wrap_agg_result(result_cols^)

    def nunique(self) raises -> DataFrame:
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            var vals = List[Int64]()
            for j in range(len(self._group_keys)):
                vals.append(
                    Int64(
                        col.take(self._group_map[self._group_keys[j]]).nunique()
                    )
                )
            result_cols.append(self._make_result_col_int64(col.name, vals^))
        return self._wrap_agg_result(result_cols^)

    def first(self) raises -> DataFrame:
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
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
            result_col._index = self._build_group_index()
            if len(self._by) == 1:
                result_col._index_name = self._by[0]
            else:
                result_col._index_names = self._by.copy()
            result_cols.append(result_col^)
        return self._wrap_agg_result(result_cols^)

    def last(self) raises -> DataFrame:
        var skip = Dict[String, Bool]()
        for i in range(len(self._by)):
            skip[self._by[i]] = True
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
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
            result_col._index = self._build_group_index()
            if len(self._by) == 1:
                result_col._index_name = self._by[0]
            else:
                result_col._index_names = self._by.copy()
            result_cols.append(result_col^)
        return self._wrap_agg_result(result_cols^)

    def size(self) raises -> Series:
        if not self._as_index:
            return Series.from_pandas(self._pd_groupby().size())
        var vals = List[Int64]()
        for i in range(len(self._group_keys)):
            vals.append(Int64(len(self._group_map[self._group_keys[i]])))
        var idx = self._build_group_index()
        # pandas groupby().size() returns a Series with name=None
        var col = Column(None, ColumnData(vals^), int64, idx^)
        if len(self._by) == 1:
            col._index_name = self._by[0]
        else:
            col._index_names = self._by.copy()
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
        if (
            func != "sum"
            and func != "mean"
            and func != "min"
            and func != "max"
            and func != "std"
            and func != "var"
            and func != "count"
            and func != "first"
            and func != "last"
        ):
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
        # Dtype-preserving broadcast for first/last.
        if func == "first" or func == "last":
            var want_first = func == "first"
            var result_cols = List[Column]()
            for i in range(len(self._df._cols)):
                ref col = self._df._cols[i]
                if col.name.value() in skip:
                    continue
                var has_mask = len(col._null_mask) > 0
                var key_to_idx = Dict[String, Int]()
                for j in range(len(self._group_keys)):
                    var key = self._group_keys[j]
                    ref indices = self._group_map[key]
                    var found = -1
                    var start = 0 if want_first else len(indices) - 1
                    var stop = len(indices) if want_first else -1
                    var step = 1 if want_first else -1
                    var ii = start
                    while ii != stop:
                        if not has_mask or not col._null_mask[indices[ii]]:
                            found = indices[ii]
                            break
                        ii += step
                    key_to_idx[key] = found
                var selected = List[Int]()
                for r in range(n_rows):
                    if row_key[r] != "":
                        selected.append(key_to_idx[row_key[r]])
                    else:
                        # Row was excluded by dropna — emit null.
                        selected.append(-1)
                var result_col = col.take_with_nulls(selected)
                result_col.name = col.name
                result_cols.append(result_col^)
            return DataFrame(result_cols^)
        # Detect whether any row has no group key (dropna null-key row).
        var any_null_row = False
        for r in range(n_rows):
            if row_key[r] == "":
                any_null_row = True
                break
        # Scalar-broadcast functions (float64 or int64-preserving).
        var needs_numeric = (
            func == "sum"
            or func == "mean"
            or func == "min"
            or func == "max"
            or func == "std"
            or func == "var"
        )
        var int_preserving = func == "sum" or func == "min" or func == "max"
        var result_cols = List[Column]()
        for i in range(len(self._df._cols)):
            ref col = self._df._cols[i]
            if col.name.value() in skip:
                continue
            if needs_numeric and not (
                col.dtype.is_integer() or col.dtype.is_float()
            ):
                continue
            # Integer-preserving path: only when no null rows can arise.
            if int_preserving and col.dtype.is_integer() and not any_null_row:
                var key_to_int = Dict[String, Int64]()
                for j in range(len(self._group_keys)):
                    var key = self._group_keys[j]
                    var sub = col.take(self._group_map[key])
                    if func == "sum":
                        key_to_int[key] = sub.sum_int64()
                    elif func == "min":
                        key_to_int[key] = sub.min_int64()
                    else:  # max
                        key_to_int[key] = sub.max_int64()
                var int_vals = List[Int64]()
                for r in range(n_rows):
                    int_vals.append(key_to_int[row_key[r]])
                result_cols.append(
                    Column(col.name, ColumnData(int_vals^), int64)
                )
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
                elif func == "max":
                    key_to_val[key] = sub.max()
                elif func == "std":
                    key_to_val[key] = sub.std()
                elif func == "var":
                    key_to_val[key] = sub.var()
                elif func == "count":
                    key_to_val[key] = Float64(sub.count())
                # No else needed: first/last are handled in the separate branch above.
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
    var _by_null_mask: List[Bool]
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
        by_null_mask: List[Bool] = List[Bool](),
    ) raises:
        self._series = series.copy()
        self._by = by.copy()
        self._by_null_mask = by_null_mask.copy()
        self._as_index = as_index
        self._sort = sort
        self._dropna = dropna
        self._group_map = Dict[String, List[Int]]()
        self._group_keys = List[String]()
        _label_groupby_indices(
            by,
            by_null_mask,
            sort,
            dropna,
            self._group_map,
            self._group_keys,
        )

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

    def _can_use_marrow_agg(self, agg: String) -> Bool:
        """Check if marrow hash-aggregate fast path can be used."""
        if not self._as_index:
            return False
        if not (
            agg == "sum"
            or agg == "mean"
            or agg == "min"
            or agg == "max"
            or agg == "count"
        ):
            return False
        # For non-count aggs, value column must be numeric.
        if agg != "count" and not (
            self._series._col.dtype.is_integer()
            or self._series._col.dtype.is_float()
        ):
            return False
        return True

    def _marrow_agg(self, agg: String) raises -> Series:
        """Run aggregation via marrow's fused hash-aggregate kernel.

        Converts labels to a StringArray key and the series column to a
        marrow array, calls the kernel, then post-processes.
        """
        # Build key array from group labels.
        var n = len(self._by)
        var sb = _MarrowStringBuilder(capacity=n)
        var has_null_mask = len(self._by_null_mask) > 0
        for i in range(n):
            if has_null_mask and self._by_null_mask[i]:
                sb.append_null()
            else:
                sb.append(self._by[i])
        var key_arr = AnyArray(sb.finish())

        # Convert value column.
        var val_arr = column_to_marrow_array(self._series._col)

        # Call marrow groupby kernel.
        var values = List[AnyArray]()
        values.append(val_arr^)
        var aggs = List[String]()
        aggs.append(agg)
        var rb = _marrow_groupby(key_arr, values, aggs)
        var num_groups = rb.num_rows()

        if num_groups == 0:
            return Series(
                Column(
                    self._series.name,
                    ColumnData(List[Float64]()),
                    float64,
                    ColumnIndex(Index(List[String]())),
                )
            )

        # Extract key column from result (column 0) — always string.
        var result_key = rb.column(0).copy()

        # Determine valid (non-null key) row mask for dropna.
        var keep = List[Int]()
        for i in range(num_groups):
            if self._dropna and not rb.column(0).is_valid(i):
                continue
            keep.append(i)

        # Sort by key if requested.
        if self._sort and len(keep) > 1:
            var sort_strs = List[String]()
            for i in range(num_groups):
                sort_strs.append(
                    String(result_key.as_string().unsafe_get(UInt(i)))
                )
            for i in range(1, len(keep)):
                var j = i
                while j > 0:
                    var a = keep[j - 1]
                    var b = keep[j]
                    if sort_strs[a] > sort_strs[b]:
                        keep[j - 1] = b
                        keep[j] = a
                        j -= 1
                    else:
                        break

        var n_keep = len(keep)

        # Build index from sorted/filtered key strings.
        var idx_keys = List[String]()
        for i in range(n_keep):
            idx_keys.append(
                String(result_key.as_string().unsafe_get(UInt(keep[i])))
            )
        var idx = ColumnIndex(Index(idx_keys^))

        # Build result values.
        var rb_col = rb.column(1).copy()
        var is_count = agg == "count"
        var orig_is_int = self._series._col.dtype.is_integer()

        if is_count or (orig_is_int and agg != "mean"):
            var vals = List[Int64]()
            var null_mask = List[Bool]()
            var has_null = False
            for i in range(n_keep):
                var ri = keep[i]
                if not rb_col.is_valid(ri):
                    vals.append(Int64(0))
                    null_mask.append(True)
                    has_null = True
                elif is_count:
                    vals.append(
                        rebind[Int64](
                            rb_col.as_primitive[_m_int64]().unsafe_get(ri)
                        )
                    )
                    null_mask.append(False)
                else:
                    vals.append(
                        Int64(
                            Float64(
                                rb_col.as_primitive[_m_float64]().unsafe_get(ri)
                            )
                        )
                    )
                    null_mask.append(False)
            var col = Column(self._series.name, ColumnData(vals^), int64, idx^)
            if has_null:
                col._null_mask = null_mask^
            return Series(col^)
        else:
            var vals = List[Float64]()
            var null_mask = List[Bool]()
            var has_null = False
            for i in range(n_keep):
                var ri = keep[i]
                if not rb_col.is_valid(ri):
                    vals.append(Float64(0))
                    null_mask.append(True)
                    has_null = True
                else:
                    vals.append(
                        rebind[Float64](
                            rb_col.as_primitive[_m_float64]().unsafe_get(ri)
                        )
                    )
                    null_mask.append(False)
            var col = Column(
                self._series.name, ColumnData(vals^), float64, idx^
            )
            if has_null:
                col._null_mask = null_mask^
            return Series(col^)

    def sum(self) raises -> Series:
        if self._can_use_marrow_agg("sum"):
            return self._marrow_agg("sum")
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        if self._series._col.dtype.is_integer():
            var result_vals = List[Int64]()
            for i in range(len(self._group_keys)):
                var key = self._group_keys[i]
                result_vals.append(
                    self._series._col.take(self._group_map[key]).sum_int64()
                )
            return Series(
                Column(self._series.name, ColumnData(result_vals^), int64, idx^)
            )
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).sum()
            )
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def mean(self) raises -> Series:
        if self._can_use_marrow_agg("mean"):
            return self._marrow_agg("mean")
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
        if self._can_use_marrow_agg("min"):
            return self._marrow_agg("min")
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        if self._series._col.dtype.is_integer():
            var result_vals = List[Int64]()
            for i in range(len(self._group_keys)):
                var key = self._group_keys[i]
                result_vals.append(
                    self._series._col.take(self._group_map[key]).min_int64()
                )
            return Series(
                Column(self._series.name, ColumnData(result_vals^), int64, idx^)
            )
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).min()
            )
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def max(self) raises -> Series:
        if self._can_use_marrow_agg("max"):
            return self._marrow_agg("max")
        var idx = ColumnIndex(Index(self._group_keys.copy()))
        if self._series._col.dtype.is_integer():
            var result_vals = List[Int64]()
            for i in range(len(self._group_keys)):
                var key = self._group_keys[i]
                result_vals.append(
                    self._series._col.take(self._group_map[key]).max_int64()
                )
            return Series(
                Column(self._series.name, ColumnData(result_vals^), int64, idx^)
            )
        var result_vals = List[Float64]()
        for i in range(len(self._group_keys)):
            var key = self._group_keys[i]
            result_vals.append(
                self._series._col.take(self._group_map[key]).max()
            )
        return Series(
            Column(self._series.name, ColumnData(result_vals^), float64, idx^)
        )

    def count(self) raises -> Series:
        if self._can_use_marrow_agg("count"):
            return self._marrow_agg("count")
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
        var n = len(self._series._col)
        var has_null_mask = len(self._by_null_mask) > 0
        var nan = Float64(0) / Float64(0)
        # Detect whether any row is null-labelled and excluded by dropna.
        var any_excluded_row = False
        if has_null_mask and self._dropna:
            for i in range(n):
                if self._by_null_mask[i]:
                    any_excluded_row = True
                    break
        # Build row → group_key mapping by inverting _group_map.
        var row_key = List[String]()
        for _ in range(n):
            row_key.append(String(""))
        for j in range(len(self._group_keys)):
            var key = self._group_keys[j]
            ref indices = self._group_map[key]
            for k in range(len(indices)):
                row_key[indices[k]] = key
        # Integer-preserving scalar-broadcast path for sum / min / max.
        var int_preserving = func == "sum" or func == "min" or func == "max"
        if (
            int_preserving
            and self._series._col.dtype.is_integer()
            and not any_excluded_row
        ):
            var key_to_int = Dict[String, Int64]()
            for i in range(len(self._group_keys)):
                var key = self._group_keys[i]
                var sub = self._series._col.take(self._group_map[key])
                if func == "sum":
                    key_to_int[key] = sub.sum_int64()
                elif func == "min":
                    key_to_int[key] = sub.min_int64()
                else:  # max
                    key_to_int[key] = sub.max_int64()
            var int_vals = List[Int64]()
            for i in range(n):
                int_vals.append(key_to_int[row_key[i]])
            var result_col = Column(
                self._series.name, ColumnData(int_vals^), int64
            )
            result_col._index = self._series._col._index.copy()
            result_col._index_name = self._series._col._index_name
            return Series(result_col^)
        # Float64-returning scalar-broadcast functions
        if (
            func == "sum"
            or func == "mean"
            or func == "min"
            or func == "max"
            or func == "std"
            or func == "var"
        ):
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
                elif func == "std":
                    key_to_agg[key] = sub.std()
                elif func == "var":
                    key_to_agg[key] = sub.var()
            var result_vals = List[Float64]()
            var null_mask = List[Bool]()
            var any_null = False
            for i in range(n):
                if has_null_mask and self._by_null_mask[i] and self._dropna:
                    result_vals.append(nan)
                    null_mask.append(True)
                    any_null = True
                else:
                    result_vals.append(key_to_agg[row_key[i]])
                    null_mask.append(False)
            var result_col = Column(
                self._series.name, ColumnData(result_vals^), float64
            )
            if any_null:
                result_col._null_mask = null_mask^
            result_col._index = self._series._col._index.copy()
            result_col._index_name = self._series._col._index_name
            return Series(result_col^)
        # Int64-returning scalar-broadcast functions
        if func == "count" or func == "size":
            var key_to_agg = Dict[String, Int64]()
            for i in range(len(self._group_keys)):
                var key = self._group_keys[i]
                if func == "count":
                    var sub = self._series._col.take(self._group_map[key])
                    key_to_agg[key] = Int64(sub.count())
                else:
                    key_to_agg[key] = Int64(len(self._group_map[key]))
            if any_excluded_row:
                var result_vals = List[Float64]()
                var null_mask = List[Bool]()
                for i in range(n):
                    if self._by_null_mask[i] and self._dropna:
                        result_vals.append(nan)
                        null_mask.append(True)
                    else:
                        result_vals.append(Float64(key_to_agg[row_key[i]]))
                        null_mask.append(False)
                var result_col = Column(
                    self._series.name, ColumnData(result_vals^), float64
                )
                result_col._null_mask = null_mask^
                result_col._index = self._series._col._index.copy()
                result_col._index_name = self._series._col._index_name
                return Series(result_col^)
            var result_vals = List[Int64]()
            for i in range(n):
                result_vals.append(key_to_agg[row_key[i]])
            var result_col = Column(
                self._series.name, ColumnData(result_vals^), int64
            )
            result_col._index = self._series._col._index.copy()
            result_col._index_name = self._series._col._index_name
            return Series(result_col^)
        # Dtype-preserving broadcast functions (first/last)
        if func == "first" or func == "last":
            ref col = self._series._col
            var has_mask = len(col._null_mask) > 0
            var want_first = func == "first"
            var key_to_idx = Dict[String, Int]()
            for i in range(len(self._group_keys)):
                var key = self._group_keys[i]
                ref indices = self._group_map[key]
                var found = -1
                var start = 0 if want_first else len(indices) - 1
                var stop = len(indices) if want_first else -1
                var step = 1 if want_first else -1
                var j = start
                while j != stop:
                    if not has_mask or not col._null_mask[indices[j]]:
                        found = indices[j]
                        break
                    j += step
                key_to_idx[key] = found
            var selected = List[Int]()
            for i in range(n):
                if has_null_mask and self._by_null_mask[i] and self._dropna:
                    selected.append(-1)
                else:
                    selected.append(key_to_idx[row_key[i]])
            var result_col = col.take_with_nulls(selected)
            result_col.name = self._series.name
            result_col._index = col._index.copy()
            result_col._index_name = col._index_name
            return Series(result_col^)
        return Series.from_pandas(self._pd_groupby().transform(func))

    def apply(self, func: String) raises -> Series:
        return Series.from_pandas(
            self._pd_groupby().apply(Python.evaluate(func))
        )


# ------------------------------------------------------------------
# Row-indexing helpers (used by LocIndexer / ILocIndexer)
# ------------------------------------------------------------------


def _df_col_index(df: DataFrame, name: String) raises -> Int:
    """Return the integer position of column *name* in *df*."""
    for i in range(len(df._cols)):
        if df._cols[i].name == name:
            return i
    raise Error("column '" + name + "' not found")


def _parse_int_label(label: String) raises -> Int:
    """Parse a decimal integer string into an ``Int``.

    Supports an optional leading ``'-'`` sign.  Raises when the string
    contains non-digit characters.
    """
    var n = len(label)
    if n == 0:
        raise Error("loc: empty row label")
    var bytes = label.as_bytes()
    var start = 0
    var negative = False
    if bytes[0] == UInt8(ord("-")):
        negative = True
        start = 1
    elif bytes[0] == UInt8(ord("+")):
        start = 1
    if start >= n:
        raise Error("loc: invalid row label: " + label)
    var result = 0
    for i in range(start, n):
        var digit = Int(bytes[i]) - ord("0")
        if digit < 0 or digit > 9:
            raise Error("loc: not an integer label: " + label)
        result = result * 10 + digit
    return -result if negative else result


def _df_row_index(df: DataFrame, label: String) raises -> Int:
    """Return the integer row position for the given row *label*.

    If the first column has a non-empty ``_index`` the label is matched
    via ``Column._index_label()``.  When the index is empty the default
    integer range index (0, 1, …) is assumed and the label must be a
    decimal integer string.
    """
    var nrows = df.shape()[0]
    if nrows == 0:
        raise Error("loc: DataFrame is empty")
    if len(df._cols) == 0:
        raise Error("loc: DataFrame has no columns")
    var n_idx = df._cols[0]._index_len()
    if n_idx > 0:
        for i in range(n_idx):
            if df._cols[0]._index_label(i) == label:
                return i
        raise Error("loc: label '" + label + "' not found in index")
    # Default RangeIndex: parse the label as an integer.
    var row = _parse_int_label(label)
    if row < 0:
        row = nrows + row
    if row < 0 or row >= nrows:
        raise Error("loc: label '" + label + "' out of range")
    return row


def _set_scalar_in_col(mut col: Column, row: Int, value: DFScalar) raises:
    """Write *value* into *col* at integer position *row*."""
    var visitor = _SetScalarInColMutVisitor(row, value)
    visit_col_data_mut_raises(visitor, col._data)


def _set_series_scalar_in_col(
    mut col: Column, row: Int, value: SeriesScalar
) raises:
    """Write a ``SeriesScalar`` cell into *col* at position *row*."""
    if value.isa[PythonObject]():
        if col._data.isa[List[PythonObject]]():
            col._data[List[PythonObject]][row] = value[PythonObject]
        else:
            raise Error("iloc: cannot assign PythonObject to typed column")
        return
    var ds: DFScalar
    if value.isa[Int64]():
        ds = DFScalar(value[Int64])
    elif value.isa[Float64]():
        ds = DFScalar(value[Float64])
    elif value.isa[Bool]():
        ds = DFScalar(value[Bool])
    else:
        ds = DFScalar(value[String])
    _set_scalar_in_col(col, row, ds)


def _row_as_series(df: DataFrame, row: Int) raises -> Series:
    """Build a ``Series`` representing row *row* of *df*.

    The returned Series has ``object_`` dtype; each element is a
    ``PythonObject`` wrapping the cell value.  The ``_index`` of the
    returned column holds the column names as ``PythonObject`` strings,
    matching the pandas ``df.iloc[i]`` behaviour.
    """
    var ncols = df.shape()[1]
    var data = List[PythonObject]()
    var index = List[PythonObject]()
    for ci in range(ncols):
        index.append(PythonObject(df._cols[ci].name.value()))
        data.append(_col_cell_pyobj(df._cols[ci], row))
    var result_col = Column(None, ColumnData(data^), object_, index^)
    return Series(result_col^)


def _df_slice_rows(df: DataFrame, start: Int, end: Int) -> DataFrame:
    """Return a new DataFrame with rows [start, end)."""
    var result_cols = List[Column]()
    for i in range(len(df._cols)):
        result_cols.append(df._cols[i].slice(start, end))
    return DataFrame(result_cols^)


# ------------------------------------------------------------------
# Public indexer structs
# ------------------------------------------------------------------


struct LocIndexer[O: MutOrigin]:
    """Label-based row indexer (.loc).

    Obtain via ``df.loc()`` (preferred) or
    ``LocIndexer(UnsafePointer(to=df))`` where *df* is a mutable
    ``DataFrame``.  The pointer must remain valid for the lifetime of
    the indexer.

    **Slice behaviour differs from pandas**: ``df.loc()[i:j]`` treats *j* as
    **exclusive** (matching Python / ``iloc`` conventions).  Pandas makes the
    end bound inclusive for label-based slicing.
    """

    var _df: UnsafePointer[DataFrame, Self.O]

    def __init__(out self, ptr: UnsafePointer[DataFrame, Self.O]):
        self._df = ptr

    def __getitem__(self, key: String) raises -> Series:
        """Return row *key* as a Series (index = column names)."""
        ref df = self._df[]
        var row = _df_row_index(df, key)
        var nrows = df.shape()[0]
        if row < 0 or row >= nrows:
            raise Error("loc: row index out of bounds")
        return _row_as_series(df, row)

    def __getitem__(self, key: Slice) raises -> DataFrame:
        """Return rows *key.start* to *key.end* (exclusive) as a DataFrame.

        Both ``start`` and ``end`` are integer row positions that are resolved
        against the DataFrame index using ``_df_row_index``.  For a default
        RangeIndex the integer values map directly to label strings ("0", "1",
        …).  ``None`` bounds (open-ended slices) map to the first/last row.
        This mirrors ``df.loc[i:j]`` in pandas for integer-labelled indexes.
        Unlike pandas, the end bound is **exclusive** to match Python slicing
        conventions.
        """
        ref df = self._df[]
        var nrows = df.shape()[0]
        var start: Int
        var end: Int
        if key.start:
            start = _df_row_index(df, String(key.start.value()))
        else:
            start = 0
        if key.end:
            end = _df_row_index(df, String(key.end.value()))
        else:
            end = nrows
        if start < 0:
            start = 0
        if end > nrows:
            end = nrows
        return _df_slice_rows(df, start, end)

    def __setitem__(self, key: String, value: Series) raises:
        """Assign Series *value* to row *key*.

        *value* must have exactly as many elements as there are columns.
        Each element is written into the corresponding column at the
        row position identified by *key*.
        """
        ref df = self._df[]
        var row = _df_row_index(df, key)
        var ncols = df.shape()[1]
        var nrows = df.shape()[0]
        if row < 0 or row >= nrows:
            raise Error("loc: row index out of bounds")
        if value.size() != ncols:
            raise Error(
                "loc: Series length "
                + String(value.size())
                + " != number of columns "
                + String(ncols)
            )
        for ci in range(ncols):
            var cell = value.iloc(ci)
            _set_series_scalar_in_col(df._cols[ci], row, cell)


struct ILocIndexer[O: MutOrigin]:
    """Integer-position-based row indexer (.iloc).

    Obtain via ``df.iloc()`` (preferred) or
    ``ILocIndexer(UnsafePointer(to=df))``.
    """

    var _df: UnsafePointer[DataFrame, Self.O]

    def __init__(out self, ptr: UnsafePointer[DataFrame, Self.O]):
        self._df = ptr

    def __getitem__(self, key: Int) raises -> Series:
        """Return row *key* (integer position) as a Series."""
        ref df = self._df[]
        var nrows = df.shape()[0]
        var row = key
        if row < 0:
            row = nrows + row
        if row < 0 or row >= nrows:
            raise Error(
                "iloc: row index "
                + String(key)
                + " out of bounds for DataFrame with "
                + String(nrows)
                + " rows"
            )
        return _row_as_series(df, row)

    def __getitem__(self, key: Slice) raises -> DataFrame:
        """Return rows *key.start* to *key.end* (exclusive) as a DataFrame.

        Negative indices and ``None`` bounds are handled the same way as
        Python slice semantics, matching ``df.iloc[start:end]`` in pandas.
        """
        ref df = self._df[]
        var nrows = df.shape()[0]
        var start: Int
        var end: Int
        if key.start:
            start = key.start.value()
        else:
            start = 0
        if key.end:
            end = key.end.value()
        else:
            end = nrows
        # Normalise negative indices.
        if start < 0:
            start = nrows + start
        if end < 0:
            end = nrows + end
        # Clamp to valid range.
        if start < 0:
            start = 0
        if end > nrows:
            end = nrows
        return _df_slice_rows(df, start, end)

    def __setitem__(self, key: Int, value: Series) raises:
        """Assign Series *value* to row *key* (integer position).

        *value* must have exactly as many elements as there are columns.
        """
        ref df = self._df[]
        var nrows = df.shape()[0]
        var row = key
        if row < 0:
            row = nrows + row
        if row < 0 or row >= nrows:
            raise Error(
                "iloc: row index "
                + String(key)
                + " out of bounds for DataFrame with "
                + String(nrows)
                + " rows"
            )
        var ncols = df.shape()[1]
        if value.size() != ncols:
            raise Error(
                "iloc: Series length "
                + String(value.size())
                + " != number of columns "
                + String(ncols)
            )
        for ci in range(ncols):
            var cell = value.iloc(ci)
            _set_series_scalar_in_col(df._cols[ci], row, cell)


struct AtIndexer[O: MutOrigin]:
    """Label-based scalar accessor (.at).

    Obtain via ``AtIndexer(UnsafePointer(to=df))``.
    """

    var _df: UnsafePointer[DataFrame, Self.O]

    def __init__(out self, ptr: UnsafePointer[DataFrame, Self.O]):
        self._df = ptr

    def __getitem__(self, row: String, col: String) raises -> DFScalar:
        """Return the scalar at row label *row*, column name *col*."""
        ref df = self._df[]
        var row_idx = _df_row_index(df, row)
        var col_idx = _df_col_index(df, col)
        return _scalar_from_col(df._cols[col_idx], row_idx)

    def __setitem__(self, row: String, col: String, value: DFScalar) raises:
        """Set the scalar at row label *row*, column name *col* to *value*."""
        ref df = self._df[]
        var row_idx = _df_row_index(df, row)
        var col_idx = _df_col_index(df, col)
        _set_scalar_in_col(df._cols[col_idx], row_idx, value)


struct IAtIndexer[O: MutOrigin]:
    """Integer-based scalar accessor (.iat).

    Obtain via ``IAtIndexer(UnsafePointer(to=df))``.
    """

    var _df: UnsafePointer[DataFrame, Self.O]

    def __init__(out self, ptr: UnsafePointer[DataFrame, Self.O]):
        self._df = ptr

    def __getitem__(self, row: Int, col: Int) raises -> DFScalar:
        """Return the scalar at integer row *row*, column position *col*."""
        ref df = self._df[]
        var nrows = df.shape()[0]
        var ncols = df.shape()[1]
        var r = row if row >= 0 else nrows + row
        if r < 0 or r >= nrows:
            raise Error(
                "iat: row index "
                + String(row)
                + " out of bounds for DataFrame with "
                + String(nrows)
                + " rows"
            )
        if col < 0 or col >= ncols:
            raise Error(
                "iat: column index "
                + String(col)
                + " out of bounds for DataFrame with "
                + String(ncols)
                + " columns"
            )
        return _scalar_from_col(df._cols[col], r)

    def __setitem__(self, row: Int, col: Int, value: DFScalar) raises:
        """Set the scalar at integer row *row*, column position *col* to *value*.
        """
        ref df = self._df[]
        var nrows = df.shape()[0]
        var ncols = df.shape()[1]
        var r = row if row >= 0 else nrows + row
        if r < 0 or r >= nrows:
            raise Error(
                "iat: row index "
                + String(row)
                + " out of bounds for DataFrame with "
                + String(nrows)
                + " rows"
            )
        if col < 0 or col >= ncols:
            raise Error(
                "iat: column index "
                + String(col)
                + " out of bounds for DataFrame with "
                + String(ncols)
                + " columns"
            )
        _set_scalar_in_col(df._cols[col], r, value)
