from python import Python, PythonObject
from utils import Variant
from memory import bitcast
from collections import Dict, Set
from math import sqrt, floor
from .dtypes import (
    BisonDtype,
    int8, int16, int32, int64,
    uint8, uint16, uint32, uint64,
    float32, float64,
    bool_, object_,
    datetime64_ns, timedelta64_ns,
)

# One active arm per Column instance, selected by dtype:
#   List[Int64]        — int8/16/32/64, uint8/16/32/64
#   List[Float64]      — float32/float64
#   List[Bool]         — bool
#   List[String]       — string / pandas StringDtype
#   List[PythonObject] — object, datetime64, timedelta64 (fallback)
comptime ColumnData = Variant[
    List[Int64],
    List[Float64],
    List[Bool],
    List[String],
    List[PythonObject],
]

# Scalar type for a single cell in row-oriented input (from_records).
# No PythonObject arm — record values must be explicitly typed.
comptime DFScalar = Variant[Int64, Float64, Bool, String]

# Scalar type returned by Series.iloc / Series.at.
# Covers all five ColumnData arm types; the PythonObject arm is used only
# for object/datetime/timedelta columns that have no native Mojo equivalent.
comptime SeriesScalar = Variant[Int64, Float64, Bool, String, PythonObject]


# ------------------------------------------------------------------
# Visit primitive — the single canonical dispatch site for ColumnData
# ------------------------------------------------------------------

trait ColumnDataVisitor:
    """Protocol for visiting the active arm of a ``ColumnData`` Variant.

    Implement one ``on_*`` method per arm.  Use a ``mut self`` field to
    accumulate or return a result.  Pass an instance to
    ``visit_col_data``, which contains the **only** ``isa`` chain in the
    codebase; all callers should delegate here instead of writing their
    own discriminant checks.
    """

    fn on_int64(mut self, data: List[Int64]): ...
    fn on_float64(mut self, data: List[Float64]): ...
    fn on_bool(mut self, data: List[Bool]): ...
    fn on_str(mut self, data: List[String]): ...
    fn on_obj(mut self, data: List[PythonObject]): ...


fn visit_col_data[V: ColumnDataVisitor](mut visitor: V, data: ColumnData):
    """Dispatch *visitor* to the active ``ColumnData`` arm.

    This is the **only** place in the codebase that reads the ``ColumnData``
    discriminant via ``isa``.  Add new ``ColumnData`` arms here and in the
    ``ColumnDataVisitor`` trait — every other dispatch site is then updated
    automatically because it delegates here.
    """
    if data.isa[List[Int64]]():
        visitor.on_int64(data[List[Int64]])
    elif data.isa[List[Float64]]():
        visitor.on_float64(data[List[Float64]])
    elif data.isa[List[Bool]]():
        visitor.on_bool(data[List[Bool]])
    elif data.isa[List[String]]():
        visitor.on_str(data[List[String]])
    else:
        visitor.on_obj(data[List[PythonObject]])


# ------------------------------------------------------------------
# Private visitor implementations used by Column methods
# ------------------------------------------------------------------

struct _LenVisitor(ColumnDataVisitor, Copyable, Movable):
    """Visitor that computes the length of the active ColumnData arm."""
    var result: Int
    fn __init__(out self): self.result = 0
    fn on_int64(mut self, data: List[Int64]): self.result = len(data)
    fn on_float64(mut self, data: List[Float64]): self.result = len(data)
    fn on_bool(mut self, data: List[Bool]): self.result = len(data)
    fn on_str(mut self, data: List[String]): self.result = len(data)
    fn on_obj(mut self, data: List[PythonObject]): self.result = len(data)


struct _DtypeSniffVisitor(ColumnDataVisitor, Copyable, Movable):
    """Visitor that maps the active ColumnData arm to its BisonDtype."""
    var result: BisonDtype
    # object_ is the safe fallback: both List[String] and List[PythonObject]
    # map to object_.  The field is always overwritten by on_*.
    fn __init__(out self): self.result = object_
    fn on_int64(mut self, data: List[Int64]): self.result = int64
    fn on_float64(mut self, data: List[Float64]): self.result = float64
    fn on_bool(mut self, data: List[Bool]): self.result = bool_
    fn on_str(mut self, data: List[String]): self.result = object_
    fn on_obj(mut self, data: List[PythonObject]): self.result = object_


struct _CopyDataVisitor(ColumnDataVisitor, Copyable, Movable):
    """Visitor that produces an independent copy of the active ColumnData arm."""
    var result: ColumnData
    # Initialised with the fallback arm (List[PythonObject]) so that the field
    # is always valid.  on_* immediately replaces it with the copied data.
    fn __init__(out self): self.result = ColumnData(List[PythonObject]())
    fn on_int64(mut self, data: List[Int64]): self.result = ColumnData(data.copy())
    fn on_float64(mut self, data: List[Float64]): self.result = ColumnData(data.copy())
    fn on_bool(mut self, data: List[Bool]): self.result = ColumnData(data.copy())
    fn on_str(mut self, data: List[String]): self.result = ColumnData(data.copy())
    fn on_obj(mut self, data: List[PythonObject]): self.result = ColumnData(data.copy())


# ------------------------------------------------------------------
# Compile-time operation selectors for Column._arith_op
# ------------------------------------------------------------------
comptime _ARITH_ADD      = 0
comptime _ARITH_SUB      = 1
comptime _ARITH_MUL      = 2
comptime _ARITH_DIV      = 3
comptime _ARITH_FLOORDIV = 4
comptime _ARITH_MOD      = 5
comptime _ARITH_POW      = 6


# ------------------------------------------------------------------
# Compile-time operation selectors for Column._cmp_op
# ------------------------------------------------------------------
comptime _CMP_EQ = 0
comptime _CMP_NE = 1
comptime _CMP_LT = 2
comptime _CMP_LE = 3
comptime _CMP_GT = 4
comptime _CMP_GE = 5

# Compile-time function type for element-wise Float64 transforms (_apply kernel)
comptime FloatTransformFn = fn(Float64) -> Float64


# ------------------------------------------------------------------
# Shared preamble holder for binary element-wise operations.
# Returned by Column._binary_op_prepare; avoids duplicating the
# length check, float64 conversion, and null-mask detection across
# _arith_op and _cmp_op.
# ------------------------------------------------------------------
struct _BinOpInputs(Movable):
    var a: List[Float64]
    var b: List[Float64]
    var has_a_mask: Bool
    var has_b_mask: Bool

    fn __init__(out self, var a: List[Float64], var b: List[Float64],
                has_a_mask: Bool, has_b_mask: Bool):
        self.a = a^
        self.b = b^
        self.has_a_mask = has_a_mask
        self.has_b_mask = has_b_mask


struct Column(Copyable, Movable, Sized):
    """A single typed array representing one column of a DataFrame or a Series.

    Data is stored as a ``ColumnData`` Variant — one typed list per column,
    selected by ``dtype``.  Only the arm matching the dtype is populated;
    all other arms are empty.  The ``dtype`` field records the
    pandas-compatible dtype string so that round-trips through ``to_pandas``
    preserve the original dtype.

    Null tracking: ``_null_mask`` is a parallel ``List[Bool]`` where ``True``
    marks a null/NaN element.  An empty mask means no nulls are present.
    """

    var name: String
    var dtype: BisonDtype
    var _data: ColumnData
    var _index: List[PythonObject]
    var _null_mask: List[Bool]

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    fn __init__(out self):
        """Empty column with object dtype — used as stub placeholder."""
        self.name  = ""
        self.dtype = object_
        self._data = ColumnData(List[PythonObject]())
        self._index = List[PythonObject]()
        self._null_mask = List[Bool]()

    fn __init__(out self, name: String, var data: ColumnData, dtype: BisonDtype):
        self.name  = name
        self.dtype = dtype
        self._data = data^
        self._index = List[PythonObject]()
        self._null_mask = List[Bool]()

    fn __init__(out self, name: String, var data: ColumnData, dtype: BisonDtype, var index: List[PythonObject]):
        self.name  = name
        self.dtype = dtype
        self._data = data^
        self._index = index^
        self._null_mask = List[Bool]()

    # ------------------------------------------------------------------
    # Traits
    # NOTE: List[PythonObject] is NOT ImplicitlyCopyable because
    # PythonObject does not implement ImplicitlyCopyable.  Any field of
    # type List[PythonObject] (currently _index) MUST use an explicit
    # .copy() call here; implicit assignment will not compile.  If you
    # add more List[PythonObject] fields to Column, remember to copy
    # them explicitly in __copyinit__ as well.
    # ------------------------------------------------------------------

    fn __copyinit__(out self, existing: Self):
        self.name  = existing.name
        self.dtype = existing.dtype
        self._data = existing._data
        # PythonObject is not ImplicitlyCopyable — explicit .copy() required.
        self._index = existing._index.copy()
        self._null_mask = existing._null_mask.copy()

    fn __moveinit__(out self, deinit existing: Self):
        self.name  = existing.name^
        self.dtype = existing.dtype^
        self._data = existing._data^
        self._index = existing._index^
        self._null_mask = existing._null_mask^

    # ------------------------------------------------------------------
    # Typed accessor helpers — unsafe direct Variant subscripts; callers
    # are responsible for checking the active arm before calling these.
    # ------------------------------------------------------------------

    fn _int64_data(ref self) -> ref [self._data] List[Int64]:
        return self._data[List[Int64]]

    fn _float64_data(ref self) -> ref [self._data] List[Float64]:
        return self._data[List[Float64]]

    fn _bool_data(ref self) -> ref [self._data] List[Bool]:
        return self._data[List[Bool]]

    fn _str_data(ref self) -> ref [self._data] List[String]:
        return self._data[List[String]]

    fn _obj_data(ref self) -> ref [self._data] List[PythonObject]:
        return self._data[List[PythonObject]]

    # ------------------------------------------------------------------
    # Explicit copy helper (used by Series / DataFrame __copyinit__)
    # ------------------------------------------------------------------

    fn copy(self) -> Column:
        """Return an independent copy of this Column."""
        var visitor = _CopyDataVisitor()
        visit_col_data(visitor, self._data)
        var idx = self._index.copy()
        var mask = self._null_mask.copy()
        var col = Column(self.name, visitor^.result, self.dtype, idx^)
        col._null_mask = mask^
        return col^

    # ------------------------------------------------------------------
    # Length
    # ------------------------------------------------------------------

    fn __len__(self) -> Int:
        var visitor = _LenVisitor()
        visit_col_data(visitor, self._data)
        return visitor.result

    # ------------------------------------------------------------------
    # Row selection helpers
    # ------------------------------------------------------------------

    fn slice(self, start: Int, end: Int) -> Column:
        """Return a new Column containing rows [start, end).

        Negative indices are not supported; out-of-range bounds are clamped.
        """
        var n = len(self)
        var s = start
        if s < 0:
            s = 0
        if s > n:
            s = n
        var e = end
        if e < 0:
            e = 0
        if e > n:
            e = n
        var has_mask = len(self._null_mask) > 0
        var new_mask = List[Bool]()
        if has_mask:
            for i in range(s, e):
                new_mask.append(self._null_mask[i])
        if self._data.isa[List[Int64]]():
            var result = List[Int64]()
            ref d = self._data[List[Int64]]
            for i in range(s, e):
                result.append(d[i])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^
        elif self._data.isa[List[Float64]]():
            var result = List[Float64]()
            ref d = self._data[List[Float64]]
            for i in range(s, e):
                result.append(d[i])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^
        elif self._data.isa[List[Bool]]():
            var result = List[Bool]()
            ref d = self._data[List[Bool]]
            for i in range(s, e):
                result.append(d[i])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^
        elif self._data.isa[List[String]]():
            var result = List[String]()
            ref d = self._data[List[String]]
            for i in range(s, e):
                result.append(d[i])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^
        else:
            var result = List[PythonObject]()
            ref d = self._data[List[PythonObject]]
            for i in range(s, e):
                result.append(d[i])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^

    fn take(self, indices: List[Int]) -> Column:
        """Return a new Column with rows selected by *indices* (arbitrary order)."""
        var has_mask = len(self._null_mask) > 0
        var new_mask = List[Bool]()
        if self._data.isa[List[Int64]]():
            var result = List[Int64]()
            ref d = self._data[List[Int64]]
            for k in range(len(indices)):
                var idx = indices[k]
                result.append(d[idx])
                if has_mask:
                    new_mask.append(self._null_mask[idx])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^
        elif self._data.isa[List[Float64]]():
            var result = List[Float64]()
            ref d = self._data[List[Float64]]
            for k in range(len(indices)):
                var idx = indices[k]
                result.append(d[idx])
                if has_mask:
                    new_mask.append(self._null_mask[idx])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^
        elif self._data.isa[List[Bool]]():
            var result = List[Bool]()
            ref d = self._data[List[Bool]]
            for k in range(len(indices)):
                var idx = indices[k]
                result.append(d[idx])
                if has_mask:
                    new_mask.append(self._null_mask[idx])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^
        elif self._data.isa[List[String]]():
            var result = List[String]()
            ref d = self._data[List[String]]
            for k in range(len(indices)):
                var idx = indices[k]
                result.append(d[idx])
                if has_mask:
                    new_mask.append(self._null_mask[idx])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^
        else:
            var result = List[PythonObject]()
            ref d = self._data[List[PythonObject]]
            for k in range(len(indices)):
                var idx = indices[k]
                result.append(d[idx])
                if has_mask:
                    new_mask.append(self._null_mask[idx])
            var col_data = ColumnData(result^)
            var col = Column(self.name, col_data^, self.dtype)
            if len(new_mask) > 0:
                col._null_mask = new_mask^
            return col^

    # ------------------------------------------------------------------
    # Null tracking
    # ------------------------------------------------------------------

    fn has_nulls(self) -> Bool:
        """Return True if any element is marked null/NaN."""
        for i in range(len(self._null_mask)):
            if self._null_mask[i]:
                return True
        return False

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    fn sum(self, skipna: Bool = True) raises -> Float64:
        """Return the sum of all values as Float64.

        When ``skipna=True`` (default) null/NaN elements are skipped.
        When ``skipna=False`` the result is NaN if any null is present.
        Raises for non-numeric column types.
        """
        if not skipna and self.has_nulls():
            # Return NaN (IEEE 754: 0/0 → quiet NaN).
            var zero = Float64(0)
            return zero / zero
        var has_mask = len(self._null_mask) > 0
        if self._data.isa[List[Int64]]():
            var total = Float64(0)
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    continue
                total += Float64(self._data[List[Int64]][i])
            return total
        elif self._data.isa[List[Float64]]():
            var total = Float64(0)
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    continue
                total += self._data[List[Float64]][i]
            return total
        elif self._data.isa[List[Bool]]():
            var total = Float64(0)
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    continue
                if self._data[List[Bool]][i]:
                    total += 1.0
            return total
        else:
            raise Error("sum: non-numeric column type")

    fn count(self) -> Int:
        """Return the number of non-null elements."""
        var n = len(self)
        if len(self._null_mask) == 0:
            return n
        var result = 0
        for i in range(n):
            if not self._null_mask[i]:
                result += 1
        return result

    fn mean(self, skipna: Bool = True) raises -> Float64:
        """Return the mean of all values as Float64.

        Returns NaN when all elements are null or the column is empty.
        Raises for non-numeric column types.
        """
        var n = self.count() if skipna else len(self)
        if n == 0:
            var zero = Float64(0)
            return zero / zero
        return self.sum(skipna) / Float64(n)

    fn min(self, skipna: Bool = True) raises -> Float64:
        """Return the minimum value as Float64.

        Returns NaN when no non-null elements exist.
        Raises for non-numeric column types.
        """
        if not skipna and self.has_nulls():
            var zero = Float64(0)
            return zero / zero
        var has_mask = len(self._null_mask) > 0
        var found = False
        var result = Float64(0)
        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    continue
                var v = Float64(self._data[List[Int64]][i])
                if not found or v < result:
                    result = v
                    found = True
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    continue
                var v = self._data[List[Float64]][i]
                if not found or v < result:
                    result = v
                    found = True
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    continue
                var v = Float64(1.0) if self._data[List[Bool]][i] else Float64(0.0)
                if not found or v < result:
                    result = v
                    found = True
        else:
            raise Error("min: non-numeric column type")
        if not found:
            var zero = Float64(0)
            return zero / zero
        return result

    fn max(self, skipna: Bool = True) raises -> Float64:
        """Return the maximum value as Float64.

        Returns NaN when no non-null elements exist.
        Raises for non-numeric column types.
        """
        if not skipna and self.has_nulls():
            var zero = Float64(0)
            return zero / zero
        var has_mask = len(self._null_mask) > 0
        var found = False
        var result = Float64(0)
        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    continue
                var v = Float64(self._data[List[Int64]][i])
                if not found or v > result:
                    result = v
                    found = True
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    continue
                var v = self._data[List[Float64]][i]
                if not found or v > result:
                    result = v
                    found = True
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    continue
                var v = Float64(1.0) if self._data[List[Bool]][i] else Float64(0.0)
                if not found or v > result:
                    result = v
                    found = True
        else:
            raise Error("max: non-numeric column type")
        if not found:
            var zero = Float64(0)
            return zero / zero
        return result

    fn var(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        """Return the variance with Bessel correction (ddof=1 by default).

        Returns NaN when n - ddof <= 0.
        Raises for non-numeric column types.
        """
        var n = self.count() if skipna else len(self)
        if n - ddof <= 0:
            var zero = Float64(0)
            return zero / zero
        var m = self.mean(skipna)
        var has_mask = len(self._null_mask) > 0
        var total = Float64(0)
        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    continue
                var diff = Float64(self._data[List[Int64]][i]) - m
                total += diff * diff
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    continue
                var diff = self._data[List[Float64]][i] - m
                total += diff * diff
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    continue
                var v = Float64(1.0) if self._data[List[Bool]][i] else Float64(0.0)
                var diff = v - m
                total += diff * diff
        else:
            raise Error("var: non-numeric column type")
        return total / Float64(n - ddof)

    fn std(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        """Return the standard deviation (square root of variance)."""
        return sqrt(self.var(ddof, skipna))

    fn nunique(self) raises -> Int:
        """Return the number of unique non-null values.

        Raises for non-numeric and non-string column types.
        """
        var has_mask = len(self._null_mask) > 0
        if self._data.isa[List[Int64]]():
            var seen = Set[Int64]()
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    continue
                seen.add(self._data[List[Int64]][i])
            return len(seen)
        elif self._data.isa[List[Float64]]():
            var seen = Set[Float64]()
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    continue
                seen.add(self._data[List[Float64]][i])
            return len(seen)
        elif self._data.isa[List[Bool]]():
            var seen = Set[Bool]()
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    continue
                seen.add(self._data[List[Bool]][i])
            return len(seen)
        elif self._data.isa[List[String]]():
            var seen = Set[String]()
            for i in range(len(self._data[List[String]])):
                if has_mask and self._null_mask[i]:
                    continue
                seen.add(self._data[List[String]][i])
            return len(seen)
        else:
            raise Error("nunique: unsupported column type")

    fn quantile(self, q: Float64 = 0.5) raises -> Float64:
        """Return the q-th quantile using linear interpolation.

        Always skips null elements (matches pandas default behaviour).
        Raises for non-numeric column types.
        """
        var vals = List[Float64]()
        var has_mask = len(self._null_mask) > 0
        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    continue
                vals.append(Float64(self._data[List[Int64]][i]))
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    continue
                vals.append(self._data[List[Float64]][i])
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    continue
                vals.append(Float64(1.0) if self._data[List[Bool]][i] else Float64(0.0))
        else:
            raise Error("quantile: non-numeric column type")
        if len(vals) == 0:
            var zero = Float64(0)
            return zero / zero
        # Insertion sort (values list is typically small).
        for i in range(1, len(vals)):
            var key = vals[i]
            var j = i - 1
            while j >= 0 and vals[j] > key:
                vals[j + 1] = vals[j]
                j -= 1
            vals[j + 1] = key
        var idx = q * Float64(len(vals) - 1)
        var lo = Int(idx)
        var hi = lo + 1
        if hi >= len(vals):
            return vals[lo]
        var frac = idx - Float64(lo)
        return vals[lo] * (1.0 - frac) + vals[hi] * frac

    fn median(self, skipna: Bool = True) raises -> Float64:
        """Return the median value.

        When skipna=False and nulls are present, returns NaN.
        """
        if not skipna and self.has_nulls():
            var zero = Float64(0)
            return zero / zero
        return self.quantile(0.5)

    fn describe(self) raises -> Column:
        """Return summary statistics as a new Column with a string index.

        Produces 8 elements: count, mean, std, min, 25%, 50%, 75%, max.
        Raises for non-numeric column types (the underlying aggregation
        methods propagate the error).
        """
        var data = List[Float64]()
        data.append(Float64(self.count()))
        data.append(self.mean())
        data.append(self.std())
        data.append(self.min())
        data.append(self.quantile(0.25))
        data.append(self.quantile(0.50))
        data.append(self.quantile(0.75))
        data.append(self.max())

        var idx = List[PythonObject]()
        idx.append(PythonObject("count"))
        idx.append(PythonObject("mean"))
        idx.append(PythonObject("std"))
        idx.append(PythonObject("min"))
        idx.append(PythonObject("25%"))
        idx.append(PythonObject("50%"))
        idx.append(PythonObject("75%"))
        idx.append(PythonObject("max"))

        return Column(self.name, ColumnData(data^), float64, idx^)

    fn value_counts(self, normalize: Bool = False, sort: Bool = True) raises -> Column:
        """Return a Column of counts (or proportions) per unique value.

        The index holds the original values; the data holds the counts.
        sort=True (default) orders results by count descending.
        Raises for object/datetime column types.
        """
        var has_mask = len(self._null_mask) > 0
        var unique_keys = List[String]()
        var counts_dict = Dict[String, Int]()

        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    continue
                var k = String(self._data[List[Int64]][i])
                if k not in counts_dict:
                    unique_keys.append(k)
                counts_dict[k] = counts_dict.get(k, 0) + 1
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    continue
                var k = String(self._data[List[Float64]][i])
                if k not in counts_dict:
                    unique_keys.append(k)
                counts_dict[k] = counts_dict.get(k, 0) + 1
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    continue
                var k = String(self._data[List[Bool]][i])
                if k not in counts_dict:
                    unique_keys.append(k)
                counts_dict[k] = counts_dict.get(k, 0) + 1
        else:
            raise Error("value_counts: unsupported column type")

        var n = len(unique_keys)

        # Materialise per-key counts in insertion order.
        var count_vals = List[Int]()
        for i in range(n):
            count_vals.append(counts_dict[unique_keys[i]])

        # Compute a sorted permutation (insertion sort, stable, count desc).
        var sorted_order = List[Int]()
        for i in range(n):
            sorted_order.append(i)
        if sort:
            for i in range(1, n):
                var key_idx = sorted_order[i]
                var key_cnt = count_vals[key_idx]
                var j = i - 1
                while j >= 0 and count_vals[sorted_order[j]] < key_cnt:
                    sorted_order[j + 1] = sorted_order[j]
                    j -= 1
                sorted_order[j + 1] = key_idx

        # Build result index (original values) and counts.
        var builtins = Python.import_module("builtins")
        var result_counts = List[Int64]()
        var result_idx = List[PythonObject]()

        if self._data.isa[List[Int64]]():
            for i in range(n):
                var si = sorted_order[i]
                result_counts.append(Int64(count_vals[si]))
                result_idx.append(builtins.int(unique_keys[si]))
        elif self._data.isa[List[Float64]]():
            for i in range(n):
                var si = sorted_order[i]
                result_counts.append(Int64(count_vals[si]))
                result_idx.append(builtins.float(unique_keys[si]))
        else:  # Bool — only remaining arm after the raise above
            for i in range(n):
                var si = sorted_order[i]
                result_counts.append(Int64(count_vals[si]))
                result_idx.append(PythonObject(unique_keys[si] == "True"))

        if normalize:
            var total = Float64(self.count())
            var norm_data = List[Float64]()
            for i in range(n):
                norm_data.append(Float64(result_counts[i]) / total)
            return Column("count", ColumnData(norm_data^), float64, result_idx^)

        return Column("count", ColumnData(result_counts^), int64, result_idx^)

    # ------------------------------------------------------------------
    # Element-wise arithmetic helpers
    # ------------------------------------------------------------------

    fn _to_float64_list(self) raises -> List[Float64]:
        """Convert the active numeric arm to a List[Float64].

        Raises for non-numeric (String, PythonObject) column types.
        """
        var result = List[Float64]()
        if self._data.isa[List[Int64]]():
            ref d = self._data[List[Int64]]
            for i in range(len(d)):
                result.append(Float64(d[i]))
        elif self._data.isa[List[Float64]]():
            ref d = self._data[List[Float64]]
            for i in range(len(d)):
                result.append(d[i])
        elif self._data.isa[List[Bool]]():
            ref d = self._data[List[Bool]]
            for i in range(len(d)):
                result.append(1.0 if d[i] else 0.0)
        else:
            raise Error("arith: non-numeric column type")
        return result^

    fn _build_result_col(self, var col_data: ColumnData, var result_mask: List[Bool], has_any_null: Bool) -> Column:
        """Wrap a computed ColumnData into a Column, attaching mask only if needed."""
        var dtype = Column._sniff_dtype(col_data)
        var col = Column(self.name, col_data^, dtype)
        if has_any_null:
            col._null_mask = result_mask^
        return col^

    fn _binary_op_prepare(self, op_name: String, other: Column) raises -> _BinOpInputs:
        """Check lengths and build the shared Float64 input arrays and null-mask flags.

        Raises if ``self`` and ``other`` differ in length.  Called at the top
        of ``_arith_op`` and ``_cmp_op`` to eliminate repeated preamble code.
        """
        if len(self) != len(other):
            raise Error(op_name + ": length mismatch (" + String(len(self)) + " vs " + String(len(other)) + ")")
        var a = self._to_float64_list()
        var b = other._to_float64_list()
        return _BinOpInputs(a^, b^, len(self._null_mask) > 0, len(other._null_mask) > 0)

    fn _arith_op[op: Int](self, op_name: String, other: Column) raises -> Column:
        """Core element-wise binary arithmetic kernel.

        ``op`` is a compile-time constant (``_ARITH_*``) that selects the
        operation; ``@parameter if`` folds the branch at compile time so each
        specialisation compiles to a tight scalar loop with no runtime dispatch.
        """
        var inp = self._binary_op_prepare(op_name, other)
        var result = List[Float64]()
        var result_mask = List[Bool]()
        var has_any_null = False
        var nan = Float64(0) / Float64(0)
        for i in range(len(inp.a)):
            var is_null = (inp.has_a_mask and self._null_mask[i]) or (inp.has_b_mask and other._null_mask[i])
            if is_null:
                result.append(nan)
                result_mask.append(True)
                has_any_null = True
            else:
                var v: Float64
                @parameter
                if op == _ARITH_ADD:
                    v = inp.a[i] + inp.b[i]
                elif op == _ARITH_SUB:
                    v = inp.a[i] - inp.b[i]
                elif op == _ARITH_MUL:
                    v = inp.a[i] * inp.b[i]
                elif op == _ARITH_DIV:
                    v = inp.a[i] / inp.b[i]
                elif op == _ARITH_FLOORDIV:
                    v = floor(inp.a[i] / inp.b[i])
                elif op == _ARITH_MOD:
                    v = inp.a[i] - floor(inp.a[i] / inp.b[i]) * inp.b[i]
                elif op == _ARITH_POW:
                    v = inp.a[i] ** inp.b[i]
                else:
                    v = Float64(0)  # unreachable: compile-time guard
                result.append(v)
                result_mask.append(False)
        return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)

    fn _arith_add(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_ADD]("add", other)

    fn _arith_sub(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_SUB]("sub", other)

    fn _arith_mul(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_MUL]("mul", other)

    fn _arith_div(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_DIV]("div", other)

    fn _arith_floordiv(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_FLOORDIV]("floordiv", other)

    fn _arith_mod(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_MOD]("mod", other)

    fn _arith_pow(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_POW]("pow", other)

    # ------------------------------------------------------------------
    # Comparison operations
    # ------------------------------------------------------------------

    fn _cmp_op[op: Int](self, op_name: String, other: Column) raises -> Column:
        """Core element-wise binary comparison kernel.

        ``op`` is a compile-time constant (``_CMP_*``) that selects the
        operation; ``@parameter if`` folds the branch at compile time so each
        specialisation compiles to a tight scalar loop with no runtime dispatch.
        Null propagation: if either input element is null, the result is null.

        When both columns are Bool, comparison is performed directly on Bool
        values without round-tripping through Float64.
        """
        if len(self) != len(other):
            raise Error(op_name + ": length mismatch (" + String(len(self)) + " vs " + String(len(other)) + ")")
        var result = List[Bool]()
        var result_mask = List[Bool]()
        var has_any_null = False
        var has_a_mask = len(self._null_mask) > 0
        var has_b_mask = len(other._null_mask) > 0
        if self._data.isa[List[Bool]]() and other._data.isa[List[Bool]]():
            ref da = self._data[List[Bool]]
            ref db = other._data[List[Bool]]
            for i in range(len(da)):
                var is_null = (has_a_mask and self._null_mask[i]) or (has_b_mask and other._null_mask[i])
                if is_null:
                    result.append(False)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v: Bool
                    @parameter
                    if op == _CMP_EQ:
                        v = da[i] == db[i]
                    elif op == _CMP_NE:
                        v = da[i] != db[i]
                    elif op == _CMP_LT:
                        v = (not da[i]) and db[i]  # False < True: False=0, True=1
                    elif op == _CMP_LE:
                        v = (not da[i]) or db[i]   # False <= True, False <= False, True <= True
                    elif op == _CMP_GT:
                        v = da[i] and (not db[i])  # True > False
                    elif op == _CMP_GE:
                        v = da[i] or (not db[i])   # True >= False, False >= False, True >= True
                    else:
                        v = False  # unreachable: compile-time guard
                    result.append(v)
                    result_mask.append(False)
        else:
            var inp = self._binary_op_prepare(op_name, other)
            for i in range(len(inp.a)):
                var is_null = (inp.has_a_mask and self._null_mask[i]) or (inp.has_b_mask and other._null_mask[i])
                if is_null:
                    result.append(False)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v: Bool
                    @parameter
                    if op == _CMP_EQ:
                        v = inp.a[i] == inp.b[i]
                    elif op == _CMP_NE:
                        v = inp.a[i] != inp.b[i]
                    elif op == _CMP_LT:
                        v = inp.a[i] < inp.b[i]
                    elif op == _CMP_LE:
                        v = inp.a[i] <= inp.b[i]
                    elif op == _CMP_GT:
                        v = inp.a[i] > inp.b[i]
                    elif op == _CMP_GE:
                        v = inp.a[i] >= inp.b[i]
                    else:
                        v = False  # unreachable: compile-time guard
                    result.append(v)
                    result_mask.append(False)
        return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)

    fn _cmp_eq(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_EQ]("eq", other)

    fn _cmp_ne(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_NE]("ne", other)

    fn _cmp_lt(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_LT]("lt", other)

    fn _cmp_le(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_LE]("le", other)

    fn _cmp_gt(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_GT]("gt", other)

    fn _cmp_ge(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_GE]("ge", other)

    # ------------------------------------------------------------------
    # Transformation kernels
    # ------------------------------------------------------------------

    fn _abs(self) raises -> Column:
        """Return element-wise absolute value.

        Int64 and Float64 arms are supported; Bool is identity.
        Nulls propagate. Raises for String/Object columns.
        """
        var has_mask = len(self._null_mask) > 0
        var result_mask = List[Bool]()
        var has_any_null = False
        var nan = Float64(0) / Float64(0)
        if self._data.isa[List[Int64]]():
            ref d = self._data[List[Int64]]
            var result = List[Int64]()
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    result.append(Int64(0))
                    result_mask.append(True)
                    has_any_null = True
                else:
                    result.append(d[i] if d[i] >= 0 else -d[i])
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[Float64]]():
            ref d = self._data[List[Float64]]
            var result = List[Float64]()
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    result.append(nan)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    result.append(d[i] if d[i] >= 0.0 else -d[i])
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[Bool]]():
            return self.copy()
        else:
            raise Error("abs: not supported for dtype " + String(self.dtype.name))

    fn _round(self, decimals: Int = 0) raises -> Column:
        """Round Float64 values to ``decimals`` decimal places.

        Int64 and Bool columns are returned unchanged. Raises for
        String/Object columns or negative ``decimals``.
        Uses banker's rounding (round-half-to-even), matching Python and
        numpy behaviour at exact half-way points.
        """
        if decimals < 0:
            raise Error("round: negative decimals not supported")
        var has_mask = len(self._null_mask) > 0
        var result_mask = List[Bool]()
        var has_any_null = False
        var nan = Float64(0) / Float64(0)
        if self._data.isa[List[Float64]]():
            ref d = self._data[List[Float64]]
            var result = List[Float64]()
            var factor = Float64(1)
            for _ in range(decimals):
                factor *= 10.0
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    result.append(nan)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    result.append(round(d[i] * factor) / factor)
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[Int64]]() or self._data.isa[List[Bool]]():
            return self.copy()
        else:
            raise Error("round: not supported for dtype " + String(self.dtype.name))

    fn _clip(self, lower: Float64, upper: Float64) raises -> Column:
        """Clamp values to [``lower``, ``upper``].

        Supports Int64 and Float64 arms. Nulls propagate.
        Raises for String/Object columns.
        """
        var has_mask = len(self._null_mask) > 0
        var result_mask = List[Bool]()
        var has_any_null = False
        var nan = Float64(0) / Float64(0)
        if self._data.isa[List[Int64]]():
            ref d = self._data[List[Int64]]
            var result = List[Int64]()
            var lo = Int64(lower)
            var hi = Int64(upper)
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    result.append(Int64(0))
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v = d[i]
                    if v < lo:
                        v = lo
                    elif v > hi:
                        v = hi
                    result.append(v)
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[Float64]]():
            ref d = self._data[List[Float64]]
            var result = List[Float64]()
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    result.append(nan)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v = d[i]
                    if v < lower:
                        v = lower
                    elif v > upper:
                        v = upper
                    result.append(v)
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        else:
            raise Error("clip: not supported for dtype " + String(self.dtype.name))

    fn _apply[F: FloatTransformFn](self) raises -> Column:
        """Apply a compile-time function element-wise over Float64 values.

        Numeric arms are converted to Float64 before application. Nulls
        propagate. Raises for String/Object columns (via _to_float64_list).
        Call as ``col._apply[my_fn]()``.
        """
        var data = self._to_float64_list()
        var has_mask = len(self._null_mask) > 0
        var result = List[Float64]()
        var result_mask = List[Bool]()
        var has_any_null = False
        var nan = Float64(0) / Float64(0)
        for i in range(len(data)):
            if has_mask and self._null_mask[i]:
                result.append(nan)
                result_mask.append(True)
                has_any_null = True
            else:
                result.append(F(data[i]))
                result_mask.append(False)
        return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)

    fn _isin_int(self, values: List[Int64]) raises -> Column:
        """Bool Column: True where element is in ``values`` (Int64 columns only).

        Nulls propagate as null.
        """
        if not self._data.isa[List[Int64]]():
            raise Error("isin: column must be Int64 to match against List[Int64]")
        ref d = self._data[List[Int64]]
        var has_mask = len(self._null_mask) > 0
        var result = List[Bool]()
        var result_mask = List[Bool]()
        var has_any_null = False
        for i in range(len(d)):
            if has_mask and self._null_mask[i]:
                result.append(False)
                result_mask.append(True)
                has_any_null = True
            else:
                var found = False
                for j in range(len(values)):
                    if d[i] == values[j]:
                        found = True
                        break
                result.append(found)
                result_mask.append(False)
        return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)

    fn _isin_float(self, values: List[Float64]) raises -> Column:
        """Bool Column: True where element is in ``values`` (Float64 columns only).

        Nulls propagate as null.
        """
        if not self._data.isa[List[Float64]]():
            raise Error("isin: column must be Float64 to match against List[Float64]")
        ref d = self._data[List[Float64]]
        var has_mask = len(self._null_mask) > 0
        var result = List[Bool]()
        var result_mask = List[Bool]()
        var has_any_null = False
        for i in range(len(d)):
            if has_mask and self._null_mask[i]:
                result.append(False)
                result_mask.append(True)
                has_any_null = True
            else:
                var found = False
                for j in range(len(values)):
                    if d[i] == values[j]:
                        found = True
                        break
                result.append(found)
                result_mask.append(False)
        return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)

    fn _between(self, left: Float64, right: Float64) raises -> Column:
        """Bool Column: True where left <= element <= right.

        Numeric arms are converted to Float64. Nulls propagate.
        Raises for String/Object columns (via _to_float64_list).
        """
        var data = self._to_float64_list()
        var has_mask = len(self._null_mask) > 0
        var result = List[Bool]()
        var result_mask = List[Bool]()
        var has_any_null = False
        for i in range(len(data)):
            if has_mask and self._null_mask[i]:
                result.append(False)
                result_mask.append(True)
                has_any_null = True
            else:
                result.append(data[i] >= left and data[i] <= right)
                result_mask.append(False)
        return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)

    fn _where_mask[mode: Int](self, cond: Column) raises -> Column:
        """Shared kernel for ``_where`` (mode=1) and ``_mask`` (mode=0).

        mode=1: keep value where cond is True, null otherwise.
        mode=0: null value where cond is True, keep otherwise.
        Supports Int64, Float64, Bool, String arms. Raises for Object dtype.
        """
        if not cond._data.isa[List[Bool]]():
            raise Error("where/mask: condition must be a Bool Series")
        if len(self) != len(cond):
            raise Error(
                "where/mask: length mismatch ("
                + String(len(self))
                + " vs "
                + String(len(cond))
                + ")"
            )
        ref cd = cond._data[List[Bool]]
        var has_cond_mask = len(cond._null_mask) > 0
        var has_self_mask = len(self._null_mask) > 0
        var result_mask = List[Bool]()
        var has_any_null = False
        var nan = Float64(0) / Float64(0)
        if self._data.isa[List[Int64]]():
            ref d = self._data[List[Int64]]
            var result = List[Int64]()
            for i in range(len(d)):
                var self_null = has_self_mask and self._null_mask[i]
                var cond_true = (not has_cond_mask or not cond._null_mask[i]) and cd[i]
                var keep: Bool
                @parameter
                if mode == 1:
                    keep = cond_true
                else:
                    keep = not cond_true
                if self_null or not keep:
                    result.append(Int64(0))
                    result_mask.append(True)
                    has_any_null = True
                else:
                    result.append(d[i])
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[Float64]]():
            ref d = self._data[List[Float64]]
            var result = List[Float64]()
            for i in range(len(d)):
                var self_null = has_self_mask and self._null_mask[i]
                var cond_true = (not has_cond_mask or not cond._null_mask[i]) and cd[i]
                var keep: Bool
                @parameter
                if mode == 1:
                    keep = cond_true
                else:
                    keep = not cond_true
                if self_null or not keep:
                    result.append(nan)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    result.append(d[i])
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[Bool]]():
            ref d = self._data[List[Bool]]
            var result = List[Bool]()
            for i in range(len(d)):
                var self_null = has_self_mask and self._null_mask[i]
                var cond_true = (not has_cond_mask or not cond._null_mask[i]) and cd[i]
                var keep: Bool
                @parameter
                if mode == 1:
                    keep = cond_true
                else:
                    keep = not cond_true
                if self_null or not keep:
                    result.append(False)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    result.append(d[i])
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[String]]():
            ref d = self._data[List[String]]
            var result = List[String]()
            for i in range(len(d)):
                var self_null = has_self_mask and self._null_mask[i]
                var cond_true = (not has_cond_mask or not cond._null_mask[i]) and cd[i]
                var keep: Bool
                @parameter
                if mode == 1:
                    keep = cond_true
                else:
                    keep = not cond_true
                if self_null or not keep:
                    result.append(String(""))
                    result_mask.append(True)
                    has_any_null = True
                else:
                    result.append(d[i])
                    result_mask.append(False)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        else:
            raise Error("where/mask: not supported for object dtype")

    fn _where(self, cond: Column) raises -> Column:
        """Keep value where ``cond`` is True; null otherwise."""
        return self._where_mask[1](cond)

    fn _mask(self, cond: Column) raises -> Column:
        """Null value where ``cond`` is True; keep otherwise."""
        return self._where_mask[0](cond)

    fn _unique(self) raises -> Column:
        """Return a Column of unique values, preserving first-occurrence order.

        Nulls are included once at the end if present. Uses a Set[T] for O(1)
        membership checks, giving O(n) overall complexity.
        Raises for Object dtype.
        """
        var has_mask = len(self._null_mask) > 0
        if self._data.isa[List[Int64]]():
            ref d = self._data[List[Int64]]
            var seen_set = Set[Int64]()
            var result = List[Int64]()
            var result_mask = List[Bool]()
            var has_any_null = False
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    has_any_null = True
                    continue
                var v = d[i]
                if v not in seen_set:
                    seen_set.add(v)
                    result.append(v)
                    result_mask.append(False)
            if has_any_null:
                result.append(Int64(0))
                result_mask.append(True)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[Float64]]():
            ref d = self._data[List[Float64]]
            var seen_set = Set[Float64]()
            var result = List[Float64]()
            var result_mask = List[Bool]()
            var has_any_null = False
            var nan = Float64(0) / Float64(0)
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    has_any_null = True
                    continue
                var v = d[i]
                if v not in seen_set:
                    seen_set.add(v)
                    result.append(v)
                    result_mask.append(False)
            if has_any_null:
                result.append(nan)
                result_mask.append(True)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[Bool]]():
            ref d = self._data[List[Bool]]
            var seen_false = False
            var seen_true = False
            var has_any_null = False
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    has_any_null = True
                elif d[i]:
                    seen_true = True
                else:
                    seen_false = True
            var result = List[Bool]()
            var result_mask = List[Bool]()
            if seen_false:
                result.append(False)
                result_mask.append(False)
            if seen_true:
                result.append(True)
                result_mask.append(False)
            if has_any_null:
                result.append(False)
                result_mask.append(True)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        elif self._data.isa[List[String]]():
            ref d = self._data[List[String]]
            var seen_set = Set[String]()
            var result = List[String]()
            var result_mask = List[Bool]()
            var has_any_null = False
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    has_any_null = True
                    continue
                var v = d[i]
                if v not in seen_set:
                    seen_set.add(v)
                    result.append(v)
                    result_mask.append(False)
            if has_any_null:
                result.append(String(""))
                result_mask.append(True)
            return self._build_result_col(ColumnData(result^), result_mask^, has_any_null)
        else:
            raise Error("unique: not supported for object dtype")

    fn _astype(self, dtype_name: String) raises -> Column:
        """Convert Column to a different dtype.

        Supported conversions:
          Int64  → Float64, Bool, Int64 (identity)
          Float64 → Int64 (truncation), Bool, Float64 (identity)
          Bool   → Int64, Float64, Bool (identity)
        The null mask is preserved unchanged across conversions.
        Raises for unsupported source/target dtype combinations.
        """
        var has_mask = len(self._null_mask) > 0
        # Determine target family from dtype_name prefix
        var to_int = (
            dtype_name == "int8"
            or dtype_name == "int16"
            or dtype_name == "int32"
            or dtype_name == "int64"
            or dtype_name == "uint8"
            or dtype_name == "uint16"
            or dtype_name == "uint32"
            or dtype_name == "uint64"
        )
        var to_float = dtype_name == "float32" or dtype_name == "float64"
        var to_bool = dtype_name == "bool"

        if self._data.isa[List[Int64]]():
            ref d = self._data[List[Int64]]
            if to_float:
                var result = List[Float64]()
                for i in range(len(d)):
                    result.append(Float64(d[i]))
                var col = Column(self.name, ColumnData(result^), float64)
                if has_mask:
                    col._null_mask = self._null_mask.copy()
                return col^
            elif to_bool:
                var result = List[Bool]()
                for i in range(len(d)):
                    result.append(d[i] != 0)
                var col = Column(self.name, ColumnData(result^), bool_)
                if has_mask:
                    col._null_mask = self._null_mask.copy()
                return col^
            elif to_int:
                return self.copy()
            else:
                raise Error("astype: unsupported target dtype '" + dtype_name + "' for Int64 source")
        elif self._data.isa[List[Float64]]():
            ref d = self._data[List[Float64]]
            if to_int:
                var result = List[Int64]()
                for i in range(len(d)):
                    result.append(Int64(d[i]))
                var col = Column(self.name, ColumnData(result^), int64)
                if has_mask:
                    col._null_mask = self._null_mask.copy()
                return col^
            elif to_bool:
                var result = List[Bool]()
                for i in range(len(d)):
                    result.append(d[i] != 0.0)
                var col = Column(self.name, ColumnData(result^), bool_)
                if has_mask:
                    col._null_mask = self._null_mask.copy()
                return col^
            elif to_float:
                return self.copy()
            else:
                raise Error("astype: unsupported target dtype '" + dtype_name + "' for Float64 source")
        elif self._data.isa[List[Bool]]():
            ref d = self._data[List[Bool]]
            if to_int:
                var result = List[Int64]()
                for i in range(len(d)):
                    result.append(Int64(1) if d[i] else Int64(0))
                var col = Column(self.name, ColumnData(result^), int64)
                if has_mask:
                    col._null_mask = self._null_mask.copy()
                return col^
            elif to_float:
                var result = List[Float64]()
                for i in range(len(d)):
                    result.append(1.0 if d[i] else 0.0)
                var col = Column(self.name, ColumnData(result^), float64)
                if has_mask:
                    col._null_mask = self._null_mask.copy()
                return col^
            elif to_bool:
                return self.copy()
            else:
                raise Error("astype: unsupported target dtype '" + dtype_name + "' for Bool source")
        else:
            raise Error("astype: not supported for source dtype '" + String(self.dtype.name) + "'")

    fn _reset_index(self, drop: Bool = False) raises -> Column:
        """Return a copy of the Column with its index cleared.

        When ``drop=True``, the existing index labels are discarded and a
        default integer index is used. ``drop=False`` is not supported
        on Series (it would require returning a DataFrame); raises an error.
        """
        if not drop:
            raise Error(
                "reset_index: drop=False would require a DataFrame return; "
                + "pass drop=True or use DataFrame.reset_index"
            )
        var c = self.copy()
        c._index = List[PythonObject]()
        return c^

    # ------------------------------------------------------------------
    # Cumulative operations
    # ------------------------------------------------------------------

    fn cumsum(self, skipna: Bool = True) raises -> Column:
        """Return a Column of cumulative sums as Float64.

        When ``skipna=True`` (default), null elements produce NaN in the
        output but do not affect subsequent cumulative values.
        When ``skipna=False``, a null element propagates NaN to all
        subsequent positions.
        Raises for non-numeric column types.
        """
        var has_mask = len(self._null_mask) > 0
        var running = Float64(0)
        var propagate_nan = False
        var result_data = List[Float64]()
        var result_mask = List[Bool]()
        var nan = Float64(0) / Float64(0)

        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    running += Float64(self._data[List[Int64]][i])
                    result_data.append(running)
                    result_mask.append(False)
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    running += self._data[List[Float64]][i]
                    result_data.append(running)
                    result_mask.append(False)
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    running += Float64(1.0) if self._data[List[Bool]][i] else Float64(0.0)
                    result_data.append(running)
                    result_mask.append(False)
        else:
            raise Error("cumsum: non-numeric column type")

        var col_data = ColumnData(result_data^)
        var dtype = Column._sniff_dtype(col_data)
        var col = Column(self.name, col_data^, dtype)
        var has_any_null = False
        for i in range(len(result_mask)):
            if result_mask[i]:
                has_any_null = True
                break
        if has_any_null:
            col._null_mask = result_mask^
        return col^

    fn cumprod(self, skipna: Bool = True) raises -> Column:
        """Return a Column of cumulative products as Float64.

        When ``skipna=True`` (default), null elements produce NaN in the
        output but do not affect subsequent cumulative values.
        When ``skipna=False``, a null element propagates NaN to all
        subsequent positions.
        Raises for non-numeric column types.
        """
        var has_mask = len(self._null_mask) > 0
        var running = Float64(1)
        var propagate_nan = False
        var result_data = List[Float64]()
        var result_mask = List[Bool]()
        var nan = Float64(0) / Float64(0)

        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    running *= Float64(self._data[List[Int64]][i])
                    result_data.append(running)
                    result_mask.append(False)
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    running *= self._data[List[Float64]][i]
                    result_data.append(running)
                    result_mask.append(False)
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    running *= Float64(1.0) if self._data[List[Bool]][i] else Float64(0.0)
                    result_data.append(running)
                    result_mask.append(False)
        else:
            raise Error("cumprod: non-numeric column type")

        var col_data = ColumnData(result_data^)
        var dtype = Column._sniff_dtype(col_data)
        var col = Column(self.name, col_data^, dtype)
        var has_any_null = False
        for i in range(len(result_mask)):
            if result_mask[i]:
                has_any_null = True
                break
        if has_any_null:
            col._null_mask = result_mask^
        return col^

    fn cummin(self, skipna: Bool = True) raises -> Column:
        """Return a Column of cumulative minimums as Float64.

        When ``skipna=True`` (default), null elements produce NaN in the
        output but do not affect subsequent cumulative values.
        When ``skipna=False``, a null element propagates NaN to all
        subsequent positions.
        Raises for non-numeric column types.
        """
        var has_mask = len(self._null_mask) > 0
        var running = Float64(0)
        var found = False
        var propagate_nan = False
        var result_data = List[Float64]()
        var result_mask = List[Bool]()
        var nan = Float64(0) / Float64(0)

        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    var v = Float64(self._data[List[Int64]][i])
                    if not found or v < running:
                        running = v
                        found = True
                    result_data.append(running)
                    result_mask.append(False)
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    var v = self._data[List[Float64]][i]
                    if not found or v < running:
                        running = v
                        found = True
                    result_data.append(running)
                    result_mask.append(False)
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    var v = Float64(1.0) if self._data[List[Bool]][i] else Float64(0.0)
                    if not found or v < running:
                        running = v
                        found = True
                    result_data.append(running)
                    result_mask.append(False)
        else:
            raise Error("cummin: non-numeric column type")

        var col_data = ColumnData(result_data^)
        var dtype = Column._sniff_dtype(col_data)
        var col = Column(self.name, col_data^, dtype)
        var has_any_null = False
        for i in range(len(result_mask)):
            if result_mask[i]:
                has_any_null = True
                break
        if has_any_null:
            col._null_mask = result_mask^
        return col^

    fn cummax(self, skipna: Bool = True) raises -> Column:
        """Return a Column of cumulative maximums as Float64.

        When ``skipna=True`` (default), null elements produce NaN in the
        output but do not affect subsequent cumulative values.
        When ``skipna=False``, a null element propagates NaN to all
        subsequent positions.
        Raises for non-numeric column types.
        """
        var has_mask = len(self._null_mask) > 0
        var running = Float64(0)
        var found = False
        var propagate_nan = False
        var result_data = List[Float64]()
        var result_mask = List[Bool]()
        var nan = Float64(0) / Float64(0)

        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    var v = Float64(self._data[List[Int64]][i])
                    if not found or v > running:
                        running = v
                        found = True
                    result_data.append(running)
                    result_mask.append(False)
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    var v = self._data[List[Float64]][i]
                    if not found or v > running:
                        running = v
                        found = True
                    result_data.append(running)
                    result_mask.append(False)
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                var is_null = has_mask and self._null_mask[i]
                if is_null:
                    if not skipna:
                        propagate_nan = True
                    result_data.append(nan)
                    result_mask.append(True)
                elif propagate_nan:
                    result_data.append(nan)
                    result_mask.append(True)
                else:
                    var v = Float64(1.0) if self._data[List[Bool]][i] else Float64(0.0)
                    if not found or v > running:
                        running = v
                        found = True
                    result_data.append(running)
                    result_mask.append(False)
        else:
            raise Error("cummax: non-numeric column type")

        var col_data = ColumnData(result_data^)
        var dtype = Column._sniff_dtype(col_data)
        var col = Column(self.name, col_data^, dtype)
        var has_any_null = False
        for i in range(len(result_mask)):
            if result_mask[i]:
                has_any_null = True
                break
        if has_any_null:
            col._null_mask = result_mask^
        return col^

    # ------------------------------------------------------------------
    # Pandas interop
    # ------------------------------------------------------------------

    @staticmethod
    fn from_pandas(pd_series: PythonObject, name: String) raises -> Column:
        """Build a Column by copying values from a pandas Series."""
        var dtype_str = String(pd_series.dtype)
        var n = Int(pd_series.__len__())
        var py_list = pd_series.tolist()
        var py_index = pd_series.index.tolist()
        var idx_list = List[PythonObject]()
        for i in range(n):
            idx_list.append(py_index[i])

        # Build the null mask once, used by every branch below.
        var null_list = pd_series.isna().tolist()
        var null_mask = List[Bool]()
        for i in range(n):
            null_mask.append(Bool(null_list[i].__bool__()))

        var bison_dtype: BisonDtype
        if (
            dtype_str == "int8"   or dtype_str == "int16"  or
            dtype_str == "int32"  or dtype_str == "int64"  or
            dtype_str == "uint8"  or dtype_str == "uint16" or
            dtype_str == "uint32" or dtype_str == "uint64"
        ):
            bison_dtype = int64
        elif dtype_str == "float32" or dtype_str == "float64":
            bison_dtype = float64
        elif dtype_str == "bool":
            bison_dtype = bool_
        elif dtype_str.startswith("datetime64"):
            bison_dtype = datetime64_ns
        elif dtype_str.startswith("timedelta64"):
            bison_dtype = timedelta64_ns
        else:
            bison_dtype = object_

        if bison_dtype == int64:
            var data = List[Int64]()
            for i in range(n):
                if null_mask[i]:
                    data.append(Int64(0))  # placeholder for null
                else:
                    data.append(Int64(Int(py=py_list[i])))
            var col = Column(name, ColumnData(data^), bison_dtype, idx_list^)
            col._null_mask = null_mask.copy()
            return col^
        elif bison_dtype == float64:
            var data = List[Float64]()
            var struct_mod = Python.import_module("struct")
            var pack_fmt = "d"   # pack as IEEE-754 double-precision float
            var unpack_fmt = "q" # unpack as signed 64-bit integer (same bytes, avoids overflow in Int)
            for i in range(n):
                if null_mask[i]:
                    data.append(Float64(0))  # placeholder for null
                else:
                    var packed = struct_mod.unpack(unpack_fmt, struct_mod.pack(pack_fmt, py_list[i]))
                    var bits = Int64(Int(py=packed[0]))
                    data.append(bitcast[DType.float64](bits))
            var col = Column(name, ColumnData(data^), bison_dtype, idx_list^)
            col._null_mask = null_mask.copy()
            return col^
        elif bison_dtype == bool_:
            var data = List[Bool]()
            for i in range(n):
                if null_mask[i]:
                    data.append(False)  # placeholder for null
                else:
                    data.append(Bool(py_list[i].__bool__()))
            var col = Column(name, ColumnData(data^), bison_dtype, idx_list^)
            col._null_mask = null_mask.copy()
            return col^
        elif dtype_str == "string":
            var data = List[String]()
            for i in range(n):
                if null_mask[i]:
                    data.append(String(""))  # placeholder for null
                else:
                    data.append(String(py_list[i]))
            var col = Column(name, ColumnData(data^), object_, idx_list^)
            col._null_mask = null_mask.copy()
            return col^
        else:
            var data = List[PythonObject]()
            for i in range(n):
                data.append(py_list[i])
            var col = Column(name, ColumnData(data^), bison_dtype, idx_list^)
            col._null_mask = null_mask^
            return col^

    @staticmethod
    fn _sniff_dtype(data: ColumnData) -> BisonDtype:
        """Return the BisonDtype that matches the active ColumnData arm."""
        var visitor = _DtypeSniffVisitor()
        visit_col_data(visitor, data)
        return visitor.result

    fn to_pandas(self) raises -> PythonObject:
        """Reconstruct a pandas Series from stored values."""
        var pd = Python.import_module("pandas")
        var py_list = Python.evaluate("[]")
        var py_none = Python.evaluate("None")
        var has_mask = len(self._null_mask) > 0
        if self._data.isa[List[Int64]]():
            ref d = self._data[List[Int64]]
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    _ = py_list.append(py_none)
                else:
                    _ = py_list.append(d[i])
        elif self._data.isa[List[Float64]]():
            ref d = self._data[List[Float64]]
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    _ = py_list.append(py_none)
                else:
                    _ = py_list.append(d[i])
        elif self._data.isa[List[Bool]]():
            ref d = self._data[List[Bool]]
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    _ = py_list.append(py_none)
                else:
                    _ = py_list.append(d[i])
        elif self._data.isa[List[String]]():
            ref d = self._data[List[String]]
            for i in range(len(d)):
                if has_mask and self._null_mask[i]:
                    _ = py_list.append(py_none)
                else:
                    _ = py_list.append(d[i])
        else:
            ref d = self._data[List[PythonObject]]
            for i in range(len(d)):
                _ = py_list.append(d[i])
        if len(self._index) > 0:
            var idx_py = Python.evaluate("[]")
            for i in range(len(self._index)):
                _ = idx_py.append(self._index[i])
            return pd.Series(py_list, name=self.name, dtype=self.dtype.name, index=idx_py)
        return pd.Series(py_list, name=self.name, dtype=self.dtype.name)
