from std.python import Python, PythonObject
from std.utils import Variant
from std.memory import bitcast
from std.collections import Dict, Set
from math import sqrt, floor
from .dtypes import (
    BisonDtype,
    int8, int16, int32, int64,
    uint8, uint16, uint32, uint64,
    float32, float64,
    bool_, object_,
    datetime64_ns, timedelta64_ns,
    dtype_from_string,
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
#
# Implemented as a thin struct rather than a bare Variant alias so that
# Int (Mojo's native integer type) implicitly converts to Int64 at
# construction time.  All four typed arms plus Int are accepted; Int is
# normalised to Int64 immediately, so dispatch sites only ever see Int64.
struct DFScalar(Copyable, Movable, ImplicitlyCopyable):
    var _v: Variant[Int64, Float64, Bool, String]

    @implicit
    def __init__(out self, value: Int64):
        self._v = Variant[Int64, Float64, Bool, String](value)

    @implicit
    def __init__(out self, value: Float64):
        self._v = Variant[Int64, Float64, Bool, String](value)

    @implicit
    def __init__(out self, value: Bool):
        self._v = Variant[Int64, Float64, Bool, String](value)

    @implicit
    def __init__(out self, value: String):
        self._v = Variant[Int64, Float64, Bool, String](value)

    @implicit
    def __init__(out self, value: Int):
        self._v = Variant[Int64, Float64, Bool, String](Int64(value))

    def __init__(out self, *, copy: Self):
        self._v = copy._v

    def __init__(out self, *, deinit take: Self):
        self._v = take._v^

    def isa[T: Copyable & Movable](self) -> Bool:
        return self._v.isa[T]()

    def __getitem__[T: Copyable & Movable](ref self) -> ref [self._v] T:
        return self._v[T]

    def __getitem_param__[T: Copyable & Movable](ref self) -> ref [self._v] T:
        return self._v[T]

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
    ``visit_col_data``, which contains the **only** non-raises ``isa`` chain
    in the codebase; all callers should delegate here instead of writing their
    own discriminant checks.

    For visitors that need to call Python APIs or otherwise raise, implement
    ``ColumnDataVisitorRaises`` and use ``visit_col_data_raises`` instead.
    """

    def on_int64(mut self, data: List[Int64]): ...
    def on_float64(mut self, data: List[Float64]): ...
    def on_bool(mut self, data: List[Bool]): ...
    def on_str(mut self, data: List[String]): ...
    def on_obj(mut self, data: List[PythonObject]): ...


def visit_col_data[V: ColumnDataVisitor](mut visitor: V, data: ColumnData):
    """Dispatch *visitor* to the active ``ColumnData`` arm (non-raises).

    This is the **only** non-raises place in the codebase that reads the
    ``ColumnData`` discriminant via ``isa``.  Add new ``ColumnData`` arms here,
    in ``ColumnDataVisitor``, and in ``visit_col_data_raises`` — every other
    dispatch site is then updated automatically because it delegates here.
    For visitors that may raise, use ``visit_col_data_raises`` instead.
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
# Raises-capable visitor — for operations that perform Python interop
# or other potentially-failing work (e.g. to_pandas).
# ------------------------------------------------------------------

trait ColumnDataVisitorRaises:
    """Raises-capable counterpart to ``ColumnDataVisitor``.

    Use when ``on_*`` methods must call Python APIs or otherwise raise.
    Implement one ``on_*`` method per ``ColumnData`` arm and pass an
    instance to ``visit_col_data_raises``.
    """

    def on_int64(mut self, data: List[Int64]) raises: ...
    def on_float64(mut self, data: List[Float64]) raises: ...
    def on_bool(mut self, data: List[Bool]) raises: ...
    def on_str(mut self, data: List[String]) raises: ...
    def on_obj(mut self, data: List[PythonObject]) raises: ...


def visit_col_data_raises[V: ColumnDataVisitorRaises](mut visitor: V, data: ColumnData) raises:
    """Raises-capable dispatch for visitors that may raise (e.g. Python interop).

    Mirrors ``visit_col_data`` but each ``on_*`` call site is in a ``raises``
    context.  Add new ``ColumnData`` arms here, in ``ColumnDataVisitorRaises``,
    *and* in ``visit_col_data``.
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
    def __init__(out self): self.result = 0
    def on_int64(mut self, data: List[Int64]): self.result = len(data)
    def on_float64(mut self, data: List[Float64]): self.result = len(data)
    def on_bool(mut self, data: List[Bool]): self.result = len(data)
    def on_str(mut self, data: List[String]): self.result = len(data)
    def on_obj(mut self, data: List[PythonObject]): self.result = len(data)


struct _DtypeSniffVisitor(ColumnDataVisitor, Copyable, Movable):
    """Visitor that maps the active ColumnData arm to its BisonDtype."""
    var result: BisonDtype
    # object_ is the safe fallback: both List[String] and List[PythonObject]
    # map to object_.  The field is always overwritten by on_*.
    def __init__(out self): self.result = object_
    def on_int64(mut self, data: List[Int64]): self.result = int64
    def on_float64(mut self, data: List[Float64]): self.result = float64
    def on_bool(mut self, data: List[Bool]): self.result = bool_
    def on_str(mut self, data: List[String]): self.result = object_
    def on_obj(mut self, data: List[PythonObject]): self.result = object_


struct _CopyDataVisitor(ColumnDataVisitor, Copyable, Movable):
    """Visitor that produces an independent copy of the active ColumnData arm."""
    var result: ColumnData
    # Initialised with the fallback arm (List[PythonObject]) so that the field
    # is always valid.  on_* immediately replaces it with the copied data.
    def __init__(out self): self.result = ColumnData(List[PythonObject]())
    def on_int64(mut self, data: List[Int64]): self.result = ColumnData(data.copy())
    def on_float64(mut self, data: List[Float64]): self.result = ColumnData(data.copy())
    def on_bool(mut self, data: List[Bool]): self.result = ColumnData(data.copy())
    def on_str(mut self, data: List[String]): self.result = ColumnData(data.copy())
    def on_obj(mut self, data: List[PythonObject]): self.result = ColumnData(data.copy())


struct _ToPandasVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that appends each element of the active ColumnData arm to a
    Python list, respecting a parallel null mask.

    ``py_list`` must already be a Python list object; elements are appended
    in order.  Null entries (``null_mask[i] == True``) are appended as the
    provided ``py_none`` value.  The ``List[PythonObject]`` arm is assumed
    to carry its own ``None`` representations and is appended unconditionally.
    """
    var py_list: PythonObject
    var py_none: PythonObject
    var null_mask: List[Bool]

    def __init__(out self, py_list: PythonObject, py_none: PythonObject,
                null_mask: List[Bool]):
        self.py_list = py_list
        self.py_none = py_none
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                _ = self.py_list.append(self.py_none)
            else:
                _ = self.py_list.append(data[i])

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                _ = self.py_list.append(self.py_none)
            else:
                _ = self.py_list.append(data[i])

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                _ = self.py_list.append(self.py_none)
            else:
                _ = self.py_list.append(data[i])

    def on_str(mut self, data: List[String]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                _ = self.py_list.append(self.py_none)
            else:
                _ = self.py_list.append(data[i])

    def on_obj(mut self, data: List[PythonObject]) raises:
        for i in range(len(data)):
            _ = self.py_list.append(data[i])


# ------------------------------------------------------------------
# Aggregation visitors (issue #81)
# ------------------------------------------------------------------

struct _SumVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Accumulates element-wise sum into Float64, skipping masked nulls."""
    var result: Float64
    var null_mask: List[Bool]

    def __init__(out self, null_mask: List[Bool]):
        self.result = Float64(0)
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self.result += Float64(data[i])

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self.result += data[i]

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            if data[i]:
                self.result += 1.0

    def on_str(mut self, data: List[String]) raises:
        raise Error("sum: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("sum: non-numeric column type")


struct _MinVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Finds the minimum value as Float64, skipping masked nulls."""
    var result: Float64
    var found: Bool
    var null_mask: List[Bool]

    def __init__(out self, null_mask: List[Bool]):
        self.result = Float64(0)
        self.found = False
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var v = Float64(data[i])
            if not self.found or v < self.result:
                self.result = v
                self.found = True

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var v = data[i]
            if not self.found or v < self.result:
                self.result = v
                self.found = True

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var v = Float64(1.0) if data[i] else Float64(0.0)
            if not self.found or v < self.result:
                self.result = v
                self.found = True

    def on_str(mut self, data: List[String]) raises:
        raise Error("min: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("min: non-numeric column type")


struct _MaxVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Finds the maximum value as Float64, skipping masked nulls."""
    var result: Float64
    var found: Bool
    var null_mask: List[Bool]

    def __init__(out self, null_mask: List[Bool]):
        self.result = Float64(0)
        self.found = False
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var v = Float64(data[i])
            if not self.found or v > self.result:
                self.result = v
                self.found = True

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var v = data[i]
            if not self.found or v > self.result:
                self.result = v
                self.found = True

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var v = Float64(1.0) if data[i] else Float64(0.0)
            if not self.found or v > self.result:
                self.result = v
                self.found = True

    def on_str(mut self, data: List[String]) raises:
        raise Error("max: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("max: non-numeric column type")


struct _VarVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Accumulates squared deviations from the mean for variance computation."""
    var total: Float64
    var mean: Float64
    var null_mask: List[Bool]

    def __init__(out self, mean: Float64, null_mask: List[Bool]):
        self.total = Float64(0)
        self.mean = mean
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var diff = Float64(data[i]) - self.mean
            self.total += diff * diff

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var diff = data[i] - self.mean
            self.total += diff * diff

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var v = Float64(1.0) if data[i] else Float64(0.0)
            var diff = v - self.mean
            self.total += diff * diff

    def on_str(mut self, data: List[String]) raises:
        raise Error("var: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("var: non-numeric column type")


struct _NuniqueVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Counts unique non-null values across all supported column types."""
    var result: Int
    var null_mask: List[Bool]

    def __init__(out self, null_mask: List[Bool]):
        self.result = 0
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var seen = Set[Int64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            seen.add(data[i])
        self.result = len(seen)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var seen = Set[Float64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            seen.add(data[i])
        self.result = len(seen)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var seen = Set[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            seen.add(data[i])
        self.result = len(seen)

    def on_str(mut self, data: List[String]) raises:
        var has_mask = len(self.null_mask) > 0
        var seen = Set[String]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            seen.add(data[i])
        self.result = len(seen)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("nunique: unsupported column type")


struct _QuantileCollectVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Collects non-null numeric values into a Float64 list for quantile computation."""
    var vals: List[Float64]
    var null_mask: List[Bool]

    def __init__(out self, null_mask: List[Bool]):
        self.vals = List[Float64]()
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self.vals.append(Float64(data[i]))

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self.vals.append(data[i])

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self.vals.append(Float64(1.0) if data[i] else Float64(0.0))

    def on_str(mut self, data: List[String]) raises:
        raise Error("quantile: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("quantile: non-numeric column type")


# ------------------------------------------------------------------
# Transformation visitors (issue #127)
#
# Each visitor accumulates col_data + result_mask + has_any_null.
# The Column method calls self._build_result_col(...) on these fields,
# so no visitor references Column directly (avoiding a forward reference).
# When is_identity is True the method returns self.copy() instead.
# ------------------------------------------------------------------

struct _AbsVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Element-wise absolute value; Bool arm is identity."""
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var null_mask: List[Bool]
    var is_identity: Bool
    var dtype_name: String

    def __init__(out self, null_mask: List[Bool], dtype_name: String):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.null_mask = null_mask.copy()
        self.is_identity = False
        self.dtype_name = dtype_name

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var result = List[Int64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(data[i] if data[i] >= 0 else -data[i])
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var nan = Float64(0) / Float64(0)
        var result = List[Float64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(data[i] if data[i] >= 0.0 else -data[i])
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        self.is_identity = True

    def on_str(mut self, data: List[String]) raises:
        raise Error("abs: not supported for dtype " + self.dtype_name)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("abs: not supported for dtype " + self.dtype_name)


struct _RoundVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Rounds Float64 values; Int64 and Bool arms are identity."""
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var null_mask: List[Bool]
    var decimals: Int
    var is_identity: Bool
    var dtype_name: String

    def __init__(out self, null_mask: List[Bool], decimals: Int, dtype_name: String):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.null_mask = null_mask.copy()
        self.decimals = decimals
        self.is_identity = False
        self.dtype_name = dtype_name

    def on_int64(mut self, data: List[Int64]) raises:
        self.is_identity = True

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var nan = Float64(0) / Float64(0)
        var result = List[Float64]()
        var factor = Float64(1)
        for _ in range(self.decimals):
            factor *= 10.0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(round(data[i] * factor) / factor)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        self.is_identity = True

    def on_str(mut self, data: List[String]) raises:
        raise Error("round: not supported for dtype " + self.dtype_name)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("round: not supported for dtype " + self.dtype_name)


struct _ClipVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Clamps values to [lower, upper]; supports Int64 and Float64 only.

    Either bound may be ``None``, in which case no clipping is applied on
    that side.  This avoids the need for sentinel magic values such as ±1e308.
    """
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var null_mask: List[Bool]
    var lower: Optional[Float64]
    var upper: Optional[Float64]
    var dtype_name: String

    def __init__(out self, null_mask: List[Bool], lower: Optional[Float64],
                upper: Optional[Float64], dtype_name: String):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.null_mask = null_mask.copy()
        self.lower = lower
        self.upper = upper
        self.dtype_name = dtype_name

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var has_lo = self.lower.__bool__()
        var has_hi = self.upper.__bool__()
        var lo = Int64(0)
        var hi = Int64(0)
        if has_lo:
            lo = Int64(self.lower.value())
        if has_hi:
            hi = Int64(self.upper.value())
        var result = List[Int64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v = data[i]
                if has_lo and v < lo:
                    v = lo
                elif has_hi and v > hi:
                    v = hi
                result.append(v)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var has_lo = self.lower.__bool__()
        var has_hi = self.upper.__bool__()
        var lo = Float64(0)
        var hi = Float64(0)
        if has_lo:
            lo = self.lower.value()
        if has_hi:
            hi = self.upper.value()
        var nan = Float64(0) / Float64(0)
        var result = List[Float64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v = data[i]
                if has_lo and v < lo:
                    v = lo
                elif has_hi and v > hi:
                    v = hi
                result.append(v)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        raise Error("clip: not supported for dtype " + self.dtype_name)

    def on_str(mut self, data: List[String]) raises:
        raise Error("clip: not supported for dtype " + self.dtype_name)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("clip: not supported for dtype " + self.dtype_name)


struct _WhereMaskVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Applies where/mask logic: keep or null each element based on a Bool condition.

    keep_on_true=True  → where semantics: keep value when condition is True.
    keep_on_true=False → mask semantics: null value when condition is True.
    When ``other`` is provided, non-kept cells are filled with that scalar
    instead of null.
    """
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var self_null_mask: List[Bool]
    var cond_data: List[Bool]
    var cond_null_mask: List[Bool]
    var keep_on_true: Bool
    var other: Optional[DFScalar]

    def __init__(out self, self_null_mask: List[Bool], cond_data: List[Bool],
                cond_null_mask: List[Bool], keep_on_true: Bool,
                other: Optional[DFScalar] = None):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.self_null_mask = self_null_mask.copy()
        self.cond_data = cond_data.copy()
        self.cond_null_mask = cond_null_mask.copy()
        self.keep_on_true = keep_on_true
        self.other = other

    def on_int64(mut self, data: List[Int64]) raises:
        var has_self_mask = len(self.self_null_mask) > 0
        var has_cond_mask = len(self.cond_null_mask) > 0
        var has_other = self.other.__bool__()
        var other_val: Int64 = 0
        var other_is_null = True
        if has_other:
            var fv = self.other.value()
            if fv.isa[Int64]():
                other_val = fv[Int64]; other_is_null = False
            elif fv.isa[Float64]():
                other_val = Int64(Int(fv[Float64])); other_is_null = False
            elif fv.isa[Bool]():
                other_val = Int64(1) if fv[Bool] else Int64(0); other_is_null = False
        var result = List[Int64]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            var cond_true = (not has_cond_mask or not self.cond_null_mask[i]) and self.cond_data[i]
            var keep = cond_true if self.keep_on_true else not cond_true
            if keep:
                result.append(data[i])
                self.result_mask.append(self_null)
                if self_null:
                    self.has_any_null = True
            elif other_is_null:
                result.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(other_val)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_self_mask = len(self.self_null_mask) > 0
        var has_cond_mask = len(self.cond_null_mask) > 0
        var nan = Float64(0) / Float64(0)
        var has_other = self.other.__bool__()
        var other_val: Float64 = nan
        var other_is_null = True
        if has_other:
            var fv = self.other.value()
            if fv.isa[Float64]():
                other_val = fv[Float64]; other_is_null = False
            elif fv.isa[Int64]():
                other_val = Float64(fv[Int64]); other_is_null = False
            elif fv.isa[Bool]():
                other_val = 1.0 if fv[Bool] else 0.0; other_is_null = False
        var result = List[Float64]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            var cond_true = (not has_cond_mask or not self.cond_null_mask[i]) and self.cond_data[i]
            var keep = cond_true if self.keep_on_true else not cond_true
            if keep:
                result.append(data[i])
                self.result_mask.append(self_null)
                if self_null:
                    self.has_any_null = True
            elif other_is_null:
                result.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(other_val)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_self_mask = len(self.self_null_mask) > 0
        var has_cond_mask = len(self.cond_null_mask) > 0
        var has_other = self.other.__bool__()
        var other_val: Bool = False
        var other_is_null = True
        if has_other:
            var fv = self.other.value()
            if fv.isa[Bool]():
                other_val = fv[Bool]; other_is_null = False
            elif fv.isa[Int64]():
                other_val = fv[Int64] != 0; other_is_null = False
        var result = List[Bool]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            var cond_true = (not has_cond_mask or not self.cond_null_mask[i]) and self.cond_data[i]
            var keep = cond_true if self.keep_on_true else not cond_true
            if keep:
                result.append(data[i])
                self.result_mask.append(self_null)
                if self_null:
                    self.has_any_null = True
            elif other_is_null:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(other_val)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        var has_self_mask = len(self.self_null_mask) > 0
        var has_cond_mask = len(self.cond_null_mask) > 0
        var has_other = self.other.__bool__()
        var other_val: String = ""
        var other_is_null = True
        if has_other:
            var fv = self.other.value()
            if fv.isa[String]():
                other_val = fv[String]; other_is_null = False
        var result = List[String]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            var cond_true = (not has_cond_mask or not self.cond_null_mask[i]) and self.cond_data[i]
            var keep = cond_true if self.keep_on_true else not cond_true
            if keep:
                result.append(data[i])
                self.result_mask.append(self_null)
                if self_null:
                    self.has_any_null = True
            elif other_is_null:
                result.append(String(""))
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(other_val)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("where/mask: not supported for object dtype")


struct _CombineFirstVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Row-wise null-coalesce: keeps self's value where non-null, takes other's otherwise.

    Used by ``_combine_first_col`` (backing ``DataFrame.combine_first`` and
    ``DataFrame.update``).  For each row i: if self is non-null keep self;
    else take other (which may itself be null).
    Raises if the two ColumnData arms have different types.
    """
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var self_null_mask: List[Bool]
    var other_data: ColumnData
    var other_null_mask: List[Bool]

    def __init__(out self, self_null_mask: List[Bool], other_data: ColumnData,
                other_null_mask: List[Bool]):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.self_null_mask = self_null_mask.copy()
        var v = _CopyDataVisitor()
        visit_col_data(v, other_data)
        self.other_data = v^.result
        self.other_null_mask = other_null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        if not self.other_data.isa[List[Int64]]():
            raise Error("combine_first: dtype mismatch between columns")
        ref od = self.other_data[List[Int64]]
        var has_self_mask = len(self.self_null_mask) > 0
        var has_other_mask = len(self.other_null_mask) > 0
        var result = List[Int64]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            if not self_null:
                result.append(data[i])
                self.result_mask.append(False)
            else:
                var other_null = has_other_mask and self.other_null_mask[i]
                if other_null:
                    result.append(Int64(0))
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(od[i])
                    self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        if not self.other_data.isa[List[Float64]]():
            raise Error("combine_first: dtype mismatch between columns")
        ref od = self.other_data[List[Float64]]
        var has_self_mask = len(self.self_null_mask) > 0
        var has_other_mask = len(self.other_null_mask) > 0
        var nan = Float64(0) / Float64(0)
        var result = List[Float64]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            if not self_null:
                result.append(data[i])
                self.result_mask.append(False)
            else:
                var other_null = has_other_mask and self.other_null_mask[i]
                if other_null:
                    result.append(nan)
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(od[i])
                    self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        if not self.other_data.isa[List[Bool]]():
            raise Error("combine_first: dtype mismatch between columns")
        ref od = self.other_data[List[Bool]]
        var has_self_mask = len(self.self_null_mask) > 0
        var has_other_mask = len(self.other_null_mask) > 0
        var result = List[Bool]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            if not self_null:
                result.append(data[i])
                self.result_mask.append(False)
            else:
                var other_null = has_other_mask and self.other_null_mask[i]
                if other_null:
                    result.append(False)
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(od[i])
                    self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        if not self.other_data.isa[List[String]]():
            raise Error("combine_first: dtype mismatch between columns")
        ref od = self.other_data[List[String]]
        var has_self_mask = len(self.self_null_mask) > 0
        var has_other_mask = len(self.other_null_mask) > 0
        var result = List[String]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            if not self_null:
                result.append(data[i])
                self.result_mask.append(False)
            else:
                var other_null = has_other_mask and self.other_null_mask[i]
                if other_null:
                    result.append(String(""))
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(od[i])
                    self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("combine_first: not supported for object dtype")


struct _IsInVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Bool Column: True where each element appears in a list of DFScalar values.

    Dispatches on the active ``ColumnData`` arm and performs scalar-type
    coercion internally, so no ``isa`` chain is needed outside the visitor
    framework.  Used by ``Column._isin_scalars``.
    """
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var null_mask: List[Bool]
    var scalars: List[DFScalar]

    def __init__(out self, null_mask: List[Bool], scalars: List[DFScalar]):
        # Initialised with the fallback arm (List[PythonObject]) so that the
        # field is always valid.  on_* immediately replaces it with the Bool result.
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.null_mask = null_mask.copy()
        self.scalars = scalars.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var typed_vals = List[Int64]()
        for k in range(len(self.scalars)):
            if self.scalars[k].isa[Int64]():
                typed_vals.append(self.scalars[k][Int64])
            elif self.scalars[k].isa[Float64]():
                typed_vals.append(Int64(Int(self.scalars[k][Float64])))
            elif self.scalars[k].isa[Bool]():
                typed_vals.append(Int64(1) if self.scalars[k][Bool] else Int64(0))
        var result = List[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var found = False
                for j in range(len(typed_vals)):
                    if data[i] == typed_vals[j]:
                        found = True
                        break
                result.append(found)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var typed_vals = List[Float64]()
        for k in range(len(self.scalars)):
            if self.scalars[k].isa[Float64]():
                typed_vals.append(self.scalars[k][Float64])
            elif self.scalars[k].isa[Int64]():
                typed_vals.append(Float64(self.scalars[k][Int64]))
            elif self.scalars[k].isa[Bool]():
                typed_vals.append(Float64(1.0) if self.scalars[k][Bool] else Float64(0.0))
        var result = List[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var found = False
                for j in range(len(typed_vals)):
                    if data[i] == typed_vals[j]:
                        found = True
                        break
                result.append(found)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var typed_vals = List[Bool]()
        for k in range(len(self.scalars)):
            if self.scalars[k].isa[Bool]():
                typed_vals.append(self.scalars[k][Bool])
        var result = List[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var found = False
                for j in range(len(typed_vals)):
                    if data[i] == typed_vals[j]:
                        found = True
                        break
                result.append(found)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        var has_mask = len(self.null_mask) > 0
        var typed_vals = List[String]()
        for k in range(len(self.scalars)):
            if self.scalars[k].isa[String]():
                typed_vals.append(self.scalars[k][String])
        var result = List[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var found = False
                for j in range(len(typed_vals)):
                    if data[i] == typed_vals[j]:
                        found = True
                        break
                result.append(found)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("isin: not supported for object dtype")


struct _UniqueVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Returns unique values in first-occurrence order; nulls appended once at end."""
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var null_mask: List[Bool]

    def __init__(out self, null_mask: List[Bool]):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var seen_set = Set[Int64]()
        var result = List[Int64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                self.has_any_null = True
                continue
            var v = data[i]
            if v not in seen_set:
                seen_set.add(v)
                result.append(v)
                self.result_mask.append(False)
        if self.has_any_null:
            result.append(Int64(0))
            self.result_mask.append(True)
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var nan = Float64(0) / Float64(0)
        var seen_set = Set[Float64]()
        var result = List[Float64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                self.has_any_null = True
                continue
            var v = data[i]
            if v not in seen_set:
                seen_set.add(v)
                result.append(v)
                self.result_mask.append(False)
        if self.has_any_null:
            result.append(nan)
            self.result_mask.append(True)
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var seen_false = False
        var seen_true = False
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                self.has_any_null = True
            elif data[i]:
                seen_true = True
            else:
                seen_false = True
        var result = List[Bool]()
        if seen_false:
            result.append(False)
            self.result_mask.append(False)
        if seen_true:
            result.append(True)
            self.result_mask.append(False)
        if self.has_any_null:
            result.append(False)
            self.result_mask.append(True)
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        var has_mask = len(self.null_mask) > 0
        var seen_set = Set[String]()
        var result = List[String]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                self.has_any_null = True
                continue
            var v = data[i]
            if v not in seen_set:
                seen_set.add(v)
                result.append(v)
                self.result_mask.append(False)
        if self.has_any_null:
            result.append(String(""))
            self.result_mask.append(True)
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("unique: not supported for object dtype")


struct _AstypeVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Converts the active ColumnData arm to a target dtype."""
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var null_mask: List[Bool]
    var is_identity: Bool
    var to_int: Bool
    var to_float: Bool
    var to_bool: Bool
    var target_dtype_name: String
    var source_dtype_name: String

    def __init__(out self, null_mask: List[Bool], to_int: Bool, to_float: Bool,
                to_bool: Bool, target_dtype_name: String, source_dtype_name: String):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.null_mask = null_mask.copy()
        self.is_identity = False
        self.to_int = to_int
        self.to_float = to_float
        self.to_bool = to_bool
        self.target_dtype_name = target_dtype_name
        self.source_dtype_name = source_dtype_name

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        if self.to_float:
            var nan = Float64(0) / Float64(0)
            var result = List[Float64]()
            for i in range(len(data)):
                if has_mask and self.null_mask[i]:
                    result.append(nan)
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(Float64(data[i]))
                    self.result_mask.append(False)
            self.col_data = ColumnData(result^)
        elif self.to_bool:
            var result = List[Bool]()
            for i in range(len(data)):
                if has_mask and self.null_mask[i]:
                    result.append(False)
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(data[i] != 0)
                    self.result_mask.append(False)
            self.col_data = ColumnData(result^)
        elif self.to_int:
            self.is_identity = True
        else:
            raise Error("astype: unsupported target dtype '" + self.target_dtype_name + "' for Int64 source")

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        if self.to_int:
            var result = List[Int64]()
            for i in range(len(data)):
                if has_mask and self.null_mask[i]:
                    result.append(Int64(0))
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(Int64(data[i]))
                    self.result_mask.append(False)
            self.col_data = ColumnData(result^)
        elif self.to_bool:
            var result = List[Bool]()
            for i in range(len(data)):
                if has_mask and self.null_mask[i]:
                    result.append(False)
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(data[i] != 0.0)
                    self.result_mask.append(False)
            self.col_data = ColumnData(result^)
        elif self.to_float:
            self.is_identity = True
        else:
            raise Error("astype: unsupported target dtype '" + self.target_dtype_name + "' for Float64 source")

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        if self.to_int:
            var result = List[Int64]()
            for i in range(len(data)):
                if has_mask and self.null_mask[i]:
                    result.append(Int64(0))
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(Int64(1) if data[i] else Int64(0))
                    self.result_mask.append(False)
            self.col_data = ColumnData(result^)
        elif self.to_float:
            var nan = Float64(0) / Float64(0)
            var result = List[Float64]()
            for i in range(len(data)):
                if has_mask and self.null_mask[i]:
                    result.append(nan)
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    result.append(1.0 if data[i] else 0.0)
                    self.result_mask.append(False)
            self.col_data = ColumnData(result^)
        elif self.to_bool:
            self.is_identity = True
        else:
            raise Error("astype: unsupported target dtype '" + self.target_dtype_name + "' for Bool source")

    def on_str(mut self, data: List[String]) raises:
        raise Error("astype: not supported for source dtype '" + self.source_dtype_name + "'")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("astype: not supported for source dtype '" + self.source_dtype_name + "'")


# ------------------------------------------------------------------
# Row-selection visitors (issue #161)
# ------------------------------------------------------------------

struct _SliceVisitor(ColumnDataVisitor, Copyable, Movable):
    """Extracts the subarray [start, end) from the active ColumnData arm."""
    var start: Int
    var end: Int
    var result: ColumnData

    def __init__(out self, start: Int, end: Int):
        self.start = start
        self.end = end
        self.result = ColumnData(List[PythonObject]())

    def on_int64(mut self, data: List[Int64]):
        var result = List[Int64]()
        for i in range(self.start, self.end):
            result.append(data[i])
        self.result = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]):
        var result = List[Float64]()
        for i in range(self.start, self.end):
            result.append(data[i])
        self.result = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]):
        var result = List[Bool]()
        for i in range(self.start, self.end):
            result.append(data[i])
        self.result = ColumnData(result^)

    def on_str(mut self, data: List[String]):
        var result = List[String]()
        for i in range(self.start, self.end):
            result.append(data[i])
        self.result = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]):
        var result = List[PythonObject]()
        for i in range(self.start, self.end):
            result.append(data[i])
        self.result = ColumnData(result^)


struct _TakeVisitor(ColumnDataVisitor, Copyable, Movable):
    """Selects rows by arbitrary *indices* from the active ColumnData arm."""
    var indices: List[Int]
    var result: ColumnData

    def __init__(out self, indices: List[Int]):
        self.indices = indices.copy()
        self.result = ColumnData(List[PythonObject]())

    def on_int64(mut self, data: List[Int64]):
        var result = List[Int64]()
        for k in range(len(self.indices)):
            result.append(data[self.indices[k]])
        self.result = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]):
        var result = List[Float64]()
        for k in range(len(self.indices)):
            result.append(data[self.indices[k]])
        self.result = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]):
        var result = List[Bool]()
        for k in range(len(self.indices)):
            result.append(data[self.indices[k]])
        self.result = ColumnData(result^)

    def on_str(mut self, data: List[String]):
        var result = List[String]()
        for k in range(len(self.indices)):
            result.append(data[self.indices[k]])
        self.result = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]):
        var result = List[PythonObject]()
        for k in range(len(self.indices)):
            result.append(data[self.indices[k]])
        self.result = ColumnData(result^)


struct _ValueCountsCountVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Phase 1 of value_counts: counts unique values via stringified keys.

    Builds ``unique_keys`` (insertion-order list of string representations)
    and ``counts_dict`` (key → count).  Raises for String/PythonObject arms.
    """
    var has_mask: Bool
    var null_mask: List[Bool]
    var unique_keys: List[String]
    var counts_dict: Dict[String, Int]

    def __init__(out self, has_mask: Bool, null_mask: List[Bool]):
        self.has_mask = has_mask
        self.null_mask = null_mask.copy()
        self.unique_keys = List[String]()
        self.counts_dict = Dict[String, Int]()

    def on_int64(mut self, data: List[Int64]) raises:
        for i in range(len(data)):
            if self.has_mask and self.null_mask[i]:
                continue
            var k = String(data[i])
            if k not in self.counts_dict:
                self.unique_keys.append(k)
            self.counts_dict[k] = self.counts_dict.get(k, 0) + 1

    def on_float64(mut self, data: List[Float64]) raises:
        for i in range(len(data)):
            if self.has_mask and self.null_mask[i]:
                continue
            var k = String(data[i])
            if k not in self.counts_dict:
                self.unique_keys.append(k)
            self.counts_dict[k] = self.counts_dict.get(k, 0) + 1

    def on_bool(mut self, data: List[Bool]) raises:
        for i in range(len(data)):
            if self.has_mask and self.null_mask[i]:
                continue
            var k = String(data[i])
            if k not in self.counts_dict:
                self.unique_keys.append(k)
            self.counts_dict[k] = self.counts_dict.get(k, 0) + 1

    def on_str(mut self, data: List[String]) raises:
        raise Error("value_counts: unsupported column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("value_counts: unsupported column type")


struct _ValueCountsIndexVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Phase 2 of value_counts: builds the typed Python index.

    Given ``sorted_order``, ``unique_keys``, and ``count_vals`` from phase 1,
    converts string keys back to typed ``PythonObject`` values and accumulates
    ``result_counts``.  Raises for String/PythonObject arms.
    """
    var sorted_order: List[Int]
    var unique_keys: List[String]
    var count_vals: List[Int]
    var result_idx: List[PythonObject]
    var result_counts: List[Int64]

    def __init__(out self, sorted_order: List[Int], unique_keys: List[String],
                count_vals: List[Int]):
        self.sorted_order = sorted_order.copy()
        self.unique_keys = unique_keys.copy()
        self.count_vals = count_vals.copy()
        self.result_idx = List[PythonObject]()
        self.result_counts = List[Int64]()

    def on_int64(mut self, data: List[Int64]) raises:
        var builtins = Python.import_module("builtins")
        for i in range(len(self.sorted_order)):
            var si = self.sorted_order[i]
            self.result_counts.append(Int64(self.count_vals[si]))
            self.result_idx.append(builtins.int(self.unique_keys[si]))

    def on_float64(mut self, data: List[Float64]) raises:
        var builtins = Python.import_module("builtins")
        for i in range(len(self.sorted_order)):
            var si = self.sorted_order[i]
            self.result_counts.append(Int64(self.count_vals[si]))
            self.result_idx.append(builtins.float(self.unique_keys[si]))

    def on_bool(mut self, data: List[Bool]) raises:
        for i in range(len(self.sorted_order)):
            var si = self.sorted_order[i]
            self.result_counts.append(Int64(self.count_vals[si]))
            self.result_idx.append(PythonObject(self.unique_keys[si] == "True"))

    def on_str(mut self, data: List[String]) raises:
        raise Error("value_counts: unsupported column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("value_counts: unsupported column type")


struct _ToFloat64Visitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Converts a numeric ColumnData arm to a List[Float64].

    Raises for String and PythonObject arms (non-numeric).
    """
    var result: List[Float64]

    def __init__(out self):
        self.result = List[Float64]()

    def on_int64(mut self, data: List[Int64]) raises:
        for i in range(len(data)):
            self.result.append(Float64(data[i]))

    def on_float64(mut self, data: List[Float64]) raises:
        for i in range(len(data)):
            self.result.append(data[i])

    def on_bool(mut self, data: List[Bool]) raises:
        for i in range(len(data)):
            self.result.append(1.0 if data[i] else 0.0)

    def on_str(mut self, data: List[String]) raises:
        raise Error("arith: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("arith: non-numeric column type")


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
comptime FloatTransformFn = def(Float64) -> Float64


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

    def __init__(out self, var a: List[Float64], var b: List[Float64],
                has_a_mask: Bool, has_b_mask: Bool):
        self.a = a^
        self.b = b^
        self.has_a_mask = has_a_mask
        self.has_b_mask = has_b_mask


struct _ReindexRowsVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Select/insert rows by a pre-built index list.

    ``indices[i] >= 0``  → take that row from the source data.
    ``indices[i] == -1`` → insert a null row, or a fill row if *fill_value* is set.

    The parallel ``src_null_mask`` carries the source column's null mask so that
    nulls from existing rows are propagated correctly.
    """

    var indices: List[Int]
    var fill_value: Optional[DFScalar]
    var src_null_mask: List[Bool]
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, indices: List[Int], fill_value: Optional[DFScalar],
                src_null_mask: List[Bool]):
        self.indices = indices.copy()
        self.fill_value = fill_value
        self.src_null_mask = src_null_mask.copy()
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False

    def on_int64(mut self, data: List[Int64]) raises:
        var fill: Int64 = 0
        var is_null_fill: Bool = True
        if self.fill_value:
            var fv = self.fill_value.value()
            if fv.isa[Int64]():
                fill = fv[Int64]; is_null_fill = False
            elif fv.isa[Float64]():
                fill = Int64(Int(fv[Float64])); is_null_fill = False
            elif fv.isa[Bool]():
                fill = Int64(1) if fv[Bool] else Int64(0); is_null_fill = False
        var has_src_mask = len(self.src_null_mask) > 0
        var result = List[Int64]()
        for i in range(len(self.indices)):
            var idx = self.indices[i]
            if idx >= 0:
                result.append(data[idx])
                var is_null = has_src_mask and self.src_null_mask[idx]
                self.result_mask.append(is_null)
                if is_null:
                    self.has_any_null = True
            else:
                result.append(fill)
                self.result_mask.append(is_null_fill)
                if is_null_fill:
                    self.has_any_null = True
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var fill: Float64 = Float64(0) / Float64(0)  # NaN
        var is_null_fill: Bool = True
        if self.fill_value:
            var fv = self.fill_value.value()
            if fv.isa[Float64]():
                fill = fv[Float64]; is_null_fill = False
            elif fv.isa[Int64]():
                fill = Float64(fv[Int64]); is_null_fill = False
            elif fv.isa[Bool]():
                fill = 1.0 if fv[Bool] else 0.0; is_null_fill = False
        var has_src_mask = len(self.src_null_mask) > 0
        var result = List[Float64]()
        for i in range(len(self.indices)):
            var idx = self.indices[i]
            if idx >= 0:
                result.append(data[idx])
                var is_null = has_src_mask and self.src_null_mask[idx]
                self.result_mask.append(is_null)
                if is_null:
                    self.has_any_null = True
            else:
                result.append(fill)
                self.result_mask.append(is_null_fill)
                if is_null_fill:
                    self.has_any_null = True
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var fill: Bool = False
        var is_null_fill: Bool = True
        if self.fill_value:
            var fv = self.fill_value.value()
            if fv.isa[Bool]():
                fill = fv[Bool]; is_null_fill = False
            elif fv.isa[Int64]():
                fill = fv[Int64] != 0; is_null_fill = False
        var has_src_mask = len(self.src_null_mask) > 0
        var result = List[Bool]()
        for i in range(len(self.indices)):
            var idx = self.indices[i]
            if idx >= 0:
                result.append(data[idx])
                var is_null = has_src_mask and self.src_null_mask[idx]
                self.result_mask.append(is_null)
                if is_null:
                    self.has_any_null = True
            else:
                result.append(fill)
                self.result_mask.append(is_null_fill)
                if is_null_fill:
                    self.has_any_null = True
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        var fill: String = ""
        var is_null_fill: Bool = True
        if self.fill_value:
            var fv = self.fill_value.value()
            if fv.isa[String]():
                fill = fv[String]; is_null_fill = False
        var has_src_mask = len(self.src_null_mask) > 0
        var result = List[String]()
        for i in range(len(self.indices)):
            var idx = self.indices[i]
            if idx >= 0:
                result.append(data[idx])
                var is_null = has_src_mask and self.src_null_mask[idx]
                self.result_mask.append(is_null)
                if is_null:
                    self.has_any_null = True
            else:
                result.append(fill)
                self.result_mask.append(is_null_fill)
                if is_null_fill:
                    self.has_any_null = True
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        var py_none = Python.evaluate("None")
        var fill: PythonObject = py_none
        var is_null_fill: Bool = True
        if self.fill_value:
            var fv = self.fill_value.value()
            if fv.isa[String]():
                fill = PythonObject(fv[String]); is_null_fill = False
            elif fv.isa[Int64]():
                fill = PythonObject(Int(fv[Int64])); is_null_fill = False
            elif fv.isa[Float64]():
                fill = PythonObject(fv[Float64]); is_null_fill = False
            elif fv.isa[Bool]():
                fill = PythonObject(fv[Bool]); is_null_fill = False
        var has_src_mask = len(self.src_null_mask) > 0
        var result = List[PythonObject]()
        for i in range(len(self.indices)):
            var idx = self.indices[i]
            if idx >= 0:
                result.append(data[idx])
                var is_null = has_src_mask and self.src_null_mask[idx]
                self.result_mask.append(is_null)
                if is_null:
                    self.has_any_null = True
            else:
                result.append(fill)
                self.result_mask.append(is_null_fill)
                if is_null_fill:
                    self.has_any_null = True
        self.col_data = ColumnData(result^)


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

    def __init__(out self):
        """Empty column with object dtype — used as stub placeholder."""
        self.name  = ""
        self.dtype = object_
        self._data = ColumnData(List[PythonObject]())
        self._index = List[PythonObject]()
        self._null_mask = List[Bool]()

    def __init__(out self, name: String, var data: ColumnData, dtype: BisonDtype):
        self.name  = name
        self.dtype = dtype
        self._data = data^
        self._index = List[PythonObject]()
        self._null_mask = List[Bool]()

    def __init__(out self, name: String, var data: ColumnData, dtype: BisonDtype, var index: List[PythonObject]):
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

    def __init__(out self, *, copy: Self):
        self.name  = copy.name
        self.dtype = copy.dtype
        self._data = copy._data
        # PythonObject is not ImplicitlyCopyable — explicit .copy() required.
        self._index = copy._index.copy()
        self._null_mask = copy._null_mask.copy()

    def __init__(out self, *, deinit take: Self):
        self.name  = take.name^
        self.dtype = take.dtype^
        self._data = take._data^
        self._index = take._index^
        self._null_mask = take._null_mask^

    # ------------------------------------------------------------------
    # Typed accessor helpers — unsafe direct Variant subscripts; callers
    # are responsible for checking the active arm before calling these.
    # ------------------------------------------------------------------

    def _int64_data(ref self) -> ref [self._data] List[Int64]:
        return self._data[List[Int64]]

    def _float64_data(ref self) -> ref [self._data] List[Float64]:
        return self._data[List[Float64]]

    def _bool_data(ref self) -> ref [self._data] List[Bool]:
        return self._data[List[Bool]]

    def _str_data(ref self) -> ref [self._data] List[String]:
        return self._data[List[String]]

    def _obj_data(ref self) -> ref [self._data] List[PythonObject]:
        return self._data[List[PythonObject]]

    # ------------------------------------------------------------------
    # Explicit copy helper (used by Series / DataFrame __copyinit__)
    # ------------------------------------------------------------------

    def copy(self) -> Column:
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

    def __len__(self) -> Int:
        var visitor = _LenVisitor()
        visit_col_data(visitor, self._data)
        return visitor.result

    # ------------------------------------------------------------------
    # Row selection helpers
    # ------------------------------------------------------------------

    def slice(self, start: Int, end: Int) -> Column:
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
        var new_mask = List[Bool]()
        if len(self._null_mask) > 0:
            for i in range(s, e):
                new_mask.append(self._null_mask[i])
        var visitor = _SliceVisitor(s, e)
        visit_col_data(visitor, self._data)
        var col = Column(self.name, visitor^.result, self.dtype)
        if len(new_mask) > 0:
            col._null_mask = new_mask^
        return col^

    def take(self, indices: List[Int]) -> Column:
        """Return a new Column with rows selected by *indices* (arbitrary order)."""
        var has_mask = len(self._null_mask) > 0
        var new_mask = List[Bool]()
        if has_mask:
            for k in range(len(indices)):
                new_mask.append(self._null_mask[indices[k]])
        var visitor = _TakeVisitor(indices)
        visit_col_data(visitor, self._data)
        var col = Column(self.name, visitor^.result, self.dtype)
        if len(new_mask) > 0:
            col._null_mask = new_mask^
        return col^

    # ------------------------------------------------------------------
    # Null tracking
    # ------------------------------------------------------------------

    def has_nulls(self) -> Bool:
        """Return True if any element is marked null/NaN."""
        for i in range(len(self._null_mask)):
            if self._null_mask[i]:
                return True
        return False

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def sum(self, skipna: Bool = True) raises -> Float64:
        """Return the sum of all values as Float64.

        When ``skipna=True`` (default) null/NaN elements are skipped.
        When ``skipna=False`` the result is NaN if any null is present.
        Raises for non-numeric column types.
        """
        if not skipna and self.has_nulls():
            # Return NaN (IEEE 754: 0/0 → quiet NaN).
            var zero = Float64(0)
            return zero / zero
        var visitor = _SumVisitor(self._null_mask)
        visit_col_data_raises(visitor, self._data)
        return visitor.result

    def count(self) -> Int:
        """Return the number of non-null elements."""
        var n = len(self)
        if len(self._null_mask) == 0:
            return n
        var result = 0
        for i in range(n):
            if not self._null_mask[i]:
                result += 1
        return result

    def mean(self, skipna: Bool = True) raises -> Float64:
        """Return the mean of all values as Float64.

        Returns NaN when all elements are null or the column is empty.
        Raises for non-numeric column types.
        """
        var n = self.count() if skipna else len(self)
        if n == 0:
            var zero = Float64(0)
            return zero / zero
        return self.sum(skipna) / Float64(n)

    def min(self, skipna: Bool = True) raises -> Float64:
        """Return the minimum value as Float64.

        Returns NaN when no non-null elements exist.
        Raises for non-numeric column types.
        """
        if not skipna and self.has_nulls():
            var zero = Float64(0)
            return zero / zero
        var visitor = _MinVisitor(self._null_mask)
        visit_col_data_raises(visitor, self._data)
        if not visitor.found:
            var zero = Float64(0)
            return zero / zero
        return visitor.result

    def max(self, skipna: Bool = True) raises -> Float64:
        """Return the maximum value as Float64.

        Returns NaN when no non-null elements exist.
        Raises for non-numeric column types.
        """
        if not skipna and self.has_nulls():
            var zero = Float64(0)
            return zero / zero
        var visitor = _MaxVisitor(self._null_mask)
        visit_col_data_raises(visitor, self._data)
        if not visitor.found:
            var zero = Float64(0)
            return zero / zero
        return visitor.result

    def var(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        """Return the variance with Bessel correction (ddof=1 by default).

        Returns NaN when n - ddof <= 0.
        Raises for non-numeric column types.
        """
        var n = self.count() if skipna else len(self)
        if n - ddof <= 0:
            var zero = Float64(0)
            return zero / zero
        var m = self.mean(skipna)
        var visitor = _VarVisitor(m, self._null_mask)
        visit_col_data_raises(visitor, self._data)
        return visitor.total / Float64(n - ddof)

    def std(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        """Return the standard deviation (square root of variance)."""
        return sqrt(self.var(ddof, skipna))

    def nunique(self) raises -> Int:
        """Return the number of unique non-null values.

        Raises for non-numeric and non-string column types.
        """
        var visitor = _NuniqueVisitor(self._null_mask)
        visit_col_data_raises(visitor, self._data)
        return visitor.result

    def quantile(self, q: Float64 = 0.5) raises -> Float64:
        """Return the q-th quantile using linear interpolation.

        Always skips null elements (matches pandas default behaviour).
        Raises for non-numeric column types.
        """
        var visitor = _QuantileCollectVisitor(self._null_mask)
        visit_col_data_raises(visitor, self._data)
        var vals = visitor.vals.copy()
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

    def median(self, skipna: Bool = True) raises -> Float64:
        """Return the median value.

        When skipna=False and nulls are present, returns NaN.
        """
        if not skipna and self.has_nulls():
            var zero = Float64(0)
            return zero / zero
        return self.quantile(0.5)

    def describe(self) raises -> Column:
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

    def value_counts(self, normalize: Bool = False, sort: Bool = True) raises -> Column:
        """Return a Column of counts (or proportions) per unique value.

        The index holds the original values; the data holds the counts.
        sort=True (default) orders results by count descending.
        Raises for object/datetime column types.
        """
        var has_mask = len(self._null_mask) > 0

        # Phase 1: count unique values (raises for unsupported arms).
        var count_visitor = _ValueCountsCountVisitor(has_mask, self._null_mask)
        visit_col_data_raises(count_visitor, self._data)
        var n = len(count_visitor.unique_keys)

        # Materialise per-key counts in insertion order.
        var count_vals = List[Int]()
        for i in range(n):
            count_vals.append(count_visitor.counts_dict[count_visitor.unique_keys[i]])

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

        # Phase 2: build typed index (raises for unsupported arms).
        var index_visitor = _ValueCountsIndexVisitor(
            sorted_order, count_visitor.unique_keys, count_vals
        )
        visit_col_data_raises(index_visitor, self._data)
        var result_counts = index_visitor.result_counts.copy()
        var result_idx = index_visitor.result_idx.copy()

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

    def _to_float64_list(self) raises -> List[Float64]:
        """Convert the active numeric arm to a List[Float64].

        Raises for non-numeric (String, PythonObject) column types.
        """
        var visitor = _ToFloat64Visitor()
        visit_col_data_raises(visitor, self._data)
        return visitor.result.copy()

    def _build_result_col(self, var col_data: ColumnData, var result_mask: List[Bool], has_any_null: Bool) -> Column:
        """Wrap a computed ColumnData into a Column, attaching mask only if needed."""
        var dtype = Column._sniff_dtype(col_data)
        var col = Column(self.name, col_data^, dtype)
        if has_any_null:
            col._null_mask = result_mask^
        return col^

    def _binary_op_prepare(self, op_name: String, other: Column) raises -> _BinOpInputs:
        """Check lengths and build the shared Float64 input arrays and null-mask flags.

        Raises if ``self`` and ``other`` differ in length.  Called at the top
        of ``_arith_op`` and ``_cmp_op`` to eliminate repeated preamble code.
        """
        if len(self) != len(other):
            raise Error(op_name + ": length mismatch (" + String(len(self)) + " vs " + String(len(other)) + ")")
        var a = self._to_float64_list()
        var b = other._to_float64_list()
        return _BinOpInputs(a^, b^, len(self._null_mask) > 0, len(other._null_mask) > 0)

    def _arith_op[op: Int](self, op_name: String, other: Column) raises -> Column:
        """Core element-wise binary arithmetic kernel.

        ``op`` is a compile-time constant (``_ARITH_*``) that selects the
        operation; ``@parameter`` folds the branch at compile time so each
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

    def _arith_add(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_ADD]("add", other)

    def _arith_sub(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_SUB]("sub", other)

    def _arith_mul(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_MUL]("mul", other)

    def _arith_div(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_DIV]("div", other)

    def _arith_floordiv(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_FLOORDIV]("floordiv", other)

    def _arith_mod(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_MOD]("mod", other)

    def _arith_pow(self, other: Column) raises -> Column:
        return self._arith_op[_ARITH_POW]("pow", other)

    # ------------------------------------------------------------------
    # Comparison operations
    # ------------------------------------------------------------------

    def _cmp_op[op: Int](self, op_name: String, other: Column) raises -> Column:
        """Core element-wise binary comparison kernel.

        ``op`` is a compile-time constant (``_CMP_*``) that selects the
        operation; ``@parameter`` folds the branch at compile time so each
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

    def _cmp_eq(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_EQ]("eq", other)

    def _cmp_ne(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_NE]("ne", other)

    def _cmp_lt(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_LT]("lt", other)

    def _cmp_le(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_LE]("le", other)

    def _cmp_gt(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_GT]("gt", other)

    def _cmp_ge(self, other: Column) raises -> Column:
        return self._cmp_op[_CMP_GE]("ge", other)

    # ------------------------------------------------------------------
    # Transformation kernels
    # ------------------------------------------------------------------

    def _abs(self) raises -> Column:
        """Return element-wise absolute value.

        Int64 and Float64 arms are supported; Bool is identity.
        Nulls propagate. Raises for String/Object columns.
        """
        var visitor = _AbsVisitor(self._null_mask, self.dtype.name)
        visit_col_data_raises(visitor, self._data)
        if visitor.is_identity:
            return self.copy()
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    def _round(self, decimals: Int = 0) raises -> Column:
        """Round Float64 values to ``decimals`` decimal places.

        Int64 and Bool columns are returned unchanged. Raises for
        String/Object columns or negative ``decimals``.
        Uses banker's rounding (round-half-to-even), matching Python and
        numpy behaviour at exact half-way points.
        """
        if decimals < 0:
            raise Error("round: negative decimals not supported")
        var visitor = _RoundVisitor(self._null_mask, decimals, self.dtype.name)
        visit_col_data_raises(visitor, self._data)
        if visitor.is_identity:
            return self.copy()
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    def _clip(self, lower: Optional[Float64], upper: Optional[Float64]) raises -> Column:
        """Clamp values to [``lower``, ``upper``].

        Either bound may be ``None`` (no clipping on that side).  Supports
        Int64 and Float64 arms. Nulls propagate. Raises for String/Object
        columns.
        """
        var visitor = _ClipVisitor(self._null_mask, lower, upper, self.dtype.name)
        visit_col_data_raises(visitor, self._data)
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    def _apply[F: FloatTransformFn](self) raises -> Column:
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

    def _isin_kernel[T: Comparable & Copyable & Movable](
        self, d: List[T], values: List[T]
    ) -> Column:
        """Shared kernel for all _isin_* methods.

        Builds a Bool Column: True where element is in ``values``.
        Nulls propagate as null.
        """
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

    def _isin_int(self, values: List[Int64]) raises -> Column:
        """Bool Column: True where element is in ``values`` (Int64 columns only).

        Nulls propagate as null.
        """
        if not self._data.isa[List[Int64]]():
            raise Error("isin: column must be Int64 to match against List[Int64]")
        return self._isin_kernel(self._data[List[Int64]], values)

    def _isin_float(self, values: List[Float64]) raises -> Column:
        """Bool Column: True where element is in ``values`` (Float64 columns only).

        Nulls propagate as null.
        """
        if not self._data.isa[List[Float64]]():
            raise Error("isin: column must be Float64 to match against List[Float64]")
        return self._isin_kernel(self._data[List[Float64]], values)

    def _isin_str(self, values: List[String]) raises -> Column:
        """Bool Column: True where element is in ``values`` (String columns only).

        Nulls propagate as null.
        """
        if not self._data.isa[List[String]]():
            raise Error("isin: column must be String to match against List[String]")
        return self._isin_kernel(self._data[List[String]], values)

    def _isin_bool(self, values: List[Bool]) raises -> Column:
        """Bool Column: True where element is in ``values`` (Bool columns only).

        Nulls propagate as null.
        """
        if not self._data.isa[List[Bool]]():
            raise Error("isin: column must be Bool to match against List[Bool]")
        return self._isin_kernel(self._data[List[Bool]], values)

    def _isin_scalars(self, scalars: List[DFScalar]) raises -> Column:
        """Bool Column: True where each element appears in ``scalars``.

        Dispatches on the active ``ColumnData`` arm via ``_IsInVisitor``,
        which performs scalar-type coercion internally.  Raises for object
        dtype columns.  Nulls propagate as null.
        """
        var visitor = _IsInVisitor(self._null_mask, scalars)
        visit_col_data_raises(visitor, self._data)
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    def _between(self, left: Float64, right: Float64) raises -> Column:
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

    def _where_mask[mode: Int](self, cond: Column,
                              other: Optional[DFScalar] = None) raises -> Column:
        """Shared kernel for ``_where`` (mode=1) and ``_mask`` (mode=0).

        mode=1: keep value where cond is True, replace otherwise.
        mode=0: replace value where cond is True, keep otherwise.
        When ``other`` is None, replaced cells become null.
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
        var keep_on_true = (mode == 1)
        var visitor = _WhereMaskVisitor(
            self._null_mask, cond._data[List[Bool]].copy(), cond._null_mask,
            keep_on_true, other
        )
        visit_col_data_raises(visitor, self._data)
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    def _where(self, cond: Column, other: Optional[DFScalar] = None) raises -> Column:
        """Keep value where ``cond`` is True; replace with ``other`` (or null) otherwise."""
        return self._where_mask[1](cond, other)

    def _mask(self, cond: Column, other: Optional[DFScalar] = None) raises -> Column:
        """Replace with ``other`` (or null) where ``cond`` is True; keep otherwise."""
        return self._where_mask[0](cond, other)

    def _combine_first_col(self, other: Column) raises -> Column:
        """Return a Column whose values come from self where non-null, otherwise from other.

        Both columns must have the same length and the same ColumnData arm type.
        Raises on length mismatch or dtype mismatch.
        """
        if len(self) != len(other):
            raise Error(
                "combine_first: length mismatch ("
                + String(len(self))
                + " vs "
                + String(len(other))
                + ")"
            )
        var visitor = _CombineFirstVisitor(self._null_mask, other._data, other._null_mask)
        visit_col_data_raises(visitor, self._data)
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    def _unique(self) raises -> Column:
        """Return a Column of unique values, preserving first-occurrence order.

        Nulls are included once at the end if present. Uses a Set[T] for O(1)
        membership checks, giving O(n) overall complexity.
        Raises for Object dtype.
        """
        var visitor = _UniqueVisitor(self._null_mask)
        visit_col_data_raises(visitor, self._data)
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    def _astype(self, target_dtype: BisonDtype) raises -> Column:
        """Convert Column to a different dtype.

        Supported conversions:
          Int64  → Float64, Bool, Int64 (identity)
          Float64 → Int64 (truncation), Bool, Float64 (identity)
          Bool   → Int64, Float64, Bool (identity)
        The null mask is propagated via _build_result_col (same as other kernels).
        Raises for unsupported source/target dtype combinations.
        """
        var visitor = _AstypeVisitor(
            self._null_mask,
            target_dtype.is_integer(),
            target_dtype.is_float(),
            target_dtype == bool_,
            target_dtype.name,
            self.dtype.name,
        )
        visit_col_data_raises(visitor, self._data)
        if visitor.is_identity:
            return self.copy()
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    def _reset_index(self, drop: Bool = False) raises -> Column:
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

    def _to_pyobj_index(self) raises -> List[PythonObject]:
        """Extract column values as a List[PythonObject] for use as a row index.

        Converts each typed value to its Python equivalent using the
        ``_ToPandasVisitor`` pattern so that the result can be stored in
        ``Column._index`` of other columns.
        """
        var py_list = Python.evaluate("[]")
        var py_none = Python.evaluate("None")
        var visitor = _ToPandasVisitor(py_list, py_none, self._null_mask)
        visit_col_data_raises(visitor, self._data)
        var n = Int(py_list.__len__())
        var result = List[PythonObject]()
        for i in range(n):
            result.append(py_list[i])
        return result^

    def _reindex_rows(self, indices: List[Int],
                     fill_value: Optional[DFScalar]) raises -> Column:
        """Return a new Column with rows selected or inserted according to *indices*.

        ``indices[i] >= 0``  → take that row from self.
        ``indices[i] == -1`` → insert a null row, or a fill row when *fill_value*
                                is provided.
        Existing null mask entries are propagated for taken rows.
        """
        var visitor = _ReindexRowsVisitor(indices, fill_value, self._null_mask)
        visit_col_data_raises(visitor, self._data)
        return self._build_result_col(visitor.col_data.copy(), visitor.result_mask.copy(), visitor.has_any_null)

    # ------------------------------------------------------------------
    # Cumulative operations
    # ------------------------------------------------------------------

    def cumsum(self, skipna: Bool = True) raises -> Column:
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

    def cumprod(self, skipna: Bool = True) raises -> Column:
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

    def cummin(self, skipna: Bool = True) raises -> Column:
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

    def cummax(self, skipna: Bool = True) raises -> Column:
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
    def from_pandas(pd_series: PythonObject, name: String) raises -> Column:
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
    def _sniff_dtype(data: ColumnData) -> BisonDtype:
        """Return the BisonDtype that matches the active ColumnData arm."""
        var visitor = _DtypeSniffVisitor()
        visit_col_data(visitor, data)
        return visitor.result

    @staticmethod
    def _null_column(name: String, dtype: BisonDtype, n: Int, var index: List[PythonObject]) raises -> Column:
        """Create an all-null Column of length *n* with the given *dtype*.

        The underlying storage uses the canonical Mojo type for *dtype*
        (Int64 for integer families, Float64 for float families, Bool for
        bool, PythonObject for everything else).  Every element is marked
        null via ``_null_mask``.
        """
        var null_mask = List[Bool]()
        for _ in range(n):
            null_mask.append(True)
        var c: Column
        if dtype.is_integer():
            var data = List[Int64]()
            for _ in range(n):
                data.append(Int64(0))
            c = Column(name, ColumnData(data^), dtype, index^)
        elif dtype.is_float():
            var data = List[Float64]()
            for _ in range(n):
                data.append(Float64(0) / Float64(0))
            c = Column(name, ColumnData(data^), dtype, index^)
        elif dtype == bool_:
            var data = List[Bool]()
            for _ in range(n):
                data.append(False)
            c = Column(name, ColumnData(data^), bool_, index^)
        else:
            var data = List[PythonObject]()
            var py_none = Python.evaluate("None")
            for _ in range(n):
                data.append(py_none)
            c = Column(name, ColumnData(data^), dtype, index^)
        c._null_mask = null_mask^
        return c^

    @staticmethod
    def _fill_scalar(name: String, value: DFScalar, n: Int, index: List[PythonObject]) -> Column:
        """Create a Column of length *n* with every element equal to *value*.

        The dtype is inferred from the DFScalar arm: Int64 → int64, Float64 → float64,
        Bool → bool, String → object (matching pandas string storage).
        """
        if value.isa[Int64]():
            var v = value[Int64]
            var data = List[Int64]()
            for i in range(n):
                data.append(v)
            return Column(name, ColumnData(data^), int64, index.copy())
        elif value.isa[Float64]():
            var v = value[Float64]
            var data = List[Float64]()
            for i in range(n):
                data.append(v)
            return Column(name, ColumnData(data^), float64, index.copy())
        elif value.isa[Bool]():
            var v = value[Bool]
            var data = List[Bool]()
            for i in range(n):
                data.append(v)
            return Column(name, ColumnData(data^), bool_, index.copy())
        else:
            var v = value[String]
            var data = List[String]()
            for i in range(n):
                data.append(v)
            return Column(name, ColumnData(data^), object_, index.copy())

    def to_pandas(self) raises -> PythonObject:
        """Reconstruct a pandas Series from stored values."""
        var pd = Python.import_module("pandas")
        var py_list = Python.evaluate("[]")
        var py_none = Python.evaluate("None")
        var visitor = _ToPandasVisitor(py_list, py_none, self._null_mask)
        visit_col_data_raises(visitor, self._data)
        if len(self._index) > 0:
            var idx_py = Python.evaluate("[]")
            for i in range(len(self._index)):
                _ = idx_py.append(self._index[i])
            return pd.Series(py_list, name=self.name, dtype=self.dtype.name, index=idx_py)
        return pd.Series(py_list, name=self.name, dtype=self.dtype.name)
