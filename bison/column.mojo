from std.python import Python, PythonObject
from std.utils import Variant
from std.memory import bitcast
from std.collections import Dict, Set, Optional
from std.math import sqrt, floor, ceil, exp, log, log10
from marrow.arrays import (
    AnyArray,
    BoolArray,
    Float64Array,
    Int64Array,
    PrimitiveArray,
    StringArray,
)
from marrow.buffers import Bitmap
from marrow.builders import (
    array as _marrow_array,
)
from marrow.dtypes import (
    bool_ as _m_bool_,
    float64 as _m_float64,
    int64 as _m_int64,
    string as _m_string,
    Float64Type,
    Int64Type,
)
from marrow.kernels.aggregate import (
    sum_ as _marrow_sum,
    min_ as _marrow_min,
    max_ as _marrow_max,
)
from marrow.scalars import AnyScalar
from .index import Index, ColumnIndex
from .dtypes import (
    BisonDtype,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
    float32,
    float64,
    bool_,
    object_,
    string_,
    datetime64_ns,
    timedelta64_ns,
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


# ------------------------------------------------------------------
# Dual-backend storage scaffolding (issue #619, Phase 1)
# ------------------------------------------------------------------
# These types introduce the new storage representation alongside the
# existing ``ColumnData`` Variant.  They are wired through ``Column._storage``
# but every Column constructed in Phase 1 keeps an *empty* ``LegacyObjectData``
# arm in the new field — all behaviour is still driven by ``_data`` and
# ``_null_mask``.  Phase 2 reshapes the visitor traits to operate on
# ``ColumnStorage`` directly; Phase 3 starts populating ``_storage`` from
# the construction paths (``from_pandas``, ``from_records`` …).
#
# Storage layout (post-#647):
#
#   * int64 / float64 / bool / string (no nulls) columns → ``AnyArray`` arm.
#     Validity lives inside the marrow array's bitmap (Arrow semantics —
#     set bit = valid).
#   * object / datetime64 / timedelta64 columns → ``LegacyObjectData`` arm
#     with the ``List[PythonObject]`` payload and its null mask.
#   * string-with-nulls columns (marrow cannot build a string+null array
#     today) → ``LegacyObjectData`` arm carrying only the null mask; the
#     string data is lost on marrow write paths and must be managed by
#     callers via the LegacyObjectData fallback.
#
# ``Column.is_null`` / ``is_valid`` / ``has_nulls`` dispatch on the active
# arm of ``_storage`` — no side-table ``_storage_active`` flag is needed.
struct LegacyObjectData(Copyable, Movable):
    """Wrapper for ``List[PythonObject]`` columns under the new storage model.

    Holds both the data list and the null mask together, so the
    ``ColumnStorage`` Variant has a single arm carrying everything an
    object-typed column needs.  Once the migration is complete this is
    the only arm that retains a side-table NullMask — marrow-backed arms
    keep validity inside the array bitmap.
    """

    var data: List[PythonObject]
    var null_mask: NullMask

    def __init__(out self):
        """Empty object data — used as the Phase 1 placeholder."""
        self.data = List[PythonObject]()
        self.null_mask = NullMask()

    def __init__(
        out self, var data: List[PythonObject], var null_mask: NullMask
    ):
        self.data = data^
        self.null_mask = null_mask^

    def __init__(out self, *, copy: Self):
        self.data = copy.data.copy()
        self.null_mask = copy.null_mask.copy()

    def __init__(out self, *, deinit take: Self):
        self.data = take.data^
        self.null_mask = take.null_mask^


# New storage Variant — Phase 2+ visitor dispatch will branch on this.
# Variant is non-trivially copyable, so callers must use explicit ``.copy()``
# (matches ``ColumnData`` and ``ColumnIndex`` conventions).
comptime ColumnStorage = Variant[AnyArray, LegacyObjectData]


# Sentinel type for a null / missing cell in DFScalar.
struct _Null(Copyable, ImplicitlyCopyable, Movable):
    """Sentinel for a missing / null cell in DFScalar."""

    def __init__(out self):
        pass


# Scalar type for a single cell in row-oriented input (from_records).
# No PythonObject arm — record values must be explicitly typed.
#
# Implemented as a thin struct rather than a bare Variant alias so that
# Int (Mojo's native integer type) implicitly converts to Int64 at
# construction time.  All four typed arms plus Int and _Null are accepted;
# Int is normalised to Int64 immediately, so dispatch sites only ever see
# Int64.  Use DFScalar.null() / is_null() to represent missing values.
struct DFScalar(Copyable, ImplicitlyCopyable, Movable):
    var _v: Variant[Int64, Float64, Bool, String, _Null]

    @implicit
    def __init__(out self, value: Int64):
        self._v = Variant[Int64, Float64, Bool, String, _Null](value)

    @implicit
    def __init__(out self, value: Float64):
        self._v = Variant[Int64, Float64, Bool, String, _Null](value)

    @implicit
    def __init__(out self, value: Bool):
        self._v = Variant[Int64, Float64, Bool, String, _Null](value)

    @implicit
    def __init__(out self, value: String):
        self._v = Variant[Int64, Float64, Bool, String, _Null](value)

    @implicit
    def __init__(out self, value: Int):
        self._v = Variant[Int64, Float64, Bool, String, _Null](Int64(value))

    @implicit
    def __init__(out self, value: _Null):
        self._v = Variant[Int64, Float64, Bool, String, _Null](value)

    def __init__(out self, *, copy: Self):
        self._v = copy._v

    def __init__(out self, *, deinit take: Self):
        self._v = take._v^

    @staticmethod
    def null() -> Self:
        """Return a null sentinel scalar."""
        return Self(_Null())

    def is_null(self) -> Bool:
        """Return True if this scalar represents a missing / null value."""
        return self._v.isa[_Null]()

    def isa[T: Copyable & Movable](self) -> Bool:
        return self._v.isa[T]()

    def __getitem__[T: Copyable & Movable](ref self) -> ref[self._v] T:
        return self._v[T]

    def __getitem_param__[T: Copyable & Movable](ref self) -> ref[self._v] T:
        return self._v[T]


struct DictSplitResult(Copyable, Movable):
    """Holds the result of ``DataFrame.to_dict["split"]()`` /
    ``DataFrame.to_dict["tight"]()``.

    Fields
    ------
    columns      : Ordered list of column names.
    index        : Stringified row-index labels (one per row).
    data         : Row-major list of value lists.
    index_names  : Index level names — non-empty only for orient ``"tight"``.
    column_names : Column level names — non-empty only for orient ``"tight"``.
    """

    var columns: List[String]
    var index: List[String]
    var data: List[List[DFScalar]]
    var index_names: List[String]
    var column_names: List[String]

    def __init__(
        out self,
        var columns: List[String],
        var index: List[String],
        var data: List[List[DFScalar]],
        var index_names: List[String],
        var column_names: List[String],
    ):
        self.columns = columns^
        self.index = index^
        self.data = data^
        self.index_names = index_names^
        self.column_names = column_names^


# Scalar type returned by Series.iloc / Series.at.
# Covers all five ColumnData arm types; the PythonObject arm is used only
# for object/datetime/timedelta columns that have no native Mojo equivalent.
struct SeriesScalar(Copyable, ImplicitlyCopyable, Movable):
    var _v: Variant[Int64, Float64, Bool, String, PythonObject]

    @implicit
    def __init__(out self, value: Int64):
        self._v = Variant[Int64, Float64, Bool, String, PythonObject](value)

    @implicit
    def __init__(out self, value: Float64):
        self._v = Variant[Int64, Float64, Bool, String, PythonObject](value)

    @implicit
    def __init__(out self, value: Bool):
        self._v = Variant[Int64, Float64, Bool, String, PythonObject](value)

    @implicit
    def __init__(out self, value: String):
        self._v = Variant[Int64, Float64, Bool, String, PythonObject](value)

    @implicit
    def __init__(out self, value: PythonObject):
        self._v = Variant[Int64, Float64, Bool, String, PythonObject](value)

    @implicit
    def __init__(out self, value: Int):
        self._v = Variant[Int64, Float64, Bool, String, PythonObject](
            Int64(value)
        )

    def __init__(out self, *, copy: Self):
        self._v = copy._v

    def __init__(out self, *, deinit take: Self):
        self._v = take._v^

    def isa[T: Copyable & Movable](self) -> Bool:
        return self._v.isa[T]()

    def __getitem__[T: Copyable & Movable](ref self) -> ref[self._v] T:
        return self._v[T]

    def __getitem_param__[T: Copyable & Movable](ref self) -> ref[self._v] T:
        return self._v[T]


# ------------------------------------------------------------------
# Visitor traits — parameter constraints for ``Column._visit`` and
# ``Column._visit_raises``, which dispatch through the typed caches
# (#619 Phase 6c).  There are no standalone ``visit_col_data`` /
# ``visit_col_data_raises`` dispatcher functions anymore — visitors
# are always invoked via the ``Column._visit*`` methods.
# ------------------------------------------------------------------


trait ColumnDataVisitor:
    """Protocol for visiting a typed column payload (non-raises).

    Implement one ``on_*`` method per ``ColumnData`` arm.  Use a
    ``mut self`` field to accumulate or return a result.  Pass an
    instance to ``Column._visit[V]``.
    """

    def on_int64(mut self, data: List[Int64]):
        ...

    def on_float64(mut self, data: List[Float64]):
        ...

    def on_bool(mut self, data: List[Bool]):
        ...

    def on_str(mut self, data: List[String]):
        ...

    def on_obj(mut self, data: List[PythonObject]):
        ...


# ------------------------------------------------------------------
# Raises-capable visitor — for operations that perform Python interop
# or other potentially-failing work (e.g. to_pandas).
# ------------------------------------------------------------------


trait ColumnDataVisitorRaises:
    """Raises-capable counterpart to ``ColumnDataVisitor``.

    Use when ``on_*`` methods must call Python APIs or otherwise raise.
    Implement one ``on_*`` method per arm and pass an instance to
    ``Column._visit_raises[V]``.
    """

    def on_int64(mut self, data: List[Int64]) raises:
        ...

    def on_float64(mut self, data: List[Float64]) raises:
        ...

    def on_bool(mut self, data: List[Bool]) raises:
        ...

    def on_str(mut self, data: List[String]) raises:
        ...

    def on_obj(mut self, data: List[PythonObject]) raises:
        ...


# ------------------------------------------------------------------
# DFScalar visit primitive — single canonical dispatch site
# ------------------------------------------------------------------


trait DFScalarVisitor:
    """Protocol for visiting the active arm of a ``DFScalar`` Variant.

    Implement one ``on_*`` method per arm.  Use a ``mut self`` field to
    accumulate or return a result.  Pass an instance to ``visit_scalar``,
    which contains the **only** non-raises ``isa`` chain over ``DFScalar``;
    all callers should delegate here instead of writing their own discriminant
    checks.

    For visitors that need to call Python APIs or otherwise raise, implement
    ``DFScalarVisitorRaises`` and use ``visit_scalar_raises`` instead.
    """

    def on_int64(mut self, value: Int64):
        ...

    def on_float64(mut self, value: Float64):
        ...

    def on_bool(mut self, value: Bool):
        ...

    def on_str(mut self, value: String):
        ...

    def on_null(mut self):
        ...


def visit_scalar[V: DFScalarVisitor](mut visitor: V, scalar: DFScalar):
    """Dispatch *visitor* to the active ``DFScalar`` arm (non-raises).

    This is the **only** non-raises place in the codebase that reads the
    ``DFScalar`` discriminant via ``isa``.  Add new ``DFScalar`` arms here,
    in ``DFScalarVisitor``, and in ``visit_scalar_raises`` — every other
    dispatch site is then updated automatically because it delegates here.
    For visitors that may raise, use ``visit_scalar_raises`` instead.
    """
    if scalar.isa[Int64]():
        visitor.on_int64(scalar[Int64])
    elif scalar.isa[Float64]():
        visitor.on_float64(scalar[Float64])
    elif scalar.isa[Bool]():
        visitor.on_bool(scalar[Bool])
    elif scalar.isa[String]():
        visitor.on_str(scalar[String])
    else:
        visitor.on_null()


trait DFScalarVisitorRaises:
    """Raises-capable counterpart to ``DFScalarVisitor``.

    Use when ``on_*`` methods must call Python APIs or otherwise raise.
    Implement one ``on_*`` method per ``DFScalar`` arm and pass an instance
    to ``visit_scalar_raises``.
    """

    def on_int64(mut self, value: Int64) raises:
        ...

    def on_float64(mut self, value: Float64) raises:
        ...

    def on_bool(mut self, value: Bool) raises:
        ...

    def on_str(mut self, value: String) raises:
        ...

    def on_null(mut self) raises:
        ...


def visit_scalar_raises[
    V: DFScalarVisitorRaises
](mut visitor: V, scalar: DFScalar) raises:
    """Raises-capable dispatch for visitors that may raise.

    Mirrors ``visit_scalar`` but each ``on_*`` call site is in a ``raises``
    context.  Add new ``DFScalar`` arms here, in ``DFScalarVisitorRaises``,
    *and* in ``visit_scalar``.
    """
    if scalar.isa[Int64]():
        visitor.on_int64(scalar[Int64])
    elif scalar.isa[Float64]():
        visitor.on_float64(scalar[Float64])
    elif scalar.isa[Bool]():
        visitor.on_bool(scalar[Bool])
    elif scalar.isa[String]():
        visitor.on_str(scalar[String])
    else:
        visitor.on_null()


# ------------------------------------------------------------------
# Private visitor implementations used by Column methods
# ------------------------------------------------------------------


struct _CopyDataVisitor(ColumnDataVisitor, Copyable, Movable):
    """Visitor that produces an independent copy of the active ColumnData arm.
    """

    var result: ColumnData

    # Initialised with the fallback arm (List[PythonObject]) so that the field
    # is always valid.  on_* immediately replaces it with the copied data.
    def __init__(out self):
        self.result = ColumnData(List[PythonObject]())

    def on_int64(mut self, data: List[Int64]):
        self.result = ColumnData(data.copy())

    def on_float64(mut self, data: List[Float64]):
        self.result = ColumnData(data.copy())

    def on_bool(mut self, data: List[Bool]):
        self.result = ColumnData(data.copy())

    def on_str(mut self, data: List[String]):
        self.result = ColumnData(data.copy())

    def on_obj(mut self, data: List[PythonObject]):
        self.result = ColumnData(data.copy())


struct _FillScalarVisitor(Copyable, DFScalarVisitorRaises, Movable):
    """Visitor that builds a typed ColumnData of length *n* from a DFScalar.

    After visiting, construct the Column via
    ``Column(name, visitor._col_data^, visitor._dtype, index)``.
    The dtype is inferred from the DFScalar arm: Int64 → int64,
    Float64 → float64, Bool → bool_, String → string_ (#644).
    ``on_null`` raises because a null fill value is not meaningful here.
    ``_col_data`` is initialised with the List[PythonObject] fallback arm
    (following _CopyDataVisitor), but it is always replaced by an ``on_*``
    call before the visitor result is consumed.
    """

    var _n: Int
    var _col_data: ColumnData
    var _dtype: BisonDtype

    def __init__(out self, n: Int):
        self._n = n
        self._col_data = ColumnData(List[PythonObject]())
        self._dtype = object_

    def on_int64(mut self, value: Int64) raises:
        var data = List[Int64]()
        for _ in range(self._n):
            data.append(value)
        self._col_data = ColumnData(data^)
        self._dtype = int64

    def on_float64(mut self, value: Float64) raises:
        var data = List[Float64]()
        for _ in range(self._n):
            data.append(value)
        self._col_data = ColumnData(data^)
        self._dtype = float64

    def on_bool(mut self, value: Bool) raises:
        var data = List[Bool]()
        for _ in range(self._n):
            data.append(value)
        self._col_data = ColumnData(data^)
        self._dtype = bool_

    def on_str(mut self, value: String) raises:
        var data = List[String]()
        for _ in range(self._n):
            data.append(value)
        self._col_data = ColumnData(data^)
        self._dtype = string_

    def on_null(mut self) raises:
        raise Error("_fill_scalar: fill value cannot be null")


struct _FillnaVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Replaces null elements with a typed fill value, producing new ColumnData.

    Type coercion rules per arm:
    - Int64:   accepts Int64, Float64 (truncated), Bool (0/1).
    - Float64: accepts Float64, Int64 (widened), Bool (0.0/1.0).
    - Bool:    accepts Bool, Int64 (!=0), Float64 (!=0.0).
    - String:  accepts any scalar type via string conversion.
    - PythonObject: raises (unsupported).
    After visiting, ``col_data`` holds the filled data with no nulls remaining.
    """

    var col_data: ColumnData
    var null_mask: NullMask
    var value: DFScalar

    def __init__(out self, null_mask: NullMask, value: DFScalar):
        self.col_data = ColumnData(List[PythonObject]())
        self.null_mask = null_mask.copy()
        self.value = value

    def on_int64(mut self, data: List[Int64]) raises:
        var fill_val: Int64
        if self.value.isa[Int64]():
            fill_val = self.value[Int64]
        elif self.value.isa[Float64]():
            fill_val = Int64(Int(self.value[Float64]))
        elif self.value.isa[Bool]():
            fill_val = Int64(1) if self.value[Bool] else Int64(0)
        else:
            raise Error("fillna: cannot fill Int64 column with String value")
        var result = List[Int64]()
        for i in range(len(data)):
            if self.null_mask[i]:
                result.append(fill_val)
            else:
                result.append(data[i])
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var fill_val: Float64
        if self.value.isa[Float64]():
            fill_val = self.value[Float64]
        elif self.value.isa[Int64]():
            fill_val = Float64(self.value[Int64])
        elif self.value.isa[Bool]():
            fill_val = Float64(1.0) if self.value[Bool] else Float64(0.0)
        else:
            raise Error("fillna: cannot fill Float64 column with String value")
        var result = List[Float64]()
        for i in range(len(data)):
            if self.null_mask[i]:
                result.append(fill_val)
            else:
                result.append(data[i])
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var fill_val: Bool
        if self.value.isa[Bool]():
            fill_val = self.value[Bool]
        elif self.value.isa[Int64]():
            fill_val = self.value[Int64] != Int64(0)
        elif self.value.isa[Float64]():
            fill_val = self.value[Float64] != Float64(0.0)
        else:
            raise Error("fillna: cannot fill Bool column with String value")
        var result = List[Bool]()
        for i in range(len(data)):
            if self.null_mask[i]:
                result.append(fill_val)
            else:
                result.append(data[i])
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        var fill_val: String
        if self.value.isa[String]():
            fill_val = self.value[String]
        elif self.value.isa[Int64]():
            fill_val = String(Int(self.value[Int64]))
        elif self.value.isa[Float64]():
            fill_val = String(self.value[Float64])
        else:
            fill_val = String("True") if self.value[Bool] else String("False")
        var result = List[String]()
        for i in range(len(data)):
            if self.null_mask[i]:
                result.append(fill_val)
            else:
                result.append(data[i])
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("fillna: PythonObject columns are not supported")


struct _FfillVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Forward-fill: propagate the last non-null value forward over nulls.

    Produces ``col_data`` (filled), ``result_mask``, and ``has_any_null``.
    Leading nulls (before any non-null value) remain null.
    """

    var col_data: ColumnData
    var result_mask: NullMask
    var has_any_null: Bool
    var null_mask: NullMask

    def __init__(out self, null_mask: NullMask):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = NullMask()
        self.has_any_null = False
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var result = List[Int64]()
        var last_val = Int64(0)
        var found = False
        for i in range(len(data)):
            if self.null_mask[i]:
                if found:
                    result.append(last_val)
                    self.result_mask.append_valid()
                else:
                    result.append(Int64(0))
                    self.result_mask.append_null()
                    self.has_any_null = True
            else:
                last_val = data[i]
                found = True
                result.append(data[i])
                self.result_mask.append_valid()
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var result = List[Float64]()
        var last_val = Float64(0)
        var found = False
        for i in range(len(data)):
            if self.null_mask[i]:
                if found:
                    result.append(last_val)
                    self.result_mask.append_valid()
                else:
                    result.append(Float64(0))
                    self.result_mask.append_null()
                    self.has_any_null = True
            else:
                last_val = data[i]
                found = True
                result.append(data[i])
                self.result_mask.append_valid()
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var result = List[Bool]()
        var last_val = False
        var found = False
        for i in range(len(data)):
            if self.null_mask[i]:
                if found:
                    result.append(last_val)
                    self.result_mask.append_valid()
                else:
                    result.append(False)
                    self.result_mask.append_null()
                    self.has_any_null = True
            else:
                last_val = data[i]
                found = True
                result.append(data[i])
                self.result_mask.append_valid()
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        var result = List[String]()
        var last_val = String("")
        var found = False
        for i in range(len(data)):
            if self.null_mask[i]:
                if found:
                    result.append(last_val)
                    self.result_mask.append_valid()
                else:
                    result.append(String(""))
                    self.result_mask.append_null()
                    self.has_any_null = True
            else:
                last_val = data[i]
                found = True
                result.append(data[i])
                self.result_mask.append_valid()
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        var none_val = Python.evaluate("None")
        var result = List[PythonObject]()
        var last_val = none_val
        var found = False
        for i in range(len(data)):
            if self.null_mask[i]:
                if found:
                    result.append(last_val)
                    self.result_mask.append_valid()
                else:
                    result.append(none_val)
                    self.result_mask.append_null()
                    self.has_any_null = True
            else:
                last_val = data[i]
                found = True
                result.append(data[i])
                self.result_mask.append_valid()
        self.col_data = ColumnData(result^)


struct _BfillVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Backward-fill: propagate the next non-null value backward over nulls.

    Produces ``col_data`` (filled), ``result_mask``, and ``has_any_null``.
    Trailing nulls (after the last non-null value) remain null.
    """

    var col_data: ColumnData
    var result_mask: NullMask
    var has_any_null: Bool
    var null_mask: NullMask

    def __init__(out self, null_mask: NullMask):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = NullMask()
        self.has_any_null = False
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var n = len(data)
        var rev_data = List[Int64]()
        var rev_mask = List[Bool]()
        var next_val = Int64(0)
        var found = False
        for ri in range(n):
            var i = n - 1 - ri
            if not self.null_mask[i]:
                next_val = data[i]
                found = True
                rev_data.append(data[i])
                rev_mask.append(False)
            elif found:
                rev_data.append(next_val)
                rev_mask.append(False)
            else:
                rev_data.append(Int64(0))
                rev_mask.append(True)
                self.has_any_null = True
        var result = List[Int64]()
        for ri in range(n):
            result.append(rev_data[n - 1 - ri])
            self.result_mask.append(rev_mask[n - 1 - ri])
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var n = len(data)
        var rev_data = List[Float64]()
        var rev_mask = List[Bool]()
        var next_val = Float64(0)
        var found = False
        for ri in range(n):
            var i = n - 1 - ri
            if not self.null_mask[i]:
                next_val = data[i]
                found = True
                rev_data.append(data[i])
                rev_mask.append(False)
            elif found:
                rev_data.append(next_val)
                rev_mask.append(False)
            else:
                rev_data.append(Float64(0))
                rev_mask.append(True)
                self.has_any_null = True
        var result = List[Float64]()
        for ri in range(n):
            result.append(rev_data[n - 1 - ri])
            self.result_mask.append(rev_mask[n - 1 - ri])
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var n = len(data)
        var rev_data = List[Bool]()
        var rev_mask = List[Bool]()
        var next_val = False
        var found = False
        for ri in range(n):
            var i = n - 1 - ri
            if not self.null_mask[i]:
                next_val = data[i]
                found = True
                rev_data.append(data[i])
                rev_mask.append(False)
            elif found:
                rev_data.append(next_val)
                rev_mask.append(False)
            else:
                rev_data.append(False)
                rev_mask.append(True)
                self.has_any_null = True
        var result = List[Bool]()
        for ri in range(n):
            result.append(rev_data[n - 1 - ri])
            self.result_mask.append(rev_mask[n - 1 - ri])
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        var n = len(data)
        var rev_data = List[String]()
        var rev_mask = List[Bool]()
        var next_val = String("")
        var found = False
        for ri in range(n):
            var i = n - 1 - ri
            if not self.null_mask[i]:
                next_val = data[i]
                found = True
                rev_data.append(data[i])
                rev_mask.append(False)
            elif found:
                rev_data.append(next_val)
                rev_mask.append(False)
            else:
                rev_data.append(String(""))
                rev_mask.append(True)
                self.has_any_null = True
        var result = List[String]()
        for ri in range(n):
            result.append(rev_data[n - 1 - ri])
            self.result_mask.append(rev_mask[n - 1 - ri])
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        var n = len(data)
        var none_val = Python.evaluate("None")
        var rev_data = List[PythonObject]()
        var rev_mask = List[Bool]()
        var next_val = none_val
        var found = False
        for ri in range(n):
            var i = n - 1 - ri
            if not self.null_mask[i]:
                next_val = data[i]
                found = True
                rev_data.append(data[i])
                rev_mask.append(False)
            elif found:
                rev_data.append(next_val)
                rev_mask.append(False)
            else:
                rev_data.append(none_val)
                rev_mask.append(True)
                self.has_any_null = True
        var result = List[PythonObject]()
        for ri in range(n):
            result.append(rev_data[n - 1 - ri])
            self.result_mask.append(rev_mask[n - 1 - ri])
        self.col_data = ColumnData(result^)


struct _ToPandasVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that appends each element of the active ColumnData arm to a
    Python list, respecting a parallel null mask.

    ``py_list`` must already be a Python list object; elements are appended
    in order.  Null entries (``null_mask[i] == True``) are appended as the
    provided ``py_none`` value for all arms, including ``List[PythonObject]``.
    """

    var py_list: PythonObject
    var py_none: PythonObject
    var null_mask: NullMask

    def __init__(
        out self,
        py_list: PythonObject,
        py_none: PythonObject,
        null_mask: NullMask,
    ):
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
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                _ = self.py_list.append(self.py_none)
            else:
                _ = self.py_list.append(data[i])


# ------------------------------------------------------------------
# Marrow SIMD aggregation helpers (issue #582)
# ------------------------------------------------------------------


def _to_numeric_marrow_array(col: Column) raises -> AnyArray:
    """Return the AnyArray storage arm for a numeric Column.

    Preserved for the SIMD aggregation call sites in ``Column.sum`` /
    ``Column.min`` / ``Column.max``.  Numeric-only; raises on any other
    arm or when the column is not AnyArray-backed.
    """
    if (col.is_int() or col.is_float()) and col._storage.isa[AnyArray]():
        return col._storage[AnyArray].copy()
    raise Error("_to_numeric_marrow_array: not a numeric AnyArray column")


def _init_storage_from_column_data(
    mut col: Column, var data: ColumnData
) -> None:
    """Populate ``col._storage`` from a typed ``ColumnData`` payload.

    Numeric / bool / string arms become an ``AnyArray`` (marrow-backed);
    object / datetime / timedelta arms become ``LegacyObjectData``
    carrying the ``List[PythonObject]`` payload.  The column's dtype is
    used to disambiguate the LegacyObjectData fallback path.

    This is the canonical post-#658 constructor helper — it replaces the
    former ``_column_to_marrow_array`` detour that read from typed caches.
    """
    if data.isa[List[Int64]]():
        ref src = data[List[Int64]]
        var vals = List[Optional[Int]](capacity=len(src))
        for i in range(len(src)):
            vals.append(Int(src[i]))
        try:
            col._storage = ColumnStorage(
                AnyArray(_marrow_array[Int64Type](vals^))
            )
        except:
            col._storage = ColumnStorage(
                LegacyObjectData(List[PythonObject](), NullMask())
            )
        return
    if data.isa[List[Float64]]():
        ref src = data[List[Float64]]
        var vals = List[Optional[Float64]](capacity=len(src))
        for i in range(len(src)):
            vals.append(src[i])
        try:
            col._storage = ColumnStorage(
                AnyArray(_marrow_array[Float64Type](vals^))
            )
        except:
            col._storage = ColumnStorage(
                LegacyObjectData(List[PythonObject](), NullMask())
            )
        return
    if data.isa[List[Bool]]():
        ref src = data[List[Bool]]
        var vals = List[Optional[Bool]](capacity=len(src))
        for i in range(len(src)):
            vals.append(src[i])
        try:
            col._storage = ColumnStorage(AnyArray(_marrow_array(vals^)))
        except:
            col._storage = ColumnStorage(
                LegacyObjectData(List[PythonObject](), NullMask())
            )
        return
    if data.isa[List[String]]():
        ref src = data[List[String]]
        try:
            col._storage = ColumnStorage(AnyArray(_marrow_array(src.copy())))
        except:
            col._storage = ColumnStorage(
                LegacyObjectData(List[PythonObject](), NullMask())
            )
        return
    # Object / datetime / timedelta: LegacyObjectData arm.
    col._storage = ColumnStorage(
        LegacyObjectData(data[List[PythonObject]].copy(), NullMask())
    )


def _marrow_scalar_to_float64(scalar: AnyScalar) -> Float64:
    """Extract a Float64 from a marrow AnyScalar (int64 or float64)."""
    if scalar.type() == _m_float64:
        return Float64(scalar.as_primitive[Float64Type]().value())
    else:
        return Float64(scalar.as_primitive[Int64Type]().value())


# ------------------------------------------------------------------
# Aggregation visitors (issue #81)
# ------------------------------------------------------------------


struct _SumVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Accumulates element-wise sum into Float64, skipping masked nulls."""

    var result: Float64
    var null_mask: NullMask

    def __init__(out self, null_mask: NullMask):
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


struct _ExtremumVisitor[find_min: Bool](
    ColumnDataVisitorRaises, Copyable, Movable
):
    """Finds the minimum (find_min=True) or maximum (find_min=False) value as
    Float64, skipping masked nulls.  Unifies the former _MinVisitor/_MaxVisitor.
    """

    var result: Float64
    var found: Bool
    var null_mask: NullMask
    var _label: String

    def __init__(out self, null_mask: NullMask):
        self.result = Float64(0)
        self.found = False
        self.null_mask = null_mask.copy()
        comptime if Self.find_min:
            self._label = "min"
        else:
            self._label = "max"

    def _update(mut self, v: Float64):
        comptime if Self.find_min:
            if not self.found or v < self.result:
                self.result = v
                self.found = True
        else:
            if not self.found or v > self.result:
                self.result = v
                self.found = True

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self._update(Float64(data[i]))

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self._update(data[i])

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self._update(Float64(1.0) if data[i] else Float64(0.0))

    def on_str(mut self, data: List[String]) raises:
        raise Error(self._label + ": non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error(self._label + ": non-numeric column type")


struct _VarVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Accumulates squared deviations from the mean for variance computation."""

    var total: Float64
    var mean: Float64
    var null_mask: NullMask

    def __init__(out self, mean: Float64, null_mask: NullMask):
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


struct _MomentVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Accumulates sum of (x - mean)^k, skipping null values.

    Used to compute higher-order central moments for skewness (k=3) and
    excess kurtosis (k=4) calculations.
    """

    var total: Float64
    var mean: Float64
    var k: Int
    var null_mask: NullMask

    def __init__(out self, mean: Float64, k: Int, null_mask: NullMask):
        self.total = Float64(0)
        self.mean = mean
        self.k = k
        self.null_mask = null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var diff = Float64(data[i]) - self.mean
            var p = diff
            for _ in range(self.k - 1):
                p *= diff
            self.total += p

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var diff = data[i] - self.mean
            var p = diff
            for _ in range(self.k - 1):
                p *= diff
            self.total += p

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            var v = Float64(1.0) if data[i] else Float64(0.0)
            var diff = v - self.mean
            var p = diff
            for _ in range(self.k - 1):
                p *= diff
            self.total += p

    def on_str(mut self, data: List[String]) raises:
        raise Error("skew/kurt: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("skew/kurt: non-numeric column type")


struct _ArgExtremumVisitor[find_min: Bool](
    ColumnDataVisitorRaises, Copyable, Movable
):
    """Finds the positional index of the minimum (find_min=True) or maximum
    (find_min=False) value, skipping null values.  Unifies the former
    _ArgMinVisitor/_ArgMaxVisitor.
    """

    var result: Int
    var found: Bool
    var best_val: Float64
    var null_mask: NullMask
    var _label: String

    def __init__(out self, null_mask: NullMask):
        self.result = 0
        self.found = False
        self.best_val = Float64(0)
        self.null_mask = null_mask.copy()
        comptime if Self.find_min:
            self._label = "idxmin"
        else:
            self._label = "idxmax"

    def _update(mut self, v: Float64, i: Int):
        comptime if Self.find_min:
            if not self.found or v < self.best_val:
                self.best_val = v
                self.result = i
                self.found = True
        else:
            if not self.found or v > self.best_val:
                self.best_val = v
                self.result = i
                self.found = True

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self._update(Float64(data[i]), i)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self._update(data[i], i)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                continue
            self._update(Float64(1.0) if data[i] else Float64(0.0), i)

    def on_str(mut self, data: List[String]) raises:
        raise Error(self._label + ": non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error(self._label + ": non-numeric column type")


struct _NuniqueVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Counts unique non-null values across all supported column types."""

    var result: Int
    var null_mask: NullMask

    def __init__(out self, null_mask: NullMask):
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
    """Collects non-null numeric values into a Float64 list for quantile computation.
    """

    var vals: List[Float64]
    var null_mask: NullMask

    def __init__(out self, null_mask: NullMask):
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
    var null_mask: NullMask
    var is_identity: Bool
    var dtype_name: String

    def __init__(out self, null_mask: NullMask, dtype_name: String):
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
    var null_mask: NullMask
    var decimals: Int
    var is_identity: Bool
    var dtype_name: String

    def __init__(
        out self, null_mask: NullMask, decimals: Int, dtype_name: String
    ):
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
    var null_mask: NullMask
    var lower: Optional[Float64]
    var upper: Optional[Float64]
    var dtype_name: String

    def __init__(
        out self,
        null_mask: NullMask,
        lower: Optional[Float64],
        upper: Optional[Float64],
        dtype_name: String,
    ):
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
    var self_null_mask: NullMask
    var cond_data: List[Bool]
    var cond_null_mask: NullMask
    var keep_on_true: Bool
    var other: Optional[DFScalar]

    def __init__(
        out self,
        self_null_mask: NullMask,
        cond_data: List[Bool],
        cond_null_mask: NullMask,
        keep_on_true: Bool,
        other: Optional[DFScalar] = None,
    ):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.self_null_mask = self_null_mask.copy()
        self.cond_data = cond_data.copy()
        self.cond_null_mask = cond_null_mask.copy()
        self.keep_on_true = keep_on_true
        self.other = other

    def on_int64(mut self, data: List[Int64]) raises:
        var has_self_mask = self.self_null_mask.has_nulls()
        var has_cond_mask = self.cond_null_mask.has_nulls()
        var has_other = self.other.__bool__()
        var other_val: Int64 = 0
        var other_is_null = True
        if has_other:
            var fv = self.other.value()
            if fv.isa[Int64]():
                other_val = fv[Int64]
                other_is_null = False
            elif fv.isa[Float64]():
                other_val = Int64(Int(fv[Float64]))
                other_is_null = False
            elif fv.isa[Bool]():
                other_val = Int64(1) if fv[Bool] else Int64(0)
                other_is_null = False
        var result = List[Int64]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            var cond_true = (
                not has_cond_mask or not self.cond_null_mask[i]
            ) and self.cond_data[i]
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
        var has_self_mask = self.self_null_mask.has_nulls()
        var has_cond_mask = self.cond_null_mask.has_nulls()
        var nan = Float64(0) / Float64(0)
        var has_other = self.other.__bool__()
        var other_val: Float64 = nan
        var other_is_null = True
        if has_other:
            var fv = self.other.value()
            if fv.isa[Float64]():
                other_val = fv[Float64]
                other_is_null = False
            elif fv.isa[Int64]():
                other_val = Float64(fv[Int64])
                other_is_null = False
            elif fv.isa[Bool]():
                other_val = 1.0 if fv[Bool] else 0.0
                other_is_null = False
        var result = List[Float64]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            var cond_true = (
                not has_cond_mask or not self.cond_null_mask[i]
            ) and self.cond_data[i]
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
        var has_self_mask = self.self_null_mask.has_nulls()
        var has_cond_mask = self.cond_null_mask.has_nulls()
        var has_other = self.other.__bool__()
        var other_val: Bool = False
        var other_is_null = True
        if has_other:
            var fv = self.other.value()
            if fv.isa[Bool]():
                other_val = fv[Bool]
                other_is_null = False
            elif fv.isa[Int64]():
                other_val = fv[Int64] != 0
                other_is_null = False
        var result = List[Bool]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            var cond_true = (
                not has_cond_mask or not self.cond_null_mask[i]
            ) and self.cond_data[i]
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
        var has_self_mask = self.self_null_mask.has_nulls()
        var has_cond_mask = self.cond_null_mask.has_nulls()
        var has_other = self.other.__bool__()
        var other_val: String = ""
        var other_is_null = True
        if has_other:
            var fv = self.other.value()
            if fv.isa[String]():
                other_val = fv[String]
                other_is_null = False
        var result = List[String]()
        for i in range(len(data)):
            var self_null = has_self_mask and self.self_null_mask[i]
            var cond_true = (
                not has_cond_mask or not self.cond_null_mask[i]
            ) and self.cond_data[i]
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
    var self_null_mask: NullMask
    var other_data: ColumnData
    var other_null_mask: NullMask

    def __init__(
        out self,
        self_null_mask: NullMask,
        other_data: ColumnData,
        other_null_mask: NullMask,
    ):
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.self_null_mask = self_null_mask.copy()
        self.other_data = other_data.copy()
        self.other_null_mask = other_null_mask.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        if self.other_data.isa[List[Float64]]():
            # Upcast self (Int64) to Float64 to match other, then delegate to on_float64.
            var upcast = List[Float64]()
            for i in range(len(data)):
                upcast.append(Float64(data[i]))
            self.on_float64(upcast)
            return
        if not self.other_data.isa[List[Int64]]():
            raise Error("combine_first: dtype mismatch between columns")
        ref od = self.other_data[List[Int64]]
        var has_self_mask = self.self_null_mask.has_nulls()
        var has_other_mask = self.other_null_mask.has_nulls()
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
        var od: List[Float64]
        if self.other_data.isa[List[Float64]]():
            od = self.other_data[List[Float64]].copy()
        elif self.other_data.isa[List[Int64]]():
            # Upcast other (Int64) to Float64 to match self.
            od = List[Float64]()
            ref odi = self.other_data[List[Int64]]
            for i in range(len(odi)):
                od.append(Float64(odi[i]))
        else:
            raise Error("combine_first: dtype mismatch between columns")
        var nan = Float64(0) / Float64(0)
        var has_self_mask = self.self_null_mask.has_nulls()
        var has_other_mask = self.other_null_mask.has_nulls()
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
        var has_self_mask = self.self_null_mask.has_nulls()
        var has_other_mask = self.other_null_mask.has_nulls()
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
        var has_self_mask = self.self_null_mask.has_nulls()
        var has_other_mask = self.other_null_mask.has_nulls()
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
    var null_mask: NullMask
    var scalars: List[DFScalar]

    def __init__(out self, null_mask: NullMask, scalars: List[DFScalar]):
        # Initialised with the fallback arm (List[PythonObject]) so that the
        # field is always valid.  on_* immediately replaces it with the Bool result.
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False
        self.null_mask = null_mask.copy()
        self.scalars = scalars.copy()

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var val_set = Set[Int64]()
        for k in range(len(self.scalars)):
            if self.scalars[k].isa[Int64]():
                val_set.add(self.scalars[k][Int64])
            elif self.scalars[k].isa[Float64]():
                val_set.add(Int64(Int(self.scalars[k][Float64])))
            elif self.scalars[k].isa[Bool]():
                val_set.add(Int64(1) if self.scalars[k][Bool] else Int64(0))
        var result = List[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(data[i] in val_set)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var val_set = Set[Float64]()
        for k in range(len(self.scalars)):
            if self.scalars[k].isa[Float64]():
                val_set.add(self.scalars[k][Float64])
            elif self.scalars[k].isa[Int64]():
                val_set.add(Float64(self.scalars[k][Int64]))
            elif self.scalars[k].isa[Bool]():
                val_set.add(
                    Float64(1.0) if self.scalars[k][Bool] else Float64(0.0)
                )
        var result = List[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(data[i] in val_set)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var val_set = Set[Bool]()
        for k in range(len(self.scalars)):
            if self.scalars[k].isa[Bool]():
                val_set.add(self.scalars[k][Bool])
        var result = List[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(data[i] in val_set)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_str(mut self, data: List[String]) raises:
        var has_mask = len(self.null_mask) > 0
        var val_set = Set[String]()
        for k in range(len(self.scalars)):
            if self.scalars[k].isa[String]():
                val_set.add(self.scalars[k][String])
        var result = List[Bool]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                result.append(data[i] in val_set)
                self.result_mask.append(False)
        self.col_data = ColumnData(result^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("isin: not supported for object dtype")


struct _UniqueVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Returns unique values in first-occurrence order; nulls appended once at end.
    """

    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool
    var null_mask: NullMask

    def __init__(out self, null_mask: NullMask):
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
    var null_mask: NullMask
    var is_identity: Bool
    var to_int: Bool
    var to_float: Bool
    var to_bool: Bool
    var target_dtype_name: String
    var source_dtype_name: String

    def __init__(
        out self,
        null_mask: NullMask,
        to_int: Bool,
        to_float: Bool,
        to_bool: Bool,
        target_dtype_name: String,
        source_dtype_name: String,
    ):
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
            raise Error(
                "astype: unsupported target dtype '"
                + self.target_dtype_name
                + "' for Int64 source"
            )

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
            raise Error(
                "astype: unsupported target dtype '"
                + self.target_dtype_name
                + "' for Float64 source"
            )

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
            raise Error(
                "astype: unsupported target dtype '"
                + self.target_dtype_name
                + "' for Bool source"
            )

    def on_str(mut self, data: List[String]) raises:
        raise Error(
            "astype: not supported for source dtype '"
            + self.source_dtype_name
            + "'"
        )

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error(
            "astype: not supported for source dtype '"
            + self.source_dtype_name
            + "'"
        )


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


struct _ConcatDataVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Appends *other* data onto the visited arm's list."""

    var other: ColumnData
    var result: ColumnData

    def __init__(out self, other: ColumnData):
        self.other = other.copy()
        self.result = ColumnData(List[PythonObject]())

    def on_int64(mut self, data: List[Int64]) raises:
        if not self.other.isa[List[Int64]]():
            raise Error("concat: dtype mismatch")
        var out = data.copy()
        ref o = self.other[List[Int64]]
        for i in range(len(o)):
            out.append(o[i])
        self.result = ColumnData(out^)

    def on_float64(mut self, data: List[Float64]) raises:
        if not self.other.isa[List[Float64]]():
            raise Error("concat: dtype mismatch")
        var out = data.copy()
        ref o = self.other[List[Float64]]
        for i in range(len(o)):
            out.append(o[i])
        self.result = ColumnData(out^)

    def on_bool(mut self, data: List[Bool]) raises:
        if not self.other.isa[List[Bool]]():
            raise Error("concat: dtype mismatch")
        var out = data.copy()
        ref o = self.other[List[Bool]]
        for i in range(len(o)):
            out.append(o[i])
        self.result = ColumnData(out^)

    def on_str(mut self, data: List[String]) raises:
        if not self.other.isa[List[String]]():
            raise Error("concat: dtype mismatch")
        var out = data.copy()
        ref o = self.other[List[String]]
        for i in range(len(o)):
            out.append(o[i])
        self.result = ColumnData(out^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        if not self.other.isa[List[PythonObject]]():
            raise Error("concat: dtype mismatch")
        var out = data.copy()
        ref o = self.other[List[PythonObject]]
        for i in range(len(o)):
            out.append(o[i])
        self.result = ColumnData(out^)


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


struct _TakeWithNullsVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Like _TakeVisitor but index -1 emits a null placeholder row."""

    var indices: List[Int]
    var src_mask: NullMask
    var out_mask: NullMask
    var result: ColumnData

    def __init__(out self, indices: List[Int], src_mask: NullMask):
        self.indices = indices.copy()
        self.src_mask = src_mask.copy()
        self.out_mask = NullMask()
        self.result = ColumnData(List[PythonObject]())

    def on_int64(mut self, data: List[Int64]) raises:
        var out = List[Int64]()
        for k in range(len(self.indices)):
            var i = self.indices[k]
            if i < 0:
                out.append(Int64(0))
                self.out_mask.append_null()
            else:
                out.append(data[i])
                self.out_mask.append(
                    len(self.src_mask) > i and self.src_mask[i]
                )
        self.result = ColumnData(out^)

    def on_float64(mut self, data: List[Float64]) raises:
        var out = List[Float64]()
        for k in range(len(self.indices)):
            var i = self.indices[k]
            if i < 0:
                out.append(Float64(0) / Float64(0))
                self.out_mask.append_null()
            else:
                out.append(data[i])
                self.out_mask.append(
                    len(self.src_mask) > i and self.src_mask[i]
                )
        self.result = ColumnData(out^)

    def on_bool(mut self, data: List[Bool]) raises:
        var out = List[Bool]()
        for k in range(len(self.indices)):
            var i = self.indices[k]
            if i < 0:
                out.append(False)
                self.out_mask.append_null()
            else:
                out.append(data[i])
                self.out_mask.append(
                    len(self.src_mask) > i and self.src_mask[i]
                )
        self.result = ColumnData(out^)

    def on_str(mut self, data: List[String]) raises:
        var out = List[String]()
        for k in range(len(self.indices)):
            var i = self.indices[k]
            if i < 0:
                out.append("")
                self.out_mask.append_null()
            else:
                out.append(data[i])
                self.out_mask.append(
                    len(self.src_mask) > i and self.src_mask[i]
                )
        self.result = ColumnData(out^)

    def on_obj(mut self, data: List[PythonObject]) raises:
        var out = List[PythonObject]()
        for k in range(len(self.indices)):
            var i = self.indices[k]
            if i < 0:
                out.append(PythonObject(None))
                self.out_mask.append_null()
            else:
                out.append(data[i])
                self.out_mask.append(
                    len(self.src_mask) > i and self.src_mask[i]
                )
        self.result = ColumnData(out^)


struct _ValueCountsCountVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Phase 1 of value_counts: counts unique values via stringified keys.

    Builds ``unique_keys`` (insertion-order list of string representations)
    and ``counts_dict`` (key → count).  Raises for String/PythonObject arms.
    """

    var has_mask: Bool
    var null_mask: NullMask
    var unique_keys: List[String]
    var counts_dict: Dict[String, Int]

    def __init__(out self, has_mask: Bool, null_mask: NullMask):
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
    """Phase 2 of value_counts: builds the native-typed index.

    Given ``sorted_order``, ``unique_keys``, and ``count_vals`` from phase 1,
    converts string keys back to typed native values and accumulates
    ``result_counts``.  Raises for String/PythonObject arms.
    Int64 columns produce a List[Int64] index; float64 and bool columns fall
    back to List[PythonObject] (float values cannot be stored in Index).
    """

    var sorted_order: List[Int]
    var unique_keys: List[String]
    var count_vals: List[Int]
    var result_idx: ColumnIndex
    var result_counts: List[Int64]

    def __init__(
        out self,
        sorted_order: List[Int],
        unique_keys: List[String],
        count_vals: List[Int],
    ):
        self.sorted_order = sorted_order.copy()
        self.unique_keys = unique_keys.copy()
        self.count_vals = count_vals.copy()
        self.result_idx = ColumnIndex(List[PythonObject]())
        self.result_counts = List[Int64]()

    def on_int64(mut self, data: List[Int64]) raises:
        var int_idx = List[Int64]()
        for i in range(len(self.sorted_order)):
            var si = self.sorted_order[i]
            self.result_counts.append(Int64(self.count_vals[si]))
            int_idx.append(Int64(atol(self.unique_keys[si])))
        self.result_idx = ColumnIndex(int_idx^)

    def on_float64(mut self, data: List[Float64]) raises:
        var builtins = Python.import_module("builtins")
        var obj_idx = List[PythonObject]()
        for i in range(len(self.sorted_order)):
            var si = self.sorted_order[i]
            self.result_counts.append(Int64(self.count_vals[si]))
            obj_idx.append(builtins.float(self.unique_keys[si]))
        self.result_idx = ColumnIndex(obj_idx^)

    def on_bool(mut self, data: List[Bool]) raises:
        var obj_idx = List[PythonObject]()
        for i in range(len(self.sorted_order)):
            var si = self.sorted_order[i]
            self.result_counts.append(Int64(self.count_vals[si]))
            obj_idx.append(PythonObject(self.unique_keys[si] == "True"))
        self.result_idx = ColumnIndex(obj_idx^)

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
comptime _ARITH_ADD = 0
comptime _ARITH_SUB = 1
comptime _ARITH_MUL = 2
comptime _ARITH_DIV = 3
comptime _ARITH_FLOORDIV = 4
comptime _ARITH_MOD = 5
comptime _ARITH_POW = 6


def _int64_floordiv(a: Int64, b: Int64) -> Int64:
    """Integer floor division with Python semantics (rounds toward −∞).

    C-style truncating division (``/``) rounds toward zero; when the operands
    have different signs and the division is not exact the quotient must be
    decremented by one to match Python's ``//`` behaviour.
    """
    var q = a / b
    if (a < 0) != (b < 0):
        if q * b != a:
            q -= Int64(1)
    return q


def _int64_mod(a: Int64, b: Int64) -> Int64:
    """Integer modulo with Python semantics (result has the sign of the divisor).
    """
    return a - _int64_floordiv(a, b) * b


def _int64_pow(base: Int64, exp: Int64) -> Int64:
    """Integer exponentiation by squaring.

    Negative exponents yield 0 for ``|base| > 1``, matching numpy's int64
    behaviour (integer division truncates the fractional result to zero).
    """
    if exp < Int64(0):
        return Int64(0)
    var result = Int64(1)
    var b = base
    var e = exp
    while e > Int64(0):
        if e & Int64(1) == Int64(1):
            result *= b
        b *= b
        e = e >> Int64(1)
    return result


# ------------------------------------------------------------------
# Compile-time operation selectors for Column._cmp_op
# ------------------------------------------------------------------
comptime _CMP_EQ = 0
comptime _CMP_NE = 1
comptime _CMP_LT = 2
comptime _CMP_LE = 3
comptime _CMP_GT = 4
comptime _CMP_GE = 5


# ------------------------------------------------------------------
# Compile-time operation selectors for Column._bool_op
# ------------------------------------------------------------------
comptime _BOOL_AND = 0
comptime _BOOL_OR = 1
comptime _BOOL_XOR = 2


# ------------------------------------------------------------------
# Comparison visitor — dispatches on self's ColumnData arm and stores
# the RHS column's data to handle the Bool-Bool fast path internally.
# ------------------------------------------------------------------


struct _CmpOpVisitor[op: Int](ColumnDataVisitorRaises, Copyable, Movable):
    """Element-wise comparison visitor for ``Column._cmp_op``.

    At construction time the RHS column's data is split into pre-computed
    forms: ``other_bool`` (populated when the RHS holds Bool data),
    ``other_str`` (populated when the RHS holds String data), and
    ``other_float`` (a Float64 projection populated for all other numeric
    arms).  This avoids a repeated ``_ToFloat64Visitor`` call per dispatch
    and eliminates the need for a live ``ColumnData`` reference inside the
    visitor.  ``on_bool`` uses the Bool-Bool fast path when ``other_is_bool``
    is set; ``on_str`` uses the String-String fast path when ``other_is_str``
    is set (EQ and NE only); all other numeric arms delegate to
    ``_run_float64``.  Object arms raise.

    ``op`` is one of the ``_CMP_*`` compile-time constants; ``comptime if``
    folds the branch at compile time so each specialisation is a tight loop.
    """

    var self_null_mask: NullMask
    var other_null_mask: NullMask
    var other_bool: List[Bool]  # Bool RHS data; non-empty iff other_is_bool
    var other_float: List[
        Float64
    ]  # Float64 RHS data; non-empty iff not other_is_bool and not other_is_str
    var other_str: List[String]  # String RHS data; non-empty iff other_is_str
    var other_is_bool: Bool
    var other_is_str: Bool
    var result: List[Bool]
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, self_null_mask: NullMask, other: Column) raises:
        self.self_null_mask = self_null_mask.copy()
        self.other_null_mask = other.null_mask_copy()
        self.result = List[Bool]()
        self.result_mask = List[Bool]()
        self.has_any_null = False
        if other.is_bool():
            self.other_is_bool = True
            self.other_is_str = False
            self.other_bool = other._bool_data().copy()
            self.other_float = List[Float64]()
            self.other_str = List[String]()
        elif other.is_string():
            self.other_is_bool = False
            self.other_is_str = True
            self.other_bool = List[Bool]()
            self.other_float = List[Float64]()
            self.other_str = other._str_data().copy()
        else:
            self.other_is_bool = False
            self.other_is_str = False
            self.other_bool = List[Bool]()
            self.other_str = List[String]()
            self.other_float = other._to_float64_list()

    def _run_float64(mut self, a: List[Float64]):
        """Inner loop: compare ``a`` against ``other_float`` with null propagation.
        """
        var has_a_mask = self.self_null_mask.has_nulls()
        var has_b_mask = self.other_null_mask.has_nulls()
        for i in range(len(a)):
            var is_null = (has_a_mask and self.self_null_mask[i]) or (
                has_b_mask and self.other_null_mask[i]
            )
            if is_null:
                self.result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v: Bool
                comptime if Self.op == _CMP_EQ:
                    v = a[i] == self.other_float[i]
                elif Self.op == _CMP_NE:
                    v = a[i] != self.other_float[i]
                elif Self.op == _CMP_LT:
                    v = a[i] < self.other_float[i]
                elif Self.op == _CMP_LE:
                    v = a[i] <= self.other_float[i]
                elif Self.op == _CMP_GT:
                    v = a[i] > self.other_float[i]
                elif Self.op == _CMP_GE:
                    v = a[i] >= self.other_float[i]
                else:
                    v = False  # unreachable: compile-time guard
                self.result.append(v)
                self.result_mask.append(False)

    def on_int64(mut self, data: List[Int64]) raises:
        var a = List[Float64]()
        for i in range(len(data)):
            a.append(Float64(data[i]))
        self._run_float64(a)

    def on_float64(mut self, data: List[Float64]) raises:
        self._run_float64(data)

    def on_bool(mut self, data: List[Bool]) raises:
        # `data` is guaranteed to be List[Bool] by the visitor dispatch.
        # The isa check below resolves only the RHS arm.
        var has_a_mask = self.self_null_mask.has_nulls()
        var has_b_mask = self.other_null_mask.has_nulls()
        if self.other_is_bool:
            # Bool-Bool fast path: compare directly without a Float64 round-trip.
            ref db = self.other_bool
            for i in range(len(data)):
                var is_null = (has_a_mask and self.self_null_mask[i]) or (
                    has_b_mask and self.other_null_mask[i]
                )
                if is_null:
                    self.result.append(False)
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    var v: Bool
                    comptime if Self.op == _CMP_EQ:
                        v = data[i] == db[i]
                    elif Self.op == _CMP_NE:
                        v = data[i] != db[i]
                    elif Self.op == _CMP_LT:
                        v = (not data[i]) and db[i]  # False < True
                    elif Self.op == _CMP_LE:
                        v = (not data[i]) or db[i]  # False <= True, F<=F, T<=T
                    elif Self.op == _CMP_GT:
                        v = data[i] and (not db[i])  # True > False
                    elif Self.op == _CMP_GE:
                        v = data[i] or (not db[i])  # True >= False, F>=F, T>=T
                    else:
                        v = False  # unreachable: compile-time guard
                    self.result.append(v)
                    self.result_mask.append(False)
        else:
            # Mixed Bool/numeric: convert self Bool to Float64 then use general path.
            var a = List[Float64]()
            for i in range(len(data)):
                a.append(1.0 if data[i] else 0.0)
            self._run_float64(a)

    def on_str(mut self, data: List[String]) raises:
        if not self.other_is_str:
            raise Error(
                "cmp: cannot compare string column with non-string column"
            )
        var has_a_mask = self.self_null_mask.has_nulls()
        var has_b_mask = self.other_null_mask.has_nulls()
        ref ds = self.other_str
        for i in range(len(data)):
            var is_null = (has_a_mask and self.self_null_mask[i]) or (
                has_b_mask and self.other_null_mask[i]
            )
            if is_null:
                self.result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                comptime if Self.op == _CMP_EQ:
                    self.result.append(data[i] == ds[i])
                    self.result_mask.append(False)
                elif Self.op == _CMP_NE:
                    self.result.append(data[i] != ds[i])
                    self.result_mask.append(False)
                else:
                    raise Error(
                        "cmp: only == and != are supported for string columns"
                    )

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error(
            "cmp: comparison not supported for object/datetime column type"
        )


# ------------------------------------------------------------------
# Scalar comparison visitor — compares each element against a single
# Float64 constant, avoiding the broadcast allocation used by _CmpOpVisitor.
# ------------------------------------------------------------------


struct _CmpScalarVisitor[op: Int](ColumnDataVisitorRaises, Copyable, Movable):
    """Element-wise comparison visitor for ``Column._cmp_scalar_op``.

    Like ``_CmpOpVisitor`` but the RHS is a single ``Float64`` scalar rather
    than a full column.  This avoids allocating and immediately discarding a
    length-*n* broadcast list for every scalar comparison.

    ``op`` is one of the ``_CMP_*`` compile-time constants; ``comptime if``
    folds the branch at compile time so each specialisation is a tight loop.
    """

    var self_null_mask: NullMask
    var scalar: Float64
    var result: List[Bool]
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, self_null_mask: NullMask, scalar: Float64):
        self.self_null_mask = self_null_mask.copy()
        self.scalar = scalar
        self.result = List[Bool]()
        self.result_mask = List[Bool]()
        self.has_any_null = False

    def _run_float64(mut self, a: List[Float64]):
        """Inner loop: compare ``a`` against the scalar with null propagation.
        """
        var has_a_mask = self.self_null_mask.has_nulls()
        for i in range(len(a)):
            var is_null = has_a_mask and self.self_null_mask[i]
            if is_null:
                self.result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v: Bool
                comptime if Self.op == _CMP_EQ:
                    v = a[i] == self.scalar
                elif Self.op == _CMP_NE:
                    v = a[i] != self.scalar
                elif Self.op == _CMP_LT:
                    v = a[i] < self.scalar
                elif Self.op == _CMP_LE:
                    v = a[i] <= self.scalar
                elif Self.op == _CMP_GT:
                    v = a[i] > self.scalar
                elif Self.op == _CMP_GE:
                    v = a[i] >= self.scalar
                else:
                    v = False  # unreachable: compile-time guard
                self.result.append(v)
                self.result_mask.append(False)

    def on_int64(mut self, data: List[Int64]) raises:
        var a = List[Float64]()
        for i in range(len(data)):
            a.append(Float64(data[i]))
        self._run_float64(a)

    def on_float64(mut self, data: List[Float64]) raises:
        self._run_float64(data)

    def on_bool(mut self, data: List[Bool]) raises:
        var a = List[Float64]()
        for i in range(len(data)):
            a.append(1.0 if data[i] else 0.0)
        self._run_float64(a)

    def on_str(mut self, data: List[String]) raises:
        raise Error("cmp: cannot compare string column with a numeric scalar")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error(
            "cmp: comparison not supported for object/datetime column type"
        )


struct _CmpScalarInt64Visitor[op: Int](
    ColumnDataVisitorRaises, Copyable, Movable
):
    """Element-wise comparison visitor for ``Column._cmp_scalar_op_int64``.

    Like ``_CmpScalarVisitor`` but the RHS is a single ``Int64`` scalar.
    For ``List[Int64]`` columns the comparison is performed in exact integer
    arithmetic, avoiding the Float64 widening that ``_CmpScalarVisitor``
    applies and the consequent precision loss for values outside the
    ``Float64`` mantissa range (|v| > 2**53).

    For ``List[Float64]`` columns the scalar is widened to ``Float64`` for
    the comparison (which is exact for the common case where the literal
    fits in 53 bits).  ``List[Bool]`` is treated as 0/1 integer values.

    ``op`` is one of the ``_CMP_*`` compile-time constants; ``comptime if``
    folds the branch at compile time so each specialisation is a tight loop.
    """

    var self_null_mask: NullMask
    var scalar: Int64
    var result: List[Bool]
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, self_null_mask: NullMask, scalar: Int64):
        self.self_null_mask = self_null_mask.copy()
        self.scalar = scalar
        self.result = List[Bool]()
        self.result_mask = List[Bool]()
        self.has_any_null = False

    def _run_int64(mut self, a: List[Int64]):
        """Inner loop: compare ``a`` against the Int64 scalar with null propagation.
        """
        var has_a_mask = self.self_null_mask.has_nulls()
        for i in range(len(a)):
            var is_null = has_a_mask and self.self_null_mask[i]
            if is_null:
                self.result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v: Bool
                comptime if Self.op == _CMP_EQ:
                    v = a[i] == self.scalar
                elif Self.op == _CMP_NE:
                    v = a[i] != self.scalar
                elif Self.op == _CMP_LT:
                    v = a[i] < self.scalar
                elif Self.op == _CMP_LE:
                    v = a[i] <= self.scalar
                elif Self.op == _CMP_GT:
                    v = a[i] > self.scalar
                elif Self.op == _CMP_GE:
                    v = a[i] >= self.scalar
                else:
                    v = False  # unreachable: compile-time guard
                self.result.append(v)
                self.result_mask.append(False)

    def _run_float64(mut self, a: List[Float64]):
        """Inner loop: widen scalar to Float64 and compare with null propagation.
        """
        var scalar_f = Float64(self.scalar)
        var has_a_mask = self.self_null_mask.has_nulls()
        for i in range(len(a)):
            var is_null = has_a_mask and self.self_null_mask[i]
            if is_null:
                self.result.append(False)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v: Bool
                comptime if Self.op == _CMP_EQ:
                    v = a[i] == scalar_f
                elif Self.op == _CMP_NE:
                    v = a[i] != scalar_f
                elif Self.op == _CMP_LT:
                    v = a[i] < scalar_f
                elif Self.op == _CMP_LE:
                    v = a[i] <= scalar_f
                elif Self.op == _CMP_GT:
                    v = a[i] > scalar_f
                elif Self.op == _CMP_GE:
                    v = a[i] >= scalar_f
                else:
                    v = False  # unreachable: compile-time guard
                self.result.append(v)
                self.result_mask.append(False)

    def on_int64(mut self, data: List[Int64]) raises:
        self._run_int64(data)

    def on_float64(mut self, data: List[Float64]) raises:
        self._run_float64(data)

    def on_bool(mut self, data: List[Bool]) raises:
        var a = List[Int64]()
        for i in range(len(data)):
            a.append(Int64(1) if data[i] else Int64(0))
        self._run_int64(a)

    def on_str(mut self, data: List[String]) raises:
        raise Error("cmp: cannot compare string column with a numeric scalar")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error(
            "cmp: comparison not supported for object/datetime column type"
        )


# ------------------------------------------------------------------
# Boolean logical visitor — element-wise and/or/xor with Kleene three-valued
# null semantics (per docs/query-eval-spec.md § 3).
#
# Both self and other must be bool_ columns; all other arms raise.
# Kleene short-circuit rules for AND and OR differ from the simple
# "null if either null" propagation used in arithmetic / comparison:
#
#   AND: False AND Null → False   (False absorbs)
#        True  AND Null → Null
#   OR:  True  OR  Null → True    (True absorbs)
#        False OR  Null → Null
#   XOR: standard propagation — null if either operand is null.
# ------------------------------------------------------------------


struct _BoolOpVisitor[op: Int](ColumnDataVisitorRaises, Copyable, Movable):
    """Element-wise boolean logical visitor for ``Column._bool_op``.

    ``op`` is one of the ``_BOOL_*`` compile-time constants.  Both the
    self column and the RHS column must be bool_ dtype; any other arm raises
    immediately.  Null propagation follows Kleene three-valued logic for AND
    and OR, and standard (either-null → null) propagation for XOR.
    """

    var self_null_mask: NullMask
    var other_null_mask: NullMask
    var other_bool: List[Bool]
    var result: List[Bool]
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, self_null_mask: NullMask, other: Column) raises:
        if not other.is_bool():
            raise Error("bool_op: non-bool column type on right-hand side")
        self.self_null_mask = self_null_mask.copy()
        self.other_null_mask = other.null_mask_copy()
        self.other_bool = other._bool_data().copy()
        self.result = List[Bool]()
        self.result_mask = List[Bool]()
        self.has_any_null = False

    def on_bool(mut self, data: List[Bool]) raises:
        var has_a_mask = self.self_null_mask.has_nulls()
        var has_b_mask = self.other_null_mask.has_nulls()
        ref ob = self.other_bool
        for i in range(len(data)):
            var a_null = has_a_mask and self.self_null_mask[i]
            var b_null = has_b_mask and self.other_null_mask[i]
            comptime if Self.op == _BOOL_AND:
                # Kleene AND: False absorbs null
                if a_null:
                    if (not b_null) and (not ob[i]):
                        # Null AND False → False
                        self.result.append(False)
                        self.result_mask.append(False)
                    else:
                        # Null AND True → Null; Null AND Null → Null
                        self.result.append(False)
                        self.result_mask.append(True)
                        self.has_any_null = True
                elif b_null:
                    if not data[i]:
                        # False AND Null → False
                        self.result.append(False)
                        self.result_mask.append(False)
                    else:
                        # True AND Null → Null
                        self.result.append(False)
                        self.result_mask.append(True)
                        self.has_any_null = True
                else:
                    self.result.append(data[i] and ob[i])
                    self.result_mask.append(False)
            elif Self.op == _BOOL_OR:
                # Kleene OR: True absorbs null
                if a_null:
                    if (not b_null) and ob[i]:
                        # Null OR True → True
                        self.result.append(True)
                        self.result_mask.append(False)
                    else:
                        # Null OR False → Null; Null OR Null → Null
                        self.result.append(False)
                        self.result_mask.append(True)
                        self.has_any_null = True
                elif b_null:
                    if data[i]:
                        # True OR Null → True
                        self.result.append(True)
                        self.result_mask.append(False)
                    else:
                        # False OR Null → Null
                        self.result.append(False)
                        self.result_mask.append(True)
                        self.has_any_null = True
                else:
                    self.result.append(data[i] or ob[i])
                    self.result_mask.append(False)
            else:
                # XOR: standard null propagation
                if a_null or b_null:
                    self.result.append(False)
                    self.result_mask.append(True)
                    self.has_any_null = True
                else:
                    self.result.append(
                        (data[i] and not ob[i]) or (not data[i] and ob[i])
                    )
                    self.result_mask.append(False)

    def on_int64(mut self, data: List[Int64]) raises:
        raise Error("bool_op: non-bool column type (got int64)")

    def on_float64(mut self, data: List[Float64]) raises:
        raise Error("bool_op: non-bool column type (got float64)")

    def on_str(mut self, data: List[String]) raises:
        raise Error("bool_op: non-bool column type (got string)")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("bool_op: non-bool column type (got object)")


# Compile-time function type for element-wise Float64 transforms (_apply kernel)
comptime FloatTransformFn = def(Float64) thin -> Float64


# Element-wise FloatTransformFn definitions for _apply[F] (#606)
def _sqrt_fn(v: Float64) -> Float64:
    return sqrt(v)


def _exp_fn(v: Float64) -> Float64:
    return exp(v)


def _log_fn(v: Float64) -> Float64:
    return log(v)


def _log10_fn(v: Float64) -> Float64:
    return log10(v)


def _ceil_fn(v: Float64) -> Float64:
    return ceil(v)


def _floor_fn(v: Float64) -> Float64:
    return floor(v)


def _neg_fn(v: Float64) -> Float64:
    return -v


# ------------------------------------------------------------------
# Shared preamble holder for binary element-wise operations.
# Returned by Column._binary_op_prepare_unchecked; avoids duplicating the
# float64 conversion and null-mask detection across _arith_op and _cmp_op.
# ------------------------------------------------------------------
struct _BinOpInputs(Movable):
    var a: List[Float64]
    var b: List[Float64]
    var has_a_mask: Bool
    var has_b_mask: Bool

    def __init__(
        out self,
        var a: List[Float64],
        var b: List[Float64],
        has_a_mask: Bool,
        has_b_mask: Bool,
    ):
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
    var src_null_mask: NullMask
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(
        out self,
        indices: List[Int],
        fill_value: Optional[DFScalar],
        src_null_mask: NullMask,
    ):
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
                fill = fv[Int64]
                is_null_fill = False
            elif fv.isa[Float64]():
                fill = Int64(Int(fv[Float64]))
                is_null_fill = False
            elif fv.isa[Bool]():
                fill = Int64(1) if fv[Bool] else Int64(0)
                is_null_fill = False
        var has_src_mask = self.src_null_mask.has_nulls()
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
                fill = fv[Float64]
                is_null_fill = False
            elif fv.isa[Int64]():
                fill = Float64(fv[Int64])
                is_null_fill = False
            elif fv.isa[Bool]():
                fill = 1.0 if fv[Bool] else 0.0
                is_null_fill = False
        var has_src_mask = self.src_null_mask.has_nulls()
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
                fill = fv[Bool]
                is_null_fill = False
            elif fv.isa[Int64]():
                fill = fv[Int64] != 0
                is_null_fill = False
        var has_src_mask = self.src_null_mask.has_nulls()
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
                fill = fv[String]
                is_null_fill = False
        var has_src_mask = self.src_null_mask.has_nulls()
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
                fill = PythonObject(fv[String])
                is_null_fill = False
            elif fv.isa[Int64]():
                fill = PythonObject(Int(fv[Int64]))
                is_null_fill = False
            elif fv.isa[Float64]():
                fill = PythonObject(fv[Float64])
                is_null_fill = False
            elif fv.isa[Bool]():
                fill = PythonObject(fv[Bool])
                is_null_fill = False
        var has_src_mask = self.src_null_mask.has_nulls()
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


# ------------------------------------------------------------------
# Single-cell extraction visitors
# ------------------------------------------------------------------


struct _SeriesScalarAtVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that extracts one cell at *row* as a ``SeriesScalar``.

    Does not check the null mask — callers must check before dispatching.
    """

    var row: Int
    var result: SeriesScalar

    def __init__(out self, row: Int):
        self.row = row
        self.result = SeriesScalar(Int64(0))

    def on_int64(mut self, data: List[Int64]) raises:
        self.result = SeriesScalar(data[self.row])

    def on_float64(mut self, data: List[Float64]) raises:
        self.result = SeriesScalar(data[self.row])

    def on_bool(mut self, data: List[Bool]) raises:
        self.result = SeriesScalar(data[self.row])

    def on_str(mut self, data: List[String]) raises:
        self.result = SeriesScalar(data[self.row])

    def on_obj(mut self, data: List[PythonObject]) raises:
        self.result = SeriesScalar(data[self.row])


struct _CellToPyObjVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that extracts one cell at *row* as a ``PythonObject``.

    Does not check the null mask — callers that need null handling must check
    before dispatching (see ``_col_cell_pyobj``).
    """

    var row: Int
    var result: PythonObject

    def __init__(out self, row: Int):
        self.row = row
        self.result = PythonObject(0)

    def on_int64(mut self, data: List[Int64]) raises:
        self.result = PythonObject(Int(data[self.row]))

    def on_float64(mut self, data: List[Float64]) raises:
        self.result = PythonObject(data[self.row])

    def on_bool(mut self, data: List[Bool]) raises:
        self.result = PythonObject(data[self.row])

    def on_str(mut self, data: List[String]) raises:
        self.result = PythonObject(data[self.row])

    def on_obj(mut self, data: List[PythonObject]) raises:
        self.result = data[self.row]


struct _CellToStrVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that extracts one cell at *row* as a ``String``.

    Does not check the null mask — callers that need null handling must check
    before dispatching (see ``_col_cell_str``).
    """

    var row: Int
    var result: String

    def __init__(out self, row: Int):
        self.row = row
        self.result = String("")

    def on_int64(mut self, data: List[Int64]) raises:
        self.result = String(Int(data[self.row]))

    def on_float64(mut self, data: List[Float64]) raises:
        self.result = String(data[self.row])

    def on_bool(mut self, data: List[Bool]) raises:
        self.result = String("True") if data[self.row] else String("False")

    def on_str(mut self, data: List[String]) raises:
        self.result = data[self.row]

    def on_obj(mut self, data: List[PythonObject]) raises:
        self.result = String(data[self.row])


struct _ScalarFromColVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that extracts one cell at *row* as a ``DFScalar``.

    Does not check the null mask — callers that need null handling must check
    before dispatching (see ``_scalar_from_col``).
    ``List[PythonObject]`` cells are stringified since ``DFScalar`` has no
    ``PythonObject`` arm.
    """

    var row: Int
    var result: DFScalar

    def __init__(out self, row: Int):
        self.row = row
        self.result = DFScalar.null()

    def on_int64(mut self, data: List[Int64]) raises:
        self.result = DFScalar(data[self.row])

    def on_float64(mut self, data: List[Float64]) raises:
        self.result = DFScalar(data[self.row])

    def on_bool(mut self, data: List[Bool]) raises:
        self.result = DFScalar(data[self.row])

    def on_str(mut self, data: List[String]) raises:
        self.result = DFScalar(data[self.row])

    def on_obj(mut self, data: List[PythonObject]) raises:
        self.result = DFScalar(String(data[self.row]))


# ------------------------------------------------------------------
# Cumulative operation visitors
# ------------------------------------------------------------------


struct _CumSumVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that computes the cumulative sum of the active ColumnData arm.

    Stores ``col_data``, ``result_mask``, and ``has_any_null`` for use with
    ``_build_result_col``.  Integer input produces an Int64 result; Float64
    and Bool input produce Float64.
    """

    var skipna: Bool
    var null_mask: NullMask
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, skipna: Bool, null_mask: NullMask):
        self.skipna = skipna
        self.null_mask = null_mask.copy()
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var running = Int64(0)
        var result_data = List[Int64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                running += data[i]
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var running = Float64(0)
        var nan = Float64(0) / Float64(0)
        var result_data = List[Float64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                running += data[i]
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var running = Float64(0)
        var nan = Float64(0) / Float64(0)
        var result_data = List[Float64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                running += Float64(1.0) if data[i] else Float64(0.0)
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_str(mut self, data: List[String]) raises:
        raise Error("cumsum: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("cumsum: non-numeric column type")


struct _CumProdVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that computes the cumulative product of the active ColumnData arm.

    Stores ``col_data``, ``result_mask``, and ``has_any_null`` for use with
    ``_build_result_col``.  Integer input produces an Int64 result; Float64
    and Bool input produce Float64.
    """

    var skipna: Bool
    var null_mask: NullMask
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, skipna: Bool, null_mask: NullMask):
        self.skipna = skipna
        self.null_mask = null_mask.copy()
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var running = Int64(1)
        var result_data = List[Int64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                running *= data[i]
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var running = Float64(1)
        var nan = Float64(0) / Float64(0)
        var result_data = List[Float64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                running *= data[i]
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var running = Float64(1)
        var nan = Float64(0) / Float64(0)
        var result_data = List[Float64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                running *= Float64(1.0) if data[i] else Float64(0.0)
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_str(mut self, data: List[String]) raises:
        raise Error("cumprod: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("cumprod: non-numeric column type")


struct _CumMinVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that computes the cumulative minimum of the active ColumnData arm.

    Stores ``col_data``, ``result_mask``, and ``has_any_null`` for use with
    ``_build_result_col``.  Integer input produces an Int64 result; Float64
    and Bool input produce Float64.
    """

    var skipna: Bool
    var null_mask: NullMask
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, skipna: Bool, null_mask: NullMask):
        self.skipna = skipna
        self.null_mask = null_mask.copy()
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var found = False
        var running = Int64(0)
        var result_data = List[Int64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v = data[i]
                if not found or v < running:
                    running = v
                    found = True
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var found = False
        var running = Float64(0)
        var nan = Float64(0) / Float64(0)
        var result_data = List[Float64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v = data[i]
                if not found or v < running:
                    running = v
                    found = True
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var found = False
        var running = Float64(0)
        var nan = Float64(0) / Float64(0)
        var result_data = List[Float64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v = Float64(1.0) if data[i] else Float64(0.0)
                if not found or v < running:
                    running = v
                    found = True
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_str(mut self, data: List[String]) raises:
        raise Error("cummin: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("cummin: non-numeric column type")


struct _CumMaxVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that computes the cumulative maximum of the active ColumnData arm.

    Stores ``col_data``, ``result_mask``, and ``has_any_null`` for use with
    ``_build_result_col``.  Integer input produces an Int64 result; Float64
    and Bool input produce Float64.
    """

    var skipna: Bool
    var null_mask: NullMask
    var col_data: ColumnData
    var result_mask: List[Bool]
    var has_any_null: Bool

    def __init__(out self, skipna: Bool, null_mask: NullMask):
        self.skipna = skipna
        self.null_mask = null_mask.copy()
        self.col_data = ColumnData(List[PythonObject]())
        self.result_mask = List[Bool]()
        self.has_any_null = False

    def on_int64(mut self, data: List[Int64]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var found = False
        var running = Int64(0)
        var result_data = List[Int64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(Int64(0))
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v = data[i]
                if not found or v > running:
                    running = v
                    found = True
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_float64(mut self, data: List[Float64]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var found = False
        var running = Float64(0)
        var nan = Float64(0) / Float64(0)
        var result_data = List[Float64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v = data[i]
                if not found or v > running:
                    running = v
                    found = True
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var propagate_nan = False
        var found = False
        var running = Float64(0)
        var nan = Float64(0) / Float64(0)
        var result_data = List[Float64]()
        for i in range(len(data)):
            var is_null = has_mask and self.null_mask[i]
            if is_null:
                if not self.skipna:
                    propagate_nan = True
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                self.result_mask.append(True)
                self.has_any_null = True
            else:
                var v = Float64(1.0) if data[i] else Float64(0.0)
                if not found or v > running:
                    running = v
                    found = True
                result_data.append(running)
                self.result_mask.append(False)
        self.col_data = ColumnData(result_data^)

    def on_str(mut self, data: List[String]) raises:
        raise Error("cummax: non-numeric column type")

    def on_obj(mut self, data: List[PythonObject]) raises:
        raise Error("cummax: non-numeric column type")


# ------------------------------------------------------------------
# Index conversion visitor
# ------------------------------------------------------------------


struct _ToColumnIndexVisitor(ColumnDataVisitorRaises, Copyable, Movable):
    """Visitor that converts the active ColumnData arm to a ``ColumnIndex``.

    Int64 → ``List[Int64]`` ColumnIndex; String → ``Index`` (List[String])
    ColumnIndex; all other types → ``List[PythonObject]`` ColumnIndex,
    respecting the null mask.
    """

    var null_mask: NullMask
    var result: ColumnIndex

    def __init__(out self, null_mask: NullMask):
        self.null_mask = null_mask.copy()
        self.result = ColumnIndex(List[PythonObject]())

    def on_int64(mut self, data: List[Int64]) raises:
        var result = List[Int64]()
        for i in range(len(data)):
            result.append(data[i])
        self.result = ColumnIndex(result^)

    def on_float64(mut self, data: List[Float64]) raises:
        # Null positions become NaN so that a float64 index preserves null
        # semantics (consistent with how pandas represents NaN in a Float64Index).
        var has_mask = len(self.null_mask) > 0
        var nan = Float64(0) / Float64(0)
        var result = List[Float64]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(nan)
            else:
                result.append(data[i])
        self.result = ColumnIndex(result^)

    def on_bool(mut self, data: List[Bool]) raises:
        var has_mask = len(self.null_mask) > 0
        var py_none = Python.evaluate("None")
        var result = List[PythonObject]()
        for i in range(len(data)):
            if has_mask and self.null_mask[i]:
                result.append(py_none)
            else:
                result.append(PythonObject(data[i]))
        self.result = ColumnIndex(result^)

    def on_str(mut self, data: List[String]) raises:
        var result = List[String]()
        for i in range(len(data)):
            result.append(data[i])
        self.result = ColumnIndex(Index(result^))

    def on_obj(mut self, data: List[PythonObject]) raises:
        var result = List[PythonObject]()
        for i in range(len(data)):
            result.append(data[i])
        self.result = ColumnIndex(result^)


# ------------------------------------------------------------------
# Generic merge-sort permutation helpers
# ------------------------------------------------------------------
# These two functions contain the single canonical bottom-up merge-sort
# loop.  They replace the nine near-identical copy-pasted loops that used
# to live inside Series._sort_perm (5 type arms) and
# Column._sort_perm_by_index (4 type arms).
#
# _merge_sort_perm_comparable  — Int64, Float64, Bool, String (native </>)
# _merge_sort_perm_pyobj       — PythonObject (Python comparison, raises)
#
# Both accept an optional null_mask.  Pass NullMask() and na_last=True
# when null handling is not required (e.g. for index sorts).


def _merge_sort_perm_comparable[
    T: Comparable & Copyable & Movable
](
    mut perm: List[Int],
    data: List[T],
    null_mask: NullMask,
    ascending: Bool,
    na_last: Bool,
) raises:
    """Bottom-up merge-sort permutation using the native ``<`` / ``>`` operator.

    Shared by the Int64, Float64, Bool, and String data arms of
    ``Series._sort_perm`` and the Index (string), Int64, and Float64 index
    arms of ``Column._sort_perm_by_index``.

    For Bool data, Mojo's ``Bool.__lt__`` follows ``False < True`` (i.e.
    ``False`` sorts before ``True`` in ascending order), which is the same
    ordering as the original explicit ``(not d[rv]) and d[lv]`` expression.

    Pass an empty ``null_mask`` and ``na_last=True`` when null handling is
    not required (e.g. for index sorts where no index label can be null).
    """
    var n = len(perm)
    if n <= 1:
        return
    var has_mask = len(null_mask) > 0
    var scratch = List[Int](capacity=n)
    scratch.resize(n, 0)
    var width = 1
    while width < n:
        var lo = 0
        while lo < n:
            var mid_idx = lo + width
            if mid_idx >= n:
                break
            var hi = lo + 2 * width
            if hi > n:
                hi = n
            var k = lo
            var li = lo
            var ri = mid_idx
            while li < mid_idx and ri < hi:
                var lv = perm[li]
                var rv = perm[ri]
                var lnull = has_mask and null_mask[lv]
                var rnull = has_mask and null_mask[rv]
                var take_right: Bool
                if lnull and rnull:
                    take_right = False
                elif lnull:
                    take_right = na_last
                elif rnull:
                    take_right = not na_last
                elif ascending:
                    take_right = data[rv] < data[lv]
                else:
                    take_right = data[rv] > data[lv]
                if take_right:
                    scratch[k] = rv
                    ri += 1
                else:
                    scratch[k] = lv
                    li += 1
                k += 1
            while li < mid_idx:
                scratch[k] = perm[li]
                li += 1
                k += 1
            while ri < hi:
                scratch[k] = perm[ri]
                ri += 1
                k += 1
            for j in range(lo, hi):
                perm[j] = scratch[j]
            lo += 2 * width
        width *= 2


def _merge_sort_perm_pyobj(
    mut perm: List[Int],
    data: List[PythonObject],
    null_mask: NullMask,
    ascending: Bool,
    na_last: Bool,
) raises:
    """Bottom-up merge-sort permutation using Python ``<`` / ``>`` comparison.

    Used for the PythonObject data arm of ``Series._sort_perm`` and the
    PythonObject index arm of ``Column._sort_perm_by_index``.
    """
    var n = len(perm)
    if n <= 1:
        return
    var has_mask = len(null_mask) > 0
    var scratch = List[Int](capacity=n)
    scratch.resize(n, 0)
    var width = 1
    while width < n:
        var lo = 0
        while lo < n:
            var mid_idx = lo + width
            if mid_idx >= n:
                break
            var hi = lo + 2 * width
            if hi > n:
                hi = n
            var k = lo
            var li = lo
            var ri = mid_idx
            while li < mid_idx and ri < hi:
                var lv = perm[li]
                var rv = perm[ri]
                var lnull = has_mask and null_mask[lv]
                var rnull = has_mask and null_mask[rv]
                var take_right: Bool
                if lnull and rnull:
                    take_right = False
                elif lnull:
                    take_right = na_last
                elif rnull:
                    take_right = not na_last
                elif ascending:
                    take_right = Bool(data[rv] < data[lv])
                else:
                    take_right = Bool(data[rv] > data[lv])
                if take_right:
                    scratch[k] = rv
                    ri += 1
                else:
                    scratch[k] = lv
                    li += 1
                k += 1
            while li < mid_idx:
                scratch[k] = perm[li]
                li += 1
                k += 1
            while ri < hi:
                scratch[k] = perm[ri]
                ri += 1
                k += 1
            for j in range(lo, hi):
                perm[j] = scratch[j]
            lo += 2 * width
        width *= 2


struct NullMask(Copyable, Movable, Sized):
    """Tracks which elements of a ``Column`` are null/NaN.

    Backed by marrow's bit-packed ``BitmapBuilder`` (1 bit per element)
    rather than a ``List[Bool]``, for an 8x reduction in null-tracking
    memory.  An empty mask (``_length == 0`` or ``_has_nulls == False``)
    means no nulls are present and ``has_nulls()`` returns ``False``.

    Bit semantics are bison-native: a *set* bit means *null*, a *cleared*
    bit means *valid*.  This matches today's ``List[Bool]`` semantics
    (``True`` = null) and lets the common "no nulls" case skip the cost
    of writing any bits, since ``BitmapBuilder.alloc()`` is zero-filled.
    The Arrow-style inversion to validity bits happens at the conversion
    boundary in ``arrow.mojo``.

    ``BitmapBuilder`` is ``Movable`` only, so the manual ``copy``/``take``
    initialisers below are required for ``NullMask`` to satisfy the
    ``Copyable`` trait that ``Column`` relies on.

    The builder is always allocated to at least one bit because nightly
    Mojo's ``UnsafePointer.alloc(0, alignment=64)`` aborts on a null
    return; ``BitmapBuilder.alloc(_EMPTY_ALLOC_BITS)`` produces a real
    64-byte aligned allocation that's safe to free and never read from
    on the no-nulls fast path.

    Subscript access (``mask[i]`` / ``mask[i] = v``) and ``len(mask)`` are
    supported directly so that call sites can be migrated incrementally.
    Prefer ``is_null`` / ``is_valid`` / ``has_nulls`` over raw subscripting.
    """

    comptime _EMPTY_ALLOC_BITS = 1

    # Always-allocated bit-packed buffer.  When ``_capacity == 0`` the
    # builder owns a sentinel allocation but its bytes are not read.
    var _builder: Bitmap[mut=True]
    # Logical number of entries tracked (matches today's ``len`` semantics).
    var _length: Int
    # Number of bits the builder can currently hold (>= _length when set).
    var _capacity: Int
    # Cached "any bit set" flag — preserves the existing fast path where
    # ``has_nulls()`` short-circuits on the no-nulls case.
    var _has_nulls: Bool

    def __init__(out self):
        """Empty mask — no nulls."""
        self._builder = Bitmap.alloc_zeroed(Self._EMPTY_ALLOC_BITS)
        self._length = 0
        self._capacity = 0
        self._has_nulls = False

    @staticmethod
    def from_list(read mask: List[Bool]) -> Self:
        """Explicit conversion from ``List[Bool]``.

        Used by ``_build_result_col`` and other internal call sites that still
        produce ``List[Bool]`` masks (e.g. binary-op visitors).  Unlike the
        deleted ``@implicit`` bridge constructor, this is an explicit call so
        it won't silently convert stray ``List[Bool]`` assignments.
        """
        var n = len(mask)
        var any_null = False
        for i in range(n):
            if mask[i]:
                any_null = True
                break
        var result = NullMask()
        result._length = n
        result._has_nulls = any_null
        if any_null:
            result._capacity = n
            result._builder = Bitmap.alloc_zeroed(n)
            for i in range(n):
                if mask[i]:
                    result._builder.unsafe_set(i)
        else:
            result._capacity = 0
            result._builder = Bitmap.alloc_zeroed(Self._EMPTY_ALLOC_BITS)
        return result^

    def __init__(out self, *, copy: Self):
        self._length = copy._length
        self._capacity = copy._capacity
        self._has_nulls = copy._has_nulls
        # Allocate the final size directly — avoids calling the raising
        # ``BitmapBuilder.resize`` from this non-raising copy constructor.
        # Floor at ``_EMPTY_ALLOC_BITS`` so we never hit nightly Mojo's
        # zero-size alloc abort.
        var alloc_bits = (
            copy._capacity if copy._capacity > 0 else Self._EMPTY_ALLOC_BITS
        )
        self._builder = Bitmap.alloc_zeroed(alloc_bits)
        if copy._capacity > 0 and copy._has_nulls and copy._length > 0:
            self._builder.extend(
                copy._builder.view(0, copy._length), 0, copy._length
            )

    def __init__(out self, *, deinit take: Self):
        self._builder = take._builder^
        self._length = take._length
        self._capacity = take._capacity
        self._has_nulls = take._has_nulls

    def _ensure_capacity(mut self, min_bits: Int) raises:
        """Grow ``_builder`` so it can hold at least ``min_bits`` bits.

        New bytes are zero-filled by ``BufferBuilder.resize`` (and by the
        initial ``alloc``), so newly tracked entries default to *valid*.
        """
        if min_bits <= self._capacity:
            return
        var new_cap = self._capacity * 2 if self._capacity > 0 else 64
        if new_cap < min_bits:
            new_cap = min_bits
        self._builder.resize(new_cap)
        self._capacity = new_cap

    @always_inline
    def _bit_at(self, index: Int) -> Bool:
        return self._builder.unsafe_test(index)

    def __len__(self) -> Int:
        return self._length

    def __getitem__(self, index: Int) -> Bool:
        return self.is_null(index)

    def append(mut self, value: Bool) raises:
        """Append a null (``True``) or valid (``False``) entry to the mask."""
        self._length += 1
        if value:
            self._ensure_capacity(self._length)
            self._builder.unsafe_set(self._length - 1)
            self._has_nulls = True
        elif self._has_nulls:
            # Already allocated — must explicitly write a 0 bit so a
            # previously-set bit at this index doesn't leak through after
            # a resize.  ``resize`` zero-fills new bytes so growing alone
            # is safe; this only matters when ``_length`` was reduced and
            # is now growing back into reused capacity, but we keep the
            # branch defensive.
            if self._length <= self._capacity:
                self._builder.unsafe_clear(self._length - 1)

    def append_null(mut self) raises:
        """Append a null marker to the mask."""
        self.append(True)

    def append_valid(mut self) raises:
        """Append a valid marker to the mask."""
        self.append(False)

    def has_nulls(self) -> Bool:
        """Return ``True`` if any element is marked null."""
        return self._has_nulls

    def is_null(self, index: Int) -> Bool:
        """Return ``True`` if the element at *index* is null.

        Returns ``False`` when the mask is empty or *index* is out of bounds.
        """
        if not self._has_nulls or index < 0 or index >= self._length:
            return False
        return self._bit_at(index)

    def is_valid(self, index: Int) -> Bool:
        """Return ``True`` if the element at *index* is not null."""
        return not self.is_null(index)

    def set_null(mut self, index: Int) raises:
        """Mark the element at *index* as null."""
        self._ensure_capacity(index + 1)
        self._builder.unsafe_set(index)
        self._has_nulls = True

    def set_valid(mut self, index: Int):
        """Mark the element at *index* as valid (not null)."""
        if not self._has_nulls:
            return
        self._builder.unsafe_clear(index)

    def len(self) -> Int:
        """Return the number of elements tracked by this mask."""
        return self._length

    def count_valid(self) -> Int:
        """Return the number of non-null elements."""
        if not self._has_nulls:
            return self._length
        var count = 0
        for i in range(self._length):
            if not self._bit_at(i):
                count += 1
        return count

    @staticmethod
    def all_valid(n: Int) -> Self:
        """Return a mask of length *n* with all elements valid (not null)."""
        var m = Self()
        m._length = n
        return m^

    @staticmethod
    def all_null(n: Int) raises -> Self:
        """Return a mask of length *n* with all elements null."""
        var m = Self()
        m._length = n
        if n > 0:
            m._ensure_capacity(n)
            m._builder.set_range(0, n, True)
            m._has_nulls = True
        return m^


struct Column(Copyable, ImplicitlyCopyable, Movable, Sized):
    """A single typed array representing one column of a DataFrame or a Series.

    Data is stored in ``_storage``: an ``AnyArray`` (marrow-backed) for
    int64 / float64 / bool / string columns, or a ``LegacyObjectData``
    wrapper for object / datetime / timedelta columns (and for
    string-with-nulls, which marrow cannot build today).  The ``dtype``
    field records the pandas-compatible dtype string so that round-trips
    through ``to_pandas`` preserve the original dtype.

    Null tracking is handled by the ``_storage`` Variant.  ``AnyArray``
    columns store nulls in the Arrow validity bitmap; ``LegacyObjectData``
    columns carry a ``NullMask`` inside the struct.
    """

    var name: Optional[String]
    var dtype: BisonDtype
    var _index: ColumnIndex
    # Level names for a multi-key index set via DataFrame.set_index.
    # Empty when the index is a single-key or default RangeIndex.
    var _index_names: List[String]
    # The axis/index name (pandas index.name).  Empty string = no name.
    # Set by from_pandas and written back in to_pandas.  Also updated by
    # DataFrame.rename_axis / Series.rename_axis.
    var _index_name: String
    # ------------------------------------------------------------------
    # Storage (#619 / post-#658)
    # ------------------------------------------------------------------
    # Every constructor populates ``_storage`` with a concrete arm
    # (``AnyArray`` or ``LegacyObjectData``).  Readers dispatch directly
    # on the active arm via typed accessors (``_int64_list``,
    # ``_float64_list``, ``_bool_list``, ``_str_list``) which extract a
    # fresh List[T] from the underlying marrow array on demand.
    var _storage: ColumnStorage

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    def __init__(out self):
        """Empty column with object dtype — used as stub placeholder."""
        self.name = None
        self.dtype = object_
        self._index = ColumnIndex(List[PythonObject]())

        self._index_names = List[String]()
        self._index_name = String("")
        # Empty LegacyObjectData arm — coherent empty object column.
        self._storage = ColumnStorage(LegacyObjectData())

    def __init__(
        out self,
        name: Optional[String],
        var data: ColumnData,
        dtype: BisonDtype,
    ):
        self.name = name
        self.dtype = dtype
        self._index = ColumnIndex(List[PythonObject]())

        self._index_names = List[String]()
        self._index_name = String("")
        self._storage = ColumnStorage(LegacyObjectData())
        # Build _storage directly from ColumnData.  For non-object arms
        # (int64/float64/bool/string) we build a marrow AnyArray; for
        # object / datetime / timedelta we fall back to LegacyObjectData.
        _init_storage_from_column_data(self, data^)

    def __init__(
        out self,
        name: Optional[String],
        var data: ColumnData,
        dtype: BisonDtype,
        var index: ColumnIndex,
    ):
        self.name = name
        self.dtype = dtype
        self._index = index^

        self._index_names = List[String]()
        self._index_name = String("")
        self._storage = ColumnStorage(LegacyObjectData())
        _init_storage_from_column_data(self, data^)

    def __init__(
        out self,
        name: Optional[String],
        var storage: AnyArray,
        dtype: BisonDtype,
    ):
        self.name = name
        self.dtype = dtype
        self._index = ColumnIndex(List[PythonObject]())
        self._index_names = List[String]()
        self._index_name = String("")
        self._storage = ColumnStorage(storage^)

    # ------------------------------------------------------------------
    # Typed-list constructor overloads — let callers pass typed lists
    # directly without wrapping in ColumnData(). Each delegates to the
    # canonical ColumnData-taking constructors above.
    # ------------------------------------------------------------------

    def __init__(
        out self,
        name: Optional[String],
        var data: List[Int64],
        dtype: BisonDtype,
    ):
        self = Self(name, ColumnData(data^), dtype)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[Int64],
        dtype: BisonDtype,
        var index: ColumnIndex,
    ):
        self = Self(name, ColumnData(data^), dtype, index^)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[Float64],
        dtype: BisonDtype,
    ):
        self = Self(name, ColumnData(data^), dtype)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[Float64],
        dtype: BisonDtype,
        var index: ColumnIndex,
    ):
        self = Self(name, ColumnData(data^), dtype, index^)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[Bool],
        dtype: BisonDtype,
    ):
        self = Self(name, ColumnData(data^), dtype)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[Bool],
        dtype: BisonDtype,
        var index: ColumnIndex,
    ):
        self = Self(name, ColumnData(data^), dtype, index^)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[String],
        dtype: BisonDtype,
    ):
        # #644: List[String] columns always carry string_ dtype. The dtype
        # parameter is preserved for caller compatibility but ignored here
        # to keep the invariant dtype == string_ ⟺ string storage arm.
        _ = dtype
        self = Self(name, ColumnData(data^), string_)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[String],
        dtype: BisonDtype,
        var index: ColumnIndex,
    ):
        # #644: see note above — dtype arg is ignored, forced to string_.
        _ = dtype
        self = Self(name, ColumnData(data^), string_, index^)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[PythonObject],
        dtype: BisonDtype,
    ):
        self = Self(name, ColumnData(data^), dtype)

    def __init__(
        out self,
        name: Optional[String],
        var data: List[PythonObject],
        dtype: BisonDtype,
        var index: ColumnIndex,
    ):
        self = Self(name, ColumnData(data^), dtype, index^)

    # ------------------------------------------------------------------
    # Traits
    # NOTE: ColumnIndex is a Variant type. Nightly Mojo no longer allows
    # implicit copies of Variant, so it requires explicit .copy() calls.
    # ------------------------------------------------------------------

    def __init__(out self, *, copy: Self):
        self.name = copy.name
        self.dtype = copy.dtype
        self._index = copy._index.copy()
        self._index_names = copy._index_names.copy()
        self._index_name = copy._index_name
        self._storage = copy._storage.copy()

    def __init__(out self, *, deinit take: Self):
        self.name = take.name^
        self.dtype = take.dtype^
        self._index = take._index^
        self._index_names = take._index_names^
        self._index_name = take._index_name^
        self._storage = take._storage^

    # ------------------------------------------------------------------
    # Typed list accessors — extract a fresh ``List[T]`` from the active
    # ``AnyArray`` arm of ``_storage``.  These are the canonical read API
    # for per-cell access.  Each allocates a fresh list (O(n)); callers
    # that only need a single cell should use ``_int64_array()`` /
    # ``_float64_array()`` / ``_bool_array()`` / ``_string_array()`` plus
    # ``unsafe_get`` for O(1) access.
    # ------------------------------------------------------------------

    def _int64_list(self) raises -> List[Int64]:
        """Extract column values as ``List[Int64]``.

        Requires int64 ``AnyArray`` storage; raises otherwise.
        """
        if (
            not self._storage.isa[AnyArray]()
            or self._storage[AnyArray].dtype() != _m_int64
        ):
            raise Error("Column._int64_list: requires int64 AnyArray storage")
        ref a = self._storage[AnyArray].as_int64()
        var n = a.length
        var out = List[Int64](capacity=n)
        for i in range(n):
            out.append(rebind[Int64](a.unsafe_get(i)))
        return out^

    def _float64_list(self) raises -> List[Float64]:
        """Extract column values as ``List[Float64]``.

        Requires float64 ``AnyArray`` storage; raises otherwise.  Use
        ``_f64_list`` to widen int64 / bool columns to Float64.
        """
        if (
            not self._storage.isa[AnyArray]()
            or self._storage[AnyArray].dtype() != _m_float64
        ):
            raise Error(
                "Column._float64_list: requires float64 AnyArray storage"
            )
        ref a = self._storage[AnyArray].as_float64()
        var n = a.length
        var out = List[Float64](capacity=n)
        for i in range(n):
            out.append(rebind[Float64](a.unsafe_get(i)))
        return out^

    def _bool_list(self) raises -> List[Bool]:
        """Extract column values as ``List[Bool]``.

        Requires bool ``AnyArray`` storage; raises otherwise.
        """
        if (
            not self._storage.isa[AnyArray]()
            or self._storage[AnyArray].dtype() != _m_bool_
        ):
            raise Error("Column._bool_list: requires bool AnyArray storage")
        ref a = self._storage[AnyArray].as_bool()
        var n = a.length
        var out = List[Bool](capacity=n)
        var view = a.values()
        for i in range(n):
            out.append(view.test(a.offset + i))
        return out^

    def _str_list(self) raises -> List[String]:
        """Extract column values as ``List[String]``.

        Prefers string ``AnyArray`` storage; falls back to
        ``LegacyObjectData.data`` (wrapped PythonObject strings) for
        string-with-nulls columns where marrow cannot build a
        string + null array.
        """
        if self._storage.isa[AnyArray]():
            if self._storage[AnyArray].dtype() != _m_string:
                raise Error(
                    "Column._str_list: requires string AnyArray storage"
                )
            ref a = self._storage[AnyArray].as_string()
            var n = a.length
            var out = List[String](capacity=n)
            for i in range(n):
                out.append(String(a.unsafe_get(UInt(i))))
            return out^
        if self.is_string():
            ref d = self._storage[LegacyObjectData].data
            var out = List[String](capacity=len(d))
            for i in range(len(d)):
                out.append(String(d[i]))
            return out^
        raise Error("Column._str_list: requires string column")

    def _obj_list(self) raises -> List[PythonObject]:
        """Extract object column values as ``List[PythonObject]``.

        Requires ``LegacyObjectData`` storage; raises otherwise.
        """
        if not self._storage.isa[LegacyObjectData]():
            raise Error("Column._obj_list: requires LegacyObjectData storage")
        return self._storage[LegacyObjectData].data.copy()

    def _f64_list(self) raises -> List[Float64]:
        """Extract column values as ``List[Float64]``, widening int64 / bool
        to float64.

        Columns must have an ``AnyArray`` arm; raises otherwise.
        """
        if not self._storage.isa[AnyArray]():
            raise Error(
                "Column._f64_list: requires numeric/bool AnyArray storage"
            )
        ref arr = self._storage[AnyArray]
        var dt = arr.dtype()
        var n = arr.length()
        var out = List[Float64](capacity=n)
        if dt == _m_float64:
            ref a = arr.as_float64()
            for i in range(n):
                out.append(rebind[Float64](a.unsafe_get(i)))
        elif dt == _m_int64:
            ref a = arr.as_int64()
            for i in range(n):
                out.append(Float64(rebind[Int64](a.unsafe_get(i))))
        elif dt == _m_bool_:
            ref a = arr.as_bool()
            var view = a.values()
            for i in range(n):
                out.append(1.0 if view.test(a.offset + i) else 0.0)
        else:
            raise Error("Column._f64_list: non-numeric dtype")
        return out^

    # ------------------------------------------------------------------
    # Backward-compatible aliases used throughout the codebase.  These
    # return a fresh list each call — old call sites that used ``ref``
    # against the cache must now use ``var``.
    # ------------------------------------------------------------------

    def _int64_data(self) raises -> List[Int64]:
        return self._int64_list()

    def _float64_data(self) raises -> List[Float64]:
        return self._float64_list()

    def _bool_data(self) raises -> List[Bool]:
        return self._bool_list()

    def _str_data(self) raises -> List[String]:
        return self._str_list()

    # _obj_data() removed in #656 Part 2b — callers now use
    # _storage_legacy().data directly.

    def _to_col_data(self) raises -> ColumnData:
        """Build a ColumnData from the active storage arm.

        Used by visitor structs that hold a ``ColumnData`` operand (e.g.
        ``_ConcatDataVisitor``, ``_CombineFirstVisitor``).
        """
        if self.is_int():
            return ColumnData(self._int64_list())
        if self.is_float():
            return ColumnData(self._float64_list())
        if self.is_bool():
            return ColumnData(self._bool_list())
        if self.is_string():
            return ColumnData(self._str_list())
        if self._storage.isa[LegacyObjectData]():
            return ColumnData(self._storage[LegacyObjectData].data.copy())
        return ColumnData(List[PythonObject]())

    @staticmethod
    def _caches_only(
        name: Optional[String],
        dtype: BisonDtype,
        var int64_data: List[Int64],
        var f64_data: List[Float64],
        var bool_data: List[Bool],
        var str_data: List[String],
    ) raises -> Column:
        """Build a Column from a single active typed payload.

        Legacy helper — the typed caches are gone, so this just builds a
        regular Column from whichever payload is non-empty.  Preserved
        for callers still expecting the four-list signature.
        """
        if dtype == int64:
            return Column(name, ColumnData(int64_data^), dtype)
        if dtype == float64:
            return Column(name, ColumnData(f64_data^), dtype)
        if dtype == bool_:
            return Column(name, ColumnData(bool_data^), dtype)
        if dtype == string_:
            return Column(name, ColumnData(str_data^), dtype)
        # Object / datetime / timedelta — caller should not be using this
        # path.  Return an empty object column.
        return Column()

    @staticmethod
    def _caches_only_from_col_data(
        name: Optional[String],
        dtype: BisonDtype,
        var result_data: ColumnData,
    ) raises -> Column:
        """Build a Column from a ``ColumnData`` value.

        Legacy helper — the typed caches are gone, so this just delegates
        to the regular Column constructor.
        """
        return Column(name, result_data^, dtype)

    # ------------------------------------------------------------------
    # Typed-array accessor helpers — read from the ``AnyArray`` arm of
    # ``_storage``.  Callers must verify ``_storage.isa[AnyArray]`` and
    # the matching dtype before calling.  Each ``_*_array`` accessor
    # returns a copy of the typed marrow array held inside the storage
    # Variant (copies are O(1) ref-bumps on the underlying ArcPointer).
    # ------------------------------------------------------------------

    def _storage_any_array(self) -> AnyArray:
        """Return a copy of the AnyArray arm of the storage Variant.

        Caller must verify ``_storage.isa[AnyArray]``.
        """
        return self._storage[AnyArray].copy()

    def _storage_legacy(ref self) -> ref[self._storage] LegacyObjectData:
        """Return a borrow of the LegacyObjectData arm of the new storage.

        Read-side alternative to ``_obj_data()`` for object / datetime /
        timedelta columns (#646).  Callers access ``.data`` on the result
        to obtain the underlying ``List[PythonObject]``.  Caller must
        verify the column is in the LegacyObjectData arm — i.e. the dtype
        is ``object_``, ``datetime64_ns``, or ``timedelta64_ns``.
        """
        return self._storage[LegacyObjectData]

    # The four typed-array accessors below return a *copy* of the typed
    # marrow array held in ``_storage[AnyArray]``.  ``PrimitiveArray[T]`` and
    # ``StringArray`` both wrap ref-counted ``Buffer`` / ``Bitmap`` instances,
    # so a copy is O(1) (just an ArcPointer ref-bump).  Returning by value
    # avoids the nested-origin path that Mojo cannot express for chained
    # ``Variant[...]`` + ``AnyArray.as_*`` reference returns.
    def _int64_array(self) -> Int64Array:
        return self._storage[AnyArray].as_int64().copy()

    def _float64_array(self) -> Float64Array:
        return self._storage[AnyArray].as_float64().copy()

    def _bool_array(self) -> BoolArray:
        return self._storage[AnyArray].as_bool().copy()

    def _string_array(self) -> StringArray:
        return self._storage[AnyArray].as_string().copy()

    # ------------------------------------------------------------------
    # Type-predicate helpers — canonical abstraction over _data.isa checks.
    # Callers in _frame.mojo should use these instead of raw Variant queries.
    # ------------------------------------------------------------------

    def is_object(self) -> Bool:
        """Return True if this column has ``object_`` dtype.

        After #644, this is a dtype-based check (matching the other
        type predicates). Note that ``datetime64_ns`` / ``timedelta64_ns``
        columns are backed by ``List[PythonObject]`` but carry their own
        dtype, so they return ``False`` here — matching pandas
        ``dtype == object`` semantics, which also excludes datetimes.
        """
        return self.dtype == object_

    def is_numeric(self) -> Bool:
        """Return True if the active storage arm is int64 or float64."""
        return self.dtype == int64 or self.dtype == float64

    def is_int(self) -> Bool:
        """Return True if the active storage arm is ``List[Int64]``."""
        return self.dtype == int64

    def is_float(self) -> Bool:
        """Return True if the active storage arm is ``List[Float64]``."""
        return self.dtype == float64

    def is_bool(self) -> Bool:
        """Return True if the active storage arm is ``List[Bool]``."""
        return self.dtype == bool_

    def is_string(self) -> Bool:
        """Return True if this column has ``string_`` dtype.

        After #644, this is a dtype-based check (matching the other
        type predicates). Equivalent to the active ``_data`` arm being
        ``List[String]``.
        """
        return self.dtype == string_

    # ------------------------------------------------------------------
    # Null / validity helpers — dispatch on the active ``_storage`` arm.
    # ``AnyArray`` columns use marrow's validity bitmap (set bit = valid,
    # Arrow semantics); ``LegacyObjectData`` columns use their bundled
    # ``NullMask`` (set bit = null, bison semantics).  ``has_nulls`` is
    # defined further down in the "Null tracking" section; ``is_null``
    # and ``is_valid`` are the public API points.
    # ------------------------------------------------------------------

    def is_null(self, index: Int) -> Bool:
        """Return True if element at *index* is null."""
        if self._storage.isa[AnyArray]():
            return not self._storage[AnyArray].is_valid(index)
        return self._storage[LegacyObjectData].null_mask.is_null(index)

    def is_valid(self, index: Int) -> Bool:
        """Return True if element at *index* is non-null."""
        return not self.is_null(index)

    def null_mask_copy(self) raises -> NullMask:
        """Return a NullMask materialised from the active storage arm.

        ``AnyArray`` columns convert the Arrow validity bitmap (set = valid)
        to bison semantics (set = null).  ``LegacyObjectData`` columns
        return a copy of the bundled ``NullMask``.
        """
        if self._storage.isa[AnyArray]():
            ref arr = self._storage[AnyArray]
            var n = len(self)
            var mask = NullMask()
            for i in range(n):
                if arr.is_valid(i):
                    mask.append_valid()
                else:
                    mask.append_null()
            return mask^
        return self._storage[LegacyObjectData].null_mask.copy()

    def set_null_mask(mut self, var mask: NullMask) raises:
        """Set the null mask on the active storage arm.

        For ``LegacyObjectData`` columns the mask is stored directly.
        For ``AnyArray`` columns the marrow array is rebuilt from typed
        caches incorporating the new mask (string-with-nulls columns
        are demoted to ``LegacyObjectData`` because marrow lacks a
        public string+null builder).
        """
        if self._storage.isa[LegacyObjectData]():
            self._storage[LegacyObjectData].null_mask = mask^
            return

        # AnyArray arm — rebuild from typed caches + new mask.
        if not mask.has_nulls():
            # No nulls: keep the existing AnyArray as-is.
            return

        # String-with-nulls: demote to LegacyObjectData, preserving the
        # string data as List[PythonObject].  Marrow cannot build a
        # string + null array, but we keep the values round-trippable by
        # wrapping each String in a PythonObject.
        if self.is_string():
            var src = self._str_list()
            var py_data = List[PythonObject](capacity=len(src))
            for i in range(len(src)):
                py_data.append(PythonObject(src[i]))
            self._storage = ColumnStorage(LegacyObjectData(py_data^, mask^))
            return

        # Numeric / bool with nulls: rebuild marrow array from storage + mask.
        var n = len(self)
        if self.is_int():
            var src = self._int64_list()
            var vals = List[Optional[Int]](capacity=n)
            for i in range(n):
                if mask.is_null(i):
                    vals.append(None)
                else:
                    vals.append(Int(src[i]))
            self._storage = ColumnStorage(
                AnyArray(_marrow_array[Int64Type](vals^))
            )
        elif self.is_float():
            var src = self._float64_list()
            var vals = List[Optional[Float64]](capacity=n)
            for i in range(n):
                if mask.is_null(i):
                    vals.append(None)
                else:
                    vals.append(src[i])
            self._storage = ColumnStorage(
                AnyArray(_marrow_array[Float64Type](vals^))
            )
        elif self.is_bool():
            var src = self._bool_list()
            var vals = List[Optional[Bool]](capacity=n)
            for i in range(n):
                if mask.is_null(i):
                    vals.append(None)
                else:
                    vals.append(src[i])
            self._storage = ColumnStorage(AnyArray(_marrow_array(vals^)))

    def clear_null_mask(mut self) raises:
        """Reset all null bits — marks every element as valid."""
        self.set_null_mask(NullMask())

    def set_valid_at(mut self, index: Int) raises:
        """Mark a single element as valid (not null).

        For ``LegacyObjectData`` columns this updates the ``NullMask``
        in place.  For ``AnyArray`` columns the array is rebuilt.
        """
        if self._storage.isa[LegacyObjectData]():
            self._storage[LegacyObjectData].null_mask.set_valid(index)
            return
        # AnyArray — materialise mask, flip the bit, rebuild.
        var mask = self.null_mask_copy()
        mask.set_valid(index)
        self.set_null_mask(mask^)

    # ------------------------------------------------------------------
    # Storage rebuild (#619, Phase 6c / #658)
    # ------------------------------------------------------------------
    # Pre-refactor, ``_rebuild_storage`` synced typed caches back into
    # ``_storage`` after in-place cache mutations.  With the typed
    # caches removed, writes go directly through ``_flush_*_list``
    # helpers which rebuild ``_storage`` in one shot, so
    # ``_rebuild_storage`` is now a no-op kept only for call-site
    # compatibility.

    def _rebuild_storage(mut self):
        """No-op retained for backwards compatibility.

        Post-#658 (pre-refactor) this method synced typed caches back into
        ``_storage``.  With caches removed (this refactor), writes go
        directly to ``_storage`` via the ``_flush_*_list`` helpers, so
        there is nothing to rebuild.
        """
        pass

    # ------------------------------------------------------------------
    # Typed flush helpers — rebuild ``_storage`` from a caller-supplied
    # mutated List[T].  Used by write paths (``iat``/``at`` assignment,
    # merge key-column fill) that need to update a single cell.
    # ------------------------------------------------------------------

    def _flush_int64_list(mut self, var data: List[Int64]) raises:
        """Replace this column's storage with an int64 AnyArray built from
        *data*, preserving the current null mask."""
        var mask = self.null_mask_copy()
        var n = len(data)
        var vals = List[Optional[Int]](capacity=n)
        var has_mask = mask.has_nulls()
        for i in range(n):
            if has_mask and mask.is_null(i):
                vals.append(None)
            else:
                vals.append(Int(data[i]))
        self._storage = ColumnStorage(AnyArray(_marrow_array[Int64Type](vals^)))

    def _flush_float64_list(mut self, var data: List[Float64]) raises:
        """Replace this column's storage with a float64 AnyArray built from
        *data*, preserving the current null mask."""
        var mask = self.null_mask_copy()
        var n = len(data)
        var vals = List[Optional[Float64]](capacity=n)
        var has_mask = mask.has_nulls()
        for i in range(n):
            if has_mask and mask.is_null(i):
                vals.append(None)
            else:
                vals.append(data[i])
        self._storage = ColumnStorage(
            AnyArray(_marrow_array[Float64Type](vals^))
        )

    def _flush_bool_list(mut self, var data: List[Bool]) raises:
        """Replace this column's storage with a bool AnyArray built from
        *data*, preserving the current null mask."""
        var mask = self.null_mask_copy()
        var n = len(data)
        var vals = List[Optional[Bool]](capacity=n)
        var has_mask = mask.has_nulls()
        for i in range(n):
            if has_mask and mask.is_null(i):
                vals.append(None)
            else:
                vals.append(data[i])
        self._storage = ColumnStorage(AnyArray(_marrow_array(vals^)))

    def _flush_str_list(mut self, var data: List[String]) raises:
        """Replace this column's storage with a string AnyArray built from
        *data*.  For string-with-nulls columns the storage is kept in
        the ``LegacyObjectData`` arm with wrapped PythonObject strings
        (marrow cannot build a string + null array).
        """
        if self.has_nulls():
            var mask = self.null_mask_copy()
            var py_data = List[PythonObject](capacity=len(data))
            for i in range(len(data)):
                py_data.append(PythonObject(data[i]))
            self._storage = ColumnStorage(LegacyObjectData(py_data^, mask^))
            return
        self._storage = ColumnStorage(AnyArray(_marrow_array(data^)))

    # ------------------------------------------------------------------
    # Index-arm predicates and unsafe accessors — canonical abstraction
    # over ``self._index.isa[...]`` checks.  Mirror the data-arm helpers
    # above so callers never need to query the ColumnIndex variant
    # directly.
    # ------------------------------------------------------------------

    def is_str_index(self) -> Bool:
        """Return True if the active index arm is ``Index`` (string labels)."""
        return self._index.isa[Index]()

    def is_int_index(self) -> Bool:
        """Return True if the active index arm is ``List[Int64]``."""
        return self._index.isa[List[Int64]]()

    def is_float_index(self) -> Bool:
        """Return True if the active index arm is ``List[Float64]``."""
        return self._index.isa[List[Float64]]()

    def is_obj_index(self) -> Bool:
        """Return True if the active index arm is ``List[PythonObject]``."""
        return self._index.isa[List[PythonObject]]()

    def _str_index(ref self) -> ref[self._index] Index:
        return self._index[Index]

    def _int_index_data(ref self) -> ref[self._index] List[Int64]:
        return self._index[List[Int64]]

    def _float_index_data(ref self) -> ref[self._index] List[Float64]:
        return self._index[List[Float64]]

    def _obj_index_data(ref self) -> ref[self._index] List[PythonObject]:
        return self._index[List[PythonObject]]

    # ------------------------------------------------------------------
    # Cache-aware visitor dispatch (#619 Phase 6c)
    #
    # These helpers route visitor calls through the typed caches for
    # non-object columns and through ``_storage[LegacyObjectData].data``
    # for object / datetime / timedelta columns.  Empty columns fall
    # back to dtype-driven dispatch with an empty list.
    #
    # TODO(#642): Once the Mojo compiler fixes typed-downcast deadlocks,
    # delete these helpers and the visitor infrastructure entirely — all
    # readers will use direct AnyArray typed downcasts instead.
    # ------------------------------------------------------------------

    def _visit_raises[V: ColumnDataVisitorRaises](self, mut visitor: V) raises:
        """Dispatch a raising visitor through the active storage arm.

        Numeric / bool / string columns go through the ``AnyArray`` arm
        (or, for string-with-nulls, the ``LegacyObjectData`` arm);
        object / datetime / timedelta columns go through the
        ``LegacyObjectData`` arm.
        """
        if self.is_int():
            visitor.on_int64(self._int64_list())
        elif self.is_float():
            visitor.on_float64(self._float64_list())
        elif self.is_bool():
            visitor.on_bool(self._bool_list())
        elif self.is_string():
            visitor.on_str(self._str_list())
        else:
            # Object / datetime / timedelta / empty stub: route through
            # the LegacyObjectData arm of _storage.
            visitor.on_obj(self._storage[LegacyObjectData].data)

    def _visit[V: ColumnDataVisitor](self, mut visitor: V):
        """Dispatch a non-raising visitor through the active storage arm."""
        try:
            if self.is_int():
                visitor.on_int64(self._int64_list())
                return
            if self.is_float():
                visitor.on_float64(self._float64_list())
                return
            if self.is_bool():
                visitor.on_bool(self._bool_list())
                return
            if self.is_string():
                visitor.on_str(self._str_list())
                return
        except:
            pass
        visitor.on_obj(self._storage[LegacyObjectData].data)

    # ------------------------------------------------------------------
    # Explicit copy helper (used by Series / DataFrame __copyinit__)
    # ------------------------------------------------------------------

    def copy(self) -> Column:
        """Return an independent copy of this Column.

        Copies storage directly — avoids the round-trip through
        ``_CopyDataVisitor`` + constructor.
        """
        var col = Column()
        col.name = self.name
        col.dtype = self.dtype
        col._index = self._index.copy()
        col._index_names = self._index_names.copy()
        col._index_name = self._index_name
        col._storage = self._storage.copy()
        return col^

    # ------------------------------------------------------------------
    # Index helpers — dispatch-free access to the active ColumnIndex arm
    # ------------------------------------------------------------------

    def _index_len(self) -> Int:
        """Return the number of explicit index labels (0 = default RangeIndex).
        """
        if self._index.isa[Index]():
            return self._index[Index].__len__()
        elif self._index.isa[List[Int64]]():
            return len(self._index[List[Int64]])
        elif self._index.isa[List[Float64]]():
            return len(self._index[List[Float64]])
        else:
            return len(self._index[List[PythonObject]])

    def _index_label(self, i: Int) -> String:
        """Return the index label at position *i* as a String."""
        if self._index.isa[Index]():
            return self._index[Index][i]
        elif self._index.isa[List[Int64]]():
            return String(Int(self._index[List[Int64]][i]))
        elif self._index.isa[List[Float64]]():
            return String(self._index[List[Float64]][i])
        else:
            return String(self._index[List[PythonObject]][i])

    def _index_reorder(self, perm: List[Int]) -> ColumnIndex:
        """Return a new ColumnIndex with labels reordered by *perm*.

        The four arms must be handled separately because each builds a
        different concrete List type; a single generic loop is not possible
        in Mojo without full parametric polymorphism over the element type.
        """
        var n = len(perm)
        if self._index.isa[Index]():
            ref old = self._index[Index]
            var labels = List[String]()
            for k in range(n):
                labels.append(old[perm[k]])
            return ColumnIndex(Index(labels^))
        elif self._index.isa[List[Int64]]():
            ref old = self._index[List[Int64]]
            var ints = List[Int64]()
            for k in range(n):
                ints.append(old[perm[k]])
            return ColumnIndex(ints^)
        elif self._index.isa[List[Float64]]():
            ref old = self._index[List[Float64]]
            var floats = List[Float64]()
            for k in range(n):
                floats.append(old[perm[k]])
            return ColumnIndex(floats^)
        else:
            ref old = self._index[List[PythonObject]]
            var objs = List[PythonObject]()
            for k in range(n):
                objs.append(old[perm[k]])
            return ColumnIndex(objs^)

    def sort_perm(
        self, ascending: Bool, na_last: Bool = True
    ) raises -> List[Int]:
        """Return a stable merge-sort permutation over the column data values.

        ``perm[i]`` is the original index of the *i*-th element in sorted
        order.  When *na_last* is ``True`` (default), null elements are placed
        at the end; when ``False``, at the beginning.

        This is the canonical argsort helper shared by ``Series.sort_values``
        and ``DataFrame.sort_values``.
        """
        var n = self.__len__()
        var perm = List[Int]()
        for i in range(n):
            perm.append(i)
        if n <= 1:
            return perm^
        var null_mask = self.null_mask_copy()
        if self.is_int():
            var data = self._int64_list()
            _merge_sort_perm_comparable(
                perm, data, null_mask, ascending, na_last
            )
        elif self.is_bool():
            var data = self._bool_list()
            _merge_sort_perm_comparable(
                perm, data, null_mask, ascending, na_last
            )
        elif self.is_string():
            var data = self._str_list()
            _merge_sort_perm_comparable(
                perm, data, null_mask, ascending, na_last
            )
        elif self.is_float():
            var data = self._float64_list()
            _merge_sort_perm_comparable(
                perm, data, null_mask, ascending, na_last
            )
        else:
            _merge_sort_perm_pyobj(
                perm,
                self._storage[LegacyObjectData].data,
                null_mask,
                ascending,
                na_last,
            )
        return perm^

    def _sort_perm_by_index(self, ascending: Bool) raises -> List[Int]:
        """Return a stable merge-sort permutation over the current index labels.

        The result ``perm[i]`` is the original row position of the *i*-th row
        in sorted order.  The four index arms (string, int64, float64,
        PythonObject) are dispatched once so callers don't need to repeat
        the branching.
        """
        var n = self._index_len()
        var perm = List[Int]()
        for i in range(n):
            perm.append(i)
        # Index labels are never null, so pass an empty mask.
        var no_mask = NullMask()
        if self.is_str_index():
            ref idx = self._str_index()
            _merge_sort_perm_comparable(
                perm, idx._data, no_mask, ascending, True
            )
        elif self.is_int_index():
            ref idx = self._int_index_data()
            _merge_sort_perm_comparable(perm, idx, no_mask, ascending, True)
        elif self.is_float_index():
            ref idx = self._float_index_data()
            _merge_sort_perm_comparable(perm, idx, no_mask, ascending, True)
        else:
            # PythonObject fallback: use Python comparison (may raise).
            ref idx = self._obj_index_data()
            _merge_sort_perm_pyobj(perm, idx, no_mask, ascending, True)
        return perm^

    # ------------------------------------------------------------------
    # Length
    # ------------------------------------------------------------------

    def __len__(self) -> Int:
        # Dispatch on the active storage arm.  AnyArray columns report
        # their own length; LegacyObjectData columns report the length
        # of their data list.
        if self._storage.isa[AnyArray]():
            return self._storage[AnyArray].length()
        return len(self._storage[LegacyObjectData].data)

    def len(self) -> Int:
        """Return the number of elements in this column."""
        return self.__len__()

    # ------------------------------------------------------------------
    # Row selection helpers
    # ------------------------------------------------------------------

    def slice(self, start: Int, end: Int) raises -> Column:
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
        var new_mask = NullMask()
        if self.has_nulls():
            for i in range(s, e):
                new_mask.append(self.is_null(i))
        # Slice via the active storage arm.
        var col: Column
        if self._storage.isa[AnyArray]():
            # Zero-copy: AnyArray.slice() adjusts offset/length metadata only.
            var sliced = self._storage[AnyArray].slice(s, e - s)
            col = Column(self.name, sliced^, self.dtype)
        else:
            var visitor = _SliceVisitor(s, e)
            self._visit(visitor)
            col = Column(self.name, visitor^.result.copy(), self.dtype)
        if new_mask.has_nulls():
            col.set_null_mask(new_mask^)
        return col^

    def take(self, indices: List[Int]) raises -> Column:
        """Return a new Column with rows selected by *indices* (arbitrary order).
        """
        var has_mask = self.has_nulls()
        var new_mask = NullMask()
        if has_mask:
            for k in range(len(indices)):
                new_mask.append(self.is_null(indices[k]))
        # Take via the active storage arm.
        var col: Column
        if self.is_int():
            var src = self._int64_list()
            var result = List[Int64](capacity=len(indices))
            for k in range(len(indices)):
                result.append(src[indices[k]])
            col = Column(self.name, ColumnData(result^), self.dtype)
        elif self.is_bool():
            var src = self._bool_list()
            var result = List[Bool](capacity=len(indices))
            for k in range(len(indices)):
                result.append(src[indices[k]])
            col = Column(self.name, ColumnData(result^), self.dtype)
        elif self.is_string():
            var src = self._str_list()
            var result = List[String](capacity=len(indices))
            for k in range(len(indices)):
                result.append(src[indices[k]])
            col = Column(self.name, ColumnData(result^), self.dtype)
        elif self.is_float():
            var src = self._float64_list()
            var result = List[Float64](capacity=len(indices))
            for k in range(len(indices)):
                result.append(src[indices[k]])
            col = Column(self.name, ColumnData(result^), self.dtype)
        else:
            # Object / datetime / timedelta: fall back to the visitor path.
            var visitor = _TakeVisitor(indices)
            self._visit(visitor)
            col = Column(self.name, visitor^.result.copy(), self.dtype)
        if new_mask.has_nulls():
            col.set_null_mask(new_mask^)
        return col^

    def take_with_nulls(self, indices: List[Int]) raises -> Column:
        """Like take() but index -1 inserts a null placeholder row."""
        var n = len(indices)
        var src_has_nulls = self.has_nulls()
        var out_mask = NullMask()

        if self._storage.isa[AnyArray]() and self.is_int():
            var src = self._int64_list()
            var result = List[Int64](capacity=n)
            for k in range(n):
                var i = indices[k]
                if i < 0:
                    result.append(Int64(0))
                    out_mask.append_null()
                else:
                    result.append(src[i])
                    out_mask.append(src_has_nulls and self.is_null(i))
            var col = Column(self.name, ColumnData(result^), self.dtype)
            if out_mask.has_nulls():
                col.set_null_mask(out_mask^)
            return col^
        elif self._storage.isa[AnyArray]() and self.is_float():
            var src = self._float64_list()
            var result = List[Float64](capacity=n)
            for k in range(n):
                var i = indices[k]
                if i < 0:
                    result.append(Float64(0) / Float64(0))
                    out_mask.append_null()
                else:
                    result.append(src[i])
                    out_mask.append(src_has_nulls and self.is_null(i))
            var col = Column(self.name, ColumnData(result^), self.dtype)
            if out_mask.has_nulls():
                col.set_null_mask(out_mask^)
            return col^
        elif self._storage.isa[AnyArray]() and self.is_bool():
            var src = self._bool_list()
            var result = List[Bool](capacity=n)
            for k in range(n):
                var i = indices[k]
                if i < 0:
                    result.append(False)
                    out_mask.append_null()
                else:
                    result.append(src[i])
                    out_mask.append(src_has_nulls and self.is_null(i))
            var col = Column(self.name, ColumnData(result^), self.dtype)
            if out_mask.has_nulls():
                col.set_null_mask(out_mask^)
            return col^
        elif self._storage.isa[AnyArray]() and self.is_string():
            var src = self._str_list()
            var result = List[String](capacity=n)
            for k in range(n):
                var i = indices[k]
                if i < 0:
                    result.append(String(""))
                    out_mask.append_null()
                else:
                    result.append(src[i])
                    out_mask.append(src_has_nulls and self.is_null(i))
            var col = Column(self.name, ColumnData(result^), self.dtype)
            if out_mask.has_nulls():
                col.set_null_mask(out_mask^)
            return col^
        else:
            # Object / datetime / timedelta: fall back to visitor path.
            var visitor = _TakeWithNullsVisitor(indices, self.null_mask_copy())
            self._visit_raises(visitor)
            var obj_mask = visitor.out_mask.copy()
            var col = Column._caches_only_from_col_data(
                self.name, self.dtype, visitor^.result.copy()
            )
            if obj_mask.has_nulls():
                col.set_null_mask(obj_mask^)
            return col^

    def concat(self, other: Column) raises -> Column:
        """Return a new Column with *other* appended row-wise."""
        var visitor = _ConcatDataVisitor(other._to_col_data())
        self._visit_raises(visitor)
        # Use _caches_only_from_col_data to skip marrow AnyArray construction
        # (#659 perf fix).
        var col = Column._caches_only_from_col_data(
            self.name, self.dtype, visitor^.result.copy()
        )
        # Merge null masks only when at least one side has nulls
        if self.has_nulls() or other.has_nulls():
            var n_self = len(self)
            var n_other = len(other)
            var merged = NullMask()
            for i in range(n_self):
                merged.append(self.has_nulls() and self.is_null(i))
            for i in range(n_other):
                merged.append(other.has_nulls() and other.is_null(i))
            if merged.has_nulls():
                col.set_null_mask(merged^)
        return col^

    # ------------------------------------------------------------------
    # Null tracking
    # ------------------------------------------------------------------

    def has_nulls(self) -> Bool:
        """Return True if any element is marked null/NaN.

        Consults the active ``ColumnStorage`` arm (marrow array bitmap
        or ``LegacyObjectData.null_mask``).
        """
        if self._storage.isa[AnyArray]():
            return self._storage[AnyArray].null_count() > 0
        return self._storage[LegacyObjectData].null_mask.has_nulls()

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
        # Prefer the marrow storage when active (#619 Phase 4).
        if self._storage.isa[AnyArray]():
            var dt = self._storage[AnyArray].dtype()
            if dt == _m_int64 or dt == _m_float64:
                return _marrow_scalar_to_float64(
                    _marrow_sum(self._storage[AnyArray])
                )
        elif self.is_int() or self.is_float():
            # Legacy path: rebuild AnyArray from caches.
            var arr = _to_numeric_marrow_array(self)
            return _marrow_scalar_to_float64(_marrow_sum(arr))
        # Fall back to visitor for Bool/String/PythonObject or legacy mode.
        var visitor = _SumVisitor(self.null_mask_copy())
        self._visit_raises(visitor)
        return visitor.result

    def count(self) -> Int:
        """Return the number of non-null elements."""
        var n = len(self)
        if not self.has_nulls():
            return n
        var result = 0
        for i in range(n):
            if not self.is_null(i):
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
        # Prefer the marrow storage when active (#619 Phase 4).
        if self._storage.isa[AnyArray]():
            var dt = self._storage[AnyArray].dtype()
            if dt == _m_int64 or dt == _m_float64:
                if self.count() == 0:
                    var zero = Float64(0)
                    return zero / zero
                return _marrow_scalar_to_float64(
                    _marrow_min(self._storage[AnyArray])
                )
        elif self.is_int() or self.is_float():
            if self.count() == 0:
                var zero = Float64(0)
                return zero / zero
            var arr = _to_numeric_marrow_array(self)
            return _marrow_scalar_to_float64(_marrow_min(arr))
        # Fall back to visitor for Bool/String/PythonObject or legacy mode.
        var visitor = _ExtremumVisitor[True](self.null_mask_copy())
        self._visit_raises(visitor)
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
        # Prefer the marrow storage when active (#619 Phase 4).
        if self._storage.isa[AnyArray]():
            var dt = self._storage[AnyArray].dtype()
            if dt == _m_int64 or dt == _m_float64:
                if self.count() == 0:
                    var zero = Float64(0)
                    return zero / zero
                return _marrow_scalar_to_float64(
                    _marrow_max(self._storage[AnyArray])
                )
        elif self.is_int() or self.is_float():
            if self.count() == 0:
                var zero = Float64(0)
                return zero / zero
            var arr = _to_numeric_marrow_array(self)
            return _marrow_scalar_to_float64(_marrow_max(arr))
        # Fall back to visitor for Bool/String/PythonObject or legacy mode.
        var visitor = _ExtremumVisitor[False](self.null_mask_copy())
        self._visit_raises(visitor)
        if not visitor.found:
            var zero = Float64(0)
            return zero / zero
        return visitor.result

    def sum_int64(self) raises -> Int64:
        """Return the sum of an Int64 column as Int64, skipping nulls."""
        ref data = self._int64_data()
        var has_mask = self.has_nulls()
        var result = Int64(0)
        for i in range(len(data)):
            if has_mask and self.is_null(i):
                continue
            result += data[i]
        return result

    def min_int64(self) raises -> Int64:
        """Return the minimum of an Int64 column as Int64, skipping nulls."""
        ref data = self._int64_data()
        var has_mask = self.has_nulls()
        var found = False
        var result = Int64(0)
        for i in range(len(data)):
            if has_mask and self.is_null(i):
                continue
            if not found or data[i] < result:
                result = data[i]
                found = True
        return result

    def max_int64(self) raises -> Int64:
        """Return the maximum of an Int64 column as Int64, skipping nulls."""
        ref data = self._int64_data()
        var has_mask = self.has_nulls()
        var found = False
        var result = Int64(0)
        for i in range(len(data)):
            if has_mask and self.is_null(i):
                continue
            if not found or data[i] > result:
                result = data[i]
                found = True
        return result

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
        # Fast path: compute variance directly from storage for numeric/bool columns.
        if self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        ):
            var data = self._f64_list()
            var total = Float64(0)
            var has_mask = self.has_nulls()
            for i in range(len(data)):
                if has_mask and self.is_null(i):
                    continue
                var diff = data[i] - m
                total += diff * diff
            return total / Float64(n - ddof)
        var visitor = _VarVisitor(m, self.null_mask_copy())
        self._visit_raises(visitor)
        return visitor.total / Float64(n - ddof)

    def std(self, ddof: Int = 1, skipna: Bool = True) raises -> Float64:
        """Return the standard deviation (square root of variance)."""
        return sqrt(self.var(ddof, skipna))

    def skew(self, skipna: Bool = True) raises -> Float64:
        """Return the adjusted Fisher-Pearson standardised skewness coefficient.

        Formula: ``n / ((n-1) * (n-2)) * sum((x-mean)^3) / std^3``.
        Returns NaN when n < 3 or std == 0.
        """
        var n = self.count() if skipna else len(self)
        if n < 3:
            var zero = Float64(0)
            return zero / zero
        var m = self.mean(skipna)
        var s = self.std(1, skipna)
        if s == 0.0:
            var zero = Float64(0)
            return zero / zero
        # Fast path: compute moment directly from storage for numeric/bool columns.
        var skew_total: Float64
        if self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        ):
            var data = self._f64_list()
            var total = Float64(0)
            var has_mask = self.has_nulls()
            for i in range(len(data)):
                if has_mask and self.is_null(i):
                    continue
                var diff = data[i] - m
                total += diff * diff * diff
            skew_total = total
        else:
            var visitor = _MomentVisitor(m, 3, self.null_mask_copy())
            self._visit_raises(visitor)
            skew_total = visitor.total
        return (
            Float64(n) / Float64((n - 1) * (n - 2)) * skew_total / (s * s * s)
        )

    def kurt(self, skipna: Bool = True) raises -> Float64:
        """Return the excess kurtosis with bias correction (pandas default).

        Formula: ``n*(n+1)/((n-1)*(n-2)*(n-3)) * sum((x-mean)^4/s^4)
        - 3*(n-1)^2/((n-2)*(n-3))``.
        Returns NaN when n < 4 or std == 0.
        """
        var n = self.count() if skipna else len(self)
        if n < 4:
            var zero = Float64(0)
            return zero / zero
        var m = self.mean(skipna)
        var s = self.std(1, skipna)
        if s == 0.0:
            var zero = Float64(0)
            return zero / zero
        # Fast path: compute moment directly from storage for numeric/bool columns.
        var kurt_total: Float64
        if self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        ):
            var data = self._f64_list()
            var total = Float64(0)
            var has_mask = self.has_nulls()
            for i in range(len(data)):
                if has_mask and self.is_null(i):
                    continue
                var diff = data[i] - m
                var d2 = diff * diff
                total += d2 * d2
            kurt_total = total
        else:
            var visitor = _MomentVisitor(m, 4, self.null_mask_copy())
            self._visit_raises(visitor)
            kurt_total = visitor.total
        var fn_ = Float64(n)
        var term1 = (
            fn_
            * (fn_ + 1.0)
            / ((fn_ - 1.0) * (fn_ - 2.0) * (fn_ - 3.0))
            * kurt_total
            / (s * s * s * s)
        )
        var term2 = (
            3.0 * (fn_ - 1.0) * (fn_ - 1.0) / ((fn_ - 2.0) * (fn_ - 3.0))
        )
        return term1 - term2

    def argmin(self, skipna: Bool = True) raises -> Int:
        """Return the positional index of the minimum value.

        Returns -1 if the column is empty or all-null.
        Raises if ``skipna=False`` and any null is present.
        """
        if not skipna and self.has_nulls():
            raise Error(
                "argmin: cannot compute with NaN values when skipna=False"
            )
        var visitor = _ArgExtremumVisitor[True](self.null_mask_copy())
        self._visit_raises(visitor)
        if not visitor.found:
            return -1
        return visitor.result

    def argmax(self, skipna: Bool = True) raises -> Int:
        """Return the positional index of the maximum value.

        Returns -1 if the column is empty or all-null.
        Raises if ``skipna=False`` and any null is present.
        """
        if not skipna and self.has_nulls():
            raise Error(
                "argmax: cannot compute with NaN values when skipna=False"
            )
        var visitor = _ArgExtremumVisitor[False](self.null_mask_copy())
        self._visit_raises(visitor)
        if not visitor.found:
            return -1
        return visitor.result

    def cov(
        self, other: Column, ddof: Int = 1, skipna: Bool = True
    ) raises -> Float64:
        """Return the sample covariance with ``other``.

        Pairs where either column has a null are excluded when ``skipna=True``.
        Raises if the columns have different lengths.
        """
        var n = len(self)
        if n != len(other):
            raise Error("cov: columns must be the same length")
        var xs = self._to_float64_list()
        var ys = other._to_float64_list()
        var has_x_mask = self.has_nulls()
        var has_y_mask = other.has_nulls()
        var sum_x = Float64(0)
        var sum_y = Float64(0)
        var count = 0
        for i in range(n):
            var x_null = has_x_mask and self.is_null(i)
            var y_null = has_y_mask and other.is_null(i)
            if x_null or y_null:
                if not skipna:
                    var zero = Float64(0)
                    return zero / zero
                continue
            sum_x += xs[i]
            sum_y += ys[i]
            count += 1
        if count - ddof <= 0:
            var zero = Float64(0)
            return zero / zero
        var mean_x = sum_x / Float64(count)
        var mean_y = sum_y / Float64(count)
        var total = Float64(0)
        for i in range(n):
            var x_null = has_x_mask and self.is_null(i)
            var y_null = has_y_mask and other.is_null(i)
            if x_null or y_null:
                continue
            total += (xs[i] - mean_x) * (ys[i] - mean_y)
        return total / Float64(count - ddof)

    def corr(self, other: Column, skipna: Bool = True) raises -> Float64:
        """Return the Pearson correlation coefficient with ``other``.

        Pairs where either column has a null are excluded when ``skipna=True``.
        Raises if the columns have different lengths.
        """
        var n = len(self)
        if n != len(other):
            raise Error("corr: columns must be the same length")
        var xs = self._to_float64_list()
        var ys = other._to_float64_list()
        var has_x_mask = self.has_nulls()
        var has_y_mask = other.has_nulls()
        var sum_x = Float64(0)
        var sum_y = Float64(0)
        var count = 0
        for i in range(n):
            var x_null = has_x_mask and self.is_null(i)
            var y_null = has_y_mask and other.is_null(i)
            if x_null or y_null:
                if not skipna:
                    var zero = Float64(0)
                    return zero / zero
                continue
            sum_x += xs[i]
            sum_y += ys[i]
            count += 1
        if count <= 1:
            var zero = Float64(0)
            return zero / zero
        var mean_x = sum_x / Float64(count)
        var mean_y = sum_y / Float64(count)
        var sum_xy = Float64(0)
        var sum_x2 = Float64(0)
        var sum_y2 = Float64(0)
        for i in range(n):
            var x_null = has_x_mask and self.is_null(i)
            var y_null = has_y_mask and other.is_null(i)
            if x_null or y_null:
                continue
            var dx = xs[i] - mean_x
            var dy = ys[i] - mean_y
            sum_xy += dx * dy
            sum_x2 += dx * dx
            sum_y2 += dy * dy
        var denom = sqrt(sum_x2 * sum_y2)
        if denom == 0.0:
            var zero = Float64(0)
            return zero / zero
        return sum_xy / denom

    def nunique(self) raises -> Int:
        """Return the number of unique non-null values.

        Raises for non-numeric and non-string column types.
        """
        # Fast path: count unique Float64 values from numeric/bool storage.
        if self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        ):
            var data = self._f64_list()
            var has_mask = self.has_nulls()
            var seen = Set[Float64]()
            for i in range(len(data)):
                if has_mask and self.is_null(i):
                    continue
                seen.add(data[i])
            return len(seen)
        var visitor = _NuniqueVisitor(self.null_mask_copy())
        self._visit_raises(visitor)
        return visitor.result

    def quantile(self, q: Float64 = 0.5, skipna: Bool = True) raises -> Float64:
        """Return the q-th quantile using linear interpolation.

        When ``skipna=True`` (default) null/NaN elements are skipped.
        When ``skipna=False`` the result is NaN if any null is present.
        Raises for non-numeric column types.
        """
        if not skipna and self.has_nulls():
            var zero = Float64(0)
            return zero / zero
        # Fast path: collect non-null Float64 values from numeric/bool storage.
        var vals: List[Float64]
        if self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        ):
            var data = self._f64_list()
            vals = List[Float64]()
            var has_mask = self.has_nulls()
            for i in range(len(data)):
                if has_mask and self.is_null(i):
                    continue
                vals.append(data[i])
        else:
            var visitor = _QuantileCollectVisitor(self.null_mask_copy())
            self._visit_raises(visitor)
            vals = visitor.vals.copy()
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
        return self.quantile(0.5, skipna)

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

        var labels = List[String]()
        labels.append("count")
        labels.append("mean")
        labels.append("std")
        labels.append("min")
        labels.append("25%")
        labels.append("50%")
        labels.append("75%")
        labels.append("max")

        var idx = ColumnIndex(Index(labels^))
        return Column(self.name, ColumnData(data^), float64, idx^)

    def value_counts(
        self, normalize: Bool = False, sort: Bool = True
    ) raises -> Column:
        """Return a Column of counts (or proportions) per unique value.

        The index holds the original values; the data holds the counts.
        sort=True (default) orders results by count descending.
        Raises for object/datetime column types.
        """
        var has_mask = self.has_nulls()

        # Phase 1: count unique values (raises for unsupported arms).
        var count_visitor = _ValueCountsCountVisitor(
            has_mask, self.null_mask_copy()
        )
        self._visit_raises(count_visitor)
        var n = len(count_visitor.unique_keys)

        # Materialise per-key counts in insertion order.
        var count_vals = List[Int]()
        for i in range(n):
            count_vals.append(
                count_visitor.counts_dict[count_visitor.unique_keys[i]]
            )

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
        self._visit_raises(index_visitor)
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
        # Fast path: extract directly from AnyArray when available.
        if self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        ):
            return self._f64_list()
        var visitor = _ToFloat64Visitor()
        self._visit_raises(visitor)
        return visitor.result.copy()

    def _build_result_col(
        self,
        var col_data: ColumnData,
        var result_mask: List[Bool],
        has_any_null: Bool,
    ) raises -> Column:
        """Wrap a computed ColumnData into a Column, attaching mask only if needed.
        """
        var dtype = Column._sniff_dtype(col_data)
        var col = Column(self.name, col_data^, dtype)
        if has_any_null:
            col.set_null_mask(NullMask.from_list(result_mask))
        return col^

    def _binary_op_prepare_unchecked(
        self, other: Column
    ) raises -> _BinOpInputs:
        """Build the shared Float64 input arrays and null-mask flags.

        Precondition: ``len(self) == len(other)``.  Both ``_arith_op`` and
        ``_cmp_op`` enforce this invariant with an explicit length check before
        calling this helper.  Violating the precondition leads to an
        out-of-bounds access in the caller's loop.
        """
        var a = self._to_float64_list()
        var b = other._to_float64_list()
        return _BinOpInputs(a^, b^, self.has_nulls(), other.has_nulls())

    def _arith_op[
        op: Int
    ](self, op_name: String, other: Column) raises -> Column:
        """Core element-wise binary arithmetic kernel.

        ``op`` is a compile-time constant (``_ARITH_*``) that selects the
        operation; ``comptime if`` folds the branch at compile time so each
        specialisation compiles to a tight scalar loop with no runtime dispatch.

        When both columns hold ``int64`` data and the operation is not true
        division (``_ARITH_DIV``), the kernel works directly on ``List[Int64]``
        and returns an ``int64`` column, matching pandas' dtype-preserving
        behaviour.  True division always yields ``float64``.
        """
        if len(self) != len(other):
            raise Error(
                op_name
                + ": length mismatch ("
                + String(len(self))
                + " vs "
                + String(len(other))
                + ")"
            )

        # Int64 fast path: int64 op int64 → int64 for all ops except true division.
        comptime if op != _ARITH_DIV:
            if self.dtype == int64 and other.dtype == int64:
                var a = self._int64_list()
                var b = other._int64_list()
                var has_a_mask = self.has_nulls()
                var has_b_mask = other.has_nulls()
                var result = List[Int64]()
                var result_mask = List[Bool]()
                var has_any_null = False
                for i in range(len(a)):
                    var is_null = (has_a_mask and self.is_null(i)) or (
                        has_b_mask and other.is_null(i)
                    )
                    if is_null:
                        result.append(Int64(0))
                        result_mask.append(True)
                        has_any_null = True
                    else:
                        var v: Int64
                        comptime if op == _ARITH_ADD:
                            v = a[i] + b[i]
                        elif op == _ARITH_SUB:
                            v = a[i] - b[i]
                        elif op == _ARITH_MUL:
                            v = a[i] * b[i]
                        elif op == _ARITH_FLOORDIV:
                            v = _int64_floordiv(a[i], b[i])
                        elif op == _ARITH_MOD:
                            v = _int64_mod(a[i], b[i])
                        elif op == _ARITH_POW:
                            v = _int64_pow(a[i], b[i])
                        else:
                            v = Int64(0)  # unreachable
                        result.append(v)
                        result_mask.append(False)
                return self._build_result_col(
                    ColumnData(result^), result_mask^, has_any_null
                )

        # Float64 path: used when either operand is float64/bool, or for true division.
        var inp = self._binary_op_prepare_unchecked(other)
        var result = List[Float64]()
        var result_mask = List[Bool]()
        var has_any_null = False
        var nan = Float64(0) / Float64(0)
        for i in range(len(inp.a)):
            var is_null = (inp.has_a_mask and self.is_null(i)) or (
                inp.has_b_mask and other.is_null(i)
            )
            if is_null:
                result.append(nan)
                result_mask.append(True)
                has_any_null = True
            else:
                var v: Float64
                comptime if op == _ARITH_ADD:
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
        return self._build_result_col(
            ColumnData(result^), result_mask^, has_any_null
        )

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

        Dispatches through ``_CmpOpVisitor[op]`` which handles all
        ``ColumnData`` arms internally — no raw ``isa`` checks at this call
        site.  ``op`` is a compile-time constant (``_CMP_*``) that selects
        the operation.  Null propagation: if either element is null, the
        result element is null.
        """
        if len(self) != len(other):
            raise Error(
                op_name
                + ": length mismatch ("
                + String(len(self))
                + " vs "
                + String(len(other))
                + ")"
            )
        # Storage-aware fast path using Float64 lists extracted from storage.
        # When both columns are numeric/bool with AnyArray storage, skip the
        # visitor dispatch and the _ToFloat64Visitor conversion entirely.
        var self_numeric = self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        )
        var other_numeric = other._storage.isa[AnyArray]() and (
            other.is_int() or other.is_float() or other.is_bool()
        )
        if self_numeric and other_numeric:
            var lhs = self._f64_list()
            var rhs = other._f64_list()
            var n = len(lhs)
            var result = List[Bool](capacity=n)
            var result_mask = List[Bool]()
            var has_a_mask = self.has_nulls()
            var has_b_mask = other.has_nulls()
            var has_any_null = False
            for i in range(n):
                var is_null = (has_a_mask and self.is_null(i)) or (
                    has_b_mask and other.is_null(i)
                )
                if is_null:
                    result.append(False)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v: Bool
                    comptime if op == _CMP_EQ:
                        v = lhs[i] == rhs[i]
                    elif op == _CMP_NE:
                        v = lhs[i] != rhs[i]
                    elif op == _CMP_LT:
                        v = lhs[i] < rhs[i]
                    elif op == _CMP_LE:
                        v = lhs[i] <= rhs[i]
                    elif op == _CMP_GT:
                        v = lhs[i] > rhs[i]
                    else:
                        v = lhs[i] >= rhs[i]
                    result.append(v)
                    result_mask.append(False)
            return self._build_result_col(
                ColumnData(result^), result_mask^, has_any_null
            )
        var visitor = _CmpOpVisitor[op](self.null_mask_copy(), other)
        self._visit_raises(visitor)
        return self._build_result_col(
            ColumnData(visitor.result.copy()),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

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

    def _cmp_scalar_op[op: Int](self, scalar: Float64) raises -> Column:
        """Core element-wise scalar comparison kernel.

        Like ``_cmp_op`` but compares every element against a single
        ``Float64`` constant rather than a parallel column.  This avoids the
        broadcast allocation that ``_cmp_op`` would require when the caller
        wraps a scalar in a full-length list.

        ``op`` is a compile-time constant (``_CMP_*``) that selects the
        operation.  Null propagation: null elements produce a null result.
        """
        # Storage-aware fast path using Float64 list extracted from storage.
        if self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        ):
            var data = self._f64_list()
            var n = len(data)
            var result = List[Bool](capacity=n)
            var result_mask = List[Bool]()
            var has_any_null = self.has_nulls()
            for i in range(n):
                if has_any_null and self.is_null(i):
                    result.append(False)
                    result_mask.append(True)
                else:
                    var v: Bool
                    comptime if op == _CMP_EQ:
                        v = data[i] == scalar
                    elif op == _CMP_NE:
                        v = data[i] != scalar
                    elif op == _CMP_LT:
                        v = data[i] < scalar
                    elif op == _CMP_LE:
                        v = data[i] <= scalar
                    elif op == _CMP_GT:
                        v = data[i] > scalar
                    else:
                        v = data[i] >= scalar
                    result.append(v)
                    if has_any_null:
                        result_mask.append(False)
            return self._build_result_col(
                ColumnData(result^), result_mask^, has_any_null
            )
        var visitor = _CmpScalarVisitor[op](self.null_mask_copy(), scalar)
        self._visit_raises(visitor)
        return self._build_result_col(
            ColumnData(visitor.result.copy()),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _cmp_scalar_eq(self, scalar: Float64) raises -> Column:
        return self._cmp_scalar_op[_CMP_EQ](scalar)

    def _cmp_scalar_ne(self, scalar: Float64) raises -> Column:
        return self._cmp_scalar_op[_CMP_NE](scalar)

    def _cmp_scalar_lt(self, scalar: Float64) raises -> Column:
        return self._cmp_scalar_op[_CMP_LT](scalar)

    def _cmp_scalar_le(self, scalar: Float64) raises -> Column:
        return self._cmp_scalar_op[_CMP_LE](scalar)

    def _cmp_scalar_gt(self, scalar: Float64) raises -> Column:
        return self._cmp_scalar_op[_CMP_GT](scalar)

    def _cmp_scalar_ge(self, scalar: Float64) raises -> Column:
        return self._cmp_scalar_op[_CMP_GE](scalar)

    def _cmp_scalar_op_int64[op: Int](self, scalar: Int64) raises -> Column:
        """Core element-wise scalar comparison kernel for ``Int64`` scalars.

        Like ``_cmp_scalar_op`` but accepts an ``Int64`` scalar so that
        integer columns are compared in exact integer arithmetic rather than
        being widened to ``Float64`` first.  This avoids precision loss for
        large integer values (|v| > 2**53).

        ``op`` is a compile-time constant (``_CMP_*``) that selects the
        operation.  Null propagation: null elements produce a null result.
        """
        # Storage-aware fast path using Float64 list from storage.  For
        # small scalars (|v| <= 2**53) the Float64 widening is exact; for
        # larger values we fall through to the legacy int64 visitor.
        if self._storage.isa[AnyArray]() and (
            self.is_int() or self.is_float() or self.is_bool()
        ):
            var f_scalar = Float64(scalar)
            if Int64(f_scalar) == scalar:
                var data = self._f64_list()
                var n = len(data)
                var result = List[Bool](capacity=n)
                var result_mask = List[Bool]()
                var has_any_null = self.has_nulls()
                for i in range(n):
                    if has_any_null and self.is_null(i):
                        result.append(False)
                        result_mask.append(True)
                    else:
                        var v: Bool
                        comptime if op == _CMP_EQ:
                            v = data[i] == f_scalar
                        elif op == _CMP_NE:
                            v = data[i] != f_scalar
                        elif op == _CMP_LT:
                            v = data[i] < f_scalar
                        elif op == _CMP_LE:
                            v = data[i] <= f_scalar
                        elif op == _CMP_GT:
                            v = data[i] > f_scalar
                        else:
                            v = data[i] >= f_scalar
                        result.append(v)
                        if has_any_null:
                            result_mask.append(False)
                return self._build_result_col(
                    ColumnData(result^), result_mask^, has_any_null
                )
        var visitor = _CmpScalarInt64Visitor[op](self.null_mask_copy(), scalar)
        self._visit_raises(visitor)
        return self._build_result_col(
            ColumnData(visitor.result.copy()),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _cmp_scalar_eq(self, scalar: Int64) raises -> Column:
        return self._cmp_scalar_op_int64[_CMP_EQ](scalar)

    def _cmp_scalar_ne(self, scalar: Int64) raises -> Column:
        return self._cmp_scalar_op_int64[_CMP_NE](scalar)

    def _cmp_scalar_lt(self, scalar: Int64) raises -> Column:
        return self._cmp_scalar_op_int64[_CMP_LT](scalar)

    def _cmp_scalar_le(self, scalar: Int64) raises -> Column:
        return self._cmp_scalar_op_int64[_CMP_LE](scalar)

    def _cmp_scalar_gt(self, scalar: Int64) raises -> Column:
        return self._cmp_scalar_op_int64[_CMP_GT](scalar)

    def _cmp_scalar_ge(self, scalar: Int64) raises -> Column:
        return self._cmp_scalar_op_int64[_CMP_GE](scalar)

    # ------------------------------------------------------------------
    # Boolean logical kernels
    # ------------------------------------------------------------------

    def _bool_op[
        op: Int
    ](self, op_name: String, other: Column) raises -> Column:
        """Core element-wise boolean logical kernel with Kleene null semantics.

        Dispatches through ``_BoolOpVisitor[op]``.  Both self and other must
        be bool_ columns; any other dtype raises.  ``op`` is one of the
        ``_BOOL_*`` compile-time constants.

        AND and OR use Kleene three-valued logic (False absorbs null for AND;
        True absorbs null for OR).  XOR uses standard null propagation.
        """
        if len(self) != len(other):
            raise Error(
                op_name
                + ": length mismatch ("
                + String(len(self))
                + " vs "
                + String(len(other))
                + ")"
            )
        # Fast path: both columns are bool — extract lists from storage.
        if (
            self.is_bool()
            and other.is_bool()
            and self._storage.isa[AnyArray]()
            and other._storage.isa[AnyArray]()
        ):
            var lhs = self._bool_list()
            var rhs = other._bool_list()
            var n = len(lhs)
            var has_a_mask = self.has_nulls()
            var has_b_mask = other.has_nulls()
            var result = List[Bool](capacity=n)
            var result_mask = List[Bool]()
            var has_any_null = False
            for i in range(n):
                var a_null = has_a_mask and self.is_null(i)
                var b_null = has_b_mask and other.is_null(i)
                comptime if op == _BOOL_AND:
                    if a_null:
                        if (not b_null) and (not rhs[i]):
                            result.append(False)
                            result_mask.append(False)
                        else:
                            result.append(False)
                            result_mask.append(True)
                            has_any_null = True
                    elif b_null:
                        if not lhs[i]:
                            result.append(False)
                            result_mask.append(False)
                        else:
                            result.append(False)
                            result_mask.append(True)
                            has_any_null = True
                    else:
                        result.append(lhs[i] and rhs[i])
                        result_mask.append(False)
                elif op == _BOOL_OR:
                    if a_null:
                        if (not b_null) and rhs[i]:
                            result.append(True)
                            result_mask.append(False)
                        else:
                            result.append(False)
                            result_mask.append(True)
                            has_any_null = True
                    elif b_null:
                        if lhs[i]:
                            result.append(True)
                            result_mask.append(False)
                        else:
                            result.append(False)
                            result_mask.append(True)
                            has_any_null = True
                    else:
                        result.append(lhs[i] or rhs[i])
                        result_mask.append(False)
                else:
                    if a_null or b_null:
                        result.append(False)
                        result_mask.append(True)
                        has_any_null = True
                    else:
                        result.append(
                            (lhs[i] and not rhs[i]) or (not lhs[i] and rhs[i])
                        )
                        result_mask.append(False)
            return self._build_result_col(
                ColumnData(result^), result_mask^, has_any_null
            )
        if not self.is_bool():
            raise Error("bool_op: non-bool column type on left-hand side")
        var visitor = _BoolOpVisitor[op](self.null_mask_copy(), other)
        self._visit_raises(visitor)
        return self._build_result_col(
            ColumnData(visitor.result.copy()),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _bool_and(self, other: Column) raises -> Column:
        return self._bool_op[_BOOL_AND]("and", other)

    def _bool_or(self, other: Column) raises -> Column:
        return self._bool_op[_BOOL_OR]("or", other)

    def _bool_xor(self, other: Column) raises -> Column:
        return self._bool_op[_BOOL_XOR]("xor", other)

    def _bool_invert(self) raises -> Column:
        """Element-wise boolean NOT.  Returns a bool_ Column with the same
        null mask; null elements remain null.  Raises if self is not bool_.
        """
        if not self.is_bool():
            raise Error("bool_op: non-bool column type (invert)")
        var data = self._bool_list()
        var n = len(data)
        var result = List[Bool](capacity=n)
        var result_mask = List[Bool]()
        var has_any_null = False
        var has_input_mask = self.has_nulls()
        for i in range(n):
            if has_input_mask and self.is_null(i):
                result.append(False)
                result_mask.append(True)
                has_any_null = True
            else:
                result.append(not data[i])
                result_mask.append(False)
        return self._build_result_col(
            ColumnData(result^), result_mask^, has_any_null
        )

    # ------------------------------------------------------------------
    # Transformation kernels
    # ------------------------------------------------------------------

    def _abs(self) raises -> Column:
        """Return element-wise absolute value.

        Int64 and Float64 arms are supported; Bool is identity.
        Nulls propagate. Raises for String/Object columns.
        """
        # Fast path: use storage lists when available.
        if self.is_int() and self._storage.isa[AnyArray]():
            var data = self._int64_list()
            var n = len(data)
            var has_mask = self.has_nulls()
            var result = List[Int64](capacity=n)
            var result_mask = List[Bool]()
            var has_any_null = False
            for i in range(n):
                if has_mask and self.is_null(i):
                    result.append(Int64(0))
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v = data[i]
                    result.append(v if v >= 0 else -v)
                    result_mask.append(False)
            return self._build_result_col(
                ColumnData(result^), result_mask^, has_any_null
            )
        if self.is_float() and self._storage.isa[AnyArray]():
            var data = self._float64_list()
            var n = len(data)
            var has_mask = self.has_nulls()
            var nan = Float64(0) / Float64(0)
            var result = List[Float64](capacity=n)
            var result_mask = List[Bool]()
            var has_any_null = False
            for i in range(n):
                if has_mask and self.is_null(i):
                    result.append(nan)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v = data[i]
                    result.append(v if v >= 0 else -v)
                    result_mask.append(False)
            return self._build_result_col(
                ColumnData(result^), result_mask^, has_any_null
            )
        if self.is_bool() and self._storage.isa[AnyArray]():
            return self.copy()
        var visitor = _AbsVisitor(self.null_mask_copy(), self.dtype.name)
        self._visit_raises(visitor)
        if visitor.is_identity:
            return self.copy()
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _round(self, decimals: Int = 0) raises -> Column:
        """Round Float64 values to ``decimals`` decimal places.

        Int64 and Bool columns are returned unchanged. Raises for
        String/Object columns or negative ``decimals``.
        Uses banker's rounding (round-half-to-even), matching Python and
        numpy behaviour at exact half-way points.
        """
        if decimals < 0:
            raise Error("round: negative decimals not supported")
        # Fast path: use storage arms when available.
        if (self.is_int() or self.is_bool()) and self._storage.isa[AnyArray]():
            return self.copy()
        if self.is_float() and self._storage.isa[AnyArray]():
            var data = self._float64_list()
            var n = len(data)
            var has_mask = self.has_nulls()
            var nan = Float64(0) / Float64(0)
            var scale = Float64(10) ** Float64(decimals)
            var result = List[Float64](capacity=n)
            var result_mask = List[Bool]()
            var has_any_null = False
            for i in range(n):
                if has_mask and self.is_null(i):
                    result.append(nan)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v = data[i] * scale
                    var lo = floor(v)
                    var frac = v - lo
                    if frac > 0.5:
                        v = lo + 1.0
                    elif frac < 0.5:
                        v = lo
                    else:
                        # Banker's rounding: round to even
                        var lo_int = Int64(lo)
                        if lo_int % 2 != 0:
                            v = lo + 1.0
                        else:
                            v = lo
                    result.append(v / scale)
                    result_mask.append(False)
            return self._build_result_col(
                ColumnData(result^), result_mask^, has_any_null
            )
        var visitor = _RoundVisitor(
            self.null_mask_copy(), decimals, self.dtype.name
        )
        self._visit_raises(visitor)
        if visitor.is_identity:
            return self.copy()
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _clip(
        self, lower: Optional[Float64], upper: Optional[Float64]
    ) raises -> Column:
        """Clamp values to [``lower``, ``upper``].

        Either bound may be ``None`` (no clipping on that side).  Supports
        Int64 and Float64 arms. Nulls propagate. Raises for String/Object
        columns.
        """
        # Fast path for float64/bool columns using storage.
        # Int64 columns use the legacy visitor to preserve Int64 output.
        if (
            self._storage.isa[AnyArray]()
            and (self.is_float() or self.is_bool())
            and self.dtype != int64
        ):
            var data = self._f64_list()
            var n = len(data)
            var has_mask = self.has_nulls()
            var has_lo = lower.__bool__()
            var has_hi = upper.__bool__()
            var lo = Float64(0)
            var hi = Float64(0)
            if has_lo:
                lo = lower.value()
            if has_hi:
                hi = upper.value()
            var nan = Float64(0) / Float64(0)
            var result_data = List[Float64](capacity=n)
            var result_mask = List[Bool]()
            var has_any_null = False
            for i in range(n):
                if has_mask and self.is_null(i):
                    result_data.append(nan)
                    result_mask.append(True)
                    has_any_null = True
                else:
                    var v = data[i]
                    if has_lo and v < lo:
                        v = lo
                    if has_hi and v > hi:
                        v = hi
                    result_data.append(v)
                    result_mask.append(False)
            return self._build_result_col(
                ColumnData(result_data^), result_mask^, has_any_null
            )
        var visitor = _ClipVisitor(
            self.null_mask_copy(), lower, upper, self.dtype.name
        )
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _apply[F: FloatTransformFn](self) raises -> Column:
        """Apply a compile-time function element-wise over Float64 values.

        Numeric arms are converted to Float64 before application. Nulls
        propagate. Raises for String/Object columns (via _to_float64_list).
        Call as ``col._apply[my_fn]()``.
        """
        var data = self._to_float64_list()
        var has_mask = self.has_nulls()
        var result = List[Float64]()
        var result_mask = List[Bool]()
        var has_any_null = False
        var nan = Float64(0) / Float64(0)
        for i in range(len(data)):
            if has_mask and self.is_null(i):
                result.append(nan)
                result_mask.append(True)
                has_any_null = True
            else:
                result.append(F(data[i]))
                result_mask.append(False)
        return self._build_result_col(
            ColumnData(result^), result_mask^, has_any_null
        )

    # ------------------------------------------------------------------
    # Element-wise math transforms via _apply[F]  (see #606, #607)
    # ------------------------------------------------------------------
    # NOTE: _abs and _round use dedicated visitors instead of _apply[F]
    # because they preserve input dtype (Int64 in -> Int64 out for _abs,
    # identity for Int64/Bool in _round). _apply[F] always returns
    # Float64.  New element-wise transforms that produce Float64 should
    # use _apply[F] — see the methods below.

    def _sqrt(self) raises -> Column:
        return self._apply[_sqrt_fn]()

    def _exp(self) raises -> Column:
        return self._apply[_exp_fn]()

    def _log(self) raises -> Column:
        return self._apply[_log_fn]()

    def _log10(self) raises -> Column:
        return self._apply[_log10_fn]()

    def _ceil(self) raises -> Column:
        return self._apply[_ceil_fn]()

    def _floor(self) raises -> Column:
        return self._apply[_floor_fn]()

    def _neg(self) raises -> Column:
        return self._apply[_neg_fn]()

    def _isin_kernel[
        T: Comparable & Copyable & Movable
    ](self, d: List[T], values: List[T]) raises -> Column:
        """Shared kernel for all _isin_* methods.

        Builds a Bool Column: True where element is in ``values``.
        Nulls propagate as null.
        """
        var has_mask = self.has_nulls()
        var result = List[Bool]()
        var result_mask = List[Bool]()
        var has_any_null = False
        for i in range(len(d)):
            if has_mask and self.is_null(i):
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
        return self._build_result_col(
            ColumnData(result^), result_mask^, has_any_null
        )

    def _isin_int(self, values: List[Int64]) raises -> Column:
        """Bool Column: True where element is in ``values`` (Int64 columns only).

        Nulls propagate as null.
        """
        if not self.is_int():
            raise Error(
                "isin: column must be Int64 to match against List[Int64]"
            )
        return self._isin_kernel(self._int64_data(), values)

    def _isin_float(self, values: List[Float64]) raises -> Column:
        """Bool Column: True where element is in ``values`` (Float64 columns only).

        Nulls propagate as null.
        """
        if not self.is_float():
            raise Error(
                "isin: column must be Float64 to match against List[Float64]"
            )
        return self._isin_kernel(self._float64_data(), values)

    def _isin_str(self, values: List[String]) raises -> Column:
        """Bool Column: True where element is in ``values`` (String columns only).

        Nulls propagate as null.
        """
        if not self.is_string():
            raise Error(
                "isin: column must be String to match against List[String]"
            )
        return self._isin_kernel(self._str_data(), values)

    def _isin_bool(self, values: List[Bool]) raises -> Column:
        """Bool Column: True where element is in ``values`` (Bool columns only).

        Nulls propagate as null.
        """
        if not self.is_bool():
            raise Error("isin: column must be Bool to match against List[Bool]")
        return self._isin_kernel(self._bool_data(), values)

    def _isin_scalars(self, scalars: List[DFScalar]) raises -> Column:
        """Bool Column: True where each element appears in ``scalars``.

        Dispatches on the active ``ColumnData`` arm via ``_IsInVisitor``,
        which performs scalar-type coercion internally.  Raises for object
        dtype columns.  Nulls propagate as null.
        """
        var visitor = _IsInVisitor(self.null_mask_copy(), scalars)
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _between(self, left: Float64, right: Float64) raises -> Column:
        """Bool Column: True where left <= element <= right.

        Numeric arms are converted to Float64. Nulls propagate.
        Raises for String/Object columns (via _to_float64_list).
        """
        var data = self._to_float64_list()
        var has_mask = self.has_nulls()
        var result = List[Bool]()
        var result_mask = List[Bool]()
        var has_any_null = False
        for i in range(len(data)):
            if has_mask and self.is_null(i):
                result.append(False)
                result_mask.append(True)
                has_any_null = True
            else:
                result.append(data[i] >= left and data[i] <= right)
                result_mask.append(False)
        return self._build_result_col(
            ColumnData(result^), result_mask^, has_any_null
        )

    def _where_mask[
        mode: Int
    ](self, cond: Column, other: Optional[DFScalar] = None) raises -> Column:
        """Shared kernel for ``_where`` (mode=1) and ``_mask`` (mode=0).

        mode=1: keep value where cond is True, replace otherwise.
        mode=0: replace value where cond is True, keep otherwise.
        When ``other`` is None, replaced cells become null.
        Supports Int64, Float64, Bool, String arms. Raises for Object dtype.
        """
        if not cond.is_bool():
            raise Error("where/mask: condition must be a Bool Series")
        if len(self) != len(cond):
            raise Error(
                "where/mask: length mismatch ("
                + String(len(self))
                + " vs "
                + String(len(cond))
                + ")"
            )
        var keep_on_true = mode == 1
        var cond_bools = cond._bool_list()
        var visitor = _WhereMaskVisitor(
            self.null_mask_copy(),
            cond_bools^,
            cond.null_mask_copy(),
            keep_on_true,
            other,
        )
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _where(
        self, cond: Column, other: Optional[DFScalar] = None
    ) raises -> Column:
        """Keep value where ``cond`` is True; replace with ``other`` (or null) otherwise.
        """
        return self._where_mask[1](cond, other)

    def _mask(
        self, cond: Column, other: Optional[DFScalar] = None
    ) raises -> Column:
        """Replace with ``other`` (or null) where ``cond`` is True; keep otherwise.
        """
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
        var visitor = _CombineFirstVisitor(
            self.null_mask_copy(), other._to_col_data(), other.null_mask_copy()
        )
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def _unique(self) raises -> Column:
        """Return a Column of unique values, preserving first-occurrence order.

        Nulls are included once at the end if present. Uses a Set[T] for O(1)
        membership checks, giving O(n) overall complexity.
        Raises for Object dtype.
        """
        var visitor = _UniqueVisitor(self.null_mask_copy())
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

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
            self.null_mask_copy(),
            target_dtype.is_integer(),
            target_dtype.is_float(),
            target_dtype == bool_,
            target_dtype.name,
            self.dtype.name,
        )
        self._visit_raises(visitor)
        if visitor.is_identity:
            return self.copy()
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

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
        c._index = ColumnIndex(List[PythonObject]())
        return c^

    def _to_column_index(self) raises -> ColumnIndex:
        """Extract column values as a ColumnIndex for use as a row index.

        Int64 columns produce a List[Int64] ColumnIndex; String columns produce
        an Index (List[String]) ColumnIndex; all other types fall back to
        List[PythonObject].
        """
        var visitor = _ToColumnIndexVisitor(self.null_mask_copy())
        self._visit_raises(visitor)
        return visitor.result.copy()

    # Kept for backward compatibility with callers that still need raw PythonObject.
    def _to_pyobj_index(self) raises -> List[PythonObject]:
        """Extract column values as a List[PythonObject] for use as a row index.

        Converts each typed value to its Python equivalent using the
        ``_ToPandasVisitor`` pattern so that the result can be stored in
        ``Column._index`` of other columns.
        """
        var py_list = Python.evaluate("[]")
        var py_none = Python.evaluate("None")
        var visitor = _ToPandasVisitor(py_list, py_none, self.null_mask_copy())
        self._visit_raises(visitor)
        var n = Int(py_list.__len__())
        var result = List[PythonObject]()
        for i in range(n):
            result.append(py_list[i])
        return result^

    def _reindex_rows(
        self, indices: List[Int], fill_value: Optional[DFScalar]
    ) raises -> Column:
        """Return a new Column with rows selected or inserted according to *indices*.

        ``indices[i] >= 0``  → take that row from self.
        ``indices[i] == -1`` → insert a null row, or a fill row when *fill_value*
                                is provided.
        Existing null mask entries are propagated for taken rows.
        """
        var visitor = _ReindexRowsVisitor(
            indices, fill_value, self.null_mask_copy()
        )
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    # ------------------------------------------------------------------
    # Time-series transforms
    # ------------------------------------------------------------------

    def shift(self, periods: Int = 1) raises -> Column:
        """Return a Column with values shifted by *periods* positions.

        Positive *periods* lags the series (first *periods* rows become null);
        negative *periods* leads (last *|periods|* rows become null).
        Supports all column types: integer, float, bool, string, and object.
        """
        var n = len(self)
        var indices = List[Int]()
        if periods >= 0:
            var shift_count = periods if periods <= n else n
            for _ in range(shift_count):
                indices.append(-1)
            for i in range(n - shift_count):
                indices.append(i)
        else:
            var shift_count = -periods if -periods <= n else n
            for i in range(shift_count, n):
                indices.append(i)
            for _ in range(shift_count):
                indices.append(-1)
        return self._reindex_rows(indices, Optional[DFScalar](None))

    def diff(self, periods: Int = 1) raises -> Column:
        """Return the first discrete difference along the column.

        ``result[i] = self[i] - self[i - periods]``.
        Exposed positions (the first *periods* rows for positive *periods*,
        or the last *|periods|* rows for negative *periods*) are null.
        Raises for non-numeric column types.
        """
        return self._arith_sub(self.shift(periods))

    def pct_change(self, periods: Int = 1) raises -> Column:
        """Return the percentage change between elements *periods* apart.

        ``result[i] = (self[i] - self[i - periods]) / self[i - periods]``.
        Exposed positions are null.
        Raises for non-numeric column types.
        """
        return self.diff(periods)._arith_div(self.shift(periods))

    # ------------------------------------------------------------------
    # Cumulative operations
    # ------------------------------------------------------------------

    def _cumop_f64_list(self, skipna: Bool, mode: Int) raises -> Column:
        """Shared cumulative fast path over the float64 list extracted
        from the AnyArray arm.

        ``mode``: 1=sum, 2=prod, 3=min, 4=max.
        Only called for float64/bool columns (int64 uses the visitor to
        preserve Int64 output).
        """
        var data = self._f64_list()
        var n = len(data)
        var has_mask = self.has_nulls()
        var propagate_nan = False
        var nan = Float64(0) / Float64(0)
        var running: Float64
        if mode == 2:
            running = Float64(1)
        else:
            running = Float64(0)
        var first_seen = mode <= 2  # sum/prod: running init is correct
        var result_data = List[Float64](capacity=n)
        var result_mask = List[Bool]()
        var has_any_null = False
        for i in range(n):
            var is_null = has_mask and self.is_null(i)
            if is_null:
                if not skipna:
                    propagate_nan = True
                result_data.append(nan)
                result_mask.append(True)
                has_any_null = True
            elif propagate_nan:
                result_data.append(nan)
                result_mask.append(True)
                has_any_null = True
            else:
                var v = data[i]
                if mode == 1:
                    running += v
                elif mode == 2:
                    running *= v
                elif mode == 3:
                    if not first_seen or v < running:
                        running = v
                        first_seen = True
                else:
                    if not first_seen or v > running:
                        running = v
                        first_seen = True
                result_data.append(running)
                result_mask.append(False)
        return self._build_result_col(
            ColumnData(result_data^), result_mask^, has_any_null
        )

    def cumsum(self, skipna: Bool = True) raises -> Column:
        """Return a Column of cumulative sums, preserving dtype.

        Integer input produces an Int64 result.  Float64 and Bool input
        produce a Float64 result.
        When ``skipna=True`` (default), null elements produce NaN/null in the
        output but do not affect subsequent cumulative values.
        When ``skipna=False``, a null element propagates NaN/null to all
        subsequent positions.
        Raises for non-numeric column types.
        """
        # Fast path for float64/bool columns using storage.
        # Int64 columns use the legacy visitor to preserve Int64 output.
        if (
            self._storage.isa[AnyArray]()
            and (self.is_float() or self.is_bool())
            and self.dtype != int64
        ):
            return self._cumop_f64_list(skipna, 1)
        var visitor = _CumSumVisitor(skipna, self.null_mask_copy())
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def cumprod(self, skipna: Bool = True) raises -> Column:
        """Return a Column of cumulative products, preserving dtype.

        When ``skipna=True`` (default), null elements produce NaN/null in the
        output but do not affect subsequent cumulative values.
        When ``skipna=False``, a null element propagates NaN/null to all
        subsequent positions.
        Raises for non-numeric column types.
        """
        if (
            self._storage.isa[AnyArray]()
            and (self.is_float() or self.is_bool())
            and self.dtype != int64
        ):
            return self._cumop_f64_list(skipna, 2)
        var visitor = _CumProdVisitor(skipna, self.null_mask_copy())
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def cummin(self, skipna: Bool = True) raises -> Column:
        """Return a Column of cumulative minimums, preserving dtype.

        When ``skipna=True`` (default), null elements produce NaN/null in the
        output but do not affect subsequent cumulative values.
        When ``skipna=False``, a null element propagates NaN/null to all
        subsequent positions.
        Raises for non-numeric column types.
        """
        if (
            self._storage.isa[AnyArray]()
            and (self.is_float() or self.is_bool())
            and self.dtype != int64
        ):
            return self._cumop_f64_list(skipna, 3)
        var visitor = _CumMinVisitor(skipna, self.null_mask_copy())
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    def cummax(self, skipna: Bool = True) raises -> Column:
        """Return a Column of cumulative maximums, preserving dtype.

        When ``skipna=True`` (default), null elements produce NaN/null in the
        output but do not affect subsequent cumulative values.
        When ``skipna=False``, a null element propagates NaN/null to all
        subsequent positions.
        Raises for non-numeric column types.
        """
        if (
            self._storage.isa[AnyArray]()
            and (self.is_float() or self.is_bool())
            and self.dtype != int64
        ):
            return self._cumop_f64_list(skipna, 4)
        var visitor = _CumMaxVisitor(skipna, self.null_mask_copy())
        self._visit_raises(visitor)
        return self._build_result_col(
            visitor.col_data.copy(),
            visitor.result_mask.copy(),
            visitor.has_any_null,
        )

    # ------------------------------------------------------------------
    # Pandas interop
    # ------------------------------------------------------------------

    @staticmethod
    def from_pandas(
        pd_series: PythonObject, name: Optional[String]
    ) raises -> Column:
        """Build a Column by copying values from a pandas Series."""
        var dtype_str = String(pd_series.dtype)
        var n = Int(pd_series.__len__())
        var py_list = pd_series.tolist()
        var pd_idx = pd_series.index
        var py_index = pd_idx.tolist()

        # Detect the pandas index type and convert to the most-native ColumnIndex.
        var bison_idx: ColumnIndex
        var idx_class = String(pd_idx.__class__.__name__)
        var idx_dtype = String(pd_idx.dtype)
        if idx_class == "RangeIndex":
            var idx_start = Int(py=pd_idx.start)
            var idx_stop = Int(py=pd_idx.stop)
            var idx_step = Int(py=pd_idx.step)
            if idx_start == 0 and idx_step == 1 and idx_stop == n:
                # Default 0-based RangeIndex — use empty list (no explicit index).
                bison_idx = ColumnIndex(List[PythonObject]())
            else:
                # Non-default RangeIndex — materialise as Int64.
                var int_idx = List[Int64]()
                for i in range(n):
                    int_idx.append(Int64(idx_start + i * idx_step))
                bison_idx = ColumnIndex(int_idx^)
        elif (
            idx_dtype == "int8"
            or idx_dtype == "int16"
            or idx_dtype == "int32"
            or idx_dtype == "int64"
            or idx_dtype == "uint8"
            or idx_dtype == "uint16"
            or idx_dtype == "uint32"
            or idx_dtype == "uint64"
        ):
            var int_idx = List[Int64]()
            for i in range(n):
                int_idx.append(Int64(Int(py=py_index[i])))
            bison_idx = ColumnIndex(int_idx^)
        elif idx_dtype == "float32" or idx_dtype == "float64":
            var struct_mod = Python.import_module("struct")
            var pack_fmt = "d"
            var unpack_fmt = "q"
            var flt_idx = List[Float64]()
            for i in range(n):
                var packed = struct_mod.unpack(
                    unpack_fmt, struct_mod.pack(pack_fmt, py_index[i])
                )
                var bits = Int64(Int(py=packed[0]))
                flt_idx.append(bitcast[DType.float64](bits))
            bison_idx = ColumnIndex(flt_idx^)
        elif idx_dtype == "object" and idx_class == "Index":
            # Treat as a string index (most common object-dtype index).
            var str_idx = List[String]()
            for i in range(n):
                str_idx.append(String(py_index[i]))
            bison_idx = ColumnIndex(Index(str_idx^))
        else:
            # Fallback: keep as PythonObject (DatetimeIndex, Float64Index, …).
            var obj_idx = List[PythonObject]()
            for i in range(n):
                obj_idx.append(py_index[i])
            bison_idx = ColumnIndex(obj_idx^)

        # Build the null mask once, used by every branch below.
        var null_list = pd_series.isna().tolist()
        var null_mask = NullMask()
        for i in range(n):
            if Bool(null_list[i].__bool__()):
                null_mask.append_null()
            else:
                null_mask.append_valid()

        # Capture the pandas index name (may be None).
        var idx_name = String("")
        var raw_idx_name = pd_idx.name
        if raw_idx_name.__class__.__name__ != "NoneType":
            idx_name = String(raw_idx_name)

        var bison_dtype: BisonDtype
        if (
            dtype_str == "int8"
            or dtype_str == "int16"
            or dtype_str == "int32"
            or dtype_str == "int64"
            or dtype_str == "uint8"
            or dtype_str == "uint16"
            or dtype_str == "uint32"
            or dtype_str == "uint64"
        ):
            bison_dtype = int64
        elif dtype_str == "float32" or dtype_str == "float64":
            bison_dtype = float64
        elif dtype_str == "bool" or dtype_str == "boolean":
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
            var col = Column(name, ColumnData(data^), bison_dtype, bison_idx^)
            col.set_null_mask(null_mask.copy())
            col._index_name = idx_name
            return col^
        elif bison_dtype == float64:
            var data = List[Float64]()
            var struct_mod = Python.import_module("struct")
            var pack_fmt = "d"  # pack as IEEE-754 double-precision float
            var unpack_fmt = (  # unpack as signed 64-bit integer (same bytes, avoids overflow in Int)
                "q"
            )
            for i in range(n):
                if null_mask[i]:
                    data.append(Float64(0))  # placeholder for null
                else:
                    var packed = struct_mod.unpack(
                        unpack_fmt, struct_mod.pack(pack_fmt, py_list[i])
                    )
                    var bits = Int64(Int(py=packed[0]))
                    data.append(bitcast[DType.float64](bits))
            var col = Column(name, ColumnData(data^), bison_dtype, bison_idx^)
            col.set_null_mask(null_mask.copy())
            col._index_name = idx_name
            return col^
        elif bison_dtype == bool_:
            var data = List[Bool]()
            for i in range(n):
                if null_mask[i]:
                    data.append(False)  # placeholder for null
                else:
                    data.append(Bool(py_list[i].__bool__()))
            var col = Column(name, ColumnData(data^), bison_dtype, bison_idx^)
            col.set_null_mask(null_mask.copy())
            col._index_name = idx_name
            return col^
        elif dtype_str == "string":
            var data = List[String]()
            for i in range(n):
                if null_mask[i]:
                    data.append(String(""))  # placeholder for null
                else:
                    data.append(String(py_list[i]))
            # #644: string-backed columns carry string_ dtype.
            var col = Column(name, ColumnData(data^), string_, bison_idx^)
            col.set_null_mask(null_mask.copy())
            col._index_name = idx_name
            return col^
        else:
            # Promote pure-string object columns to List[String] so that
            # marrow fast paths (GroupBy, SIMD kernels) can activate.
            if bison_dtype == object_:
                var all_str = True
                var builtins = Python.import_module("builtins")
                var str_type = builtins.str
                var isinstance_fn = builtins.isinstance
                for i in range(n):
                    if null_mask[i]:
                        continue
                    if not isinstance_fn(py_list[i], str_type).__bool__():
                        all_str = False
                        break
                if all_str:
                    var str_data = List[String]()
                    for i in range(n):
                        if null_mask[i]:
                            str_data.append(String(""))
                        else:
                            str_data.append(String(py_list[i]))
                    # #644: promoted string columns carry string_ dtype.
                    var col = Column(
                        name, ColumnData(str_data^), string_, bison_idx^
                    )
                    col.set_null_mask(null_mask^)
                    col._index_name = idx_name
                    return col^
            var data = List[PythonObject]()
            for i in range(n):
                data.append(py_list[i])
            var col = Column(name, ColumnData(data^), bison_dtype, bison_idx^)
            col._index_name = idx_name
            # Object-arm columns: populate the LegacyObjectData arm
            # with the null mask so downstream readers see a coherent view.
            col._storage = ColumnStorage(
                LegacyObjectData(
                    col._storage_legacy().data.copy(),
                    null_mask^,
                )
            )
            return col^

    @staticmethod
    def _sniff_dtype(data: ColumnData) -> BisonDtype:
        """Return the BisonDtype that matches the active ColumnData arm."""
        if data.isa[List[Int64]]():
            return int64
        if data.isa[List[Float64]]():
            return float64
        if data.isa[List[Bool]]():
            return bool_
        if data.isa[List[String]]():
            return string_
        return object_

    @staticmethod
    def _null_column(
        name: Optional[String],
        dtype: BisonDtype,
        n: Int,
        var index: ColumnIndex,
    ) raises -> Column:
        """Create an all-null Column of length *n* with the given *dtype*.

        The underlying storage uses the canonical Mojo type for *dtype*
        (Int64 for integer families, Float64 for float families, Bool for
        bool, PythonObject for everything else).  Every element is marked
        null via ``set_null_mask``.
        """
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
        elif dtype == string_:
            # #644: string_ columns must be backed by List[String] to
            # preserve the dtype == string_ ⟺ string storage arm invariant.
            var data = List[String]()
            for _ in range(n):
                data.append(String(""))
            c = Column(name, ColumnData(data^), string_, index^)
        else:
            var data = List[PythonObject]()
            var py_none = Python.evaluate("None")
            for _ in range(n):
                data.append(py_none)
            c = Column(name, ColumnData(data^), dtype, index^)
        c.set_null_mask(NullMask.all_null(n))
        return c^

    @staticmethod
    def _fill_scalar(
        name: Optional[String], value: DFScalar, n: Int, index: ColumnIndex
    ) raises -> Column:
        """Create a Column of length *n* with every element equal to *value*.

        The dtype is inferred from the DFScalar arm: Int64 → int64, Float64 → float64,
        Bool → bool_, String → string_ (#644).
        Raises if *value* is null — use the null-mask path instead.
        """
        var visitor = _FillScalarVisitor(n)
        visit_scalar_raises(visitor, value)
        var dtype = visitor._dtype
        return Column(name, visitor^._col_data.copy(), dtype, index.copy())

    def to_pandas(self) raises -> PythonObject:
        """Reconstruct a pandas Series from stored values."""
        var pd = Python.import_module("pandas")
        var py_list = Python.evaluate("[]")
        var py_none = Python.evaluate("None")
        var visitor = _ToPandasVisitor(py_list, py_none, self.null_mask_copy())
        self._visit_raises(visitor)
        # Detect integer columns that contain nulls and promote to the
        # corresponding pandas nullable integer dtype (e.g. "Int64") so that
        # the None entries in py_list are accepted without raising.
        var dtype_name = self.dtype.name
        # #644: map string_ → pandas "object" so to_pandas / from_pandas
        # round-trips are idempotent. Pandas "string" dtype is the nullable
        # StringDtype with pd.NA semantics; keeping "object" preserves
        # existing behaviour.
        if dtype_name == "string":
            dtype_name = "object"
        if self.dtype.is_integer():
            var has_nulls = False
            for i in range(len(self)):
                if self.is_null(i):
                    has_nulls = True
                    break
            if has_nulls:
                if dtype_name == "int8":
                    dtype_name = "Int8"
                elif dtype_name == "int16":
                    dtype_name = "Int16"
                elif dtype_name == "int32":
                    dtype_name = "Int32"
                elif dtype_name == "int64":
                    dtype_name = "Int64"
                elif dtype_name == "uint8":
                    dtype_name = "UInt8"
                elif dtype_name == "uint16":
                    dtype_name = "UInt16"
                elif dtype_name == "uint32":
                    dtype_name = "UInt32"
                else:  # uint64
                    dtype_name = "UInt64"
        var n_idx = self._index_len()
        var pd_name: PythonObject
        if not self.name:
            pd_name = Python.evaluate("None")
        else:
            pd_name = PythonObject(self.name.value())
        var pd_index: PythonObject
        if n_idx > 0:
            var idx_py = Python.evaluate("[]")
            if self._index.isa[Index]():
                ref str_idx = self._index[Index]
                for i in range(n_idx):
                    _ = idx_py.append(PythonObject(str_idx[i]))
            elif self._index.isa[List[Int64]]():
                ref int_idx = self._index[List[Int64]]
                for i in range(n_idx):
                    _ = idx_py.append(PythonObject(Int(int_idx[i])))
            elif self._index.isa[List[Float64]]():
                ref flt_idx = self._index[List[Float64]]
                for i in range(n_idx):
                    _ = idx_py.append(PythonObject(flt_idx[i]))
            else:
                ref obj_idx = self._index[List[PythonObject]]
                for i in range(n_idx):
                    _ = idx_py.append(obj_idx[i])
            if len(self._index_names) > 1:
                # MultiIndex: build pd.MultiIndex from the list of tuples.
                var py_names = Python.evaluate("[]")
                for k in range(len(self._index_names)):
                    _ = py_names.append(PythonObject(self._index_names[k]))
                pd_index = pd.MultiIndex.from_tuples(idx_py, names=py_names)
            elif self._index_name:
                pd_index = pd.Index(idx_py, name=self._index_name)
            else:
                pd_index = idx_py
        elif self._index_name:
            pd_index = pd.RangeIndex(self.__len__(), name=self._index_name)
        else:
            return pd.Series(py_list, name=pd_name, dtype=dtype_name)
        return pd.Series(
            py_list, name=pd_name, dtype=dtype_name, index=pd_index
        )


# ------------------------------------------------------------------
# Cell-access helpers used by both dataframe.mojo and series.mojo
# ------------------------------------------------------------------


def _csv_quote_field(field: String, sep: String) -> String:
    """Return *field* quoted for CSV output if it contains *sep*, a
    newline, or a double-quote character; otherwise return *field* as-is.
    Double-quote characters inside the field are escaped by doubling them.
    """
    var needs_quote = (
        field.find(sep) >= 0 or field.find("\n") >= 0 or field.find('"') >= 0
    )
    if not needs_quote:
        return field
    return '"' + field.replace('"', '""') + '"'


def _col_cell_pyobj(col: Column, row: Int) raises -> PythonObject:
    """Return a ``PythonObject`` representation of cell *row* in *col*.

    Null cells (masked entries) are returned as Python ``None``.
    """
    if col.has_nulls() and row < len(col) and col.is_null(row):
        return Python.evaluate("None")
    # Fast path: read from AnyArray storage via O(1) unsafe_get.
    if col._storage.isa[AnyArray]():
        if col.is_int():
            ref a = col._storage[AnyArray].as_int64()
            return PythonObject(Int(rebind[Int64](a.unsafe_get(row))))
        if col.is_bool():
            ref a = col._storage[AnyArray].as_bool()
            return PythonObject(a.values().test(a.offset + row))
        if col.is_string():
            ref a = col._storage[AnyArray].as_string()
            return PythonObject(String(a.unsafe_get(UInt(row))))
        if col.is_float():
            ref a = col._storage[AnyArray].as_float64()
            return PythonObject(rebind[Float64](a.unsafe_get(row)))
    # LegacyObjectData fallback: includes object / datetime / timedelta
    # columns as well as string-with-nulls columns (which store wrapped
    # PythonObject strings).
    if col.is_string():
        ref d = col._storage[LegacyObjectData].data
        return PythonObject(String(d[row]))
    var visitor = _CellToPyObjVisitor(row)
    col._visit_raises(visitor)
    return visitor.result


def _scalar_from_col(col: Column, row: Int) raises -> DFScalar:
    """Extract cell (*row*) from *col* as a ``DFScalar``.

    Returns ``DFScalar.null()`` when the cell is masked.
    ``List[PythonObject]`` cells are stringified since ``DFScalar`` has no
    ``PythonObject`` arm.
    """
    if col.has_nulls() and col.is_null(row):
        return DFScalar.null()
    # Fast path: read from AnyArray storage via O(1) unsafe_get.
    if col._storage.isa[AnyArray]():
        if col.is_int():
            ref a = col._storage[AnyArray].as_int64()
            return DFScalar(rebind[Int64](a.unsafe_get(row)))
        if col.is_bool():
            ref a = col._storage[AnyArray].as_bool()
            return DFScalar(a.values().test(a.offset + row))
        if col.is_string():
            ref a = col._storage[AnyArray].as_string()
            return DFScalar(String(a.unsafe_get(UInt(row))))
        if col.is_float():
            ref a = col._storage[AnyArray].as_float64()
            return DFScalar(rebind[Float64](a.unsafe_get(row)))
    if col.is_string():
        ref d = col._storage[LegacyObjectData].data
        return DFScalar(String(d[row]))
    var visitor = _ScalarFromColVisitor(row)
    col._visit_raises(visitor)
    return visitor.result


def _col_cell_str(col: Column, row: Int) raises -> String:
    """Return the string representation of cell *row* in *col*.

    Null cells (masked entries) are returned as an empty string.
    """
    if col.has_nulls() and row < len(col) and col.is_null(row):
        return String("")
    # Fast path: read from AnyArray storage via O(1) unsafe_get.
    if col._storage.isa[AnyArray]():
        if col.is_int():
            ref a = col._storage[AnyArray].as_int64()
            return String(Int(rebind[Int64](a.unsafe_get(row))))
        if col.is_bool():
            ref a = col._storage[AnyArray].as_bool()
            if a.values().test(a.offset + row):
                return String("True")
            return String("False")
        if col.is_string():
            ref a = col._storage[AnyArray].as_string()
            return String(a.unsafe_get(UInt(row)))
        if col.is_float():
            ref a = col._storage[AnyArray].as_float64()
            return String(rebind[Float64](a.unsafe_get(row)))
    if col.is_string():
        ref d = col._storage[LegacyObjectData].data
        return String(d[row])
    var visitor = _CellToStrVisitor(row)
    col._visit_raises(visitor)
    return visitor.result


def _series_scalar_at(col: Column, row: Int) raises -> SeriesScalar:
    """Extract cell (*row*) from *col* as a ``SeriesScalar``.

    Unlike ``_scalar_from_col`` (which returns ``DFScalar`` and stringifies
    ``PythonObject`` cells), this preserves the original typed arm including
    ``PythonObject``.
    """
    # Fast path: read from AnyArray storage via O(1) unsafe_get.
    if col._storage.isa[AnyArray]():
        if col.is_int():
            ref a = col._storage[AnyArray].as_int64()
            return SeriesScalar(rebind[Int64](a.unsafe_get(row)))
        if col.is_bool():
            ref a = col._storage[AnyArray].as_bool()
            return SeriesScalar(a.values().test(a.offset + row))
        if col.is_string():
            ref a = col._storage[AnyArray].as_string()
            return SeriesScalar(String(a.unsafe_get(UInt(row))))
        if col.is_float():
            ref a = col._storage[AnyArray].as_float64()
            return SeriesScalar(rebind[Float64](a.unsafe_get(row)))
    if col.is_string():
        ref d = col._storage[LegacyObjectData].data
        return SeriesScalar(String(d[row]))
    var visitor = _SeriesScalarAtVisitor(row)
    col._visit_raises(visitor)
    return visitor.result
