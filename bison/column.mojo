from python import Python, PythonObject
from utils import Variant
from memory import bitcast
from collections import Dict
from math import sqrt
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


struct Column(Copyable, Movable):
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
        var n = self.__len__()
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
        var n = self.count() if skipna else self.__len__()
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
        var n = self.count() if skipna else self.__len__()
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

        Raises for non-numeric column types.
        """
        var seen = Dict[String, Bool]()
        var has_mask = len(self._null_mask) > 0
        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    continue
                seen[String(self._data[List[Int64]][i])] = True
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    continue
                seen[String(self._data[List[Float64]][i])] = True
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    continue
                seen[String(self._data[List[Bool]][i])] = True
        else:
            raise Error("nunique: non-numeric column type")
        return len(seen)

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
