from python import Python, PythonObject
from utils import Variant
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
    # Typed accessor helpers — the only sites that call isa/get
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
        if self._data.isa[List[Int64]]():
            var d = self._data[List[Int64]].copy()
            var idx = self._index.copy()
            var mask = self._null_mask.copy()
            var col = Column(self.name, ColumnData(d^), self.dtype, idx^)
            col._null_mask = mask^
            return col^
        elif self._data.isa[List[Float64]]():
            var d = self._data[List[Float64]].copy()
            var idx = self._index.copy()
            var mask = self._null_mask.copy()
            var col = Column(self.name, ColumnData(d^), self.dtype, idx^)
            col._null_mask = mask^
            return col^
        elif self._data.isa[List[Bool]]():
            var d = self._data[List[Bool]].copy()
            var idx = self._index.copy()
            var mask = self._null_mask.copy()
            var col = Column(self.name, ColumnData(d^), self.dtype, idx^)
            col._null_mask = mask^
            return col^
        elif self._data.isa[List[String]]():
            var d = self._data[List[String]].copy()
            var idx = self._index.copy()
            var mask = self._null_mask.copy()
            var col = Column(self.name, ColumnData(d^), self.dtype, idx^)
            col._null_mask = mask^
            return col^
        else:
            var d = self._data[List[PythonObject]].copy()
            var idx = self._index.copy()
            var mask = self._null_mask.copy()
            var col = Column(self.name, ColumnData(d^), self.dtype, idx^)
            col._null_mask = mask^
            return col^

    # ------------------------------------------------------------------
    # Length
    # ------------------------------------------------------------------

    fn __len__(self) -> Int:
        if self._data.isa[List[Int64]]():
            return len(self._data[List[Int64]])
        elif self._data.isa[List[Float64]]():
            return len(self._data[List[Float64]])
        elif self._data.isa[List[Bool]]():
            return len(self._data[List[Bool]])
        elif self._data.isa[List[String]]():
            return len(self._data[List[String]])
        else:
            return len(self._data[List[PythonObject]])

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
            for i in range(n):
                if null_mask[i]:
                    data.append(Float64(0))  # placeholder for null
                else:
                    data.append(Float64(String(py_list[i])))
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
        if data.isa[List[Int64]]():
            return int64
        elif data.isa[List[Float64]]():
            return float64
        elif data.isa[List[Bool]]():
            return bool_
        else:
            return object_  # List[String] and List[PythonObject] both map to object_

    fn to_pandas(self) raises -> PythonObject:
        """Reconstruct a pandas Series from stored values."""
        var pd = Python.import_module("pandas")
        var py_list = Python.evaluate("[]")
        var py_none = Python.evaluate("None")
        var has_mask = len(self._null_mask) > 0
        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                if has_mask and self._null_mask[i]:
                    _ = py_list.append(py_none)
                else:
                    _ = py_list.append(self._data[List[Int64]][i])
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                if has_mask and self._null_mask[i]:
                    _ = py_list.append(py_none)
                else:
                    _ = py_list.append(self._data[List[Float64]][i])
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                if has_mask and self._null_mask[i]:
                    _ = py_list.append(py_none)
                else:
                    _ = py_list.append(self._data[List[Bool]][i])
        elif self._data.isa[List[String]]():
            for i in range(len(self._data[List[String]])):
                if has_mask and self._null_mask[i]:
                    _ = py_list.append(py_none)
                else:
                    _ = py_list.append(self._data[List[String]][i])
        else:
            for i in range(len(self._data[List[PythonObject]])):
                _ = py_list.append(self._data[List[PythonObject]][i])
        if len(self._index) > 0:
            var idx_py = Python.evaluate("[]")
            for i in range(len(self._index)):
                _ = idx_py.append(self._index[i])
            return pd.Series(py_list, name=self.name, dtype=self.dtype.name, index=idx_py)
        return pd.Series(py_list, name=self.name, dtype=self.dtype.name)
