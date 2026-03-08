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
    """

    var name: String
    var dtype: BisonDtype
    var _data: ColumnData
    var _index: List[PythonObject]

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    fn __init__(out self):
        """Empty column with object dtype — used as stub placeholder."""
        self.name  = ""
        self.dtype = object_
        self._data = ColumnData(List[PythonObject]())
        self._index = List[PythonObject]()

    fn __init__(out self, name: String, var data: ColumnData, dtype: BisonDtype):
        self.name  = name
        self.dtype = dtype
        self._data = data^
        self._index = List[PythonObject]()

    fn __init__(out self, name: String, var data: ColumnData, dtype: BisonDtype, var index: List[PythonObject]):
        self.name  = name
        self.dtype = dtype
        self._data = data^
        self._index = index^

    # ------------------------------------------------------------------
    # Traits — Variant is Copyable so __copyinit__ is trivial
    # ------------------------------------------------------------------

    fn __copyinit__(out self, existing: Self):
        self.name  = existing.name
        self.dtype = existing.dtype
        self._data = existing._data
        self._index = existing._index.copy()

    fn __moveinit__(out self, deinit existing: Self):
        self.name  = existing.name^
        self.dtype = existing.dtype^
        self._data = existing._data^
        self._index = existing._index^

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
            return Column(self.name, ColumnData(d^), self.dtype, idx^)
        elif self._data.isa[List[Float64]]():
            var d = self._data[List[Float64]].copy()
            var idx = self._index.copy()
            return Column(self.name, ColumnData(d^), self.dtype, idx^)
        elif self._data.isa[List[Bool]]():
            var d = self._data[List[Bool]].copy()
            var idx = self._index.copy()
            return Column(self.name, ColumnData(d^), self.dtype, idx^)
        elif self._data.isa[List[String]]():
            var d = self._data[List[String]].copy()
            var idx = self._index.copy()
            return Column(self.name, ColumnData(d^), self.dtype, idx^)
        else:
            var d = self._data[List[PythonObject]].copy()
            var idx = self._index.copy()
            return Column(self.name, ColumnData(d^), self.dtype, idx^)

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
    # Aggregation
    # ------------------------------------------------------------------

    fn sum(self) raises -> Float64:
        """Return the sum of all values as Float64. Raises for non-numeric types."""
        if self._data.isa[List[Int64]]():
            var total = Float64(0)
            for i in range(len(self._data[List[Int64]])):
                total += Float64(self._data[List[Int64]][i])
            return total
        elif self._data.isa[List[Float64]]():
            var total = Float64(0)
            for i in range(len(self._data[List[Float64]])):
                total += self._data[List[Float64]][i]
            return total
        elif self._data.isa[List[Bool]]():
            var total = Float64(0)
            for i in range(len(self._data[List[Bool]])):
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
                data.append(Int64(Int(py=py_list[i])))
            return Column(name, ColumnData(data^), bison_dtype, idx_list^)
        elif bison_dtype == float64:
            var data = List[Float64]()
            for i in range(n):
                data.append(Float64(String(py_list[i])))
            return Column(name, ColumnData(data^), bison_dtype, idx_list^)
        elif bison_dtype == bool_:
            var data = List[Bool]()
            for i in range(n):
                data.append(Bool(py_list[i].__bool__()))
            return Column(name, ColumnData(data^), bison_dtype, idx_list^)
        elif dtype_str == "string":
            var data = List[String]()
            for i in range(n):
                data.append(String(py_list[i]))
            return Column(name, ColumnData(data^), object_, idx_list^)
        else:
            var data = List[PythonObject]()
            for i in range(n):
                data.append(py_list[i])
            return Column(name, ColumnData(data^), bison_dtype, idx_list^)

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
        if self._data.isa[List[Int64]]():
            for i in range(len(self._data[List[Int64]])):
                _ = py_list.append(self._data[List[Int64]][i])
        elif self._data.isa[List[Float64]]():
            for i in range(len(self._data[List[Float64]])):
                _ = py_list.append(self._data[List[Float64]][i])
        elif self._data.isa[List[Bool]]():
            for i in range(len(self._data[List[Bool]])):
                _ = py_list.append(self._data[List[Bool]][i])
        elif self._data.isa[List[String]]():
            for i in range(len(self._data[List[String]])):
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
