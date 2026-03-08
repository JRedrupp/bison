from python import Python, PythonObject
from .dtypes import (
    BisonDtype,
    int8, int16, int32, int64,
    uint8, uint16, uint32, uint64,
    float32, float64,
    bool_, object_,
    datetime64_ns, timedelta64_ns,
)


struct Column(Copyable, Movable):
    """A single typed array representing one column of a DataFrame or a Series.

    Data is stored as a ``List[PythonObject]`` (one element per row).  The
    ``dtype`` field records the pandas-compatible dtype string so that
    round-trips through ``to_pandas`` preserve the original dtype.
    """

    var name: String
    var dtype: BisonDtype
    var _data: List[PythonObject]

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    fn __init__(out self):
        """Empty column with object dtype — used as stub placeholder."""
        self.name  = ""
        self.dtype = object_
        self._data = List[PythonObject]()

    fn __init__(out self, name: String, owned data: List[PythonObject], dtype: BisonDtype = object_):
        self.name  = name
        self.dtype = dtype
        self._data = data^

    # ------------------------------------------------------------------
    # Traits
    # ------------------------------------------------------------------

    fn __copyinit__(out self, existing: Self):
        self.name  = existing.name
        self.dtype = existing.dtype
        self._data = existing._data.copy()

    fn __moveinit__(out self, deinit existing: Self):
        self.name  = existing.name^
        self.dtype = existing.dtype^
        self._data = existing._data^

    # ------------------------------------------------------------------
    # Explicit copy helper (used by Series / DataFrame __copyinit__)
    # ------------------------------------------------------------------

    fn copy(self) -> Column:
        """Return an independent copy of this Column."""
        var new_data = self._data.copy()
        return Column(self.name, new_data^, self.dtype)

    # ------------------------------------------------------------------
    # Length
    # ------------------------------------------------------------------

    fn __len__(self) -> Int:
        return len(self._data)

    # ------------------------------------------------------------------
    # Pandas interop
    # ------------------------------------------------------------------

    @staticmethod
    fn from_pandas(pd_series: PythonObject, name: String) raises -> Column:
        """Build a Column by copying values from a pandas Series."""
        var dtype_str = String(pd_series.dtype)
        var n = Int(pd_series.__len__())
        var py_list = pd_series.tolist()

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

        var data = List[PythonObject]()
        for i in range(n):
            data.append(py_list[i])
        return Column(name, data^, bison_dtype)

    fn to_pandas(self) raises -> PythonObject:
        """Reconstruct a pandas Series from stored values."""
        var pd = Python.import_module("pandas")
        var py_list = Python.evaluate("[]")
        for i in range(len(self._data)):
            _ = py_list.append(self._data[i])
        return pd.Series(py_list, name=self.name, dtype=self.dtype.name)
