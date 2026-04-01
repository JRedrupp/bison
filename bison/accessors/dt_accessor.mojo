from std.python import Python, PythonObject
from std.collections import Optional
from ..column import Column, ColumnData
from ..dtypes import int64, object_, datetime64_ns


struct DatetimeMethods:
    """Accessor for datetime properties on a Series (.dt accessor)."""

    var _data: List[PythonObject]
    var _null_mask: List[Bool]
    var _name: Optional[String]

    def __init__(out self):
        self._data = List[PythonObject]()
        self._null_mask = List[Bool]()
        self._name = None

    def __init__(
        out self,
        var data: List[PythonObject],
        var null_mask: List[Bool],
        name: Optional[String],
    ):
        self._data = data^
        self._null_mask = null_mask^
        self._name = name

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_null(self, i: Int) -> Bool:
        return len(self._null_mask) > i and self._null_mask[i]

    # ------------------------------------------------------------------
    # Integer properties
    # ------------------------------------------------------------------

    def _int_prop[attr: StringLiteral](self) raises -> Column:
        """Extract an integer attribute from each Timestamp in `_data`.

        `attr` is a compile-time `StringLiteral` (e.g. `"year"`, `"month"`).
        `PythonObject.__getattr__` resolves the attribute name at the Python
        level without requiring a runtime call to Python's `getattr()`.
        """
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].__getattr__(attr))))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def year(self) raises -> Column:
        return self._int_prop["year"]()

    def month(self) raises -> Column:
        return self._int_prop["month"]()

    def day(self) raises -> Column:
        return self._int_prop["day"]()

    def hour(self) raises -> Column:
        return self._int_prop["hour"]()

    def minute(self) raises -> Column:
        return self._int_prop["minute"]()

    def second(self) raises -> Column:
        return self._int_prop["second"]()

    def dayofweek(self) raises -> Column:
        return self._int_prop["dayofweek"]()

    def dayofyear(self) raises -> Column:
        return self._int_prop["dayofyear"]()

    def quarter(self) raises -> Column:
        return self._int_prop["quarter"]()

    # ------------------------------------------------------------------
    # Object-returning properties
    # ------------------------------------------------------------------

    def date(self) raises -> Column:
        var result = List[PythonObject]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append(True)
            else:
                result.append(self._data[i].date())
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = new_mask^
        return col^

    def time(self) raises -> Column:
        var result = List[PythonObject]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append(True)
            else:
                result.append(self._data[i].time())
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = new_mask^
        return col^

    # ------------------------------------------------------------------
    # Timezone / rounding — return datetime columns
    # ------------------------------------------------------------------

    def tz_localize(self, tz: String) raises -> Column:
        var result = List[PythonObject]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append(True)
            else:
                result.append(self._data[i].tz_localize(tz))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), datetime64_ns)
        col._null_mask = new_mask^
        return col^

    def tz_convert(self, tz: String) raises -> Column:
        var result = List[PythonObject]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append(True)
            else:
                result.append(self._data[i].tz_convert(tz))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), datetime64_ns)
        col._null_mask = new_mask^
        return col^

    def floor(self, freq: String) raises -> Column:
        var result = List[PythonObject]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append(True)
            else:
                result.append(self._data[i].floor(freq))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), datetime64_ns)
        col._null_mask = new_mask^
        return col^

    def ceil(self, freq: String) raises -> Column:
        var result = List[PythonObject]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append(True)
            else:
                result.append(self._data[i].ceil(freq))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), datetime64_ns)
        col._null_mask = new_mask^
        return col^

    def round(self, freq: String) raises -> Column:
        var result = List[PythonObject]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append(True)
            else:
                result.append(self._data[i].round(freq))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), datetime64_ns)
        col._null_mask = new_mask^
        return col^
