from std.python import Python, PythonObject
from ..column import Column, ColumnData
from ..dtypes import int64, object_, datetime64_ns


struct DatetimeMethods:
    """Accessor for datetime properties on a Series (.dt accessor)."""

    var _data: List[PythonObject]
    var _null_mask: List[Bool]
    var _name: String

    def __init__(out self):
        self._data = List[PythonObject]()
        self._null_mask = List[Bool]()
        self._name = ""

    def __init__(
        out self,
        var data: List[PythonObject],
        var null_mask: List[Bool],
        name: String,
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

    def year(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].year)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def month(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].month)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def day(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].day)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def hour(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].hour)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def minute(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].minute)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def second(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].second)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def dayofweek(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].dayofweek)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def dayofyear(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].dayofyear)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    def quarter(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(Int(py=self._data[i].quarter)))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

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
