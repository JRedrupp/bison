from std.python import Python, PythonObject
from std.collections import Optional
from ..column import Column, NullMask
from ..dtypes import int64, object_, datetime64_ns


struct DatetimeMethods:
    """Accessor for datetime properties on a Series (.dt accessor)."""

    var _data: List[PythonObject]
    var _null_mask: NullMask
    var _name: Optional[String]

    def __init__(out self):
        self._data = List[PythonObject]()
        self._null_mask = NullMask()
        self._name = None

    def __init__(
        out self,
        var data: List[PythonObject],
        var null_mask: NullMask,
        name: Optional[String],
    ):
        self._data = data^
        self._null_mask = null_mask^
        self._name = name

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_null(self, i: Int) -> Bool:
        return self._null_mask.is_null(i)

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
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append_null()
            else:
                result.append(Int64(Int(py=self._data[i].__getattr__(attr))))
                new_mask.append_valid()
        var col = Column(self._name, result^, int64)
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
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append_null()
            else:
                result.append(self._data[i].date())
                new_mask.append_valid()
        var col = Column(self._name, result^, object_)
        col._null_mask = new_mask^
        return col^

    def time(self) raises -> Column:
        var result = List[PythonObject]()
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append_null()
            else:
                result.append(self._data[i].time())
                new_mask.append_valid()
        var col = Column(self._name, result^, object_)
        col._null_mask = new_mask^
        return col^

    # ------------------------------------------------------------------
    # Timezone / rounding — return datetime columns
    # ------------------------------------------------------------------

    def _apply_ts_method[
        method: StringLiteral
    ](self, arg: String) raises -> Column:
        """Apply a single-arg Timestamp method to each element, returning a datetime64_ns Column.

        `method` is a compile-time ``StringLiteral`` (e.g. ``"floor"``,
        ``"tz_localize"``).  ``PythonObject.__getattr__`` resolves the method
        name at the Python level without a runtime ``getattr()`` call.
        """
        var result = List[PythonObject]()
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(PythonObject(None))
                new_mask.append_null()
            else:
                result.append(self._data[i].__getattr__(method)(arg))
                new_mask.append_valid()
        var col = Column(self._name, result^, datetime64_ns)
        col._null_mask = new_mask^
        return col^

    def tz_localize(self, tz: String) raises -> Column:
        return self._apply_ts_method["tz_localize"](tz)

    def tz_convert(self, tz: String) raises -> Column:
        return self._apply_ts_method["tz_convert"](tz)

    def floor(self, freq: String) raises -> Column:
        return self._apply_ts_method["floor"](freq)

    def ceil(self, freq: String) raises -> Column:
        return self._apply_ts_method["ceil"](freq)

    def round(self, freq: String) raises -> Column:
        return self._apply_ts_method["round"](freq)
