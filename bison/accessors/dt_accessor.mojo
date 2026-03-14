from std.python import PythonObject
from .._errors import _not_implemented


struct DatetimeMethods:
    """Accessor for datetime properties on a Series (.dt accessor)."""

    fn __init__(out self):
        pass

    fn year(self) raises -> PythonObject:
        _not_implemented("Series.dt.year")
        return PythonObject(None)

    fn month(self) raises -> PythonObject:
        _not_implemented("Series.dt.month")
        return PythonObject(None)

    fn day(self) raises -> PythonObject:
        _not_implemented("Series.dt.day")
        return PythonObject(None)

    fn hour(self) raises -> PythonObject:
        _not_implemented("Series.dt.hour")
        return PythonObject(None)

    fn minute(self) raises -> PythonObject:
        _not_implemented("Series.dt.minute")
        return PythonObject(None)

    fn second(self) raises -> PythonObject:
        _not_implemented("Series.dt.second")
        return PythonObject(None)

    fn dayofweek(self) raises -> PythonObject:
        _not_implemented("Series.dt.dayofweek")
        return PythonObject(None)

    fn dayofyear(self) raises -> PythonObject:
        _not_implemented("Series.dt.dayofyear")
        return PythonObject(None)

    fn quarter(self) raises -> PythonObject:
        _not_implemented("Series.dt.quarter")
        return PythonObject(None)

    fn date(self) raises -> PythonObject:
        _not_implemented("Series.dt.date")
        return PythonObject(None)

    fn time(self) raises -> PythonObject:
        _not_implemented("Series.dt.time")
        return PythonObject(None)

    fn tz_localize(self, tz: String) raises -> PythonObject:
        _not_implemented("Series.dt.tz_localize")
        return PythonObject(None)

    fn tz_convert(self, tz: String) raises -> PythonObject:
        _not_implemented("Series.dt.tz_convert")
        return PythonObject(None)

    fn floor(self, freq: String) raises -> PythonObject:
        _not_implemented("Series.dt.floor")
        return PythonObject(None)

    fn ceil(self, freq: String) raises -> PythonObject:
        _not_implemented("Series.dt.ceil")
        return PythonObject(None)

    fn round(self, freq: String) raises -> PythonObject:
        _not_implemented("Series.dt.round")
        return PythonObject(None)
