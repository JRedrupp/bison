from .._errors import _not_implemented
from ..column import Column


struct DatetimeMethods:
    """Accessor for datetime properties on a Series (.dt accessor)."""

    fn __init__(out self):
        pass

    fn year(self) raises -> Column:
        _not_implemented("Series.dt.year")
        return Column()

    fn month(self) raises -> Column:
        _not_implemented("Series.dt.month")
        return Column()

    fn day(self) raises -> Column:
        _not_implemented("Series.dt.day")
        return Column()

    fn hour(self) raises -> Column:
        _not_implemented("Series.dt.hour")
        return Column()

    fn minute(self) raises -> Column:
        _not_implemented("Series.dt.minute")
        return Column()

    fn second(self) raises -> Column:
        _not_implemented("Series.dt.second")
        return Column()

    fn dayofweek(self) raises -> Column:
        _not_implemented("Series.dt.dayofweek")
        return Column()

    fn dayofyear(self) raises -> Column:
        _not_implemented("Series.dt.dayofyear")
        return Column()

    fn quarter(self) raises -> Column:
        _not_implemented("Series.dt.quarter")
        return Column()

    fn date(self) raises -> Column:
        _not_implemented("Series.dt.date")
        return Column()

    fn time(self) raises -> Column:
        _not_implemented("Series.dt.time")
        return Column()

    fn tz_localize(self, tz: String) raises -> Column:
        _not_implemented("Series.dt.tz_localize")
        return Column()

    fn tz_convert(self, tz: String) raises -> Column:
        _not_implemented("Series.dt.tz_convert")
        return Column()

    fn floor(self, freq: String) raises -> Column:
        _not_implemented("Series.dt.floor")
        return Column()

    fn ceil(self, freq: String) raises -> Column:
        _not_implemented("Series.dt.ceil")
        return Column()

    fn round(self, freq: String) raises -> Column:
        _not_implemented("Series.dt.round")
        return Column()
