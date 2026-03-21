from .._errors import _not_implemented
from ..column import Column


struct DatetimeMethods:
    """Accessor for datetime properties on a Series (.dt accessor)."""

    def __init__(out self):
        pass

    def year(self) raises -> Column:
        _not_implemented("Series.dt.year")
        return Column()

    def month(self) raises -> Column:
        _not_implemented("Series.dt.month")
        return Column()

    def day(self) raises -> Column:
        _not_implemented("Series.dt.day")
        return Column()

    def hour(self) raises -> Column:
        _not_implemented("Series.dt.hour")
        return Column()

    def minute(self) raises -> Column:
        _not_implemented("Series.dt.minute")
        return Column()

    def second(self) raises -> Column:
        _not_implemented("Series.dt.second")
        return Column()

    def dayofweek(self) raises -> Column:
        _not_implemented("Series.dt.dayofweek")
        return Column()

    def dayofyear(self) raises -> Column:
        _not_implemented("Series.dt.dayofyear")
        return Column()

    def quarter(self) raises -> Column:
        _not_implemented("Series.dt.quarter")
        return Column()

    def date(self) raises -> Column:
        _not_implemented("Series.dt.date")
        return Column()

    def time(self) raises -> Column:
        _not_implemented("Series.dt.time")
        return Column()

    def tz_localize(self, tz: String) raises -> Column:
        _not_implemented("Series.dt.tz_localize")
        return Column()

    def tz_convert(self, tz: String) raises -> Column:
        _not_implemented("Series.dt.tz_convert")
        return Column()

    def floor(self, freq: String) raises -> Column:
        _not_implemented("Series.dt.floor")
        return Column()

    def ceil(self, freq: String) raises -> Column:
        _not_implemented("Series.dt.ceil")
        return Column()

    def round(self, freq: String) raises -> Column:
        _not_implemented("Series.dt.round")
        return Column()
