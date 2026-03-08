from .._errors import _not_implemented


struct StringMethods:
    """Vectorized string operations on a Series (.str accessor)."""

    fn contains(self, pat: String, case: Bool = True, na: Bool = False) raises -> PythonObject:
        _not_implemented("Series.str.contains")
        return PythonObject(None)

    fn startswith(self, pat: String) raises -> PythonObject:
        _not_implemented("Series.str.startswith")
        return PythonObject(None)

    fn endswith(self, pat: String) raises -> PythonObject:
        _not_implemented("Series.str.endswith")
        return PythonObject(None)

    fn replace(self, pat: String, repl: String, regex: Bool = True) raises -> PythonObject:
        _not_implemented("Series.str.replace")
        return PythonObject(None)

    fn split(self, pat: String = " ", n: Int = -1, expand: Bool = False) raises -> PythonObject:
        _not_implemented("Series.str.split")
        return PythonObject(None)

    fn strip(self, to_strip: String = "") raises -> PythonObject:
        _not_implemented("Series.str.strip")
        return PythonObject(None)

    fn lstrip(self, to_strip: String = "") raises -> PythonObject:
        _not_implemented("Series.str.lstrip")
        return PythonObject(None)

    fn rstrip(self, to_strip: String = "") raises -> PythonObject:
        _not_implemented("Series.str.rstrip")
        return PythonObject(None)

    fn upper(self) raises -> PythonObject:
        _not_implemented("Series.str.upper")
        return PythonObject(None)

    fn lower(self) raises -> PythonObject:
        _not_implemented("Series.str.lower")
        return PythonObject(None)

    fn len(self) raises -> PythonObject:
        _not_implemented("Series.str.len")
        return PythonObject(None)

    fn get(self, i: Int) raises -> PythonObject:
        _not_implemented("Series.str.get")
        return PythonObject(None)

    fn slice(self, start: Int = 0, stop: Int = -1, step: Int = 1) raises -> PythonObject:
        _not_implemented("Series.str.slice")
        return PythonObject(None)

    fn cat(self, sep: String = "") raises -> String:
        _not_implemented("Series.str.cat")
        return ""

    fn find(self, sub: String, start: Int = 0, end: Int = -1) raises -> PythonObject:
        _not_implemented("Series.str.find")
        return PythonObject(None)

    fn count(self, pat: String) raises -> PythonObject:
        _not_implemented("Series.str.count")
        return PythonObject(None)

    fn match(self, pat: String) raises -> PythonObject:
        _not_implemented("Series.str.match")
        return PythonObject(None)
