from std.python import Python, PythonObject
from .._errors import _not_implemented
from ..column import Column, ColumnData
from ..dtypes import BisonDtype, object_, bool_, int64


struct StringMethods:
    """Vectorized string operations on a Series (.str accessor)."""

    var _data: List[String]
    var _null_mask: List[Bool]
    var _name: String

    fn __init__(out self):
        self._data = List[String]()
        self._null_mask = List[Bool]()
        self._name = ""

    fn __init__(out self, var data: List[String], var null_mask: List[Bool], name: String):
        self._data = data^
        self._null_mask = null_mask^
        self._name = name

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    fn _is_null(self, i: Int) -> Bool:
        return len(self._null_mask) > i and self._null_mask[i]

    # ------------------------------------------------------------------
    # Case / transform
    # ------------------------------------------------------------------

    fn upper(self) raises -> Column:
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            else:
                result.append(self._data[i].upper())
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = self._null_mask.copy()
        return col^

    fn lower(self) raises -> Column:
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            else:
                result.append(self._data[i].lower())
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = self._null_mask.copy()
        return col^

    # ------------------------------------------------------------------
    # Stripping
    # ------------------------------------------------------------------

    fn strip(self, to_strip: String = "") raises -> Column:
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            elif to_strip == "":
                result.append(String(self._data[i].strip()))
            else:
                result.append(String(self._data[i].strip(to_strip)))
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = self._null_mask.copy()
        return col^

    fn lstrip(self, to_strip: String = "") raises -> Column:
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            elif to_strip == "":
                result.append(String(self._data[i].lstrip()))
            else:
                result.append(String(self._data[i].lstrip(to_strip)))
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = self._null_mask.copy()
        return col^

    fn rstrip(self, to_strip: String = "") raises -> Column:
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            elif to_strip == "":
                result.append(String(self._data[i].rstrip()))
            else:
                result.append(String(self._data[i].rstrip(to_strip)))
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = self._null_mask.copy()
        return col^

    # ------------------------------------------------------------------
    # Predicates
    # ------------------------------------------------------------------

    fn contains(self, pat: String, `case`: Bool = True, na: Bool = False) raises -> Column:
        var result = List[Bool]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(na)
                new_mask.append(False)
            else:
                if `case`:
                    result.append(self._data[i].find(pat) != -1)
                else:
                    result.append(self._data[i].lower().find(pat.lower()) != -1)
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), bool_)
        col._null_mask = new_mask^
        return col^

    fn startswith(self, pat: String) raises -> Column:
        var result = List[Bool]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(False)
                new_mask.append(True)
            else:
                result.append(self._data[i].startswith(pat))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), bool_)
        col._null_mask = new_mask^
        return col^

    fn endswith(self, pat: String) raises -> Column:
        var result = List[Bool]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(False)
                new_mask.append(True)
            else:
                result.append(self._data[i].endswith(pat))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), bool_)
        col._null_mask = new_mask^
        return col^

    # ------------------------------------------------------------------
    # Replace / split
    # ------------------------------------------------------------------

    fn replace(self, pat: String, repl: String, regex: Bool = True) raises -> Column:
        var result = List[String]()
        var re_mod = Python.import_module("re")
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            elif regex:
                var py_s = PythonObject(self._data[i])
                var sub_result = re_mod.sub(pat, repl, py_s)
                result.append(String(sub_result))
            else:
                result.append(self._data[i].replace(pat, repl))
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = self._null_mask.copy()
        return col^

    fn split(self, pat: String = " ", n: Int = -1, expand: Bool = False) raises -> List[List[String]]:
        """Split strings around given separator/delimiter.

        Returns a List[List[String]] where each inner list contains the split
        parts. Null entries produce an empty inner list; callers can use the
        Series null mask to distinguish empty-from-null vs. empty-from-split.
        """
        var result = List[List[String]]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(List[String]())
            else:
                var parts_raw = (
                    self._data[i].split(pat, n)
                    if n != -1
                    else self._data[i].split(pat)
                )
                var parts = List[String]()
                for j in range(len(parts_raw)):
                    parts.append(String(parts_raw[j]))
                result.append(parts^)
        return result^

    # ------------------------------------------------------------------
    # Numeric operations
    # ------------------------------------------------------------------

    fn len(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                result.append(Int64(len(self._data[i])))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    fn find(self, sub: String, start: Int = 0, end: Int = -1) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(-1))
                new_mask.append(True)
            else:
                var s = self._data[i]
                var pos = s.find(sub, start)
                if end != -1 and pos >= end:
                    pos = -1
                result.append(Int64(pos))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    fn count(self, pat: String) raises -> Column:
        var result = List[Int64]()
        var new_mask = List[Bool]()
        var re_mod = Python.import_module("re")
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append(True)
            else:
                var py_s = PythonObject(self._data[i])
                var matches = re_mod.findall(pat, py_s)
                result.append(Int64(Int(py=matches.__len__())))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), int64)
        col._null_mask = new_mask^
        return col^

    # ------------------------------------------------------------------
    # Indexing / slicing
    # ------------------------------------------------------------------

    fn get(self, i: Int) raises -> Column:
        var result = List[String]()
        var new_mask = List[Bool]()
        for idx in range(len(self._data)):
            if self._is_null(idx):
                result.append(String(""))
                new_mask.append(True)
            else:
                var s = self._data[idx]
                if i < 0 or i >= len(s):
                    result.append(String(""))
                    new_mask.append(True)
                else:
                    var j = 0
                    var char_val = String("")
                    for ch in s.codepoint_slices():
                        if j == i:
                            char_val = String(ch)
                            break
                        j += 1
                    result.append(char_val)
                    new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = new_mask^
        return col^

    fn slice(self, start: Int = 0, stop: Int = -1, step: Int = 1) raises -> Column:
        var result = List[String]()
        var new_mask = List[Bool]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
                new_mask.append(True)
            else:
                var s = self._data[i]
                var slen = len(s)
                var actual_stop = slen if stop == -1 or stop > slen else stop
                var sub = String("")
                var j = 0
                for ch in s.codepoint_slices():
                    if j >= actual_stop:
                        break
                    if j >= start and (j - start) % step == 0:
                        sub += String(ch)
                    j += 1
                result.append(sub)
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), object_)
        col._null_mask = new_mask^
        return col^

    # ------------------------------------------------------------------
    # Concatenation
    # ------------------------------------------------------------------

    fn cat(self, sep: String = "") raises -> String:
        var result = String("")
        var first = True
        for i in range(len(self._data)):
            if self._is_null(i):
                continue
            if not first:
                result += sep
            result += self._data[i]
            first = False
        return result

    # ------------------------------------------------------------------
    # Regex operations
    # ------------------------------------------------------------------

    fn match(self, pat: String) raises -> Column:
        var result = List[Bool]()
        var new_mask = List[Bool]()
        var re_mod = Python.import_module("re")
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(False)
                new_mask.append(True)
            else:
                var py_s = PythonObject(self._data[i])
                var m = re_mod.match(pat, py_s)
                result.append(Bool(m.__bool__()))
                new_mask.append(False)
        var col = Column(self._name, ColumnData(result^), bool_)
        col._null_mask = new_mask^
        return col^
