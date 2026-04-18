from std.python import Python, PythonObject
from std.collections import Optional
from .._errors import _not_implemented
from ..column import Column, NullMask
from ..dtypes import BisonDtype, object_, bool_, int64, string_


# Compile-time function type for element-wise String→String transforms
# used with _apply_str_transform[F] below. F must be a module-level
# (non-capturing) def matching this signature.
comptime StrTransformFn = def(String) thin -> String


def _upper_fn(s: String) -> String:
    """Return *s* upper-cased; used with ``_apply_str_transform[_upper_fn]``."""
    return s.upper()


def _lower_fn(s: String) -> String:
    """Return *s* lower-cased; used with ``_apply_str_transform[_lower_fn]``."""
    return s.lower()


struct StringMethods:
    """Vectorized string operations on a Series (.str accessor)."""

    var _data: List[String]
    var _null_mask: NullMask
    var _name: Optional[String]

    def __init__(out self):
        self._data = List[String]()
        self._null_mask = NullMask()
        self._name = None

    def __init__(
        out self,
        var data: List[String],
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

    def _make_str_col(self, var result: List[String]) raises -> Column:
        """Build a string Column copying this accessor's null mask."""
        var col = Column(self._name, result^, string_)
        col.set_null_mask(self._null_mask.copy())
        return col^

    def _make_str_col_masked(
        self, var result: List[String], var mask: NullMask
    ) raises -> Column:
        """Build a string Column using an explicit null mask."""
        var col = Column(self._name, result^, string_)
        col.set_null_mask(mask^)
        return col^

    def _make_bool_col(
        self, var result: List[Bool], var mask: NullMask
    ) raises -> Column:
        """Build a bool Column using an explicit null mask."""
        var col = Column(self._name, result^, bool_)
        col.set_null_mask(mask^)
        return col^

    def _make_int64_col(
        self, var result: List[Int64], var mask: NullMask
    ) raises -> Column:
        """Build an int64 Column using an explicit null mask."""
        var col = Column(self._name, result^, int64)
        col.set_null_mask(mask^)
        return col^

    def _apply_str_transform[F: StrTransformFn](self) raises -> Column:
        """Apply a compile-time string transform element-wise, propagating nulls.

        *F* must be a module-level (non-capturing) function matching
        ``StrTransformFn``.  Null positions receive an empty string in the
        output; the original null mask is copied onto the result column.

        Call as ``self._apply_str_transform[_upper_fn]()``.
        """
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            else:
                result.append(F(self._data[i]))
        return self._make_str_col(result^)

    # ------------------------------------------------------------------
    # Case / transform
    # ------------------------------------------------------------------

    def upper(self) raises -> Column:
        return self._apply_str_transform[_upper_fn]()

    def lower(self) raises -> Column:
        return self._apply_str_transform[_lower_fn]()

    # ------------------------------------------------------------------
    # Stripping
    # ------------------------------------------------------------------

    def strip(self, to_strip: String = "") raises -> Column:
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            elif to_strip == "":
                result.append(String(self._data[i].strip()))
            else:
                result.append(String(self._data[i].strip(to_strip)))
        return self._make_str_col(result^)

    def lstrip(self, to_strip: String = "") raises -> Column:
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            elif to_strip == "":
                result.append(String(self._data[i].lstrip()))
            else:
                result.append(String(self._data[i].lstrip(to_strip)))
        return self._make_str_col(result^)

    def rstrip(self, to_strip: String = "") raises -> Column:
        var result = List[String]()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
            elif to_strip == "":
                result.append(String(self._data[i].rstrip()))
            else:
                result.append(String(self._data[i].rstrip(to_strip)))
        return self._make_str_col(result^)

    # ------------------------------------------------------------------
    # Predicates
    # ------------------------------------------------------------------

    def contains(
        self, pat: String, `case`: Bool = True, na: Bool = False
    ) raises -> Column:
        var result = List[Bool]()
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(na)
                new_mask.append_valid()
            else:
                if `case`:
                    result.append(self._data[i].find(pat) != -1)
                else:
                    result.append(self._data[i].lower().find(pat.lower()) != -1)
                new_mask.append_valid()
        return self._make_bool_col(result^, new_mask^)

    def startswith(self, pat: String) raises -> Column:
        var result = List[Bool]()
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(False)
                new_mask.append_null()
            else:
                result.append(self._data[i].startswith(pat))
                new_mask.append_valid()
        return self._make_bool_col(result^, new_mask^)

    def endswith(self, pat: String) raises -> Column:
        var result = List[Bool]()
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(False)
                new_mask.append_null()
            else:
                result.append(self._data[i].endswith(pat))
                new_mask.append_valid()
        return self._make_bool_col(result^, new_mask^)

    # ------------------------------------------------------------------
    # Replace / split
    # ------------------------------------------------------------------

    def replace(
        self, pat: String, repl: String, regex: Bool = True
    ) raises -> Column:
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
        return self._make_str_col(result^)

    def split(
        self, pat: String = " ", n: Int = -1, expand: Bool = False
    ) raises -> List[List[String]]:
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
                var parts_raw = self._data[i].split(
                    pat, n
                ) if n != -1 else self._data[i].split(pat)
                var parts = List[String]()
                for j in range(len(parts_raw)):
                    parts.append(String(parts_raw[j]))
                result.append(parts^)
        return result^

    # ------------------------------------------------------------------
    # Numeric operations
    # ------------------------------------------------------------------

    def len(self) raises -> Column:
        var result = List[Int64]()
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append_null()
            else:
                result.append(Int64(self._data[i].byte_length()))
                new_mask.append_valid()
        return self._make_int64_col(result^, new_mask^)

    def find(self, sub: String, start: Int = 0, end: Int = -1) raises -> Column:
        var result = List[Int64]()
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(-1))
                new_mask.append_null()
            else:
                var s = self._data[i]
                var pos = s.find(sub, start)
                if end != -1 and pos >= end:
                    pos = -1
                result.append(Int64(pos))
                new_mask.append_valid()
        return self._make_int64_col(result^, new_mask^)

    def count(self, pat: String) raises -> Column:
        var result = List[Int64]()
        var new_mask = NullMask()
        var re_mod = Python.import_module("re")
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(Int64(0))
                new_mask.append_null()
            else:
                var py_s = PythonObject(self._data[i])
                var matches = re_mod.findall(pat, py_s)
                result.append(Int64(Int(py=matches.__len__())))
                new_mask.append_valid()
        return self._make_int64_col(result^, new_mask^)

    # ------------------------------------------------------------------
    # Indexing / slicing
    # ------------------------------------------------------------------

    def get(self, i: Int) raises -> Column:
        var result = List[String]()
        var new_mask = NullMask()
        for idx in range(len(self._data)):
            if self._is_null(idx):
                result.append(String(""))
                new_mask.append_null()
            else:
                var s = self._data[idx]
                if i < 0 or i >= s.byte_length():
                    result.append(String(""))
                    new_mask.append_null()
                else:
                    var j = 0
                    var char_val = String("")
                    for ch in s.codepoint_slices():
                        if j == i:
                            char_val = String(ch)
                            break
                        j += 1
                    result.append(char_val)
                    new_mask.append_valid()
        return self._make_str_col_masked(result^, new_mask^)

    def slice(
        self, start: Int = 0, stop: Int = -1, step: Int = 1
    ) raises -> Column:
        var result = List[String]()
        var new_mask = NullMask()
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(String(""))
                new_mask.append_null()
            else:
                var s = self._data[i]
                var slen = s.byte_length()
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
                new_mask.append_valid()
        return self._make_str_col_masked(result^, new_mask^)

    # ------------------------------------------------------------------
    # Concatenation
    # ------------------------------------------------------------------

    def cat(self, sep: String = "") raises -> String:
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

    def match(self, pat: String) raises -> Column:
        var result = List[Bool]()
        var new_mask = NullMask()
        var re_mod = Python.import_module("re")
        for i in range(len(self._data)):
            if self._is_null(i):
                result.append(False)
                new_mask.append_null()
            else:
                var py_s = PythonObject(self._data[i])
                var m = re_mod.match(pat, py_s)
                result.append(Bool(m.__bool__()))
                new_mask.append_valid()
        return self._make_bool_col(result^, new_mask^)
