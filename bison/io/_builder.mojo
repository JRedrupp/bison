"""Shared null-aware column-building helpers for csv.mojo and json.mojo.

Both IO modules follow the same pattern when constructing a typed Column from
raw values:

1. Allocate a ``List[T]`` and a ``NullMask``.
2. Iterate over *n* values, appending a typed element or a null sentinel
   depending on whether the current cell is null.
3. Wrap the list + optional mask into a ``Column``.

This module centralizes that repeated structure so that ``csv.mojo`` and
``json.mojo`` each replace their 4 inline dtype-dispatch blocks with 4
one-liner calls.
"""

from std.python import Python, PythonObject
from ..column import Column, NullMask
from ..dtypes import int64, float64, bool_, string_


# ------------------------------------------------------------------
# Shared string / NA helpers (used by CSV; re-exported for callers)
# ------------------------------------------------------------------


def _in_na_set(s: String, na_set: List[String]) -> Bool:
    """Return True if *s* matches any string in *na_set*."""
    for i in range(len(na_set)):
        if s == na_set[i]:
            return True
    return False


def _is_bool_value(s: String) -> Bool:
    """Return True if *s* is a case-insensitive match for 'true' or 'false'."""
    var lower = s.lower()
    return lower == "true" or lower == "false"


def _parse_bool_value(s: String) -> Bool:
    """Parse a bool CSV token — 'true' (case-insensitive) returns True."""
    return s.lower() == "true"


# ------------------------------------------------------------------
# JSON Python-type helper
# ------------------------------------------------------------------


def _json_py_type(val: PythonObject) raises -> String:
    """Return the Python class name of a JSON value."""
    return String(val.__class__.__name__)


# ------------------------------------------------------------------
# CSV typed column builders
# Input: List[String] raw values + List[String] NA set.
# ------------------------------------------------------------------


def _csv_col_bool(
    name: String, raw: List[String], na_set: List[String]
) raises -> Column:
    """Build a bool Column from raw CSV strings, tracking nulls via *na_set*."""
    var n = len(raw)
    var data = List[Bool]()
    var null_mask = NullMask()
    for i in range(n):
        if _in_na_set(raw[i], na_set):
            data.append(False)
            null_mask.append_null()
        else:
            data.append(_parse_bool_value(raw[i]))
            null_mask.append_valid()
    var col = Column(name, data^, bool_)
    if null_mask.has_nulls():
        col.set_null_mask(null_mask^)
    return col^


def _csv_col_int64(
    name: String, raw: List[String], na_set: List[String]
) raises -> Column:
    """Build an int64 Column from raw CSV strings, tracking nulls via *na_set*.
    """
    var n = len(raw)
    var data = List[Int64]()
    var null_mask = NullMask()
    for i in range(n):
        if _in_na_set(raw[i], na_set):
            data.append(Int64(0))
            null_mask.append_null()
        else:
            data.append(Int64(atol(raw[i])))
            null_mask.append_valid()
    var col = Column(name, data^, int64)
    if null_mask.has_nulls():
        col.set_null_mask(null_mask^)
    return col^


def _csv_col_float64(
    name: String, raw: List[String], na_set: List[String]
) raises -> Column:
    """Build a float64 Column from raw CSV strings, tracking nulls via *na_set*.
    """
    var n = len(raw)
    var data = List[Float64]()
    var null_mask = NullMask()
    for i in range(n):
        if _in_na_set(raw[i], na_set):
            data.append(Float64(0))
            null_mask.append_null()
        else:
            data.append(atof(raw[i]))
            null_mask.append_valid()
    var col = Column(name, data^, float64)
    if null_mask.has_nulls():
        col.set_null_mask(null_mask^)
    return col^


def _csv_col_string(
    name: String, raw: List[String], na_set: List[String]
) raises -> Column:
    """Build a string Column from raw CSV strings, tracking nulls via *na_set*.
    """
    var n = len(raw)
    var data = List[String]()
    var null_mask = NullMask()
    for i in range(n):
        if _in_na_set(raw[i], na_set):
            data.append(String(""))
            null_mask.append_null()
        else:
            data.append(raw[i])
            null_mask.append_valid()
    var col = Column(name, data^, string_)
    if null_mask.has_nulls():
        col.set_null_mask(null_mask^)
    return col^


# ------------------------------------------------------------------
# JSON typed column builders
# Input: PythonObject list of JSON values + length n.
# Null detection: Python NoneType.
# ------------------------------------------------------------------


def _json_col_bool(
    name: String, py_values: PythonObject, n: Int
) raises -> Column:
    """Build a bool Column from a Python list of JSON values."""
    var data = List[Bool]()
    var null_mask = NullMask()
    for ri in range(n):
        var val = py_values[ri]
        if _json_py_type(val) == "NoneType":
            data.append(False)
            null_mask.append_null()
        else:
            data.append(Bool(val.__bool__()))
            null_mask.append_valid()
    var col = Column(name, data^, bool_)
    if null_mask.has_nulls():
        col.set_null_mask(null_mask^)
    return col^


def _json_col_int64(
    name: String, py_values: PythonObject, n: Int
) raises -> Column:
    """Build an int64 Column from a Python list of JSON values."""
    var data = List[Int64]()
    var null_mask = NullMask()
    for ri in range(n):
        var val = py_values[ri]
        if _json_py_type(val) == "NoneType":
            data.append(Int64(0))
            null_mask.append_null()
        else:
            data.append(Int64(Int(py=val)))
            null_mask.append_valid()
    var col = Column(name, data^, int64)
    if null_mask.has_nulls():
        col.set_null_mask(null_mask^)
    return col^


def _json_col_float64(
    name: String, py_values: PythonObject, n: Int
) raises -> Column:
    """Build a float64 Column from a Python list of JSON values."""
    var data = List[Float64]()
    var null_mask = NullMask()
    for ri in range(n):
        var val = py_values[ri]
        if _json_py_type(val) == "NoneType":
            data.append(Float64(0))
            null_mask.append_null()
        else:
            data.append(atof(String(val)))
            null_mask.append_valid()
    var col = Column(name, data^, float64)
    if null_mask.has_nulls():
        col.set_null_mask(null_mask^)
    return col^


def _json_col_string(
    name: String, py_values: PythonObject, n: Int
) raises -> Column:
    """Build a string Column from a Python list of JSON values."""
    var data = List[String]()
    var null_mask = NullMask()
    for ri in range(n):
        var val = py_values[ri]
        if _json_py_type(val) == "NoneType":
            data.append(String(""))
            null_mask.append_null()
        else:
            data.append(String(val))
            null_mask.append_valid()
    var col = Column(name, data^, string_)
    if null_mask.has_nulls():
        col.set_null_mask(null_mask^)
    return col^
