from std.python import Python, PythonObject
from std.collections import Optional
from ..dataframe import DataFrame, _sort_col_names
from ..column import Column, NullMask
from ..dtypes import BisonDtype, int64, float64, bool_, object_, string_
from .._errors import _not_implemented


# ------------------------------------------------------------------
# Promotion helpers — used by _vstack to upcast heterogeneous pieces
# into a unified output list.  Dispatch uses ``Column`` predicate
# methods (``is_int`` / ``is_float`` / ``is_bool`` / ``is_string``) so
# the underlying ``ColumnData`` discriminant is touched in exactly one
# place per type — ``column.mojo`` — and never here.  Arms that cannot
# occur for a given target dtype raise — ``_promote_dtype`` guarantees
# they never reach here.
# ------------------------------------------------------------------


def _promote_piece_to_float64(piece: Column, mut out: List[Float64]) raises:
    if piece.is_float():
        ref src = piece._float64_data()
        for k in range(len(src)):
            out.append(src[k])
    elif piece.is_int():
        ref src = piece._int64_data()
        for k in range(len(src)):
            out.append(Float64(Int(src[k])))
    elif piece.is_bool():
        ref src = piece._bool_data()
        for k in range(len(src)):
            out.append(Float64(1) if src[k] else Float64(0))
    else:
        raise Error("concat: cannot promote non-numeric column to float64")


def _promote_piece_to_int64(piece: Column, mut out: List[Int64]) raises:
    if piece.is_int():
        ref src = piece._int64_data()
        for k in range(len(src)):
            out.append(src[k])
    elif piece.is_bool():
        ref src = piece._bool_data()
        for k in range(len(src)):
            out.append(Int64(1) if src[k] else Int64(0))
    else:
        raise Error("concat: cannot promote non-int/bool column to int64")


def _promote_piece_to_object(
    piece: Column, mut out: List[PythonObject], py_str: PythonObject
) raises:
    """Append a piece into ``out`` as ``PythonObject``.

    Used for non-numeric same-dtype concatenation (e.g. ``datetime64_ns``)
    where pieces may be backed by either ``List[String]`` or
    ``List[PythonObject]``.  String elements are wrapped via Python's
    ``str()`` to materialise a ``PythonObject``.
    """
    if piece.is_string():
        ref src = piece._str_data()
        for k in range(len(src)):
            out.append(py_str(src[k]))
    elif piece.is_object():
        ref src = piece._storage_legacy().data
        for k in range(len(src)):
            out.append(src[k])
    else:
        raise Error("concat: cannot promote non-string/object column to object")


# ------------------------------------------------------------------
# Mask helper — appends one piece's null-mask contribution into the
# rolling output mask.  Centralises the per-piece mask logic shared by
# every concat path.
# ------------------------------------------------------------------


def _append_piece_mask(
    mut mask: NullMask, piece: Column, need_mask: Bool
) raises:
    if not need_mask:
        return
    var has_m = piece.has_nulls()
    var n = len(piece)
    for k in range(n):
        if has_m:
            mask.append(piece.is_null(k))
        else:
            mask.append_valid()


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------


def _null_col(
    name: Optional[String], n: Int, dtype: BisonDtype
) raises -> Column:
    """Return a Column of *n* null rows using *dtype* as the stored arm."""
    if dtype == int64:
        var data = List[Int64]()
        for _ in range(n):
            data.append(Int64(0))
        var col = Column(name, data^, dtype)
        col.set_null_mask(NullMask.all_null(n))
        return col^
    elif dtype == float64:
        var data = List[Float64]()
        for _ in range(n):
            data.append(Float64(0))
        var col = Column(name, data^, dtype)
        col.set_null_mask(NullMask.all_null(n))
        return col^
    elif dtype == bool_:
        var data = List[Bool]()
        for _ in range(n):
            data.append(False)
        var col = Column(name, data^, dtype)
        col.set_null_mask(NullMask.all_null(n))
        return col^
    elif dtype == string_:
        # #644: preserve the string_ ⟺ List[String] invariant for all-null
        # string columns inserted by outer joins / aligns.
        var data = List[String]()
        for _ in range(n):
            data.append(String(""))
        var col = Column(name, data^, string_)
        col.set_null_mask(NullMask.all_null(n))
        return col^
    else:
        var data = List[PythonObject]()
        for _ in range(n):
            data.append(Python.evaluate("None"))
        var col = Column(name, data^, object_)
        col.set_null_mask(NullMask.all_null(n))
        return col^


def _build_key_col(keys: List[String], counts: List[Int]) raises -> Column:
    """Build a string Column named '__key__' repeating keys[i] counts[i] times.
    """
    var data = List[String]()
    for i in range(len(keys)):
        for _ in range(counts[i]):
            data.append(keys[i])
    var col = Column(Optional[String]("__key__"), data^, string_)
    col._try_activate_storage()
    return col^


def _promote_dtype(a: BisonDtype, b: BisonDtype) raises -> BisonDtype:
    """Return the common promoted dtype for *a* and *b*.

    Promotion rules (matches pandas scalar casting):

    * Same dtype → same dtype
    * int64 + float64 → float64
    * bool_ + int64  → int64
    * bool_ + float64 → float64

    Raises ``Error`` for all other combinations (e.g. int64 vs string).
    """
    if a == b:
        return a
    if (a == int64 and b == float64) or (a == float64 and b == int64):
        return float64
    if (a == bool_ and b == int64) or (a == int64 and b == bool_):
        return int64
    if (a == bool_ and b == float64) or (a == float64 and b == bool_):
        return float64
    raise Error(
        "concat: dtype mismatch for column: cannot combine '"
        + a.name
        + "' and '"
        + b.name
        + "'"
    )


def _dtype_for(dfs: List[DataFrame], col_name: String) raises -> BisonDtype:
    """Return the promoted dtype for *col_name* across all DataFrames.

    Scans every frame that contains *col_name* and promotes across all of them
    using :func:`_promote_dtype`.  Raises if two frames carry the column with
    incompatible dtypes (e.g. ``int64`` vs ``object_``).  Returns ``object_``
    if no frame has the column.
    """
    var result: Optional[BisonDtype] = None
    for i in range(len(dfs)):
        for j in range(len(dfs[i]._cols)):
            if dfs[i]._cols[j].name == col_name:
                var dt = dfs[i]._cols[j].dtype
                if result:
                    result = _promote_dtype(result.value(), dt)
                else:
                    result = dt
    if result:
        return result.value()
    return object_


def _common_dtype(pieces: List[Column]) -> Optional[BisonDtype]:
    """Return the shared dtype if all pieces use the same typed data arm.

    Returns ``None`` when pieces are heterogeneous so callers know to fall
    back to ``List[PythonObject]`` (object dtype) storage.  Returns the
    common :class:`BisonDtype` otherwise:

    * ``int64``   — every piece holds ``List[Int64]``
    * ``float64`` — every piece holds ``List[Float64]``
    * ``bool_``   — every piece holds ``List[Bool]``
    * ``string_`` — every piece holds ``List[String]``
    """
    if len(pieces) == 0:
        return None
    var is_int = pieces[0].is_int()
    var is_float = pieces[0].is_float()
    var is_bool = pieces[0].is_bool()
    var is_str = pieces[0].is_string()
    for i in range(1, len(pieces)):
        if is_int and not pieces[i].is_int():
            is_int = False
        if is_float and not pieces[i].is_float():
            is_float = False
        if is_bool and not pieces[i].is_bool():
            is_bool = False
        if is_str and not pieces[i].is_string():
            is_str = False
    if is_int:
        return int64
    if is_float:
        return float64
    if is_bool:
        return bool_
    if is_str:
        return string_
    return None


def _vstack(pieces: List[Column]) raises -> Column:
    """Vertically stack a list of same-named Columns row-wise.

    Uses the same dtype-reconciliation policy as ``_promote_dtype``:

    * Same dtype → concatenate using that typed arm.
    * Numeric promotion (int64+float64→float64, bool_+int64→int64,
      bool_+float64→float64) → upcast and concatenate.
    * Non-numeric same dtype (e.g. datetime64_ns) → concatenate as
      ``List[PythonObject]``.
    * Incompatible dtypes (e.g. int64 vs string) → raise, consistent with
      ``Column.concat`` used by ``DataFrame.append``.
    """
    if len(pieces) == 0:
        return Column()
    var col_name = pieces[0].name

    # Validate dtype compatibility and determine the promoted target dtype.
    # Raises for incompatible combinations (e.g. int64 vs string/object_).
    var target_dtype = pieces[0].dtype
    for i in range(1, len(pieces)):
        target_dtype = _promote_dtype(target_dtype, pieces[i].dtype)

    # Detect whether any piece has a null mask.
    var need_mask = False
    for i in range(len(pieces)):
        if pieces[i].has_nulls():
            need_mask = True

    # Check if all pieces share the same typed arm (no promotion needed).
    var common = _common_dtype(pieces)

    if common and common.value() == int64:
        var data = List[Int64]()
        var mask = NullMask()
        for i in range(len(pieces)):
            ref src = pieces[i]._int64_data()
            for k in range(len(src)):
                data.append(src[k])
            _append_piece_mask(mask, pieces[i], need_mask)
        var col = Column(col_name, data^, target_dtype)
        if need_mask:
            col.set_null_mask(mask^)
        return col^
    elif common and common.value() == float64:
        var data = List[Float64]()
        var mask = NullMask()
        for i in range(len(pieces)):
            ref src = pieces[i]._float64_data()
            for k in range(len(src)):
                data.append(src[k])
            _append_piece_mask(mask, pieces[i], need_mask)
        var col = Column(col_name, data^, target_dtype)
        if need_mask:
            col.set_null_mask(mask^)
        return col^
    elif common and common.value() == bool_:
        var data = List[Bool]()
        var mask = NullMask()
        for i in range(len(pieces)):
            ref src = pieces[i]._bool_data()
            for k in range(len(src)):
                data.append(src[k])
            _append_piece_mask(mask, pieces[i], need_mask)
        var col = Column(col_name, data^, target_dtype)
        if need_mask:
            col.set_null_mask(mask^)
        return col^
    elif common and common.value() == string_:
        # #644: All pieces hold List[String]; keep the string arm intact.
        var data = List[String]()
        var mask = NullMask()
        for i in range(len(pieces)):
            ref src = pieces[i]._str_data()
            for k in range(len(src)):
                data.append(src[k])
            _append_piece_mask(mask, pieces[i], need_mask)
        var col = Column(col_name, data^, target_dtype)
        if need_mask:
            col.set_null_mask(mask^)
        return col^
    elif target_dtype == float64:
        # Numeric promotion: pieces are a mix of float64, int64, and/or bool_.
        var data = List[Float64]()
        var mask = NullMask()
        for i in range(len(pieces)):
            _promote_piece_to_float64(pieces[i], data)
            _append_piece_mask(mask, pieces[i], need_mask)
        var col = Column(col_name, data^, float64)
        if need_mask:
            col.set_null_mask(mask^)
        return col^
    elif target_dtype == int64:
        # Numeric promotion: pieces are a mix of int64 and bool_.
        var data = List[Int64]()
        var mask = NullMask()
        for i in range(len(pieces)):
            _promote_piece_to_int64(pieces[i], data)
            _append_piece_mask(mask, pieces[i], need_mask)
        var col = Column(col_name, data^, int64)
        if need_mask:
            col.set_null_mask(mask^)
        return col^
    else:
        # Same non-numeric dtype backed by List[PythonObject] (e.g. datetime64_ns),
        # or object_ columns with mixed List[String]/List[PythonObject] backing.
        # _promote_dtype already validated that all pieces share the same dtype.
        var py_builtins = Python.import_module("builtins")
        var py_str = py_builtins.str
        var data = List[PythonObject]()
        var mask = NullMask()
        for i in range(len(pieces)):
            _promote_piece_to_object(pieces[i], data, py_str)
            _append_piece_mask(mask, pieces[i], need_mask)
        var col = Column(col_name, data^, target_dtype)
        if need_mask:
            col.set_null_mask(mask^)
        return col^


def _sort_result_cols(cols: List[Column]) raises -> List[Column]:
    """Return a copy of *cols* sorted ascending by column name."""
    var n = len(cols)
    var order = List[Int]()
    for i in range(n):
        order.append(i)
    for i in range(n):
        var min_idx = i
        for j in range(i + 1, n):
            if cols[order[j]].name.value() < cols[order[min_idx]].name.value():
                min_idx = j
        if min_idx != i:
            var tmp = order[i]
            order[i] = order[min_idx]
            order[min_idx] = tmp
    var result = List[Column]()
    for i in range(n):
        result.append(cols[order[i]].copy())
    return result^


def _concat_axis0(
    dfs: List[DataFrame],
    join: String,
    ignore_index: Bool,
    sort: Bool,
) raises -> DataFrame:
    """Vertical concatenation (axis=0): stack rows from each DataFrame."""
    # 1. Determine output column set.
    var col_names = List[String]()
    if join == "inner":
        # Intersection: only columns that appear in ALL DataFrames.
        if len(dfs) > 0:
            for j in range(len(dfs[0]._cols)):
                col_names.append(dfs[0]._cols[j].name.value())
            for i in range(1, len(dfs)):
                var keep = List[String]()
                for k in range(len(col_names)):
                    var found = False
                    for j in range(len(dfs[i]._cols)):
                        if dfs[i]._cols[j].name == col_names[k]:
                            found = True
                    if found:
                        keep.append(col_names[k])
                col_names = keep^
    else:
        # Outer (default): union of all column names, first-seen order.
        for i in range(len(dfs)):
            for j in range(len(dfs[i]._cols)):
                var name = dfs[i]._cols[j].name.value()
                var seen = False
                for k in range(len(col_names)):
                    if col_names[k] == name:
                        seen = True
                if not seen:
                    col_names.append(name)

    if sort:
        col_names = _sort_col_names(col_names)

    # 2. For each output column, gather one piece from every DataFrame and stack.
    var result_cols = List[Column]()
    for c in range(len(col_names)):
        var col_name = col_names[c]
        var dtype = _dtype_for(dfs, col_name)
        var pieces = List[Column]()
        for i in range(len(dfs)):
            var nrows = dfs[i].shape()[0]
            var found = False
            for j in range(len(dfs[i]._cols)):
                if dfs[i]._cols[j].name == col_name:
                    var piece = dfs[i]._cols[j].copy()
                    # Cast to promoted dtype if this frame's column differs.
                    if piece.dtype != dtype:
                        piece = piece._astype(dtype)
                    pieces.append(piece^)
                    found = True
            if not found:
                pieces.append(_null_col(col_name, nrows, dtype))
        result_cols.append(_vstack(pieces))

    return DataFrame(result_cols^)


def _concat_axis1(
    dfs: List[DataFrame],
    join: String,
    ignore_index: Bool,
    sort: Bool,
) raises -> DataFrame:
    """Horizontal concatenation (axis=1): stack columns from each DataFrame."""
    if len(dfs) == 0:
        return DataFrame()

    # Determine the number of output rows.
    var nrows: Int
    if join == "inner":
        nrows = dfs[0].shape()[0]
        for i in range(1, len(dfs)):
            var r = dfs[i].shape()[0]
            if r < nrows:
                nrows = r
    else:
        nrows = 0
        for i in range(len(dfs)):
            var r = dfs[i].shape()[0]
            if r > nrows:
                nrows = r

    var result_cols = List[Column]()
    var col_idx = 0
    for i in range(len(dfs)):
        for j in range(len(dfs[i]._cols)):
            var col_name: Optional[String]
            if ignore_index:
                col_name = String(col_idx)
            else:
                col_name = dfs[i]._cols[j].name
            col_idx += 1
            var col_rows = len(dfs[i]._cols[j])
            if col_rows == nrows:
                var c = dfs[i]._cols[j].copy()
                c.name = col_name
                result_cols.append(c^)
            elif col_rows > nrows:
                # Trim to nrows (inner join).
                var c = dfs[i]._cols[j].slice(0, nrows)
                c.name = col_name
                result_cols.append(c^)
            else:
                # Pad to nrows with nulls (outer join).
                var pieces = List[Column]()
                var src = dfs[i]._cols[j].copy()
                src.name = col_name
                pieces.append(src^)
                pieces.append(
                    _null_col(col_name, nrows - col_rows, dfs[i]._cols[j].dtype)
                )
                result_cols.append(_vstack(pieces))

    if sort:
        result_cols = _sort_result_cols(result_cols)

    return DataFrame(result_cols^)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def concat(
    objs: List[DataFrame],
    axis: Int = 0,
    join: String = "outer",
    ignore_index: Bool = False,
    keys: Optional[List[String]] = None,
    sort: Bool = False,
) raises -> DataFrame:
    """Concatenate a list of DataFrames along an axis.

    Parameters
    ----------
    objs : List[DataFrame]
        Sequence of DataFrame objects to concatenate.
    axis : Int
        ``0`` (default) stacks rows; ``1`` stacks columns.
    join : String
        ``"outer"`` (default) keeps all labels; ``"inner"`` keeps only the
        labels shared by every input.
    ignore_index : Bool
        When ``True``, reset the output index (axis=0) or column labels
        (axis=1) to a default integer range.
    keys : Optional[List[String]]
        When provided, a ``"__key__"`` string column is prepended to the
        result (position 0).  For axis=0 each row is labelled with the key
        of its source DataFrame; for axis=1 each original column is labelled
        with the key of its source DataFrame.  ``len(keys)`` must equal
        ``len(objs)``.
    sort : Bool
        When ``True``, sort the non-concatenation axis labels alphabetically.
    """
    if len(objs) == 0:
        return DataFrame()

    if keys:
        if len(keys.value()) != len(objs):
            raise Error(
                "concat: len(keys) = "
                + String(len(keys.value()))
                + " but len(objs) = "
                + String(len(objs))
            )

    if axis == 1:
        var result = _concat_axis1(objs, join, ignore_index, sort)
        if keys:
            var counts = List[Int]()
            for i in range(len(objs)):
                counts.append(len(objs[i]._cols))
            var key_col = _build_key_col(keys.value(), counts)
            var new_cols = List[Column]()
            new_cols.append(key_col^)
            for i in range(len(result._cols)):
                new_cols.append(result._cols[i].copy())
            result._cols = new_cols^
        return result^

    var result = _concat_axis0(objs, join, ignore_index, sort)
    if keys:
        var counts = List[Int]()
        for i in range(len(objs)):
            counts.append(objs[i].shape()[0])
        var key_col = _build_key_col(keys.value(), counts)
        var new_cols = List[Column]()
        new_cols.append(key_col^)
        for i in range(len(result._cols)):
            new_cols.append(result._cols[i].copy())
        result._cols = new_cols^
    return result^
