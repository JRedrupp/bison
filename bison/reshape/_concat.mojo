from std.python import Python, PythonObject
from std.collections import Optional
from ..dataframe import DataFrame, _sort_col_names
from ..column import Column, ColumnData
from ..dtypes import BisonDtype, int64, float64, bool_, object_
from .._errors import _not_implemented


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------


def _null_col(name: String, n: Int, dtype: BisonDtype) raises -> Column:
    """Return a Column of *n* null rows using *dtype* as the stored arm."""
    var mask = List[Bool]()
    for _ in range(n):
        mask.append(True)
    if dtype == int64:
        var data = List[Int64]()
        for _ in range(n):
            data.append(Int64(0))
        var col = Column(name, ColumnData(data^), dtype)
        col._null_mask = mask^
        return col^
    elif dtype == float64:
        var data = List[Float64]()
        for _ in range(n):
            data.append(Float64(0))
        var col = Column(name, ColumnData(data^), dtype)
        col._null_mask = mask^
        return col^
    elif dtype == bool_:
        var data = List[Bool]()
        for _ in range(n):
            data.append(False)
        var col = Column(name, ColumnData(data^), dtype)
        col._null_mask = mask^
        return col^
    else:
        var data = List[PythonObject]()
        for _ in range(n):
            data.append(Python.evaluate("None"))
        var col = Column(name, ColumnData(data^), object_)
        col._null_mask = mask^
        return col^


def _dtype_for(dfs: List[DataFrame], col_name: String) -> BisonDtype:
    """Return the dtype of *col_name* from the first DataFrame that has it."""
    for i in range(len(dfs)):
        for j in range(len(dfs[i]._cols)):
            if dfs[i]._cols[j].name == col_name:
                return dfs[i]._cols[j].dtype
    return object_


def _vstack(pieces: List[Column]) raises -> Column:
    """Vertically stack a list of same-named Columns row-wise.

    When all pieces share the same typed data arm the result uses that arm.
    If any piece has a different arm the result falls back to a
    ``List[PythonObject]`` (object dtype) column.
    """
    if len(pieces) == 0:
        return Column()
    var col_name = pieces[0].name
    var target_dtype = pieces[0].dtype

    # Detect whether any piece has a null mask.
    var need_mask = False
    for i in range(len(pieces)):
        if len(pieces[i]._null_mask) > 0:
            need_mask = True

    # Determine the common typed arm (fall back to PythonObject on mismatch).
    var is_int = pieces[0]._data.isa[List[Int64]]()
    var is_float = pieces[0]._data.isa[List[Float64]]()
    var is_bool = pieces[0]._data.isa[List[Bool]]()
    var is_str = pieces[0]._data.isa[List[String]]()
    for i in range(1, len(pieces)):
        if is_int and not pieces[i]._data.isa[List[Int64]]():
            is_int = False
        if is_float and not pieces[i]._data.isa[List[Float64]]():
            is_float = False
        if is_bool and not pieces[i]._data.isa[List[Bool]]():
            is_bool = False
        if is_str and not pieces[i]._data.isa[List[String]]():
            is_str = False

    if is_int:
        var data = List[Int64]()
        var mask = List[Bool]()
        for i in range(len(pieces)):
            var has_m = len(pieces[i]._null_mask) > 0
            for k in range(len(pieces[i]._data[List[Int64]])):
                data.append(pieces[i]._data[List[Int64]][k])
                if need_mask:
                    if has_m:
                        mask.append(pieces[i]._null_mask[k])
                    else:
                        mask.append(False)
        var col = Column(col_name, ColumnData(data^), target_dtype)
        if need_mask:
            col._null_mask = mask^
        return col^
    elif is_float:
        var data = List[Float64]()
        var mask = List[Bool]()
        for i in range(len(pieces)):
            var has_m = len(pieces[i]._null_mask) > 0
            for k in range(len(pieces[i]._data[List[Float64]])):
                data.append(pieces[i]._data[List[Float64]][k])
                if need_mask:
                    if has_m:
                        mask.append(pieces[i]._null_mask[k])
                    else:
                        mask.append(False)
        var col = Column(col_name, ColumnData(data^), target_dtype)
        if need_mask:
            col._null_mask = mask^
        return col^
    elif is_bool:
        var data = List[Bool]()
        var mask = List[Bool]()
        for i in range(len(pieces)):
            var has_m = len(pieces[i]._null_mask) > 0
            for k in range(len(pieces[i]._data[List[Bool]])):
                data.append(pieces[i]._data[List[Bool]][k])
                if need_mask:
                    if has_m:
                        mask.append(pieces[i]._null_mask[k])
                    else:
                        mask.append(False)
        var col = Column(col_name, ColumnData(data^), target_dtype)
        if need_mask:
            col._null_mask = mask^
        return col^
    elif is_str:
        var data = List[String]()
        var mask = List[Bool]()
        for i in range(len(pieces)):
            var has_m = len(pieces[i]._null_mask) > 0
            for k in range(len(pieces[i]._data[List[String]])):
                data.append(pieces[i]._data[List[String]][k])
                if need_mask:
                    if has_m:
                        mask.append(pieces[i]._null_mask[k])
                    else:
                        mask.append(False)
        var col = Column(col_name, ColumnData(data^), target_dtype)
        if need_mask:
            col._null_mask = mask^
        return col^
    else:
        # PythonObject fallback — convert all typed arms to Python objects.
        var py_builtins = Python.import_module("builtins")
        var data = List[PythonObject]()
        var mask = List[Bool]()
        for i in range(len(pieces)):
            var has_m = len(pieces[i]._null_mask) > 0
            if pieces[i]._data.isa[List[Int64]]():
                for k in range(len(pieces[i]._data[List[Int64]])):
                    data.append(
                        py_builtins.int(Int(pieces[i]._data[List[Int64]][k]))
                    )
                    if need_mask:
                        if has_m:
                            mask.append(pieces[i]._null_mask[k])
                        else:
                            mask.append(False)
            elif pieces[i]._data.isa[List[Float64]]():
                for k in range(len(pieces[i]._data[List[Float64]])):
                    data.append(
                        py_builtins.float(pieces[i]._data[List[Float64]][k])
                    )
                    if need_mask:
                        if has_m:
                            mask.append(pieces[i]._null_mask[k])
                        else:
                            mask.append(False)
            elif pieces[i]._data.isa[List[Bool]]():
                for k in range(len(pieces[i]._data[List[Bool]])):
                    if pieces[i]._data[List[Bool]][k]:
                        data.append(py_builtins.bool(1))
                    else:
                        data.append(py_builtins.bool(0))
                    if need_mask:
                        if has_m:
                            mask.append(pieces[i]._null_mask[k])
                        else:
                            mask.append(False)
            elif pieces[i]._data.isa[List[String]]():
                for k in range(len(pieces[i]._data[List[String]])):
                    data.append(
                        py_builtins.str(pieces[i]._data[List[String]][k])
                    )
                    if need_mask:
                        if has_m:
                            mask.append(pieces[i]._null_mask[k])
                        else:
                            mask.append(False)
            else:
                for k in range(len(pieces[i]._data[List[PythonObject]])):
                    data.append(pieces[i]._data[List[PythonObject]][k])
                    if need_mask:
                        if has_m:
                            mask.append(pieces[i]._null_mask[k])
                        else:
                            mask.append(False)
        var col = Column(col_name, ColumnData(data^), object_)
        if need_mask:
            col._null_mask = mask^
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
            if cols[order[j]].name < cols[order[min_idx]].name:
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
                col_names.append(dfs[0]._cols[j].name)
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
                var name = dfs[i]._cols[j].name
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
                    pieces.append(dfs[i]._cols[j].copy())
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
            var col_name: String
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
        Hierarchical index keys — not yet implemented natively; raises if
        provided.
    sort : Bool
        When ``True``, sort the non-concatenation axis labels alphabetically.
    """
    if len(objs) == 0:
        return DataFrame()

    if keys:
        _not_implemented("concat")

    if axis == 1:
        return _concat_axis1(objs, join, ignore_index, sort)
    return _concat_axis0(objs, join, ignore_index, sort)
