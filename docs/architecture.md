# Architecture

This document describes the internal architecture of bison. It is intended for
contributors working on the library internals.

## Core types

| Type | File | Notes |
|------|------|-------|
| `DataFrame` | `_frame.mojo` | `Dict[String, Column]` backing, ordered columns |
| `Series` | `_frame.mojo` | Wraps `Column` + optional name |
| `DataFrameGroupBy` / `SeriesGroupBy` | `_frame.mojo` | Supports agg, sum, mean, count, first, last; single-key numeric aggs use marrow hash-aggregate kernel |
| `Index` | `index.mojo` | `List[String]` backed with name attribute |
| `RangeIndex` | `index.mojo` | `start, stop, step` — like pandas |
| `ColumnIndex` | `index.mojo` | Variant: `Index \| List[Int64] \| List[Float64] \| List[PythonObject]` |
| `BisonDtype` | `dtypes.mojo` | 15 comptime constants: `int8` ... `uint64`, `float32/64`, `bool_`, `string_`, `object_`, `datetime64_ns`, `timedelta64_ns` |

## Column storage

`column.mojo` is the storage layer. Each `Column` has a single canonical
storage field:

- **`_storage: ColumnStorage`** is a `Variant[AnyArray, LegacyObjectData]`.
  - `AnyArray` (Apache Arrow for Mojo) backs int64 / float64 / bool / string
    columns without nulls, plus int64 / float64 / bool with nulls (validity
    stored in the Arrow bitmap). SIMD aggregation kernels operate directly on
    `AnyArray`.
  - `LegacyObjectData` wraps a `List[PythonObject]` plus a `NullMask` and backs
    object / datetime64 / timedelta64 columns, plus string-with-nulls (marrow
    cannot build a string + null array yet).

There are no side caches — the typed caches that used to exist alongside
`_storage` ([#642](https://github.com/JRedrupp/bison/issues/642) workaround)
have been removed now that the query-evaluator compiler deadlock is gone.
Typed reads go through `_int64_list()` / `_float64_list()` / `_bool_list()` /
`_str_list()` (which allocate a fresh `List[T]` from the AnyArray) or, for
single-cell access, `col._storage[AnyArray].as_*().unsafe_get(i)` with the
appropriate `rebind` for the primitive scalar types. Writes go through the
`_flush_*_list` helpers: the caller extracts a `List[T]`, mutates, then flushes
back into storage in one shot.

Dtype promotion happens automatically (e.g. mixing int64 + float64 produces a
float64 column). GroupBy key columns may promote to `List[Float64]` to unify
key types.

### Column type predicates

**Never use `_data.isa[...]()` directly in `_frame.mojo`.** Instead use the
`Column` predicate methods:

| Predicate | Replaces |
|-----------|----------|
| `col.is_int()` | `col._data.isa[List[Int64]]()` |
| `col.is_float()` | `col._data.isa[List[Float64]]()` |
| `col.is_bool()` | `col._data.isa[List[Bool]]()` |
| `col.is_string()` | `col._data.isa[List[String]]()` |
| `col.is_object()` | `col._data.isa[List[PythonObject]]()` |
| `col.is_numeric()` | `col._data.isa[List[Int64]]() or col._data.isa[List[Float64]]()` |

The same encapsulation applies to `_index`.  **Never use `_index.isa[...]()` or
`_index[Arm]` directly in `_frame.mojo`.**  Use the index predicates and
accessors on `Column` instead:

| Predicate / Accessor | Replaces |
|----------------------|----------|
| `col.is_str_index()` | `col._index.isa[Index]()` |
| `col.is_int_index()` | `col._index.isa[List[Int64]]()` |
| `col.is_float_index()` | `col._index.isa[List[Float64]]()` |
| `col.is_obj_index()` | `col._index.isa[List[PythonObject]]()` |
| `col._str_index()` | `col._index[Index]` |
| `col._int_index_data()` | `col._index[List[Int64]]` |
| `col._float_index_data()` | `col._index[List[Float64]]` |
| `col._obj_index_data()` | `col._index[List[PythonObject]]` |

After [#644](https://github.com/JRedrupp/bison/issues/644), `is_string()` and
`is_object()` dispatch on `self.dtype` like the other predicates —
`is_string()` is equivalent to `dtype == string_`, `is_object()` is equivalent
to `dtype == object_`. Note that `is_object()` returns `False` for
`datetime64_ns` / `timedelta64_ns` columns even though they are backed by
`List[PythonObject]`; this matches pandas `dtype == object` semantics.

### Visitor dispatch

For single-cell extraction use `_series_scalar_at(col, row)` or
`_scalar_from_col(col, row)` — these read directly from the `AnyArray` storage
arm via `unsafe_get`.

For multi-arm algorithmic dispatch, use `Column._visit_raises[V]()` or
`Column._visit[V]()` which extract a fresh `List[T]` from storage on demand
and dispatch to the appropriate `on_int64` / `on_float64` / `on_bool` /
`on_str` / `on_obj` visitor method.

### Typed list accessors

After a predicate check, access the typed data via:

- `col._int64_list()` / `col._float64_list()` / `col._bool_list()` /
  `col._str_list()` — extract a fresh owned `List[T]` from `_storage[AnyArray]`.
- `col._f64_list()` — extract a Float64 list, widening int64 / bool values.
- `col._storage[AnyArray].as_int64()` (etc.) for ref-bound borrows used in
  tight loops that need O(1) `unsafe_get` access.

For writes, use the `_flush_*_list` helpers:

```mojo
var data = col._int64_list()
data[row] = new_value
col._flush_int64_list(data^)   # rebuilds _storage from the mutated list
```

`_rebuild_storage()` is retained as a no-op for backwards compatibility with
older call sites; it is safe to delete any remaining calls.

## I/O and dtype inference

### Inference order

- **CSV / JSON**: `bool` > `int64` > `float64` > `String`
- Null values tracked via `na_set` parameter and null mask
- `read_parquet` / `to_parquet` use marrow's native Parquet I/O; falls back to
  pandas for object columns
- `read_excel` delegates to pandas (stub)

### Accessors

`Series.str` returns `StringMethods` (upper, lower, strip, contains, replace,
split, and more). `Series.dt` returns `DatetimeMethods` (year, month, day,
hour, minute, second, and more).

## Element-wise math transforms

`Column._apply[F: FloatTransformFn]()` is the generic kernel for element-wise
Float64 transforms. It converts numeric arms to Float64, applies `F`, and
propagates nulls. New scalar math operations should follow this pattern:

1. Define a module-level `def _foo_fn(v: Float64) -> Float64` in `column.mojo`.
2. Add `Column._foo()` as a one-liner: `return self._apply[_foo_fn]()`.
3. Add `Series.foo()` and `DataFrame.foo()` wrappers in `_frame.mojo`.
4. Wire into `applymap`/`transform`/`pipe` string dispatch.

`_abs` and `_round` intentionally use dedicated visitors instead of `_apply[F]`
because they preserve input dtype (Int64 in -> Int64 out) or take extra
parameters (`decimals`). Do not refactor them to use `_apply`.

## Window operations

`DataFrame.rolling()`, `.expanding()`, and `.ewm()` return lightweight window
objects (`Rolling`, `Expanding`, `ExponentialMovingWindow`) that hold a
reference to the source DataFrame plus window parameters. Aggregation methods
(`.mean()`, `.sum()`, etc.) iterate over numeric columns, call pure-function
kernels from `bison/window/_kernels.mojo`, and assemble the results into a new
DataFrame.

The computation kernels operate on `List[Float64]` + `List[Bool]` (null mask)
and return a `WindowResult` struct containing the result data, null mask, and a
`has_any_null` flag. They have no dependency on DataFrame/Series/Column — this
keeps the algorithmic code decoupled and testable.

The window structs live in `_frame.mojo` (after the GroupBy structs) to avoid
circular import issues, since their methods return `DataFrame` / `Series`.
Series variants (`SeriesRolling`, `SeriesExpanding`,
`SeriesExponentialMovingWindow`) follow the same pattern but operate on a
single column.

Key algorithms:
- **Rolling sum/mean/count**: O(n) sliding window with running accumulators.
- **Rolling min/max**: O(n) monotonic deque.
- **Rolling var/std**: O(n) running sum-of-squares.
- **Expanding**: Running accumulators from position 0 (simpler than rolling).
- **EWM mean**: Recursive `adjust=True` formula matching pandas default.
- **EWM var/std**: Online weighted variance with bias correction.

## Marrow integration

Marrow (Apache Arrow for Mojo) is vendored at `vendor/marrow/` as a git
submodule. Built via `pixi run build-marrow`. The integration provides:

- **Arrow conversion layer** (`bison/arrow.mojo`):
  `column_to_marrow_array`, `marrow_array_to_column`,
  `dataframe_to_record_batch`, `record_batch_to_dataframe`,
  `dataframe_to_table`, `table_to_dataframe`. Supports int64, float64, bool,
  string columns. `List[PythonObject]` columns cannot be converted.
- **SIMD aggregation kernels** (`column.mojo`): `Column.sum/min/max` use
  `marrow.kernels.aggregate` for int64/float64.
- **Hash-aggregate GroupBy** (`_frame.mojo`): Single-key numeric GroupBy
  aggregations (sum, mean, min, max, count) use `marrow.kernels.groupby` for
  fused O(N) hash-aggregate when: `len(by) == 1`, `as_index=True`, and key
  column is Arrow-convertible (not `List[PythonObject]`).
- **Parquet I/O** (`io/parquet.mojo`): Native read/write via `marrow.parquet`.
