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

`column.mojo` is the storage layer. Each `Column` has a **dual-backend**
architecture ([#619](https://github.com/JRedrupp/bison/issues/619)):

1. **Legacy backend** (`_data: ColumnData`): A
   `Variant[List[Int64], List[Float64], List[Bool], List[String], List[PythonObject]]`
   with a parallel `List[Bool]` null mask.
2. **Marrow backend** (`_storage: ColumnStorage`): An `AnyArray` (Apache Arrow
   for Mojo) for SIMD aggregation kernels, or `LegacyObjectData` for
   `PythonObject` columns.
3. **Typed caches** (`_int64_cache`, `_f64_cache`, `_bool_cache`,
   `_str_cache`): Pre-extracted typed lists populated from `_data` at
   construction time. At most one typed cache plus `_f64_cache` is non-empty.
   After [#645](https://github.com/JRedrupp/bison/issues/645), caches are also
   the **write target** for all mutations — `_data` is only written during
   construction and is stale afterward.

The typed caches exist as a workaround for a Mojo compiler deadlock
([#642](https://github.com/JRedrupp/bison/issues/642)): typed `AnyArray`
downcasts (`arr.as_int64()` etc.) cannot co-exist on the same call graph as
`df.query()` in `column.mojo`. All high-traffic operations (comparison,
aggregation, transforms, extraction) read from caches instead of `_data`. See
`TODO(#642)` comments throughout.

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

After [#644](https://github.com/JRedrupp/bison/issues/644), `is_string()` and
`is_object()` dispatch on `self.dtype` like the other predicates —
`is_string()` is equivalent to `dtype == string_`, `is_object()` is equivalent
to `dtype == object_`. Note that `is_object()` returns `False` for
`datetime64_ns` / `timedelta64_ns` columns even though they are backed by
`List[PythonObject]`; this matches pandas `dtype == object` semantics.

### Visitor dispatch

For single-cell extraction use `_series_scalar_at(col, row)` or
`_scalar_from_col(col, row)` — these read from typed caches when available.

For multi-arm algorithmic dispatch, use `Column._visit_raises[V]()` or
`Column._visit[V]()` which route through typed caches. Post-#647, there is no
standalone `visit_col_data` / `visit_col_data_raises` dispatcher — visitors are
always invoked via the `Column._visit*` methods. Visitor structs implement
`ColumnDataVisitorRaises` and the cache dispatch calls their `on_*` methods
with cache data.

### Unsafe typed accessors

After a predicate check, access the typed data via the unsafe accessors:
`col._int64_data()`, `col._float64_data()`, `col._bool_data()`,
`col._str_data()`, `col._obj_data()`.

After [#645](https://github.com/JRedrupp/bison/issues/645):
- `_int64_data()` / `_float64_data()` / `_bool_data()` / `_str_data()` return
  refs directly into the typed caches — mutations go to the cache.
- `_obj_data()` still returns a ref into `_data` (object columns have no typed
  cache).
- After any cache mutation call `col._rebuild_marrow_only()` to sync the
  secondary `_f64_cache` (for int/bool) and rebuild the marrow backend.
- Do **not** call `_try_activate_storage()` from mutation paths — it reads from
  `_data` and will overwrite cache mutations.

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
