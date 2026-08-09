# Changelog

All notable changes to this project will be documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0-alpha] - 2026-08-09

### Changed
- Revived the project on a current Mojo/MAX toolchain after several months
  on an unmaintainable pin. Bumped `max`/`mblack` from `26.3.0.dev2026041520`
  through several nightly builds to `26.5.0rc1`, a tagged release candidate
  rather than a rolling dev snapshot.
- Forked the vendored `marrow` dependency ([JRedrupp/marrow](https://github.com/JRedrupp/marrow))
  to fix Mojo-nightly compatibility breaks the upstream project hadn't yet
  addressed, and upstreamed the accumulated fixes back to
  [kszucs/marrow](https://github.com/kszucs/marrow).
- Fixed the nightly CI drift-detector: the previous version's channel-switch
  step was a no-op, so the daily nightly job had been silently re-testing
  the same pinned build instead of catching new breaks. It now floats to
  the latest nightly and files a tracking issue on failure.

### Fixed
- `DataFrame.groupby()`/`Series.groupby()` `sum()`/`min()`/`max()` on
  integer columns via the marrow-accelerated fast path no longer crashes.
  Caused by an upstream marrow change to the fused aggregate kernel's
  accumulator dtype (float64 → int64 for integer inputs, to avoid precision
  loss above 2^53) that this project's own code hadn't been updated to match.
- `NullMask` bitmap now grows correctly on a valid append beyond its current
  capacity (#758).
- Mechanical fixes for ~15 categories of Mojo-nightly API drift across
  `bison/` and the `marrow` fork: `deinit` move-parameter naming,
  `Movable`/`Copyable` trait composition, pointer subscript syntax,
  `read`→`imm` argument passing, `Variant` subscript origin inference,
  `perf_counter_ns()`'s `UInt`→`Int` return type change, and
  `UnsafePointer`→`Pointer` for tracked-origin pointer usage, among others
  — see `docs/mojo-patterns.md` for the full reference.

### Performance
- `rolling_min`/`rolling_max`: replaced an O(n) deque pop-front with
  `std.collections.Deque` (#757).
- `DataFrame.sort_values`: avoid redundant key-column materialization on
  the first pass (#738).
- `query`/`eval`: eliminate an unnecessary `_f64_list()` copy in scalar
  comparisons and fuse two-predicate AND/OR evaluation (#740); eliminate a
  marrow bool round-trip and redundant `is_valid()` overhead in `query_and`
  (#741).

## [0.1.0-alpha] - 2026-04-20

### Added
- Restore `DataFrame.query()` and `DataFrame.eval()` as fully native Mojo
  (issue #716). The `bison.expr` package (tokenizer, AST, parser, evaluator)
  is restored. The evaluator is rewritten to use a plain recursive
  `_eval_node` function instead of the original generic visitor pattern,
  working around Mojo compiler bug #642 without re-introducing typed caches.
- `DataFrame.rolling(window)` and `Series.rolling(window)` with native Mojo
  sliding-window operations: `sum()`, `mean()`, `std()`, `var()`, `min()`,
  `max()`, `count()`. Supports `min_periods` parameter for partial windows.
- `DataFrame.expanding(min_periods)` and `Series.expanding(min_periods)` with
  native Mojo expanding-window operations: `sum()`, `mean()`, `std()`, `var()`,
  `min()`, `max()`, `count()`.
- `DataFrame.ewm()` and `Series.ewm()` with exponentially weighted moving
  operations: `mean()`, `std()`, `var()`. Supports `com`, `span`, `halflife`,
  and `alpha` parameters. Closes #688.
- New `string_` BisonDtype constant (distinct from `object_`) for columns
  backed by `List[String]`. `DataFrame.dtypes` and `Series.dtype` now return
  `"string"` for string columns instead of `"object"`. Round-trips through
  `to_pandas()` still produce pandas `"object"` dtype for compatibility, and
  `from_pandas()` ingests pandas string / pure-string-object columns as
  `string_`. `Column.is_string()` and `Column.is_object()` now dispatch on
  `dtype` (matching the other type predicates); `is_object()` returns `False`
  for `datetime64_ns` / `timedelta64_ns` columns, matching pandas
  `dtype == object` semantics. Closes #644.


## [0.1.0] - 2026-03-08

### Added
- Initial project scaffold with Pixi/Mojo setup
- `DataFrame` and `Series` structs with full pandas API stubs (~120 and ~60 methods respectively)
- `GroupBy`, `Index`, `RangeIndex` stubs
- `LocIndexer`, `ILocIndexer`, `AtIndexer`, `IAtIndexer` stubs
- String accessor (`StringMethods`) and datetime accessor (`DatetimeMethods`) stubs
- IO stubs: `read_csv`, `read_parquet`, `read_json`, `read_excel`
- `concat` stub
- `from_pandas` / `to_pandas` interop (working at stub stage)
- Test suite comparing bison output against pandas
- GitHub Actions CI and release workflows
