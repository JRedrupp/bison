# Changelog

All notable changes to this project will be documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Restore `DataFrame.query()` and `DataFrame.eval()` as fully native Mojo
  (issue #716). The `bison.expr` package (tokenizer, AST, parser, evaluator)
  is restored. The evaluator is rewritten to use a plain recursive
  `_eval_node` function instead of the original generic visitor pattern,
  working around Mojo compiler bug #642 without re-introducing typed caches.

## [0.1.0-alpha] - 2026-04-19

### Added
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
