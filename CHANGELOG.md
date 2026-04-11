# Changelog

All notable changes to this project will be documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- New `string_` BisonDtype constant (distinct from `object_`) for columns
  backed by `List[String]`. `DataFrame.dtypes` and `Series.dtype` now return
  `"string"` for string columns instead of `"object"`. Round-trips through
  `to_pandas()` still produce pandas `"object"` dtype for compatibility, and
  `from_pandas()` ingests pandas string / pure-string-object columns as
  `string_`. `Column.is_string()` and `Column.is_object()` now dispatch on
  `dtype` (matching the other type predicates); `is_object()` returns `False`
  for `datetime64_ns` / `timedelta64_ns` columns, matching pandas
  `dtype == object` semantics. Closes #644.
- `DataFrame.query()` and `DataFrame.eval()` are now implemented natively in
  Mojo via the `bison.expr` parser and evaluator. Supported grammar: column
  references, integer/float/bool/string/null literals, comparison operators
  (`<`, `<=`, `>`, `>=`, `==`, `!=`), logical operators (`not`, `and`, `or`),
  and parenthetical grouping. Kleene three-valued null semantics apply for
  logical connectives. Unsupported constructs (arithmetic, function calls,
  attribute access, indexing, membership/identity operators, comparison
  chaining) raise with `"unsupported syntax"` in the message. See
  `docs/query-eval-spec.md` for the full grammar and semantics reference.

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
