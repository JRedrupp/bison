# Changelog

All notable changes to this project will be documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- Pinned `max` (Mojo) dependency to `==26.1.0` for reproducibility

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
