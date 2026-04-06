# bison — Claude Code instructions

## Project overview

Mojo DataFrame library with a pandas-compatible API. Every method that is not yet implemented natively raises via `_not_implemented()`. The stub layer wraps a real pandas `PythonObject` so `from_pandas` / `to_pandas` work immediately without any native code.

## Layout

```
bison/
├── bison/              — the Mojo package (import bison)
│   ├── __init__.mojo   — public API exports
│   ├── _errors.mojo    — _not_implemented() helper
│   ├── _version.mojo   — AUTO-GENERATED; never edit by hand
│   ├── _frame.mojo     — core DataFrame, Series, GroupBy (~8,000 lines)
│   ├── column.mojo     — Column storage & ColumnData variant (~5,000 lines)
│   ├── arrow.mojo      — Arrow ↔ Column conversion (marrow interop)
│   ├── dataframe.mojo  — re-exports from _frame
│   ├── series.mojo     — re-exports from _frame
│   ├── groupby.mojo    — re-exports from _frame
│   ├── indexing.mojo   — re-exports indexer classes
│   ├── dtypes.mojo     — BisonDtype struct + 14 comptime dtype constants
│   ├── index.mojo      — Index, RangeIndex, ColumnIndex structs
│   ├── accessors/
│   │   ├── str_accessor.mojo   — StringMethods (.str accessor)
│   │   └── dt_accessor.mojo    — DatetimeMethods (.dt accessor)
│   ├── io/
│   │   ├── csv.mojo            — read_csv (native; dtype inference)
│   │   ├── json.mojo           — read_json (native; type inference)
│   │   ├── parquet.mojo        — read_parquet / to_parquet (native via marrow)
│   │   └── excel.mojo          — read_excel (stub → pandas/openpyxl)
│   └── reshape/
│       └── _concat.mojo        — concat axis=0 with dtype promotion
├── tests/              — one test_*.mojo file per feature area
├── benchmarks/         — bench_core.mojo + _bench_utils.mojo
├── scripts/            — gen_version.py, run_tests.sh, …
└── docs/               — index.html performance dashboard
```

## Environment

Mojo is installed via **Pixi** (`pixi install`). Do not use Magic — it is deprecated. All tasks run through Pixi:

```bash
pixi run gen-version    # write bison/_version.mojo from pixi.toml
pixi run test           # regenerates version then runs all tests
pixi run fmt            # mojo format bison/
pixi run check          # mojo package bison/ --Werror (no warnings allowed)
pixi run lint           # pre-commit run --all-files
pixi run bench          # run benchmarks (depends on gen-version)
pixi run gen-report     # merge benchmark results into docs/data.json
```

Supported platforms: `linux-64`, `osx-arm64`. Requires `pixi >= 0.41.0`, `max >= 26.2`.

## Architecture

### Column storage

`column.mojo` is the storage layer. `ColumnData` is a `Variant`:

```
ColumnData = List[Int64] | List[Float64] | List[Bool] | List[String] | List[PythonObject]
```

Each `Column` struct holds a `ColumnData` arm and a parallel `List[Bool]` null mask. Dtype promotion happens automatically (e.g. mixing int64 + float64 → float64 column). GroupBy key columns may promote to `List[Float64]` to unify key types.

### Core types

| Type | File | Notes |
|------|------|-------|
| `DataFrame` | `_frame.mojo` | `Dict[String, Column]` backing, ordered columns |
| `Series` | `_frame.mojo` | Wraps `Column` + optional name |
| `DataFrameGroupBy` / `SeriesGroupBy` | `_frame.mojo` | Supports agg, sum, mean, count, first, last; single-key numeric aggs use marrow hash-aggregate kernel |
| `Index` | `index.mojo` | `List[String]` backed with name attribute |
| `RangeIndex` | `index.mojo` | `start, stop, step` — like pandas |
| `ColumnIndex` | `index.mojo` | Variant: `Index | List[Int64] | List[Float64] | List[PythonObject]` |
| `BisonDtype` | `dtypes.mojo` | 14 comptime constants: `int8` … `uint64`, `float32/64`, `bool_`, `object_`, `datetime64_ns`, `timedelta64_ns` |

### I/O dtype inference order

- **CSV / JSON**: `bool` > `int64` > `float64` > `String`
- Null values tracked via `na_set` parameter and null mask
- `read_parquet` / `to_parquet` use marrow's native Parquet I/O; falls back to pandas for object columns
- `read_excel` delegates to pandas (stub)

### Marrow integration

Marrow (Apache Arrow for Mojo) is vendored at `vendor/marrow/` as a git submodule. Built via `pixi run build-marrow`. The integration provides:

- **Arrow conversion layer** (`bison/arrow.mojo`): `column_to_marrow_array`, `marrow_array_to_column`, `dataframe_to_record_batch`, `record_batch_to_dataframe`, `dataframe_to_table`, `table_to_dataframe`. Supports int64, float64, bool, string columns. `List[PythonObject]` columns cannot be converted.
- **SIMD aggregation kernels** (`column.mojo`): `Column.sum/min/max` use `marrow.kernels.aggregate` for int64/float64.
- **Hash-aggregate GroupBy** (`_frame.mojo`): Single-key numeric GroupBy aggregations (sum, mean, min, max, count) use `marrow.kernels.groupby` for fused O(N) hash-aggregate when: `len(by) == 1`, `as_index=True`, and key column is Arrow-convertible (not `List[PythonObject]`).
- **Parquet I/O** (`io/parquet.mojo`): Native read/write via `marrow.parquet`.

### Accessors

`Series.str` returns `StringMethods` (upper, lower, strip, contains, replace, split, …).
`Series.dt` returns `DatetimeMethods` (year, month, day, hour, minute, second, …).

## Versioning

Single source of truth: `[project] version` in `pixi.toml`. Do not edit `bison/_version.mojo` by hand — it is generated by `scripts/gen_version.py`.

To cut a release: bump `pixi.toml`, update `CHANGELOG.md`, commit, tag `vX.Y.Z`, push tags.

## Stub pattern

Every unimplemented method must follow this exact form:

```mojo
fn sort_values(self, by: String, ascending: Bool = True) raises -> Self:
    _not_implemented("DataFrame.sort_values")
    return self   # never reached; satisfies type checker
```

Rules:
- The string passed to `_not_implemented` must be `"TypeName.method_name"`.
- Never use a bare `raise Error("not yet implemented")` — the pre-commit hook will block it.

## Implementing a stub

1. Replace `_not_implemented(...)` with native Mojo code.
2. Remove the `return self` / `return PythonObject(None)` dummy if the real return is different.
3. Update the corresponding test in `tests/` — remove the "expect raise" assertion and add a real assertion comparing against pandas output.
4. Open a PR; CI will run tests automatically.

## Tests

Each `tests/test_*.mojo` file has a `main()` that calls every `test_*` function and prints `"<file>: all tests passed"` on success.

- Working paths (construction, `from_pandas`, `to_pandas`, `shape`, `columns`, etc.) assert real values.
- Stub paths assert that calling the method raises an error containing `"not implemented"`.

Run a single file: `mojo run tests/test_dataframe.mojo`

Test files by feature area:

| File | Area |
|------|------|
| `test_dataframe.mojo` | DataFrame construction, selection |
| `test_series_*.mojo` | Series construction, io, math, transforms |
| `test_aggregation.mojo` | sum, mean, std, var, min, max |
| `test_groupby.mojo` | groupby operations |
| `test_indexing.mojo` | .loc, .iloc, .at, .iat |
| `test_io.mojo` | CSV, JSON, Excel, Parquet |
| `test_reshaping.mojo` | concat, melt, pivot |
| `test_accessors.mojo` | .str and .dt accessors |
| `test_combining.mojo` | merge, join, append |
| `test_missing.mojo` | null / NaN handling |
| `test_functional.mojo` | map, apply, transform |
| `test_structural.mojo` | shape, columns, dtypes |
| `test_interop.mojo` | from_pandas / to_pandas |
| `test_index.mojo` | Index operations |
| `test_concat.mojo` | concat-specific cases |
| `test_transform.mojo` | transformation tests |
| `test_arrow.mojo` | Arrow ↔ Column round-trip conversion |
| `test_expr.mojo` | query/eval tokenizer, parser, evaluator |

Helper utilities live in `tests/_helpers.mojo`: `assert_frame_equal`, `assert_series_equal`, `make_simple_df`.

## Test caching

`scripts/run_tests.sh` rebuilds `bison.mojopkg` only when sources are newer than `.bison-cache/`. Tests run in parallel via background jobs; failures are collected and reported at the end.

## Benchmarks

`benchmarks/bench_core.mojo` measures bison vs pandas across aggregation, groupby, indexing, and I/O operations. `benchmarks/_bench_utils.mojo` provides `time_fn()` (wraps Python timeit) and `BenchResult` (outputs JSON with ratio = bison_ms / pandas_ms).

Iteration counts: `FAST_ITERS=100`, `MED_ITERS=20`, `SLOW_ITERS=3`, `IO_ITERS=5`. Results are stored in `results/<commit>.json`; the latest is symlinked as `results/latest.json`. `scripts/generate_report.py` merges history into `docs/data.json` (capped at 200 runs).

## CI / GitHub Actions

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `ci.yml` | push / PR | Matrix: `locked` (pixi.lock) + `latest` (newest MAX ≥ 26.1); lint, test, update compat table on main push |
| `nightly.yml` | daily + manual | Switches to nightly MAX channel; drops pyarrow; runs tests |
| `release.yml` | `v*.*.*` tag | Tests + creates GitHub Release |
| `benchmarks.yml` | push | Runs bench suite; updates dashboard |
| `renovate.yml` | schedule | Automated dependency bumps |

## Pre-commit hooks

Enforced by `.pre-commit-config.yaml`:
- Trailing whitespace / end-of-file fixer
- YAML, TOML, merge-conflict checks
- No bare `raise Error("not yet implemented")` — must use `_not_implemented()`
- `mojo format` auto-formatting
- `mojo package --Werror` — zero warnings policy

Run all hooks manually: `pixi run lint`

## GitHub issues

Every stub category has a corresponding issue on GitHub. When implementing a group of methods, reference the issue number in the PR description.

## Session notes

At the start of every session create `SESSION.md` at the project root if it does not already exist. While reading files and doing work, append an entry for every tech debt item, bug, or refactoring opportunity noticed. **Do not wait until the end — add entries immediately as they are found.** This includes incidental observations made while navigating code for unrelated reasons (e.g. spotting a raw `isa` chain while reading a file to implement something else).

### File structure

`SESSION.md` must contain exactly these four top-level sections, in this order. Entries go under the matching section; never mix them.

```markdown
# Session Notes

## Tech Debt

## Potential Refactorings

## Potential Design Patterns

## Code Simplifications
```

### Entry format

Each entry uses this template under its section:

```
### <Short title>

- **File**: `path/to/file.mojo` (line N if relevant)
- **Impact**: Low | Medium | High
- **Classification**: <name from refactoring.guru>
- **Details**: What the problem is and what the fix should be.
```

**Impact guidance:**

| Level | Meaning |
|-------|---------|
| High | Affects correctness, performance, or blocks future work |
| Medium | Causes friction or will compound if left alone |
| Low | Minor tidiness issue with little practical consequence |

### Classification vocabulary

Use names from the refactoring.guru catalogs:

- **Code smells**: Bloaters, OO-Abusers, Change Preventers, Dispensables, Couplers and the named smells within each group.
- **Refactoring techniques**: E.g. Extract Method, Replace Temp with Query, Introduce Null Object, Replace Conditional with Polymorphism.
- **Design patterns**: Creational, Structural, or Behavioral — use the exact pattern name (e.g. Strategy, Factory Method, Decorator).
- **Code simplifications**: Inline Variable, Remove Dead Code, Consolidate Duplicate Conditional Fragments, etc.

`SESSION.md` is for Claude's working notes only — it is gitignored and must never be committed.

## Mojo language gotchas

### `mut self` for mutating struct methods

Any `def` method that writes to a struct field must declare `mut self` explicitly.
Read-only methods omit the annotation entirely.

```mojo
struct Counter:
    var count: Int

    # Mutating — must have mut self
    def increment(mut self):
        self.count += 1

    # Read-only — no annotation needed
    def value(self) -> Int:
        return self.count
```

Without `mut self`, Mojo silently copies `self` instead of mutating it in-place, so
field updates are lost and callers see no change. This affects every stateful struct
(e.g. any struct that accumulates state across method calls).

### `ref` for non-copyable Variant arms

Accessing a `Variant` arm whose inner type does not implement `ImplicitlyCopyable`
(e.g. `List[T]`) must use a `ref` borrow, not a `var` assignment:

```mojo
# WRONG — compile error if List[T] is not ImplicitlyCopyable
var src = col._data[List[Int64]]

# CORRECT — zero-cost borrow tied to the Variant's lifetime
ref src = col._data[List[Int64]]
```

### `rebind[T]` for structurally identical but nominally different types

When two types are bit-for-bit identical but the type checker treats them as
distinct (e.g. a third-party library's `Scalar[dtype.native]` vs the stdlib's
`Int64`), use `rebind[T]` to assert the structural equivalence:

```mojo
data.append(rebind[Int64](src.unsafe_get(i)))  # Scalar[int64.native] → Int64
```

### `def main() raises:` in test files

Test-file `main()` functions must declare `raises` if they call any raising
function — omitting it is a **compile error**, not a warning.

### Nightly compiler hangs on standalone `query()`/`eval()` modules

The nightly Mojo compiler (`0.26.3.0+`) enters infinite recursion when compiling
a standalone module that calls `DataFrame.query()` or `DataFrame.eval()` (or
`eval_expr` directly). The same calls compile fine when co-located with
tokenizer/parser unit tests in `test_expr.mojo`.

**Workaround**: all query/eval conformance tests live in `tests/test_expr.mojo`,
not in a separate file. If you add new query/eval tests, add them there.

### Compile-time function types for `apply`, `applymap`, `pipe`

Mojo supports compile-time function types via `comptime`:

```mojo
comptime FloatTransformFn = def(Float64) -> Float64
```

These are used in `Column._apply[F]`, `Series.apply[F]`, `DataFrame.apply[F]`,
`DataFrame.applymap[F]`, and `DataFrame.pipe[F]`. The function must be known at
compile time — either a module-level `def` or an `@parameter` local function.

**Limitation**: `capturing [_]` is not yet supported in parameter type constraints.
`pipe[F]` requires `fn(DataFrame) raises -> DataFrame` (non-capturing). The
`capturing` syntax works in other contexts (`fn call_it[f: fn() capturing [_] -> None]()`)
but not when the captured function takes a struct argument in a parameter list.

### `fn` is deprecated on nightly — use `def` everywhere

Nightly Mojo deprecated the `fn` keyword (warning today, error soon). All function
and method definitions must use `def`. Do not introduce new `fn` declarations.

### Import aliases for stdlib names that shadow parameters

When importing a stdlib function whose name collides with a common parameter name
(`sort`, `min`, `max`, `sum`, `len`, `print`), alias the import to a leading-underscore
name and call the alias:

```mojo
from algorithm import sort as _sort_list

_sort_list(my_list)   # NOT sort(my_list) — would shadow the built-in
```

## Constraints

- `.CLAUDE` and `.claude/` are in `.gitignore` — never commit them.
- `SESSION.md` is gitignored — never commit it.
- `README.md` — plain prose, no emojis, no AI-sounding language.
- `bison/_version.mojo` — never edit by hand; generated by `scripts/gen_version.py`.
- Do not add `pixi.lock` to `.gitignore` — commit it for reproducibility.
- `docs/data.json` and `results/` are gitignored — generated at runtime.
- The zero-warnings policy is enforced by CI; `pixi run check` must pass before opening a PR.
