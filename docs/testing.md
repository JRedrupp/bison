# Testing

## Overview

Each `tests/test_*.mojo` file has a `main()` that calls every `test_*`
function and prints `"<file>: all tests passed"` on success.

- **Working paths** (construction, `from_pandas`, `to_pandas`, `shape`,
  `columns`, etc.) assert real values.
- **Stub paths** assert that calling the method raises an error containing
  `"not implemented"`.

## Running tests

Run the full suite:

```bash
pixi run test
```

Run a single file:

```bash
mojo run tests/test_dataframe.mojo
```

## Test files by feature area

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
| `test_arrow.mojo` | Arrow <-> Column round-trip conversion |
| `test_expr.mojo` | query/eval tokenizer, parser, evaluator |

## Helpers

Helper utilities live in `tests/_helpers.mojo`:

- `assert_frame_equal` — compare two DataFrames element-by-element.
- `assert_series_equal` — compare two Series element-by-element.
- `make_simple_df` — construct a small DataFrame for quick tests.

## Test caching

`scripts/run_tests.sh` rebuilds `bison.mojopkg` only when sources are newer
than `.bison-cache/`. Tests run in parallel via background jobs; failures are
collected and reported at the end.

## Writing a new test

1. Create a `def test_my_feature() raises:` function in the appropriate
   `test_*.mojo` file (or create a new file if the feature area is new).
2. Call the function from `main()`.
3. Use assertions to compare bison output against expected values. Where
   possible, also compare against pandas output via Python interop.
4. For stub methods, assert that calling the method raises with
   `"not implemented"` in the error message.
5. Run `pixi run test` to verify the full suite still passes.

**Note on query/eval tests**: due to a nightly compiler bug, all query/eval
tests must live in `tests/test_expr.mojo`. See
[Mojo patterns](mojo-patterns.md#nightly-compiler-hangs-on-standalone-queryeval-modules)
for details.
