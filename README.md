# bison
[![Nightly](https://github.com/JRedrupp/bison/actions/workflows/nightly.yml/badge.svg?branch=main)](https://github.com/JRedrupp/bison/actions/workflows/nightly.yml)

A Mojo DataFrame library with a pandas-compatible API.

bison aims to be a drop-in replacement for pandas. The goal is that swapping
`import pandas as pd` for `import bison as bs` requires minimal changes to
calling code. The library provides the full pandas DataFrame and Series API;
methods that are not yet implemented natively raise an error until they are
ported to Mojo.

## Status

361 of 366 tracked API methods are implemented natively in Mojo across
DataFrame, Series, GroupBy, string and datetime accessors, native CSV and JSON
I/O, and reshape. The five remaining DataFrame stubs raise:

```
bison.<method>: not implemented
```

`from_pandas()` and `to_pandas()` are available for wrapping and unwrapping
pandas objects. See the compatibility table below for the current counts.

Native I/O highlights:

- `read_csv` — pure Mojo reader with automatic dtype inference (`bool` >
  `int64` > `float64` > `String`), configurable delimiter, `usecols`,
  `nrows`, `skiprows`, and NA-value handling.
- `read_json` — pure Mojo reader supporting `records`, `split`, `columns`,
  `index`, `values` orient formats, and JSON Lines / NDJSON (`lines=True`).
- `read_parquet` and `read_excel` delegate to pandas (stubs).

## Install

Mojo is distributed via the MAX conda channel. [Pixi](https://pixi.sh) manages
the environment.

```bash
curl -fsSL https://pixi.sh/install.sh | sh
git clone https://github.com/JRedrupp/bison.git
cd bison
pixi install
```

## Quickstart

```mojo
import bison as bs
from python import Python

def main() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # Wrap a pandas DataFrame
    var df = bs.DataFrame.from_pandas(pd_df)
    print(df.shape())      # (3, 2)
    print(df.columns())    # ["a", "b"]

    # Sum each column natively
    var totals = df.sum()  # Series: a=6.0, b=15.0

    # Get the backing pandas object back
    var original = df.to_pandas()
```

Read a CSV file directly without pandas:

```mojo
import bison as bs

def main() raises:
    # Dtype is inferred automatically: bool > int64 > float64 > String
    var df = bs.read_csv("data.csv")
    print(df.shape())
    print(df["price"].mean())
```

## Version

```mojo
import bison as bs
print(bs.__version__)
```

## Running tests

```bash
pixi run test
```

## Benchmarks

The benchmark suite in `benchmarks/bench_core.mojo` compares bison against
pandas across aggregation, groupby, indexing, and I/O operations. Each entry
reports a ratio of bison time to pandas time; values below 1.0 mean bison is
faster.

```bash
pixi run bench        # run the full suite
pixi run gen-report   # merge results into docs/data.json
```

The `docs/` directory contains an HTML performance dashboard that plots ratio
history across commits.

## pandas compatibility

This table is generated automatically from the source by `scripts/update_compat.py`,
which counts fully stubbed APIs in the bison package. Run it locally with:

```bash
python scripts/update_compat.py
```

In CI it runs after the test suite and the result is committed back to the branch.

<!-- COMPAT_TABLE_START -->
| Category | Stubs | Implemented |
|----------|-------|-------------|
| DataFrame | 5 | 138 |
| Series | 0 | 123 |
| GroupBy (DataFrame) | 0 | 24 |
| GroupBy (Series) | 0 | 17 |
| String accessor | 0 | 21 |
| Datetime accessor | 0 | 20 |
| Index | 0 | 14 |
| IO | 0 | 8 |
| Reshape | 0 | 2 |
| **Total** | **5** | **367** |
<!-- COMPAT_TABLE_END -->

## Known limitations

### `apply` and `map` require compile-time functions

`Series.apply` and `Series.map` accept a function parameter at compile time
only. Runtime closures that capture variables are not supported:

```mojo
# Works — function is known at compile time
fn double(v: Float64) -> Float64: return v * 2.0
var result = s.apply[double]()

# Does not work — threshold is a runtime value
var threshold = 1.5
fn clip_fn(v: Float64) -> Float64: return v if v > threshold else 0.0
var result = s.apply[clip_fn]()  # compile error
```

This is a current limitation of Mojo's parametric function support
(tracked in [modularml/mojo#6130](https://github.com/modularml/mojo/issues/6130)).
Use `clip()` or `where()` for threshold-style operations in the meantime.

### Query/eval native grammar specification

`DataFrame.query()` and `DataFrame.eval()` currently delegate to pandas.
A native Mojo parser for the query/eval grammar is available as the public
`bison.expr` module (`parse`, `ParsedExpr`, `ASTNode`, `Tokenizer`). Full
native execution backed by that parser is tracked as the next milestone.

The grammar and semantics are documented in
[`docs/query-eval-spec.md`](docs/query-eval-spec.md). Use this as the
canonical reference for supported syntax, precedence, null semantics, and
unsupported expression behavior.

## Contributing

1. Pick a stub method from the table above.
2. Replace the `_not_implemented` call with a native Mojo implementation.
3. Update the corresponding test: remove the "expect raise" assertion and add
   real assertions comparing against pandas output.
4. Submit a pull request.

## License

Apache 2.0. See [LICENSE](LICENSE).
