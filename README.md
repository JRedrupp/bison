# bison
[![Nightly](https://github.com/JRedrupp/bison/actions/workflows/nightly.yml/badge.svg?branch=main)](https://github.com/JRedrupp/bison/actions/workflows/nightly.yml)

A Mojo DataFrame library with a pandas-compatible API.

bison aims to be a drop-in replacement for pandas. The goal is that swapping
`import pandas as pd` for `import bison as bs` requires minimal changes to
calling code. The library provides the full pandas DataFrame and Series API;
methods that are not yet implemented natively raise an error until they are
ported to Mojo.

## Status

Most of the pandas DataFrame and Series API is implemented natively in Mojo
across DataFrame, Series, GroupBy, string and datetime accessors, native CSV
and JSON I/O, and reshape. A small number of DataFrame methods remain as stubs
and raise:

```
bison.<method>: not implemented
```

`from_pandas()` and `to_pandas()` are available for wrapping and unwrapping
pandas objects.

Native I/O highlights:

- `read_csv` — pure Mojo reader with automatic dtype inference (`bool` >
  `int64` > `float64` > `String`), configurable delimiter, `usecols`,
  `nrows`, `skiprows`, and NA-value handling.
- `read_json` — pure Mojo reader supporting `records`, `split`, `columns`,
  `index`, `values` orient formats, and JSON Lines / NDJSON (`lines=True`).
- `read_parquet` / `to_parquet` — native Parquet I/O via marrow (Apache Arrow
  for Mojo) for int64, float64, bool, and string columns. Falls back to pandas
  for object columns or when row-group filters are supplied.
- `read_ipc` / `write_ipc` — Arrow IPC (Feather v2) via PyArrow interop.
- `read_excel` — delegates to pandas (requires `openpyxl` or `xlrd`).

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
from std.python import Python

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

## Known limitations

### `apply`, `applymap`, and `pipe` have two calling conventions

These methods each provide two overloads:

**String-based** dispatches known function names to native methods. Unknown
names raise an error listing the supported set:

```mojo
var totals = df.apply("sum", axis=0)       # -> Series (delegates to agg)
var row_sums = df.apply("sum", axis=1)     # -> Series (row-wise)
var abs_df = df.applymap("abs")            # -> DataFrame (element-wise)
var log_df = df.applymap("log")            # -> DataFrame (element-wise)
var piped = df.pipe("abs")                 # -> DataFrame
```

Supported element-wise string names for `applymap`, `transform`, and `pipe`:
`abs`, `round`, `sqrt`, `exp`, `log`, `log10`, `ceil`, `floor`, `neg`.
`transform` additionally supports `cumsum`, `cumprod`, `cummin`, `cummax`.

**Compile-time** accepts a user-defined function as a type parameter. This is
fully native Mojo with no string dispatch:

```mojo
def double(v: Float64) -> Float64:
    return v * 2.0

var result = df.apply[double]()            # element-wise on numeric columns
var mapped = df.applymap[double]()         # same as apply[double]()

def add_rank(d: DataFrame) raises -> DataFrame:
    # whole-DataFrame transform
    return d.abs()

var piped = df.pipe[add_rank]()
```

`Series.apply` and `Series.map` also accept compile-time functions:

```mojo
var result = s.apply[double]()
```

Runtime closures that capture variables are not yet supported in parameter
type constraints. Use `clip()` or `where()` for threshold-style operations
in the meantime.

### Native query/eval subset

`DataFrame.query()` and `DataFrame.eval()` are implemented natively in Mojo
using the `bison.expr` parser and evaluator. The first milestone covers a
well-defined subset of the pandas query/eval grammar:

**Supported in this release:**

- Column references by bare identifier name (`a`, `column_name`).
- Scalar literals: integer, float, boolean (`True`/`False`), string, and
  null (`None`).
- Comparison operators: `<`, `<=`, `>`, `>=`, `==`, `!=`.
- Logical operators: `not`, `and`, `or` (with Kleene three-valued null
  semantics).
- Parenthetical grouping for explicit precedence.
- Column-vs-column and column-vs-scalar comparisons in either order.

**Explicitly out of scope (raise with `"unsupported syntax"`):**

- Arithmetic expressions: `a + b`, `a * 2`, `-a`.
- Function calls: `abs(a)`, `len(a)`.
- Attribute access: `a.str.len`.
- Indexing and slicing: `a[0]`, `a[1:3]`.
- Membership and identity operators: `in`, `not in`, `is`, `is not`.
- Comparison chaining: `a < b < c`.
- Assignment expressions: `a = b`, `a := b`.

**Error messages:**

| Situation | Message contains |
|-----------|-----------------|
| Unsupported grammar | `unsupported syntax` |
| Malformed expression | `invalid expression` |
| Unknown column name | `unknown column` |
| Type mismatch | `type error` |

The full grammar, precedence table, and null-semantics truth tables are
documented in [`docs/query-eval-spec.md`](docs/query-eval-spec.md).

## Contributing

1. Pick a stub method (any `_not_implemented` call in `bison/`).
2. Replace the `_not_implemented` call with a native Mojo implementation.
3. Update the corresponding test: remove the "expect raise" assertion and add
   real assertions comparing against pandas output.
4. Submit a pull request.

## License

Apache 2.0. See [LICENSE](LICENSE).
