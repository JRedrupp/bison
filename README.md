# bison
[![Nightly](https://github.com/JRedrupp/bison/actions/workflows/nightly.yml/badge.svg?branch=main)](https://github.com/JRedrupp/bison/actions/workflows/nightly.yml)

A Mojo DataFrame library with a pandas-compatible API.

bison aims to be a drop-in replacement for pandas. The goal is that swapping
`import pandas as pd` for `import bison as bs` requires minimal changes to
calling code. The library provides the full pandas DataFrame and Series API;
methods that are not yet implemented natively raise an error until they are
ported to Mojo.

## Why bison?

If you are writing a Mojo program that processes tabular data, the alternative
to bison is wrapping pandas at the Python boundary — every DataFrame call then
crosses the Python/Mojo language boundary and carries Python object overhead.
bison runs natively in Mojo: data lives in Apache Arrow-backed columns,
aggregations use SIMD kernels from [marrow](https://github.com/kszucs/marrow),
and there is no Python interop in the hot path.

The pandas-compatible API means the transition cost is low. For most scripts
replacing `import pandas as pd` with `import bison as bs` is the bulk of the
change. See [Migrating from pandas](#migrating-from-pandas) for the full
walkthrough. Methods not yet implemented natively raise immediately with a
clear message so you know exactly what to work around.

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

## Performance

bison is faster than pandas for element-wise operations. Complex operations —
groupby, sort, merge — are slower at this stage and are being actively
optimized.

Ratio = bison time / pandas time. Values below 1.0 mean bison is faster.

| Operation | bison | pandas | Ratio |
|-----------|------:|-------:|------:|
| iloc row | 0.002 ms | 0.041 ms | 0.04x |
| series_mean | 0.047 ms | 0.134 ms | 0.35x |
| loc slice | 0.014 ms | 0.031 ms | 0.45x |
| fillna | 0.037 ms | 0.069 ms | 0.53x |
| series_sum | 0.055 ms | 0.096 ms | 0.57x |
| csv roundtrip | 506 ms | 343 ms | 1.48x |
| series_apply | 0.817 ms | 0.221 ms | 3.70x |
| sort_values | 24.4 ms | 5.68 ms | 4.29x |
| merge | 13.1 ms | 2.0 ms | 6.56x |
| groupby_sum | 64.5 ms | 4.4 ms | 14.75x |

Benchmarks run at 100k rows (aggregation, groupby, sort) and 10k rows
(apply).
Full history and per-commit charts at
[jredrupp.github.io/bison](https://jredrupp.github.io/bison/).

```bash
pixi run bench        # run the full suite
pixi run gen-report   # merge results into docs/data.json
```

## Migrating from pandas

For most scripts the required changes are minimal:

1. Replace `import pandas as pd` with `import bison as bs`.
2. Wrap your entry point in `def main() raises:`.
3. Declare variables with `var`.

```python
# Before — Python + pandas
import pandas as pd

df = pd.read_csv("sales.csv")
totals = df.groupby("region")["revenue"].sum()
df_clean = df.dropna(subset=["revenue"])
df.to_parquet("out.parquet")
```

```mojo
# After — Mojo + bison
import bison as bs

def main() raises:
    var df = bs.read_csv("sales.csv")
    var totals = df.groupby("region")["revenue"].sum()
    var df_clean = df.dropna(subset=List[String]("revenue"))
    df.to_parquet("out.parquet")
```

If a method you rely on is not yet implemented natively, bison raises
immediately with a clear message (`bison.<method>: not implemented`).

See [docs/migrating-from-pandas.md](docs/migrating-from-pandas.md) for a
full walkthrough covering common patterns, known differences, and how to
work around unimplemented methods.

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

## Documentation

- [Getting started](docs/getting-started.md) — installation, first DataFrame, core operations
- [Migrating from pandas](docs/migrating-from-pandas.md) — step-by-step guide for porting pandas scripts
- [API reference](docs/api-reference.md) — full method listing with native/stub status
- [Architecture](docs/architecture.md) — column storage, type predicates, marrow integration
- [Mojo patterns](docs/mojo-patterns.md) — language-specific tips and pitfalls
- [Testing](docs/testing.md) — how to run and write tests
- [CI/CD](docs/ci.md) — GitHub Actions, pre-commit hooks, benchmarks, releasing
- [Profiling](docs/profiling.md) — perf, samply, and callgrind guides
- [Query/eval spec](docs/query-eval-spec.md) — grammar and null semantics

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide.

1. Pick a stub method (any `_not_implemented` call in `bison/`).
2. Replace the `_not_implemented` call with a native Mojo implementation.
3. Update the corresponding test: remove the "expect raise" assertion and add
   real assertions comparing against pandas output.
4. Submit a pull request.

## License

Apache 2.0. See [LICENSE](LICENSE).
