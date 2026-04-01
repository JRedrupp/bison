# bison
[![Nightly](https://github.com/JRedrupp/bison/actions/workflows/nightly.yml/badge.svg?branch=main)](https://github.com/JRedrupp/bison/actions/workflows/nightly.yml)

A Mojo DataFrame library with a pandas-compatible API.

bison aims to be a drop-in replacement for pandas. The goal is that swapping
`import pandas as pd` for `import bison as bs` requires minimal changes to
calling code. The library provides the full pandas DataFrame and Series API;
methods that are not yet implemented natively raise an error until they are
ported to Mojo.

## Status

Core aggregation, statistics, and interop methods are implemented natively in
Mojo. Methods that are not yet ported raise:

```
bison.<method>: not implemented
```

`from_pandas()` and `to_pandas()` are available for wrapping and unwrapping
pandas objects. See the compatibility table below for the current counts.

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

## Version

```mojo
import bison as bs
print(bs.__version__)
```

## Running tests

```bash
pixi run test
```

## pandas compatibility

This table is generated automatically from the source by `scripts/update_compat.py`,
which counts `_not_implemented()` calls in the bison package. Run it locally with:

```bash
python scripts/update_compat.py
```

In CI it runs after the test suite and the result is committed back to the branch.

<!-- COMPAT_TABLE_START -->
| Category | Stubs | Implemented |
|----------|-------|-------------|
| DataFrame | 38 | 102 |
| Series | 0 | 98 |
| GroupBy (DataFrame) | 0 | 24 |
| GroupBy (Series) | 0 | 17 |
| String accessor | 0 | 21 |
| Datetime accessor | 0 | 20 |
| Index | 0 | 14 |
| IO | 0 | 8 |
| Reshape | 1 | 1 |
| **Total** | **39** | **305** |
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

## Contributing

1. Pick a stub method from the table above.
2. Replace the `_not_implemented` call with a native Mojo implementation.
3. Update the corresponding test: remove the "expect raise" assertion and add
   real assertions comparing against pandas output.
4. Submit a pull request.

## License

Apache 2.0. See [LICENSE](LICENSE).
