# Migrating from pandas

This guide walks through porting a pandas script to Mojo with bison. The API
surface is nearly identical, so most scripts require only mechanical changes.

## The minimal diff

```python
# pandas (Python)
import pandas as pd

df = pd.read_csv("sales.csv")
totals = df.groupby("region")["revenue"].sum()
df_clean = df.dropna(subset=["revenue"])
high_value = df[df["revenue"] > 10000]
df["revenue_norm"] = (df["revenue"] - df["revenue"].mean()) / df["revenue"].std()
df.to_parquet("processed.parquet")
print(df.head())
```

```mojo
# bison (Mojo)
import bison as bs

def main() raises:
    var df = bs.read_csv("sales.csv")
    var totals = df.groupby("region")["revenue"].sum()
    var df_clean = df.dropna(subset=List[String]("revenue"))
    var high_value = df[df["revenue"] > 10000]
    df["revenue_norm"] = (df["revenue"] - df["revenue"].mean()) / df["revenue"].std()
    df.to_parquet("processed.parquet")
    print(df.head())
```

The differences are:

| pandas / Python | bison / Mojo |
|-----------------|--------------|
| `import pandas as pd` | `import bison as bs` |
| `pd.` | `bs.` |
| Top-level script | `def main() raises:` entry point |
| `x = ...` | `var x = ...` |
| `["col1", "col2"]` (Python list) | `List[String]("col1", "col2")` |

## Installation

```bash
curl -fsSL https://pixi.sh/install.sh | sh
git clone https://github.com/JRedrupp/bison.git
cd bison
pixi install
```

## Common patterns

### Reading data

```mojo
# CSV — dtype inferred automatically (bool > int64 > float64 > String)
var df = bs.read_csv("data.csv")
var df = bs.read_csv("data.csv", delimiter="\t", nrows=1000)

# JSON
var df = bs.read_json("data.json")

# Parquet (native Arrow I/O)
var df = bs.read_parquet("data.parquet")

# From an existing pandas DataFrame
from python import Python
var pd = Python.import_module("pandas")
var df = bs.DataFrame.from_pandas(pd.read_csv("data.csv"))
```

### Column selection and filtering

```mojo
# Single column
var col = df["price"]

# Boolean mask
var expensive = df[df["price"] > 100.0]

# Query string (subset of pandas grammar)
var result = df.query("price > 100 and category == 'electronics'")

# iloc / loc
var row = df.iloc(0)
var subset = df.loc(bs.slice(10, 20))
```

### Aggregation

```mojo
var total = df["revenue"].sum()
var avg = df["revenue"].mean()
var med = df["revenue"].median()
var col_sums = df.sum()           # Series — one value per column
var col_means = df.mean()
```

### GroupBy

```mojo
var g = df.groupby("region")
var totals = g["revenue"].sum()
var counts = g.count()
var means = g.mean()
```

### Sorting and deduplication

```mojo
var sorted_df = df.sort_values("revenue", ascending=False)
var deduped = df.drop_duplicates()
var deduped_by = df.drop_duplicates(subset=List[String]("region", "category"))
```

### Missing data

```mojo
var has_nulls = df.isna()
var filled = df.fillna(0.0)
var dropped = df.dropna()
var ffilled = df.ffill()
```

### String operations

```mojo
var upper = df["name"].str().upper()
var contains = df["name"].str().contains("acme")
var lengths = df["name"].str().len()
```

### Writing output

```mojo
df.to_parquet("out.parquet")
var csv = df.to_csv()             # returns String
var json = df.to_json()           # returns String
var pd_df = df.to_pandas()        # back to pandas
```

## Key differences from pandas

### Mojo requires explicit types in some places

Python lists are untyped; Mojo is typed. The most common case is methods that
take a list of column names:

```python
# pandas
df.dropna(subset=["a", "b"])
df[["a", "b"]]
```

```mojo
# bison
df.dropna(subset=List[String]("a", "b"))
df[List[String]("a", "b")]
```

### `def main() raises:` is required

Mojo programs must have an explicit entry point. Any function that can raise
an error (which includes nearly all bison operations) must be declared
`raises`.

### `apply` and `applymap` use compile-time functions

Runtime closures that capture variables are not supported as type parameters in
the current Mojo release. Use compile-time functions instead:

```python
# pandas
df["price"].apply(lambda x: x * 1.1)
```

```mojo
# bison
def markup(v: Float64) -> Float64:
    return v * 1.1

df["price"].apply[markup]()
```

For threshold operations, use `clip()` or `where()` instead.

### String-based dispatch for common transforms

`applymap`, `transform`, and `pipe` accept a string name for built-in
operations, avoiding the need for a compile-time function:

```mojo
var abs_df = df.applymap("abs")
var log_df = df.applymap("log")
var cumsum = df["revenue"].transform("cumsum")
```

Supported names: `abs`, `round`, `sqrt`, `exp`, `log`, `log10`, `ceil`,
`floor`, `neg`. `transform` additionally supports `cumsum`, `cumprod`,
`cummin`, `cummax`.

## Handling unimplemented methods

Not all pandas methods are natively implemented yet. When you call a stub,
bison raises with a clear message:

```
bison.DataFrame.pivot: not implemented
```

Your options when you hit a stub:

1. **Work around it**: many operations can be composed from implemented
   methods (e.g. `merge` + `groupby` instead of `pivot`).
2. **Fall through to pandas**: convert to pandas for the unimplemented step,
   then wrap the result back with `from_pandas`:

   ```mojo
   var pd_df = df.to_pandas()
   var pd_result = pd.DataFrame.pivot(pd_df, ...)
   var result = bs.DataFrame.from_pandas(pd_result)
   ```

3. **Implement it**: stubs are straightforward to fill in. See the
   [contributing guide](../CONTRIBUTING.md).

## What is implemented

See [api-reference.md](api-reference.md) for the full method listing with
native/stub status for every DataFrame, Series, GroupBy, and accessor method.
