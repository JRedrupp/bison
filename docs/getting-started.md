# Getting started

This guide walks through installing bison and using its core features.

## Installation

bison requires Mojo, which is distributed via the MAX conda channel.
[Pixi](https://pixi.sh) manages the environment.

```bash
curl -fsSL https://pixi.sh/install.sh | sh
git clone https://github.com/JRedrupp/bison.git
cd bison
pixi install
```

Supported platforms: `linux-64`, `osx-arm64`.

## Creating a DataFrame

### From a pandas DataFrame

```mojo
import bison as bs
from python import Python

def main() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    var df = bs.DataFrame.from_pandas(pd_df)
    print(df.shape())      # (3, 2)
    print(df.columns())    # ["a", "b"]
```

### From a CSV file

```mojo
import bison as bs

def main() raises:
    # Dtype is inferred automatically: bool > int64 > float64 > String
    var df = bs.read_csv("data.csv")
    print(df.shape())
    print(df.head())
```

### From JSON

```mojo
import bison as bs

def main() raises:
    var df = bs.read_json("data.json")
    print(df.shape())
```

### From Parquet

```mojo
import bison as bs

def main() raises:
    var df = bs.read_parquet("data.parquet")
    print(df.shape())
```

## Basic operations

### Column selection

```mojo
# Select a single column (returns a Series)
var col = df["price"]

# Boolean mask selection
var expensive = df[df["price"] > 100.0]
```

### Aggregation

```mojo
# Column-wise aggregation
var totals = df.sum()          # Series with sum of each column
var averages = df.mean()       # Series with mean of each column

# Series-level aggregation
var total = df["price"].sum()
var avg = df["price"].mean()
var med = df["price"].median()
```

### Sorting

```mojo
var sorted_df = df.sort_values("price", ascending=False)
```

### Filtering with query

```mojo
# Filter rows using a query expression
var result = df.query("price > 100 and category == 'electronics'")
```

The query engine supports column references, scalar literals (int, float, bool,
string, null), comparison operators (`<`, `<=`, `>`, `>=`, `==`, `!=`),
logical operators (`not`, `and`, `or`), and parenthetical grouping. See
[query-eval-spec.md](query-eval-spec.md) for the full grammar.

### GroupBy

```mojo
var grouped = df.groupby("category")
var category_totals = grouped.sum()
var category_means = grouped.mean()
var category_counts = grouped.count()
```

### Missing data

```mojo
# Detect nulls
var null_mask = df.isna()

# Fill nulls
var filled = df.fillna(0.0)

# Drop rows with nulls
var clean = df.dropna()

# Forward/backward fill
var ffilled = df.ffill()
var bfilled = df.bfill()
```

### String operations

```mojo
# Access string methods via .str accessor
var names = df["name"]
var upper_names = names.str().upper()
var contains_a = names.str().contains("a")
var lengths = names.str().len()
```

### Datetime operations

```mojo
# Access datetime methods via .dt accessor
var dates = df["timestamp"]
var years = dates.dt().year()
var months = dates.dt().month()
```

### Math transforms

```mojo
# Element-wise transforms
var abs_df = df.abs()
var rounded = df.round(2)
var log_values = df["price"].log()

# Compile-time function transforms
def double(v: Float64) -> Float64:
    return v * 2.0

var doubled = df.apply[double]()
```

### Combining DataFrames

```mojo
# Merge (join)
var merged = left.merge(right, on="key", how="inner")

# Concatenate
var combined = bs.concat(List[bs.DataFrame](df1, df2))

# Append
var appended = df1.append(df2, ignore_index=True)
```

## Output

### Writing files

```mojo
# CSV
var csv_str = df.to_csv("output.csv")

# JSON
var json_str = df.to_json("output.json")

# Parquet (native via marrow)
df.to_parquet("output.parquet")
```

### Converting back to pandas

```mojo
var pd_df = df.to_pandas()
```

### Display

```mojo
print(df)                    # tabular display
print(df.to_string())       # string representation
print(df.to_markdown())     # markdown table
print(df.to_html())         # HTML table
```

## Checking the version

```mojo
import bison as bs
print(bs.__version__)
```

## Next steps

- [API reference](api-reference.md) — full method listing with native/stub status
- [Architecture](architecture.md) — internals for contributors
- [Testing](testing.md) — how to run and write tests
- [Mojo patterns](mojo-patterns.md) — language-specific tips and pitfalls
