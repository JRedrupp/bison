# API reference

This document lists the public API surface of bison. Methods marked **native**
are implemented in Mojo. Methods marked **stub** raise
`"not implemented"` and will be ported in future releases.

## DataFrame

### Construction

| Method | Status |
|--------|--------|
| `DataFrame()` | native |
| `DataFrame.from_pandas(pd_df)` | native |
| `DataFrame.from_dict(data)` | native |
| `DataFrame.from_records(records, columns=None)` | native |

### Properties

| Method | Status |
|--------|--------|
| `shape()` | native |
| `size()` | native |
| `empty()` | native |
| `columns()` | native |
| `ndim()` | native |
| `dtypes()` | native |
| `info()` | native |
| `memory_usage(deep=False)` | native |

### Selection and indexing

| Method | Status |
|--------|--------|
| `df["col"]` | native |
| `df[mask]` | native |
| `df["col"] = series` | native |
| `get(key, default=null)` | native |
| `head(n=5)` | native |
| `tail(n=5)` | native |
| `iloc` | native |
| `loc` | native |
| `at` | native |
| `iat` | native |
| `sample(n, frac, replace, random_state)` | native |
| `select_dtypes(include, exclude)` | native |

### Aggregation

| Method | Status |
|--------|--------|
| `sum(axis=0)` | native |
| `mean(axis=0)` | native |
| `median(axis=0)` | native (axis=0 only) |
| `min(axis=0)` | native (axis=0 only) |
| `max(axis=0)` | native (axis=0 only) |
| `std(axis=0, ddof=1)` | native (axis=0 only) |
| `var(axis=0, ddof=1)` | native (axis=0 only) |
| `count(axis=0)` | native (axis=0 only) |
| `nunique(axis=0)` | native (axis=0 only) |
| `quantile(q=0.5, axis=0)` | native (axis=0 only) |
| `describe(percentiles)` | native |

### Statistical methods

| Method | Status |
|--------|--------|
| `sem(axis=0, ddof=1)` | native |
| `skew(axis=0)` | native |
| `kurt(axis=0)` | native |
| `idxmin(axis=0)` | native (axis=0 only) |
| `idxmax(axis=0)` | native (axis=0 only) |
| `corr(method="pearson")` | native (pearson only) |
| `cov(min_periods=1, ddof=1)` | native |

### Cumulative operations

| Method | Status |
|--------|--------|
| `cumsum(axis=0)` | native |
| `cumprod(axis=0)` | native |
| `cummin(axis=0)` | native |
| `cummax(axis=0)` | native |

### Shift and difference

| Method | Status |
|--------|--------|
| `shift(periods=1)` | native (axis=0 only) |
| `diff(periods=1)` | native (axis=0 only) |
| `pct_change(periods=1)` | native (axis=0 only) |

### Transformations

| Method | Status |
|--------|--------|
| `abs()` | native |
| `sqrt()` | native |
| `exp()` | native |
| `log()` | native |
| `log10()` | native |
| `ceil()` | native |
| `floor()` | native |
| `neg()` | native |
| `round(decimals=0)` | native |
| `clip(lower, upper)` | native |
| `astype(dtype)` | native |
| `where(cond)` | native |
| `mask(cond)` | native |
| `isin(values)` | native |

### Missing data

| Method | Status |
|--------|--------|
| `isna()` / `isnull()` | native |
| `notna()` / `notnull()` | native |
| `fillna(value)` | native |
| `dropna(axis=0, how="any")` | native |
| `ffill()` | native |
| `bfill()` | native |
| `interpolate(method="linear")` | native |

### Sorting

| Method | Status |
|--------|--------|
| `sort_values(by, ascending=True)` | native |
| `sort_index(ascending=True)` | native |

### Index operations

| Method | Status |
|--------|--------|
| `reset_index(drop=False)` | native |
| `set_index(keys)` | native |
| `rename(mapper, axis=0)` | native |
| `rename_axis(mapper, axis=0)` | native |
| `reindex(new_index)` | native |

### Dropping and duplicates

| Method | Status |
|--------|--------|
| `drop(labels, axis=0)` | native |
| `drop_duplicates(subset, keep="first")` | native |
| `duplicated(subset, keep="first")` | native |

### Reshaping

| Method | Status |
|--------|--------|
| `pivot(index, columns, values)` | native |
| `pivot_table(values, index, columns, aggfunc)` | native |
| `melt(id_vars, value_vars)` | native |
| `stack()` | native |
| `unstack()` | native |
| `explode(column)` | native |
| `transpose()` / `T()` | native |
| `swaplevel()` | native |

### Combining

| Method | Status |
|--------|--------|
| `merge(right, how, on, ...)` | native |
| `join(other, on, how, ...)` | native |
| `append(other, ignore_index)` | native |
| `combine_first(other)` | native |
| `update(other)` | native |

### Apply, map, and pipe

| Method | Status |
|--------|--------|
| `apply(func_name, axis=0)` | native |
| `apply[F](axis=0)` | native |
| `applymap(func_name)` | native |
| `applymap[F]()` | native |
| `pipe(func_name)` | native |
| `pipe[F]()` | native |
| `agg(func)` | stub |
| `aggregate(func)` | stub |
| `transform(func)` | stub |
| `eval(expr)` | native |
| `query(expr)` | native |

### Time series (stubs)

| Method | Status |
|--------|--------|
| `resample(rule)` | stub |
| `rolling(window)` | stub |
| `expanding(min_periods)` | stub |
| `ewm(com, span)` | stub |

### Output

| Method | Status |
|--------|--------|
| `to_csv(path)` | native |
| `to_json(path, orient)` | native |
| `to_parquet(path)` | native |
| `to_ipc(path)` | native |
| `to_excel(path)` | native |
| `to_dict()` | native |
| `to_records(index)` | native |
| `to_numpy()` | native |
| `to_string()` | native |
| `to_html()` | native |
| `to_markdown()` | native |
| `to_pandas()` | native |

### GroupBy

| Method | Status |
|--------|--------|
| `groupby(by)` | native |

---

## Series

### Construction

| Method | Status |
|--------|--------|
| `Series()` | native |
| `Series.from_pandas(pd_s)` | native |

### Properties

| Method | Status |
|--------|--------|
| `shape()` | native |
| `size()` | native |
| `empty()` | native |
| `dtype()` | native |

### Selection

| Method | Status |
|--------|--------|
| `head(n=5)` | native |
| `tail(n=5)` | native |
| `iloc(i)` | native |
| `at(label)` | native |

### Arithmetic operators

| Method | Status |
|--------|--------|
| `add`, `sub`, `mul`, `div`, `floordiv`, `mod`, `pow` | native |
| `radd`, `rsub`, `rmul`, `rdiv`, `rfloordiv`, `rmod`, `rpow` | native |
| `__add__`, `__sub__`, `__mul__`, `__truediv__`, etc. | native |

### Comparison operators

| Method | Status |
|--------|--------|
| `eq`, `ne`, `lt`, `le`, `gt`, `ge` | native |
| Scalar comparisons (Float64, Int64, String) | native |

### Logical operators

| Method | Status |
|--------|--------|
| `and_`, `or_`, `xor`, `invert` | native |
| `__and__`, `__or__`, `__xor__`, `__invert__` | native |

### Aggregation

| Method | Status |
|--------|--------|
| `sum()` | native |
| `mean()` | native |
| `median()` | native |
| `min()` / `max()` | native |
| `std(ddof=1)` / `var(ddof=1)` | native |
| `count()` | native |
| `nunique()` | native |
| `quantile(q=0.5)` | native |
| `describe()` | native |
| `value_counts(normalize, sort)` | native |

### Statistical methods

| Method | Status |
|--------|--------|
| `sem(ddof=1)` | native |
| `skew()` | native |
| `kurt()` | native |
| `idxmin()` / `idxmax()` | native |
| `corr(other)` | native |
| `cov(other, ddof=1)` | native |

### Cumulative operations

| Method | Status |
|--------|--------|
| `cumsum()` | native |
| `cumprod()` | native |
| `cummin()` / `cummax()` | native |

### Shift and difference

| Method | Status |
|--------|--------|
| `shift(periods=1)` | native |
| `diff(periods=1)` | native |
| `pct_change(periods=1)` | native |

### Missing data

| Method | Status |
|--------|--------|
| `isna()` / `isnull()` | native |
| `notna()` / `notnull()` | native |
| `fillna(value)` | native |
| `dropna()` | native |
| `ffill()` / `bfill()` | native |

### Sorting and ranking

| Method | Status |
|--------|--------|
| `sort_values(ascending=True)` | native |
| `sort_index(ascending=True)` | native |
| `argsort()` | native |
| `rank()` | native |

### Transformations

| Method | Status |
|--------|--------|
| `apply[F]()` | native |
| `map[F]()` | native |
| `astype(dtype)` | native |
| `abs()` | native |
| `sqrt()`, `exp()`, `log()`, `log10()` | native |
| `ceil()`, `floor()`, `neg()` | native |
| `round(decimals=0)` | native |
| `clip(lower, upper)` | native |

### Filtering

| Method | Status |
|--------|--------|
| `unique()` | native |
| `isin(values)` | native |
| `between(left, right)` | native |
| `where(cond)` / `mask(cond)` | native |

### Output

| Method | Status |
|--------|--------|
| `to_list()` | native |
| `to_numpy()` | native |
| `to_frame(name)` | native |
| `to_dict()` | native |
| `to_csv(path)` | native |
| `to_json(path)` | native |
| `to_pandas()` | native |

### Accessors

| Method | Status |
|--------|--------|
| `str()` -> `StringMethods` | native |
| `dt()` -> `DatetimeMethods` | native |

---

## DataFrameGroupBy

| Method | Status |
|--------|--------|
| `sum()` | native |
| `mean()` | native |
| `median()` | native |
| `min()` / `max()` | native |
| `std(ddof=1)` / `var(ddof=1)` | native |
| `count()` | native |
| `nunique()` | native |
| `first()` / `last()` | native |
| `size()` | native |
| `agg(func)` | native |
| `apply(func)` | native |
| `transform(func)` | native |
| `filter(func)` | native |

---

## SeriesGroupBy

| Method | Status |
|--------|--------|
| `sum()` | native |
| `mean()` | native |
| `min()` / `max()` | native |
| `std(ddof=1)` / `var(ddof=1)` | native |
| `count()` | native |
| `nunique()` | native |
| `first()` / `last()` | native |
| `size()` | native |
| `agg(func)` | native |
| `apply(func)` | native |
| `transform(func)` | native |

---

## StringMethods (`.str` accessor)

All methods are native.

| Method | Description |
|--------|-------------|
| `upper()` | Convert to uppercase |
| `lower()` | Convert to lowercase |
| `strip(to_strip="")` | Strip leading/trailing characters |
| `lstrip(to_strip="")` | Strip leading characters |
| `rstrip(to_strip="")` | Strip trailing characters |
| `contains(pat, regex=False)` | Test if pattern is contained |
| `startswith(pat)` | Test if starts with pattern |
| `endswith(pat)` | Test if ends with pattern |
| `replace(pat, repl, regex=False)` | Replace occurrences |
| `split(pat, n=-1, expand=False)` | Split strings |
| `len()` | String length |
| `find(sub, start=0, end=-1)` | Find substring position |
| `count(pat)` | Count occurrences |
| `get(i)` | Get character at position |
| `slice(start=0, stop, step=1)` | Slice strings |
| `cat(sep="")` | Concatenate strings |
| `match(pat)` | Regex match |

---

## DatetimeMethods (`.dt` accessor)

All methods are native.

| Method | Description |
|--------|-------------|
| `year()` | Extract year |
| `month()` | Extract month |
| `day()` | Extract day |
| `hour()` | Extract hour |
| `minute()` | Extract minute |
| `second()` | Extract second |
| `dayofweek()` | Day of week (0=Monday) |
| `dayofyear()` | Day of year |
| `quarter()` | Quarter (1-4) |
| `date()` | Date component |
| `time()` | Time component |
| `tz_localize(tz)` | Localize timezone |
| `tz_convert(tz)` | Convert timezone |
| `floor(freq)` | Floor to frequency |
| `ceil(freq)` | Ceil to frequency |
| `round(freq)` | Round to frequency |

---

## I/O functions

| Function | Status | Notes |
|----------|--------|-------|
| `read_csv(filepath, sep, header, ...)` | native | Pure Mojo; dtype inference: bool > int64 > float64 > String |
| `read_json(path_or_buf, orient, lines)` | native | Supports records, split, columns, index, values, NDJSON |
| `read_parquet(path, engine, columns)` | native | Via marrow; falls back to pandas for object columns |
| `read_excel(io, sheet_name, header, ...)` | native | Delegates to pandas (requires openpyxl or xlrd) |
| `read_ipc(path)` | native | Arrow IPC (Feather v2) via PyArrow |
| `write_ipc(df, path)` | native | Arrow IPC (Feather v2) via PyArrow |
| `concat(dfs, axis=0)` | native | Axis=0 with dtype promotion |

---

## Data types

### BisonDtype constants

| Constant | Description |
|----------|-------------|
| `int8`, `int16`, `int32`, `int64` | Signed integers |
| `uint8`, `uint16`, `uint32`, `uint64` | Unsigned integers |
| `float32`, `float64` | Floating point |
| `bool_` | Boolean |
| `string_` | String (backed by `List[String]`) |
| `object_` | Python object |
| `datetime64_ns` | Datetime (nanosecond precision) |
| `timedelta64_ns` | Timedelta (nanosecond precision) |

### Utility functions

| Function | Description |
|----------|-------------|
| `dtype_from_string(name)` | Convert a string like `"int64"` to a `BisonDtype` |

---

## Index types

### Index

| Method | Description |
|--------|-------------|
| `Index(data, name="")` | Construct from `List[String]` |
| `len()` | Number of entries |
| `tolist()` | Convert to `List[String]` |
| `get_loc(key)` | Find position of a label |
| `unique()` | Unique values |
| `sort_values(ascending=True)` | Sort the index |

### RangeIndex

| Method | Description |
|--------|-------------|
| `RangeIndex(start=0, stop=0, step=1)` | Construct a range index |
| `len()` | Number of entries |
