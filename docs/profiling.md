# Profiling bison

This guide explains how to profile bison operations to identify where runtime
is spent during optimization sessions.

## Overview

Bison uses external profiling tools (samply, callgrind) rather than custom
instrumentation. Mojo compiles to native code, so standard Linux profiling
tools work when the binary is compiled with debug symbols. This gives
function-level and line-level cost attribution automatically.

## Prerequisites

Install samply (the default profiling tool):

```bash
cargo install samply
```

Ensure perf events are accessible:

```bash
# Check current level (needs to be <= 1)
cat /proc/sys/kernel/perf_event_paranoid

# If it shows 2 or higher, lower it:
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

## Quick start

```bash
# Profile all benchmark operations (samply, default)
pixi run profile

# Profile a single operation
pixi run profile sort

# Profile with callgrind instead
pixi run profile merge --callgrind
```

## Available operations

| Name | Operation | Description |
|------|-----------|-------------|
| `sort` | `DataFrame.sort_values` | Single-key ascending sort on 100K rows |
| `groupby` | `DataFrame.groupby().sum()` | Single-key groupby + sum aggregation |
| `merge` | `DataFrame.merge` | Inner join on integer key (100K x 10K rows) |
| `query` | `DataFrame.query` | Compound boolean expression filter |
| `csv` | `to_csv` + `read_csv` | CSV round-trip on 100K rows |
| `all` | All of the above | Runs sequentially (default) |

## Profiling tools

### samply (default)

[samply](https://github.com/mstange/samply) is a sampling profiler that
produces interactive flamegraphs viewable in
[Firefox Profiler](https://profiler.firefox.com). It runs at near-native
speed and captures real wall-clock timing.

```bash
pixi run profile sort
```

This produces `profile_results/sort.samply.json`. View the flamegraph:

```bash
samply load profile_results/sort.samply.json
```

This opens Firefox Profiler in your browser with an interactive flamegraph.
You can zoom into call stacks, filter by function name, and see
time-weighted call trees.

### callgrind (alternative)

[Callgrind](https://valgrind.org/docs/manual/cl-manual.html) is a
call-graph profiler built on valgrind. It counts instruction execution at
the function and line level, giving deterministic cost attribution that
does not vary between runs.

```bash
pixi run profile sort --callgrind
```

This produces `profile_results/callgrind.out.sort` and prints a summary.

To explore the results:

```bash
# Text-based exploration
callgrind_annotate --auto=yes profile_results/callgrind.out.sort | less

# GUI viewer (if kcachegrind is installed)
kcachegrind profile_results/callgrind.out.sort
```

**Callgrind output explained:**

- **Ir** (instruction reads) is the primary cost metric. Higher Ir = more time.
- Functions are listed in descending Ir order. Look for bison functions
  like `sort_perm`, `take`, `_row_key_str`, `_groupby_indices`.
- Line-level annotation shows which specific lines within a function are
  most expensive.

**Caveats:** Callgrind runs code under a CPU emulator (~20-50x slower than
native). It may crash on Mojo binaries that use AVX-512 instructions
(valgrind 3.22 does not support all EVEX-encoded instructions). If you
encounter `SIGILL` errors, use samply instead.

### When to use which tool

| | samply | callgrind |
|---|--------|-----------|
| **Speed** | Near-native | ~20-50x slower |
| **Accuracy** | Statistical (sampling) | Deterministic (instruction count) |
| **Output** | Interactive flamegraph | Text + kcachegrind |
| **Best for** | High-level overview, call trees | Line-level hotspot analysis |
| **AVX-512** | Works | May crash |
| **Install** | `cargo install samply` | `apt install valgrind` |

## How it works

The `pixi run profile` command:

1. Packages `bison/` into `bison.mojopkg` (cached)
2. Compiles `benchmarks/bench_profile.mojo` with debug symbols:
   ```bash
   mojo build -g --debug-info-language C -o /tmp/bison_profile ...
   ```
3. Runs the compiled binary under the selected profiler
4. Saves results to `profile_results/`

The compiler flags:
- `-g` adds full debug info (function names + line numbers in profile output)
- `--debug-info-language C` makes symbols readable by standard Linux tools
  (samply, callgrind, perf) that don't understand Mojo debug format natively

## Manual profiling

For more control, you can compile and profile manually:

```bash
# 1. Build the bison package
pixi run build-marrow
pixi run gen-version
mojo package bison/ -o .bison-cache/bison.mojopkg

# 2. Compile with debug symbols
mojo build -I .bison-cache -I . benchmarks/bench_profile.mojo \
    -g --debug-info-language C \
    -o /tmp/bison_profile

# 3a. Profile with samply
BISON_PROFILE_OP=sort samply record /tmp/bison_profile

# 3b. Or profile with perf (if installed)
BISON_PROFILE_OP=sort perf record -g /tmp/bison_profile
perf report

# 3c. Or profile with callgrind
BISON_PROFILE_OP=sort valgrind --tool=callgrind \
    --callgrind-out-file=my_profile.out /tmp/bison_profile
callgrind_annotate --auto=yes my_profile.out
```

## Interpreting results

### Reading a samply flamegraph

In Firefox Profiler:

- **Flame chart** (default view): Each row is a stack frame. Width = time
  spent. Wider bars = more time. Click to zoom in.
- **Call tree** tab: Shows hierarchical breakdown of where time is spent.
  Sort by "Self" to find the functions that do the most actual work (vs
  just calling other functions).
- **Search**: Type a function name (e.g. `sort_perm`) to highlight it
  across the flame chart.

### Common hot functions

| Function | Called by | What it does |
|----------|-----------|--------------|
| `sort_perm` | `sort_values` | Merge-sort to produce permutation array |
| `take` | `sort_values`, `groupby` | Reorder column data by index array |
| `_row_key_str` | `merge`, `groupby` | Serialize row values to string key |
| `_groupby_indices` | `groupby` | Build key-to-row-index mapping |
| `_eval_expr` | `query` | Evaluate parsed expression against DataFrame |
| `take_with_nulls` | `merge` | Reorder with null insertion for outer joins |
| `_merge_sort_perm_comparable` | `sort_perm` | The actual sort kernel |
| `_try_activate_storage` | `Column` construction | Sync marrow backend |

### Example analysis workflow

1. Run `pixi run profile sort` and open the flamegraph
2. Look at the call tree: is most time in `sort_perm` (sorting) or `take`
   (copying)?
3. If `sort_perm` dominates, the sort algorithm itself is the bottleneck
4. If `take` dominates, column copying is the bottleneck — consider
   in-place permutation or batch `take`
5. Drill into `sort_perm` to see if the merge-sort kernel
   (`_merge_sort_perm_comparable`) is the hotspot, or if it's the
   permutation composition loop

## Adding new profiling targets

To profile a new operation, add a function to `benchmarks/bench_profile.mojo`:

```mojo
def _profile_my_op(df: DataFrame, iters: Int) raises:
    """Profile description."""
    print("  my_op ...", end="")
    var t0 = perf_counter_ns()
    for _ in range(iters):
        _ = df.my_op(...)
    var ms = _elapsed_ms(t0, iters)
    print(" ", ms, "ms/call")
```

Then add the dispatch in `main()`:

```mojo
if op == "my_op" or op == "all":
    _profile_my_op(df, MY_OP_ITERS)
```

Choose an iteration count that gives ~1-2 seconds total runtime for good
profiler coverage.

## Fixture details

The profiling benchmark uses the same 100K-row fixture as `bench_core.mojo`:

| Column | Type | Description |
|--------|------|-------------|
| `key` | string | 10 unique values (`k0`...`k9`) |
| `a` | float64 | Random uniform [0, 1) |
| `b` | float64 | Random uniform [0, 1) |
| `c` | int64 | Random integers [0, 1000) |
| `id` | int64 | Unique sequential (0 to N-1) |

A secondary 10K-row fixture is used for merge (right side).
