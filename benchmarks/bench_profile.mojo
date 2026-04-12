"""Profiling benchmark: runs bison operations in isolation for external profilers.

Designed to be compiled with debug symbols and run under perf, callgrind, or
samply so that function-level and line-level cost attribution is captured by
the profiling tool — no custom instrumentation needed.

Each operation runs enough iterations to accumulate ~1-2 seconds of runtime,
giving sampling profilers good coverage.

Environment variables:
    BISON_PROFILE_OP  Which operation to profile.  One of:
                      sort, groupby, merge, query, csv, all (default: all)

Usage (via pixi):
    pixi run profile              # perf, all operations (default)
    pixi run profile sort         # perf, just sort_values
    pixi run profile sort --samply       # samply for sort
    pixi run profile merge --callgrind   # callgrind for merge

Manual usage:
    mojo build -I .bison-cache -I . benchmarks/bench_profile.mojo \\
        -g --debug-info-language C -o /tmp/bison_profile
    BISON_PROFILE_OP=sort perf record -g --call-graph dwarf /tmp/bison_profile
"""

from benchmarks._bench_utils import BenchResult, print_json
from bison import DataFrame, read_csv
from std.os import getenv
from std.python import Python
from std.time import perf_counter_ns

# ---------------------------------------------------------------------------
# Iteration counts — tuned so each operation accumulates ~1-2 s total,
# giving profilers enough samples for stable attribution.
# ---------------------------------------------------------------------------

comptime SORT_ITERS = 30
comptime GROUPBY_ITERS = 50
comptime MERGE_ITERS = 30
comptime QUERY_ITERS = 100
comptime CSV_ITERS = 10

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _elapsed_ms(t0: UInt, iters: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / Float64(iters) / 1_000_000.0


# ---------------------------------------------------------------------------
# Profile functions — tight loops that keep the target operation hot.
# ---------------------------------------------------------------------------


def _profile_sort(df: DataFrame, iters: Int) raises:
    """Profile DataFrame.sort_values (single key, ascending)."""
    print("  sort_values ...", end="")
    var by = List[String]()
    by.append("a")
    var t0 = perf_counter_ns()
    for _ in range(iters):
        _ = df.sort_values(by)
    var ms = _elapsed_ms(t0, iters)
    print(" ", ms, "ms/call")


def _profile_groupby(df: DataFrame, iters: Int) raises:
    """Profile DataFrame.groupby().sum() (single key)."""
    print("  groupby_sum ...", end="")
    var t0 = perf_counter_ns()
    for _ in range(iters):
        var by = List[String]()
        by.append("key")
        _ = df.groupby(by).sum()
    var ms = _elapsed_ms(t0, iters)
    print(" ", ms, "ms/call")


def _profile_merge(df: DataFrame, df2: DataFrame, iters: Int) raises:
    """Profile DataFrame.merge (inner join on integer key)."""
    print("  merge ...", end="")
    var t0 = perf_counter_ns()
    for _ in range(iters):
        var on_keys = List[String]()
        on_keys.append("id")
        _ = df.merge(df2, on=on_keys^)
    var ms = _elapsed_ms(t0, iters)
    print(" ", ms, "ms/call")


def _profile_query(df: DataFrame, iters: Int) raises:
    """Profile DataFrame.query with a compound expression."""
    print("  query (a > 0.5 and b < 0.3) ...", end="")
    var t0 = perf_counter_ns()
    for _ in range(iters):
        _ = df.query("a > 0.5 and b < 0.3")
    var ms = _elapsed_ms(t0, iters)
    print(" ", ms, "ms/call")


def _profile_csv(df: DataFrame, iters: Int) raises:
    """Profile CSV round-trip (to_csv + read_csv)."""
    print("  csv_roundtrip ...", end="")
    var tmp = "/tmp/_bison_profile_csv.csv"
    var t0 = perf_counter_ns()
    for _ in range(iters):
        _ = df.to_csv(tmp)
        _ = read_csv(tmp)
    var ms = _elapsed_ms(t0, iters)
    print(" ", ms, "ms/call")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() raises:
    var op = getenv("BISON_PROFILE_OP", "")

    # When invoked by run_benchmarks.sh without BISON_PROFILE_OP, emit
    # skip-JSON so the runner doesn't choke on unexpected output.
    if len(op) == 0:
        var results = List[BenchResult]()
        results.append(BenchResult.skipped_result("profile_sort"))
        results.append(BenchResult.skipped_result("profile_groupby"))
        results.append(BenchResult.skipped_result("profile_merge"))
        results.append(BenchResult.skipped_result("profile_query"))
        results.append(BenchResult.skipped_result("profile_csv"))
        print_json(results)
        return

    # ------------------------------------------------------------------
    # Build fixtures (same schema as bench_core.mojo).
    # ------------------------------------------------------------------
    var _make_fixtures = Python.evaluate(
        "lambda n: ("
        "    lambda np, pd, keys: ("
        "        pd.DataFrame({"
        "            'key': np.random.default_rng(7).choice(keys, n),"
        "            'a':   np.random.default_rng(42).random(n),"
        "            'b':   np.random.default_rng(123).random(n),"
        "            'c':   np.random.default_rng(42).integers(0, 1000, n),"
        "            'id':  np.arange(n, dtype='int64'),"
        "        }),"
        "        pd.DataFrame({"
        "            'id':  np.arange(n // 10, dtype='int64'),"
        "            'val': np.random.default_rng(999).random(n // 10),"
        "        }),"
        "    )"
        ")("
        "    __import__('numpy'),"
        "    __import__('pandas'),"
        "    ['k0','k1','k2','k3','k4','k5','k6','k7','k8','k9'],"
        ")"
    )
    var _fixtures = _make_fixtures(100_000)
    var pd_df = _fixtures[0]
    var pd_df2 = _fixtures[1]

    var df = DataFrame.from_pandas(pd_df)
    var df2 = DataFrame.from_pandas(pd_df2)

    print("bison profiling benchmark")
    print("  operation:", op)
    print("  rows:      100,000")
    print("")

    if op == "sort" or op == "all":
        _profile_sort(df, SORT_ITERS)
    if op == "groupby" or op == "all":
        _profile_groupby(df, GROUPBY_ITERS)
    if op == "merge" or op == "all":
        _profile_merge(df, df2, MERGE_ITERS)
    if op == "query" or op == "all":
        _profile_query(df, QUERY_ITERS)
    if op == "csv" or op == "all":
        _profile_csv(df, CSV_ITERS)

    print("")
    print(
        "done — use perf report, callgrind_annotate, or samply to inspect the"
        " profile data"
    )
