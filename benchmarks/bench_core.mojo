"""Core operation benchmarks: bison vs pandas.

Generates a JSON report to stdout comparing bison operations against pandas
for a curated set of DataFrame operations.  Each benchmark is attempted; if
bison raises ``"not implemented"`` the entry is emitted with ``"skipped": true``
so the dashboard can surface coverage gaps over time.

Usage::

    mojo run -I /path/to/repo/root benchmarks/bench_core.mojo

(The runner script ``scripts/run_benchmarks.sh`` handles the include path.)
"""

from benchmarks._bench_utils import BenchResult, print_json
from bison import DataFrame, DFScalar, read_csv
from std.math import sqrt
from std.python import Python, PythonObject
from std.time import perf_counter_ns

# ---------------------------------------------------------------------------
# Per-operation iteration counts.  Slow operations use fewer iterations so
# the total benchmark runtime stays under 2 minutes on a typical laptop.
# ---------------------------------------------------------------------------

comptime FAST_ITERS = 100  # <1 ms per call  (sum, mean, iloc, fillna, apply)
comptime MED_ITERS = 20  # 1–20 ms per call (groupby)
comptime SLOW_ITERS = 3  # >20 ms per call  (sort, merge)
comptime IO_ITERS = 5  # I/O-bound        (csv round-trip)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _elapsed_ms(t0: UInt, iters: Int) -> Float64:
    """Return mean milliseconds per call given a start timestamp and count."""
    return Float64(perf_counter_ns() - t0) / Float64(iters) / 1_000_000.0


def _time_pandas(
    stmt: String,
    globals_dict: PythonObject,
    iters: Int,
) raises -> Float64:
    """Time a pandas expression string using ``timeit.timeit``.

    *globals_dict* should contain every name referenced in *stmt*.
    Returns mean elapsed milliseconds per call.
    """
    var timeit = Python.import_module("timeit")
    var total_seconds = atof(
        String(timeit.timeit(stmt, globals=globals_dict, number=iters))
    )
    return (total_seconds / Float64(iters)) * 1000.0


def my_sqrt(x: Float64) -> Float64:
    """Element-wise sqrt used for the series_apply benchmark."""
    return sqrt(x)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() raises:
    var results = List[BenchResult]()

    # ------------------------------------------------------------------
    # Build fixtures in a single Python call to avoid pandas __setitem__
    # triggering sys._getframe() through Mojo's shallow call stack.
    #
    # Fixture schema:
    #   key  (str, 10 unique values)
    #   a    (float64)
    #   b    (float64)
    #   c    (int64)
    #   id   (int64, unique)
    #
    # Fixture 2 — n//10 rows (used for merge):
    #   id   (int64, unique)
    #   val  (float64)
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
        ")"  # dim table: unique 'id' for a many-to-one join
    )
    var _fixtures = _make_fixtures(100_000)
    var pd_df = _fixtures[0]
    var pd_df2 = _fixtures[1]

    var df = DataFrame.from_pandas(pd_df)
    var df2 = DataFrame.from_pandas(pd_df2)

    var np = Python.import_module("numpy")
    var pd = Python.import_module("pandas")

    # Fixture 3 — 10-row string-key lookup table (for merge_string_key benchmark)
    var pd_df_klookup = pd.DataFrame(
        Python.evaluate(
            "{'key': ['k0','k1','k2','k3','k4','k5','k6','k7','k8','k9'],"
            " 'label': list(range(10))}"
        )
    )
    var df_klookup = DataFrame.from_pandas(pd_df_klookup)

    # Shared globals dict for _time_pandas calls
    var g = Python.evaluate("{}")
    g["pd_df"] = pd_df
    g["pd_df2"] = pd_df2
    g["pd_df_klookup"] = pd_df_klookup
    g["np"] = np

    # ------------------------------------------------------------------
    # series_sum
    # ------------------------------------------------------------------
    var skipped = False
    var bison_ms = 0.0
    try:
        var t0 = perf_counter_ns()
        for _ in range(FAST_ITERS):
            _ = df["a"].sum()
        bison_ms = _elapsed_ms(t0, FAST_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    var pandas_ms = _time_pandas("pd_df['a'].sum()", g, FAST_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("series_sum"))
    else:
        results.append(
            BenchResult("series_sum", bison_ms, pandas_ms, FAST_ITERS)
        )

    # ------------------------------------------------------------------
    # series_mean
    # ------------------------------------------------------------------
    skipped = False
    try:
        var t0 = perf_counter_ns()
        for _ in range(FAST_ITERS):
            _ = df["a"].mean()
        bison_ms = _elapsed_ms(t0, FAST_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas("pd_df['a'].mean()", g, FAST_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("series_mean"))
    else:
        results.append(
            BenchResult("series_mean", bison_ms, pandas_ms, FAST_ITERS)
        )

    # ------------------------------------------------------------------
    # groupby_sum  (single key, as_index=True — native path)
    # ------------------------------------------------------------------
    skipped = False
    try:
        var t0 = perf_counter_ns()
        for _ in range(MED_ITERS):
            var by = List[String]()
            by.append("key")
            _ = df.groupby(by).sum()
        bison_ms = _elapsed_ms(t0, MED_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas("pd_df.groupby('key').sum()", g, MED_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("groupby_sum"))
    else:
        results.append(
            BenchResult("groupby_sum", bison_ms, pandas_ms, MED_ITERS)
        )

    # ------------------------------------------------------------------
    # iloc_row  (single integer-position row access via df.iloc())
    # ------------------------------------------------------------------
    skipped = False
    try:
        var t0 = perf_counter_ns()
        for _ in range(FAST_ITERS):
            _ = df.iloc()[0]
        bison_ms = _elapsed_ms(t0, FAST_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas("pd_df.iloc[0]", g, FAST_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("iloc_row"))
    else:
        results.append(BenchResult("iloc_row", bison_ms, pandas_ms, FAST_ITERS))

    # ------------------------------------------------------------------
    # loc_slice  (slice of 100 rows via df.loc(); matches issue #390)
    # ------------------------------------------------------------------
    skipped = False
    try:
        var t0 = perf_counter_ns()
        for _ in range(FAST_ITERS):
            _ = df.loc()[0:100]
        bison_ms = _elapsed_ms(t0, FAST_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas("pd_df.loc[0:100]", g, FAST_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("loc_slice"))
    else:
        results.append(
            BenchResult("loc_slice", bison_ms, pandas_ms, FAST_ITERS)
        )

    # ------------------------------------------------------------------
    # sort_values
    # ------------------------------------------------------------------
    skipped = False
    try:
        var t0 = perf_counter_ns()
        for _ in range(SLOW_ITERS):
            var by = List[String]()
            by.append("a")
            _ = df.sort_values(by)
        bison_ms = _elapsed_ms(t0, SLOW_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas("pd_df.sort_values('a')", g, SLOW_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("sort_values"))
    else:
        results.append(
            BenchResult("sort_values", bison_ms, pandas_ms, SLOW_ITERS)
        )

    # ------------------------------------------------------------------
    # merge  (many-to-one inner join on integer id; output ≈ n//10 rows)
    # ------------------------------------------------------------------
    skipped = False
    try:
        var t0 = perf_counter_ns()
        for _ in range(SLOW_ITERS):
            var on_keys = List[String]()
            on_keys.append("id")
            _ = df.merge(df2, on=on_keys^)
        bison_ms = _elapsed_ms(t0, SLOW_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas("pd_df.merge(pd_df2, on='id')", g, SLOW_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("merge"))
    else:
        results.append(BenchResult("merge", bison_ms, pandas_ms, SLOW_ITERS))

    # ------------------------------------------------------------------
    # merge_string_key  (many-to-one inner join on string key; 100k rows)
    # ------------------------------------------------------------------
    skipped = False
    try:
        var t0 = perf_counter_ns()
        for _ in range(SLOW_ITERS):
            var on_keys = List[String]()
            on_keys.append("key")
            _ = df.merge(df_klookup, on=on_keys^)
        bison_ms = _elapsed_ms(t0, SLOW_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas(
        "pd_df.merge(pd_df_klookup, on='key')", g, SLOW_ITERS
    )
    if skipped:
        results.append(BenchResult.skipped_result("merge_string_key"))
    else:
        results.append(
            BenchResult("merge_string_key", bison_ms, pandas_ms, SLOW_ITERS)
        )

    # ------------------------------------------------------------------
    # fillna  (numeric column only — the string "key" column is an
    #          object dtype that bison's native fillna does not support)
    # ------------------------------------------------------------------
    skipped = False
    try:
        var s = df["a"]
        var fill_val = DFScalar(Float64(0.0))
        var t0 = perf_counter_ns()
        for _ in range(FAST_ITERS):
            _ = s.fillna(fill_val)
        bison_ms = _elapsed_ms(t0, FAST_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas("pd_df['a'].fillna(0)", g, FAST_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("fillna"))
    else:
        results.append(BenchResult("fillna", bison_ms, pandas_ms, FAST_ITERS))

    # ------------------------------------------------------------------
    # series_apply  (compile-time sqrt; bison uses FloatTransformFn)
    # ------------------------------------------------------------------
    skipped = False
    try:
        var s = df["a"]
        var t0 = perf_counter_ns()
        for _ in range(FAST_ITERS):
            _ = s.apply[my_sqrt]()
        bison_ms = _elapsed_ms(t0, FAST_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    pandas_ms = _time_pandas("pd_df['a'].apply(np.sqrt)", g, FAST_ITERS)
    if skipped:
        results.append(BenchResult.skipped_result("series_apply"))
    else:
        results.append(
            BenchResult("series_apply", bison_ms, pandas_ms, FAST_ITERS)
        )

    # ------------------------------------------------------------------
    # csv_roundtrip  (to_csv + read_csv)
    # ------------------------------------------------------------------
    skipped = False
    try:
        var tmp = "/tmp/_bison_bench_core.csv"
        var t0 = perf_counter_ns()
        for _ in range(IO_ITERS):
            _ = df.to_csv(tmp)
            _ = read_csv(tmp)
        bison_ms = _elapsed_ms(t0, IO_ITERS)
    except e:
        if "not implemented" in String(e):
            skipped = True
        else:
            raise e^
    g["_tmp"] = "/tmp/_pandas_bench_core.csv"
    pandas_ms = _time_pandas(
        "pd_df.to_csv(_tmp, index=False); __import__('pandas').read_csv(_tmp)",
        g,
        IO_ITERS,
    )
    if skipped:
        results.append(BenchResult.skipped_result("csv_roundtrip"))
    else:
        results.append(
            BenchResult("csv_roundtrip", bison_ms, pandas_ms, IO_ITERS)
        )

    print_json(results)
