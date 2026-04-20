"""Phase-level timing breakdown for query_and.

Measures each sub-step of df.query("a > 0.5 and b < 0.3") independently
to identify which phase accounts for the 17.4x pandas gap.

Phases timed:
  (A) parse only        — _parse_expr overhead
  (B) eval only         — mask generation (compare + marrow wrap)
  (C) filter only       — df[prebuilt_mask] (bool_list + indices + take×N)
  (C1) filter, 1 col    — df_1col[mask] isolates fixed overhead in C
  (C5) filter, 5 cols   — baseline
  full query            — end-to-end, should ≈ B+C

Usage:
    mojo run benchmarks/bench_query_phases.mojo
"""

from bison import DataFrame, Series
from bison.expr import parse, eval_expr
from std.python import Python
from std.time import perf_counter_ns

comptime ITERS = 200
comptime PARSE_ITERS = 10_000


def _ms(t0: UInt, iters: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / Float64(iters) / 1_000_000.0


def main() raises:
    # ------------------------------------------------------------------
    # Build fixtures using Python.evaluate (same style as bench_core.mojo)
    # ------------------------------------------------------------------
    var _make5 = Python.evaluate(
        "lambda n: __import__('pandas').DataFrame({    'key':"
        " __import__('numpy').random.default_rng(7).choice(       "
        " ['k0','k1','k2','k3','k4','k5','k6','k7','k8','k9'], n),    'a':  "
        " __import__('numpy').random.default_rng(42).random(n),    'b':  "
        " __import__('numpy').random.default_rng(123).random(n),    'c':  "
        " __import__('numpy').random.default_rng(42).integers(0, 1000, n),   "
        " 'id':  __import__('numpy').arange(n, dtype='int64'),})"
    )
    var _make1 = Python.evaluate(
        "lambda n: __import__('pandas').DataFrame({"
        "    'a': __import__('numpy').random.default_rng(42).random(n),"
        "})"
    )

    var df5 = DataFrame.from_pandas(_make5(100_000))
    var df1 = DataFrame.from_pandas(_make1(100_000))

    print("query_and phase breakdown — 100k rows")
    print("======================================")

    # ------------------------------------------------------------------
    # Full 5-col query (baseline)
    # ------------------------------------------------------------------
    var t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = df5.query("a > 0.5 and b < 0.3")
    var full_ms = _ms(t0, ITERS)
    print("full query (5 cols) :", full_ms, "ms/call")

    # ------------------------------------------------------------------
    # Phase A: parse only
    # ------------------------------------------------------------------
    t0 = perf_counter_ns()
    for _ in range(PARSE_ITERS):
        _ = parse("a > 0.5 and b < 0.3")
    var parse_ms = _ms(t0, PARSE_ITERS)
    print("A parse only        :", parse_ms, "ms/call")

    # ------------------------------------------------------------------
    # Phase B: eval only (mask generation — compare + marrow bool Column)
    # Pre-parse once so parse overhead is excluded.
    # ------------------------------------------------------------------
    var parsed_and = parse("a > 0.5 and b < 0.3")
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = eval_expr(parsed_and, df5)
    var eval_ms = _ms(t0, ITERS)
    print("B eval only         :", eval_ms, "ms/call")

    # Single comparison — to verify AND fusion is 1-pass vs 2-pass
    var parsed_single = parse("a > 0.5")
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = eval_expr(parsed_single, df5)
    var single_ms = _ms(t0, ITERS)
    print("B single cmp        :", single_ms, "ms/call")
    print(
        "  AND/single ratio  :",
        eval_ms / single_ms,
        "x  (1.0=fused 1-pass, 2.0=not fused)",
    )

    # ------------------------------------------------------------------
    # Phase C (5-col): filter only — df5[prebuilt_mask]
    # ------------------------------------------------------------------
    var mask5 = df5.eval("a > 0.5 and b < 0.3")
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = df5[mask5]
    var filter5_ms = _ms(t0, ITERS)
    print("C filter (5 cols)   :", filter5_ms, "ms/call")

    # ------------------------------------------------------------------
    # Phase C1 (1-col): filter only — df1[prebuilt_mask_single]
    # Isolates the fixed cost: bool_list() + indices build.
    # Per-column take cost = (filter5_ms - filter1_ms) / 4
    # ------------------------------------------------------------------
    var mask1 = df1.eval("a > 0.5")
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = df1[mask1]
    var filter1_ms = _ms(t0, ITERS)
    print("C1 filter (1 col)   :", filter1_ms, "ms/call")

    var per_col_ms = (filter5_ms - filter1_ms) / 4.0
    print("  per-col take cost :", per_col_ms, "ms  (=(C5-C1)/4)")
    print(
        "  fixed filter cost :",
        filter1_ms - per_col_ms,
        "ms  (bool_list+indices, est.)",
    )

    print("")
    print("Summary (5-col query)")
    print("  A parse:    ", parse_ms, "ms  (", 100.0 * parse_ms / full_ms, "%)")
    print("  B eval:     ", eval_ms, "ms  (", 100.0 * eval_ms / full_ms, "%)")
    print(
        "  C filter:   ",
        filter5_ms,
        "ms  (",
        100.0 * filter5_ms / full_ms,
        "%)",
    )
    print(
        "    fixed:    ",
        filter1_ms - per_col_ms,
        "ms  (",
        100.0 * (filter1_ms - per_col_ms) / full_ms,
        "%)",
    )
    print(
        "    per-col:  ",
        per_col_ms,
        "ms x5 =",
        per_col_ms * 5.0,
        "ms  (",
        100.0 * per_col_ms * 5.0 / full_ms,
        "%)",
    )
    print("  total full: ", full_ms, "ms")
    print("")
    var npass = df5.query("a > 0.5 and b < 0.3").shape()[0]
    print("Selectivity: ", npass, "of 100,000 rows pass the filter")
