"""Micro-benchmark isolating each suspected loc_slice hotspot.

Validates the findings from static analysis by timing each component
independently and comparing against iloc (which avoids the String path).

Run with:
    pixi run -e default -- mojo run -I .bison-cache -I . benchmarks/bench_loc_hotspots.mojo
"""

from bison import DataFrame
from std.python import Python
from std.time import perf_counter_ns

comptime ITERS = 2000


def _elapsed_us(t0: UInt, iters: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / Float64(iters) / 1_000.0


def main() raises:
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
        "    )"
        ")("
        "    __import__('numpy'),"
        "    __import__('pandas'),"
        "    ['k0','k1','k2','k3','k4','k5','k6','k7','k8','k9'],"
        ")"
    )
    var _fixtures = _make_fixtures(100_000)
    var pd_df = _fixtures[0]
    var df = DataFrame.from_pandas(pd_df)

    print("loc_slice hotspot micro-benchmark  (n=100k rows, 5 cols)")
    print("==========================================================")
    print("")

    # ------------------------------------------------------------------
    # 1. Baseline: loc()[0:100]
    # ------------------------------------------------------------------
    var t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = df.loc()[0:100]
    var loc_us = _elapsed_us(t0, ITERS)
    print("1. df.loc()[0:100]    ", loc_us, "us/call   (full call chain)")

    # ------------------------------------------------------------------
    # 2. iloc()[0:100] — same _df_slice_rows path but no String conversion
    # ------------------------------------------------------------------
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = df.iloc()[0:100]
    var iloc_us = _elapsed_us(t0, ITERS)
    print("2. df.iloc()[0:100]   ", iloc_us, "us/call   (no String conversion)")
    print(
        "   -> String+parse overhead: ~",
        loc_us - iloc_us,
        "us  (",
        (loc_us - iloc_us) / loc_us * 100.0,
        "% of loc cost)",
    )
    print("")

    # ------------------------------------------------------------------
    # 3. iloc() with larger slice: 10x more rows
    # ------------------------------------------------------------------
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = df.iloc()[0:1000]
    var iloc_1k_us = _elapsed_us(t0, ITERS)
    print("3. df.iloc()[0:1000]  ", iloc_1k_us, "us/call   (10x larger slice)")
    print(
        "   -> iloc[1000]/iloc[100] ratio:",
        iloc_1k_us / iloc_us,
        "  (expect ~10x if slice-bound, ~1x if O(n) full-extraction-bound)",
    )
    print("")

    # ------------------------------------------------------------------
    # 4. iloc() over entire DataFrame — measures full extraction cost
    # ------------------------------------------------------------------
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = df.iloc()[0:100000]
    var iloc_full_us = _elapsed_us(t0, ITERS)
    print(
        "4. df.iloc()[0:100000]",
        iloc_full_us,
        "us/call   (full 100k-row slice)",
    )
    print(
        "   -> iloc[100k]/iloc[100] ratio:",
        iloc_full_us / iloc_us,
        "  (expect ~1000x if slice-bound, ~1x if already O(n))",
    )
    print("")

    print("--- Conclusion ---")
    print(
        "String conversion accounts for",
        (loc_us - iloc_us) / loc_us * 100.0,
        "% of loc[] cost.",
    )
    var scale_ratio = iloc_full_us / iloc_us
    if scale_ratio > 500.0:
        print(
            "Column.slice scales O(slice_length): cost is proportional to rows"
            " copied."
        )
    elif scale_ratio < 50.0:
        print(
            "Column.slice scales O(n): full-column extraction dominates even"
            " for small slices."
        )
    else:
        print(
            "Column.slice scaling factor",
            scale_ratio,
            "x for 1000x rows — suggests mixed overhead.",
        )
