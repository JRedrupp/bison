"""Phase-level timing for `df.groupby(['key']).sum()`.

Isolates each phase so bottlenecks stand out even when perf/callgrind
are unavailable (sandboxes without perf_event support, valgrind tripping
on AVX-512).  Run after a change to compare against the previous numbers
written in commit messages.
"""

from bison import DataFrame
from bison.arrow import column_to_marrow_array
from marrow.arrays import AnyArray
from marrow.kernels.groupby import groupby as _marrow_groupby
from std.python import Python, PythonObject
from std.time import perf_counter_ns


comptime ITERS = 5


def _ms(t0: UInt, iters: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / Float64(iters) / 1_000_000.0


def _run(df: DataFrame, pd_df: PythonObject, label: String) raises:
    print("=== ", label, " ===")

    # --------------------------------------------------------------
    # pandas reference
    # --------------------------------------------------------------
    var timeit = Python.import_module("timeit")
    var g = Python.evaluate("{}")
    g["pd_df"] = pd_df
    var pandas_ms = (
        Float64(
            atof(
                String(
                    timeit.timeit(
                        "pd_df.groupby('key').sum()", globals=g, number=50
                    )
                )
            )
        )
        * 1000.0
        / 50.0
    )
    print("  pandas groupby('key').sum()         :", pandas_ms, "ms/call")

    # --------------------------------------------------------------
    # End-to-end bison
    # --------------------------------------------------------------
    var t0 = perf_counter_ns()
    for _ in range(ITERS):
        var by = List[String]()
        by.append("key")
        _ = df.groupby(by).sum()
    var total_ms = _ms(t0, ITERS)
    print("  bison  df.groupby(['key']).sum()    :", total_ms, "ms/call")

    # --------------------------------------------------------------
    # Just the ctor
    # --------------------------------------------------------------
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        var by = List[String]()
        by.append("key")
        _ = df.groupby(by)
    var ctor_ms = _ms(t0, ITERS)
    print("  bison  df.groupby(['key']) ctor only:", ctor_ms, "ms/call")

    # --------------------------------------------------------------
    # Key-column conversion
    # --------------------------------------------------------------
    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = column_to_marrow_array(df._cols[0])
    var key_convert_ms = _ms(t0, ITERS)
    print("  column_to_marrow_array('key')        :", key_convert_ms, "ms/call")

    # --------------------------------------------------------------
    # Marrow kernel in isolation
    # --------------------------------------------------------------
    var key_arr = column_to_marrow_array(df._cols[0])
    var val_arrays = List[AnyArray]()
    var aggs = List[String]()
    for ci in range(1, len(df._cols)):
        val_arrays.append(column_to_marrow_array(df._cols[ci]))
        aggs.append(String("sum"))

    t0 = perf_counter_ns()
    for _ in range(ITERS):
        _ = _marrow_groupby(key_arr, val_arrays, aggs)
    var kernel_ms = _ms(t0, ITERS)
    print("  marrow groupby kernel                :", kernel_ms, "ms/call")

    print(
        "  bison/pandas ratio                   :",
        total_ms / pandas_ms,
        "x",
    )
    print("")


def main() raises:
    var _make_fixtures = Python.evaluate(
        "lambda n: ("
        "    lambda np, pd, keys: pd.DataFrame({"
        "        'key': np.random.default_rng(7).choice(keys, n),"
        "        'a':   np.random.default_rng(42).random(n),"
        "        'b':   np.random.default_rng(123).random(n),"
        "        'c':   np.random.default_rng(42).integers(0, 1000, n),"
        "        'id':  np.arange(n, dtype='int64'),"
        "    })"
        ")("
        "    __import__('numpy'),"
        "    __import__('pandas'),"
        "    ['k0','k1','k2','k3','k4','k5','k6','k7','k8','k9'],"
        ")"
    )

    var pd_df_10k = _make_fixtures(10_000)
    var df_10k = DataFrame.from_pandas(pd_df_10k)
    _run(df_10k, pd_df_10k, String("10k rows"))

    var pd_df_100k = _make_fixtures(100_000)
    var df_100k = DataFrame.from_pandas(pd_df_100k)
    _run(df_100k, pd_df_100k, String("100k rows"))
