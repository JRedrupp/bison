"""Phase-level timing for groupby_sum.

Breaks the ctor apart to prove `_groupby_indices` is the bottleneck.
"""

from bison import DataFrame
from bison.arrow import column_to_marrow_array
from bison._frame import _groupby_indices
from marrow.arrays import AnyArray
from marrow.kernels.groupby import groupby as _marrow_groupby
from std.python import Python
from std.time import perf_counter_ns


comptime ITERS = 1


def _ms(t0: UInt, iters: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / Float64(iters) / 1_000_000.0


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
    var pd_df = _make_fixtures(10_000)
    var df = DataFrame.from_pandas(pd_df)

    print("groupby_sum ctor-internals — 10k rows, 10 groups, ITERS=", ITERS)
    print("")

    # Isolated `_groupby_indices` call.
    var by = List[String]()
    by.append("key")
    var t0 = perf_counter_ns()
    for _ in range(ITERS):
        var gm = Dict[String, List[Int]]()
        var gk = List[String]()
        _groupby_indices(df, by, True, True, gm, gk)
    var gi_ms = _ms(t0, ITERS)
    print("  _groupby_indices alone          :", gi_ms, "ms/call")

    # pandas reference.
    var pd_time_ns = Python.evaluate(
        "lambda d: __import__('time').perf_counter_ns"
    )
    var timeit = Python.import_module("timeit")
    var g = Python.evaluate("{}")
    g["pd_df"] = pd_df
    var pd_groupby_ms = (
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
    print("  pandas groupby('key').sum() ref :", pd_groupby_ms, "ms/call")
