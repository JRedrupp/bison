"""Phase 0 risk-mitigation benchmark for issue #619.

Measures append-throughput for the construction paths that ``Column.from_pandas``
and ``DataFrame.from_records`` will switch from ``List[T]`` to marrow builders
in Phase 3 of the dual-backend storage migration.

For each of int64, float64, bool, and string, the benchmark times three
strategies over a 10K-element scratch buffer:

1. ``List[T].append(value)`` — what bison's construction paths use today.
2. ``Builder.append(value)`` — the safe builder path (each call grows
   capacity if needed).
3. ``Builder.reserve(n)`` once + ``Builder.unsafe_append(value)`` per call —
   the fast builder path that bypasses capacity checks (used by builders
   that already know their final size, e.g. ``from_pandas`` where ``n`` is
   ``len(pd_series)``).

The bench reports a ``builder_ratio`` for each strategy: the ratio of
builder time to ``List[T].append`` time. Anything ≤ 1.0 means the builder
is at least as fast as the legacy list path; anything > 1.5 is a flag to
use the bulk ``unsafe_append`` path in Phase 3 instead of plain ``append``.

The result is emitted in the standard ``BenchResult`` JSON envelope so the
result lands in ``results/<commit>.json`` alongside the rest of the
benchmark suite. The ``bison_ms`` field carries the builder timing and
``pandas_ms`` carries the ``List[T]`` timing — the dashboard's ratio column
becomes the builder vs list ratio.

Run via ``pixi run bench``; this file is picked up automatically by
``scripts/run_benchmarks.sh`` since it matches the ``bench_*.mojo`` glob.

Reference results (linux-x64, opt build, N=10000, REPS=5):
::

    int64    safe    0.74×    (faster than List)
    int64    fast    0.13×    (~8× faster than List)
    float64  safe    4.13×    slower
    float64  fast    1.35×    close to List
    bool     safe    1.94×    slower
    bool     fast    1.14×    close to List
    string   safe    571×     SLOWER  (upstream marrow bug — see note below)
    string   unsafe  0.69×    faster than List

**Phase 3 design implications**:

* Use the **fast path** (``reserve(N)`` once + ``unsafe_append`` per element)
  for int64/float64/bool primitive builders.  The safe path's per-call
  ``reserve(1)`` check makes it 2-4× slower than ``List[T].append``.
* Use the **unsafe path** for ``StringBuilder``: pre-compute total UTF-8
  bytes from the source pandas Series in one Python pass, allocate via
  ``StringBuilder(capacity=N, bytes_capacity=total_bytes)``, then call
  ``unsafe_append(StringSlice(s))`` per element.  Do **not** call
  ``StringBuilder.append`` in the inner loop.

**Upstream marrow bug** (to report on the marrow side, not blocking the
migration): ``StringBuilder.reserve_bytes(additional)`` at
``vendor/marrow/marrow/builders.mojo:505`` adds ``additional`` to
``self._values.size`` (allocated bytes) instead of comparing against capacity,
and unconditionally calls ``BufferBuilder.resize``.  Since
``StringBuilder.append(StringSlice)`` calls ``reserve_bytes(len(s))`` on
every append, the result is one full byte-buffer realloc + memcpy per
element — observably 571× slower than ``List[String].append`` here.
"""

from benchmarks._bench_utils import BenchResult, print_json
from marrow.builders import PrimitiveBuilder, BoolBuilder, StringBuilder
from marrow.dtypes import Int64Type, Float64Type
from std.time import perf_counter_ns

# Marrow's PrimitiveBuilder[T].append takes ``Scalar[T.native]``, which is
# nominally distinct from the stdlib's ``Int64`` / ``Float64`` (also
# ``Scalar[DType.int64]`` etc.) under Mojo's type checker even though the
# underlying representation is identical.  bison/arrow.mojo:126 uses the
# same ``rebind`` workaround on the read side.
comptime _MInt64 = Scalar[Int64Type.native]
comptime _MFloat64 = Scalar[Float64Type.native]


# 10K elements per run, repeated 5x for stable timing.  Each function
# returns a length so the compiler cannot elide the inner loop or the
# allocation; this makes the comparison apples-to-apples.
comptime N = 10_000
comptime REPS = 5


def _ms_per_call(elapsed_ns: UInt, reps: Int) -> Float64:
    """Return mean milliseconds per repetition."""
    return Float64(elapsed_ns) / Float64(reps) / 1_000_000.0


# ---------------------------------------------------------------------------
# Int64
# ---------------------------------------------------------------------------


def _bench_int64_list(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var data = List[Int64]()
        for i in range(N):
            data.append(Int64(i))
        sink += len(data)
    return _ms_per_call(perf_counter_ns() - t0, REPS)


def _bench_int64_builder_safe(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var b = PrimitiveBuilder[Int64Type](capacity=0)
        for i in range(N):
            b.append(rebind[_MInt64](Int64(i)))
        var arr = b.finish()
        sink += arr.length
    return _ms_per_call(perf_counter_ns() - t0, REPS)


def _bench_int64_builder_fast(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var b = PrimitiveBuilder[Int64Type](capacity=N)
        b.reserve(N)
        for i in range(N):
            b.unsafe_append(rebind[_MInt64](Int64(i)))
        var arr = b.finish()
        sink += arr.length
    return _ms_per_call(perf_counter_ns() - t0, REPS)


# ---------------------------------------------------------------------------
# Float64
# ---------------------------------------------------------------------------


def _bench_float64_list(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var data = List[Float64]()
        for i in range(N):
            data.append(Float64(i))
        sink += len(data)
    return _ms_per_call(perf_counter_ns() - t0, REPS)


def _bench_float64_builder_safe(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var b = PrimitiveBuilder[Float64Type](capacity=0)
        for i in range(N):
            b.append(rebind[_MFloat64](Float64(i)))
        var arr = b.finish()
        sink += arr.length
    return _ms_per_call(perf_counter_ns() - t0, REPS)


def _bench_float64_builder_fast(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var b = PrimitiveBuilder[Float64Type](capacity=N)
        b.reserve(N)
        for i in range(N):
            b.unsafe_append(rebind[_MFloat64](Float64(i)))
        var arr = b.finish()
        sink += arr.length
    return _ms_per_call(perf_counter_ns() - t0, REPS)


# ---------------------------------------------------------------------------
# Bool
# ---------------------------------------------------------------------------


def _bench_bool_list(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var data = List[Bool]()
        for i in range(N):
            data.append(i & 1 == 0)
        sink += len(data)
    return _ms_per_call(perf_counter_ns() - t0, REPS)


def _bench_bool_builder_safe(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var b = BoolBuilder(capacity=0)
        for i in range(N):
            b.append(i & 1 == 0)
        var arr = b.finish()
        sink += arr.length
    return _ms_per_call(perf_counter_ns() - t0, REPS)


def _bench_bool_builder_fast(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var b = BoolBuilder(capacity=N)
        b.reserve(N)
        for i in range(N):
            # BoolBuilder is bit-packed and has no unsafe_append;
            # reserve + append is the fast path.
            b.append(i & 1 == 0)
        var arr = b.finish()
        sink += arr.length
    return _ms_per_call(perf_counter_ns() - t0, REPS)


# ---------------------------------------------------------------------------
# String — three variants:
#   _bench_string_list:           List[String].append (the legacy bison path)
#   _bench_string_builder_safe:   StringBuilder.append (broken — see note)
#   _bench_string_builder_unsafe: StringBuilder.unsafe_append after pre-sizing
#                                 the byte buffer once (the work-around for
#                                 the upstream marrow bug below)
#
# **Upstream marrow bug surfaced here**:
# ``StringBuilder.reserve_bytes(additional)`` (vendor/marrow/marrow/builders.mojo:505)
# computes ``needed = self._values.size + additional`` and unconditionally
# calls ``BufferBuilder.resize(needed)``.  ``self._values.size`` is the
# *allocated* byte count, not the *used* byte count, so every per-element
# ``append(s)`` causes a fresh allocation + memcpy of the entire byte buffer.
# Since ``StringBuilder.append(StringSlice)`` calls ``reserve_bytes(len(s))``
# unconditionally (line 436), this manifests as O(N²) per-element append cost.
# The workaround for bison's Phase 3 (Column.from_pandas string branch) is to
# (a) compute total bytes from the source pandas series in one Python pass,
# (b) pre-allocate the byte buffer with ``StringBuilder(bytes_capacity=total)``,
# and (c) use ``unsafe_append`` per element (skipping ``reserve_bytes``).
# ---------------------------------------------------------------------------


def _bench_string_list(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        var data = List[String]()
        for i in range(N):
            data.append(String(i))
        sink += len(data)
    return _ms_per_call(perf_counter_ns() - t0, REPS)


def _bench_string_builder_safe(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        # Constructor pre-sizes both offsets and value buffer.  Average
        # decimal-int representation up to N=10K is ~4 characters; round up to
        # 8 for headroom.
        var b = StringBuilder(capacity=N, bytes_capacity=N * 8)
        for i in range(N):
            b.append(String(i))
        var arr = b.finish()
        sink += arr.length
    return _ms_per_call(perf_counter_ns() - t0, REPS)


def _bench_string_builder_unsafe(mut sink: Int) raises -> Float64:
    var t0 = perf_counter_ns()
    for _ in range(REPS):
        # Pre-size BOTH the element count and the byte buffer to a generous
        # upper bound, then call ``unsafe_append`` (which does NOT call
        # ``reserve_bytes``) inside the loop.  This is the only way to avoid
        # the per-call O(N) memcpy in the safe ``append`` path.
        var b = StringBuilder(capacity=N, bytes_capacity=N * 8)
        for i in range(N):
            var s = String(i)
            b.unsafe_append(StringSlice(s))
        var arr = b.finish()
        sink += arr.length
    return _ms_per_call(perf_counter_ns() - t0, REPS)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() raises:
    var results = List[BenchResult]()
    var sink: Int = 0

    # int64
    var i_list = _bench_int64_list(sink)
    var i_safe = _bench_int64_builder_safe(sink)
    var i_fast = _bench_int64_builder_fast(sink)
    results.append(
        BenchResult("builder_int64_safe_vs_list", i_safe, i_list, REPS)
    )
    results.append(
        BenchResult("builder_int64_fast_vs_list", i_fast, i_list, REPS)
    )

    # float64
    var f_list = _bench_float64_list(sink)
    var f_safe = _bench_float64_builder_safe(sink)
    var f_fast = _bench_float64_builder_fast(sink)
    results.append(
        BenchResult("builder_float64_safe_vs_list", f_safe, f_list, REPS)
    )
    results.append(
        BenchResult("builder_float64_fast_vs_list", f_fast, f_list, REPS)
    )

    # bool
    var b_list = _bench_bool_list(sink)
    var b_safe = _bench_bool_builder_safe(sink)
    var b_fast = _bench_bool_builder_fast(sink)
    results.append(
        BenchResult("builder_bool_safe_vs_list", b_safe, b_list, REPS)
    )
    results.append(
        BenchResult("builder_bool_fast_vs_list", b_fast, b_list, REPS)
    )

    # string
    var s_list = _bench_string_list(sink)
    var s_safe = _bench_string_builder_safe(sink)
    var s_unsafe = _bench_string_builder_unsafe(sink)
    results.append(
        BenchResult("builder_string_safe_vs_list", s_safe, s_list, REPS)
    )
    results.append(
        BenchResult("builder_string_unsafe_vs_list", s_unsafe, s_list, REPS)
    )

    print_json(results)
    # Surface the side-effect sink so the optimizer cannot elide the inner loops.
    print("# sink =", sink)
