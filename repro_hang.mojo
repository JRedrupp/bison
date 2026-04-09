# Mojo compiler deadlock reproducer
#
# The Mojo nightly compiler (26.2+) enters an infinite hang when compiling a
# file that calls DataFrame.query() AND the bison package contains a function
# that uses AnyArray typed downcasts (arr.as_int64(), etc.) on a code path
# reachable from the query evaluation chain.
#
# The hang is caused by comptime monomorphisation of rebind-based typed
# downcasts on ArcPointer[NoneType] (inside marrow's AnyArray) interacting
# with the Variant-based dispatch chain in the query/eval expression evaluator.
#
# ---- STEPS TO REPRODUCE ----
#
# 1. Apply the patch (adds a single function + 3-line call site):
#
#    git apply repro_hang.patch
#
# 2. Rebuild bison and compile the test file:
#
#    pixi run mojo package bison/ -o .bison-cache/bison.mojopkg
#    timeout 60 mojo build -I .bison-cache -I . repro_hang.mojo -o /tmp/repro_hang
#    # → Killed after 60s (compiler hangs with no output)
#
# 3. Revert and verify the control compiles in ~30s:
#
#    git checkout -- bison/column.mojo
#    pixi run mojo package bison/ -o .bison-cache/bison.mojopkg
#    timeout 60 mojo build -I .bison-cache -I . repro_hang.mojo -o /tmp/repro_hang
#    # → Compiles successfully
#
# ---- ROOT CAUSE ----
#
# The key trigger is having `arr.as_int64()` (which expands to
# `rebind[ArcPointer[PrimitiveArray[int64]]](self._data)[]`) on a code
# path transitively reachable from `DataFrame.query()` → `eval_expr` →
# `_eval_compare` → `Column._cmp_scalar_op`. The compiler's comptime
# specialisation engine deadlocks when it tries to monomorphise both:
#
#   (a) The Variant[List[Int64], ...] dispatch chain in Column.__len__
#       (reachable from the expression evaluator)
#   (b) The ArcPointer[NoneType] → ArcPointer[PrimitiveArray[T]] rebind
#       chain (reachable from the new _cmp_scalar_op storage path)
#
# The same typed downcast code works fine in arrow.mojo (different module,
# not on the query evaluation's transitive call graph) and works fine as
# dead code in column.mojo (never reached from query evaluation).
#
# ---- WHAT THE PATCH DOES ----
#
# 1. Adds `_storage_to_float64_list(arr: AnyArray)` — a module-level
#    function in column.mojo that calls arr.as_int64(), arr.as_float64(),
#    arr.as_bool() to extract values
#
# 2. Adds a 3-line call to that function inside Column._cmp_scalar_op
#    (the scalar comparison kernel used by df.query("col > value"))
#
# Neither change affects runtime behaviour (the call is guarded by
# `_storage_active` which is False for the test DataFrame). The hang
# is purely a compile-time issue.

from std.python import Python
from bison import DataFrame

def main() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var result = df.query("a > 1")
    print(result.shape()[0])
