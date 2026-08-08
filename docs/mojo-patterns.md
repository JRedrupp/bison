# Mojo patterns and gotchas

Mojo-specific patterns and pitfalls encountered during bison development. This
is a living reference for contributors.

## `mut self` for mutating struct methods

Any `def` method that writes to a struct field must declare `mut self`
explicitly. Read-only methods omit the annotation entirely.

```mojo
struct Counter:
    var count: Int

    # Mutating — must have mut self
    def increment(mut self):
        self.count += 1

    # Read-only — no annotation needed
    def value(self) -> Int:
        return self.count
```

Without `mut self`, Mojo silently copies `self` instead of mutating it
in-place, so field updates are lost and callers see no change. This affects
every stateful struct (e.g. any struct that accumulates state across method
calls).

## `ref` for non-copyable Variant arms

Accessing a `Variant` arm whose inner type does not implement
`ImplicitlyCopyable` (e.g. `List[T]`) must use a `ref` borrow, not a `var`
assignment:

```mojo
# WRONG — compile error if List[T] is not ImplicitlyCopyable
var src = col._data[List[Int64]]

# CORRECT — zero-cost borrow tied to the Variant's lifetime
ref src = col._data[List[Int64]]
```

## `rebind[T]` for structurally identical but nominally different types

When two types are bit-for-bit identical but the type checker treats them as
distinct (e.g. a third-party library's `Scalar[dtype.native]` vs the stdlib's
`Int64`), use `rebind[T]` to assert the structural equivalence:

```mojo
data.append(rebind[Int64](src.unsafe_get(i)))  # Scalar[int64.native] -> Int64
```

## `def main() raises:` in test files

Test-file `main()` functions must declare `raises` if they call any raising
function — omitting it is a **compile error**, not a warning.

## Compile-time function types for `apply`, `applymap`, `pipe`

Mojo supports compile-time function types via `comptime`:

```mojo
comptime FloatTransformFn = def(Float64) -> Float64
```

These are used in `Column._apply[F]`, `Series.apply[F]`, `DataFrame.apply[F]`,
`DataFrame.applymap[F]`, and `DataFrame.pipe[F]`. The function must be known at
compile time — either a module-level `def` or an `@parameter` local function.

**Limitation**: `capturing [_]` is not yet supported in parameter type
constraints. `pipe[F]` requires `fn(DataFrame) raises -> DataFrame`
(non-capturing). The `capturing` syntax works in other contexts
(`fn call_it[f: fn() capturing [_] -> None]()`) but not when the captured
function takes a struct argument in a parameter list.

## `fn` is deprecated on nightly — use `def` everywhere

Nightly Mojo deprecated the `fn` keyword (warning today, error soon). All
function and method definitions must use `def`. Do not introduce new `fn`
declarations.

## Compiler bug #642 — generic dispatch + AnyArray typed downcasts

Mojo compiler bug #642 (still present as of `max 26.3.0.dev2026041520`) causes
a comptime monomorphisation deadlock when a generic visitor function and
AnyArray typed downcasts (`.as_int64()`, `.as_float64()`) appear in the same
call graph.

The original `bison.expr` evaluator used a generic
`visit_ast_node_raises[V: ASTNodeVisitorRaises]` visitor which triggered this
bug. The workaround: remove all generic dispatch from the evaluator's hot path
and replace it with a plain non-generic recursive `_eval_node` function that
uses integer `if/elif` on `NK_*` constants. No traits, no template
instantiation, no generic dispatch.

**Side effect**: all `query()`/`eval()` tests must live in
`tests/test_expr.mojo`. Compiling `df.query()` or `df.eval()` in a standalone
test file (e.g. `test_functional.mojo`) hangs compilation — it only resolves
when co-compiled alongside other `bison.expr` tests. Do not add query/eval
calls to any test file other than `test_expr.mojo`.

## Import aliases for stdlib names that shadow parameters

When importing a stdlib function whose name collides with a common parameter
name (`sort`, `min`, `max`, `sum`, `len`, `print`), alias the import to a
leading-underscore name and call the alias:

```mojo
from algorithm import sort as _sort_list

_sort_list(my_list)   # NOT sort(my_list) — would shadow the built-in
```

## `mojo package`/`precompile` rejects any `def main()` file anywhere in the tree

As of the current nightly, `mojo package` (and its replacement `mojo
precompile`) aborts the whole compile with `'main()' is not supported within
packages` if it encounters ANY `.mojo` file containing `def main()`, anywhere
in the directory tree passed as input — even files that are never imported by
the package's public modules. Neither command exposes an exclude flag.

This bit the vendored `vendor/marrow/marrow` submodule: its `tests/`,
`kernels/tests/`, and `expr/tests/` directories all contain `def main()` by
design (marrow's own `TestSuite.run`/`BenchSuite.run` convention — those files
are compiled individually by marrow's own test harness, never packaged as a
whole). Packaging `vendor/marrow/marrow` directly now fails immediately on
those files.

The fix (`scripts/build_marrow.sh`, invoked by the `build-marrow` pixi task):
rsync a filtered mirror of the submodule into `.bison-cache/marrow-lib/marrow/`
with `--exclude 'tests/'` (matches the directory name `tests` anywhere in the
tree, so it catches all three offending directories in one flag), then run
`mojo package` against the mirror instead of the raw submodule path. If a
future Mojo release adds a real exclude mechanism, or marrow's test layout
changes, this workaround can be simplified or removed — see the corresponding
entry in `SESSION.md`.

## Mojo nightly 26.5 migration gotchas (bison/marrow fork bump)

These surfaced while bumping `max`/`mblack` to `26.5.0.dev2026080206` and
fixing the resulting breaks across bison and the `JRedrupp/marrow` fork.
Each was independently verified against the real compiler, not guessed from
changelogs.

- **`deinit` move-constructor parameter must be named `move`.** A custom
  `deinit` overload used for move construction must name its parameter
  `move`, not an arbitrary name like `take` — the compiler now recognizes
  the move-constructor shape by that parameter name specifically.

- **`Copyable` / `Comparable & Copyable` now imply `Movable`.** Composing
  `& Movable` explicitly alongside either of these is now a compile error
  (redundant trait composition), where it previously compiled fine.

- **Raw pointer subscripting requires `unsafe_offset=`.** Positional
  `ptr[i]` no longer resolves for `UnsafePointer.__getitem__`; use
  `ptr[unsafe_offset=i]`.

- **`read` argument-passing convention is now spelled `imm`.** `def f(read
  x: T)` should be written `def f(imm x: T)`.

- **`Variant.__getitem__[T]`/`__getitem_param__[T]` subscript syntax has an
  origin-inference bug when returned through a `ref[...]`-annotated return
  type.** A function shaped like:

  ```mojo
  def __getitem__[T: Copyable](ref self) -> ref[self._v] T:
      return self._v[T]
  ```

  fails with `cannot return reference with incompatible origin`. The
  documented fix (`ref result = self._v[T]; return result`) does **not**
  work either — the error just moves to the `return result` line. This
  reproduces on a bare `Variant` with no struct field projection involved,
  so it isn't specific to this shape of code — it's a real compiler
  limitation on this toolchain. The working alternative is to call
  `Variant.unsafe_get[T]()` directly (what `__getitem_param__` calls
  internally after its own `isa[T]()` check), replicating the same
  check-then-access sequence inline:

  ```mojo
  def __getitem__[T: Copyable](ref self) -> ref[self._v] T:
      if not self._v.isa[T]():
          abort("get: wrong variant type")
      return self._v.unsafe_get[T]()
  ```

- **`std.time.perf_counter_ns()`'s return type changed from `UInt` to
  `Int`** at some point in the 26.5 dev cycle. Code that stored its result
  in an explicitly `UInt`-typed variable or passed it to a function with a
  `UInt` parameter now fails to compile; update the annotation to `Int`.

- **marrow's fused groupby-aggregate kernel's accumulator dtype for
  integer `sum`/`min`/`max` changed from `float64` to `int64`** (upstream
  marrow commit `f95df48`, to avoid silent precision loss above 2^53 for
  large `int64` sums). Any code built against marrow's aggregate result
  columns that assumed a `float64` result for integer input columns will
  now hit a hard process abort (`AnyArray._as[T]()`'s "wrong variant type"
  check) rather than a catchable error, since the column now holds an
  `int64` variant instead. This is a real contract change in a dependency,
  not a bug to route around — read the result back as the dtype the
  operation now actually returns.

## Pinning to a stable/RC build instead of a dev nightly snapshot (26.5.0rc1)

bison was briefly bumped from a `26.5.0.dev<timestamp>` nightly snapshot to
`26.5.0rc1` (a release candidate, still on the `max-nightly` channel but a
fixed, tagged build rather than a rolling dev snapshot) — a few days'
further drift surfaced two more gotchas:

- **`std.gpu.host` stopped re-exporting `DeviceContext`/`DeviceBuffer`/
  `HostBuffer`.** These moved to `max.gpu.host` earlier in the migration
  (see the `elementwise`/`Coord`/`DeviceContext` entry above), but
  `std.gpu.host` kept re-exporting them for a while during the transition.
  That re-export is now gone — import from `max.gpu.host` directly.
- **`UnsafePointer` is now deprecated in favor of `Pointer`** for the
  common "typed handle to a value with a tracked origin" use case (as
  opposed to genuinely unsafe/untracked pointer arithmetic, which still
  needs `UnsafePointer`). `Pointer[T, O]` and `UnsafePointer[T, O]` share
  the same construction (`Pointer(to=x)`) and dereference (`ptr[]`) surface
  for this pattern, so it's a mechanical rename at call sites that only
  ever construct-and-dereference.

Also confirms the `ImplicitlyDeletable`/`Deinitable` trait-name churn noted
earlier in this doc is still ongoing at 26.5.0rc1 (flipped back to
`Deinitable` again) — treat any pin bump, even a small one, as needing a
full `pixi run check` before assuming it's a no-op.
