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

## Import aliases for stdlib names that shadow parameters

When importing a stdlib function whose name collides with a common parameter
name (`sort`, `min`, `max`, `sum`, `len`, `print`), alias the import to a
leading-underscore name and call the alias:

```mojo
from algorithm import sort as _sort_list

_sort_list(my_list)   # NOT sort(my_list) — would shadow the built-in
```
