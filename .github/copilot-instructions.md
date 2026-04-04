# Copilot instructions for bison

## Project overview

bison is a Mojo DataFrame library with a pandas-compatible API. The goal is that `import bison as bs` is a drop-in replacement for `import pandas as pd`. All methods that are not yet implemented natively raise via `_not_implemented()`.

## Repository layout

```
bison/bison/         — the Mojo package (import bison)
bison/bison/accessors/  — .str and .dt accessor structs
bison/bison/io/         — read_csv, read_parquet, read_json, read_excel stubs
bison/bison/reshape/    — concat stub
bison/tests/         — one test_*.mojo file per feature area
bison/scripts/       — gen_version.py, run_tests.sh
```

## Language and tooling

- **Language**: Mojo (`.mojo` files). Do not write Python except in `scripts/`.
- **Environment manager**: Pixi. Never use Magic (deprecated) or conda directly.
- **Tasks** (run with `pixi run <task>`):
  - `gen-version` — write `bison/_version.mojo` from `pixi.toml`
  - `test` — regenerate version then run all tests
  - `fmt` — `mojo format bison/`
  - `check` — `mojo build bison/`

## Versioning

The version lives in exactly one place: `[project] version` in `pixi.toml`. `bison/_version.mojo` is auto-generated — never edit it by hand.

## Stub pattern

Every unimplemented method must follow this form:

```mojo
fn sort_values(self, by: String, ascending: Bool = True) raises -> Self:
    _not_implemented("DataFrame.sort_values")
    return self   # never reached; required by the type checker
```

- The string passed to `_not_implemented` must be `"TypeName.method_name"`.
- The `return` after the raise is always required to satisfy Mojo's type checker.

## Implementing a stub

1. Remove the `_not_implemented(...)` call and implement the method in native Mojo.
2. Remove the dummy `return`.
3. Update the test in `tests/` — replace the "expect raise" assertion with a real assertion comparing against pandas output via Python interop.

## Internal representation

At the stub stage, `DataFrame` and `Series` hold a `PythonObject` backing a real pandas object:

```mojo
struct DataFrame:
    var _pd_df: PythonObject   # backing pandas DataFrame — stub stage only
    var _columns: List[String]
    var _nrows: Int
    var _ncols: Int
```

As methods are implemented natively, the `PythonObject` backing is replaced with native Mojo column storage.

## Tests

Each `tests/test_*.mojo` has a `main()` that calls every `test_*` function. Working paths assert real values. Stub paths assert the method raises with `"not implemented"` in the message.

Run a single test file: `mojo run tests/test_dataframe.mojo`

## Session notes

At the start of every session create `SESSION.md` at the project root if it does not already exist. While reading files and doing work, append an entry for every tech debt item, bug, or refactoring opportunity noticed. Do not wait until the end — add entries as they are found.

### Entry format

```
### <Short title>

- **File**: `path/to/file.mojo` (line N if relevant)
- **Type**: Tech Debt | Bug | Refactoring | Design Pattern
- **Classification**: <name from refactoring.guru>
- **Details**: What the problem is and what the fix should be.
```

### Classification vocabulary

Use names from the refactoring.guru catalogs:

- **Code smells**: Bloaters, OO-Abusers, Change Preventers, Dispensables, Couplers and the named smells within each group.
- **Refactoring techniques**: E.g. Extract Method, Replace Temp with Query, Introduce Null Object, Replace Conditional with Polymorphism.
- **Design patterns**: Creational, Structural, or Behavioral — use the exact pattern name (e.g. Strategy, Factory Method, Decorator).

`SESSION.md` is for Claude's working notes only — it is gitignored and must never be committed.

### IMPORTANT Session notes in pull requests

When opening a pull request ALWAYS include a `## Session Notes Needing Issues` section at the end of the PR description. List every SESSION.md entry that does **not** already carry an issue annotation (`<!-- #N -->`). Use the full entry text (heading + bullet fields) so the reviewer can create GitHub issues directly from the PR without needing to open the local file.


## Constraints

- `.CLAUDE` and `.claude/` are gitignored — never reference or commit them.
- `README.md` — plain prose, no emojis, no AI-sounding language.
- `bison/_version.mojo` — never edit by hand.
- GitHub issues #1–33 track each stub category. Reference the relevant issue in PR descriptions.
